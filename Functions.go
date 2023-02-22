// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	 "github.com/aws/aws-sdk-go-v2/aws/retry"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

//filenmae 指的是相对路径下的文件或对象名
//fsrcPath, fdstPath 指的是绝对路径下的文件或对象名
//srcPath, dstPath指的是拷贝的目录或prefix
//key代表prefix+filename

//ftype
// S_IFSOCK   0140000   socket
// S_IFLNK    0120000   symbolic link
// S_IFREG    0100000   regular file
// S_IFBLK    0060000   block device
// S_IFDIR    0040000   directory
// S_IFCHR    0020000   character device
// S_IFIFO    0010000   FIFO
func GetFileMetadata(srcPath string, fsrcPath string) FileInfo {
	filename, err := filepath.Rel(srcPath, fsrcPath)
	if err != nil {
		log.Println("Unable to get relative path:", srcPath, err)
	}

	info, err := os.Lstat(fsrcPath) //读取文件属性
	if err != nil {
		return FileInfo{Filename: filename, CStatus: CopyInfo{CopyStatus: "notFound"}}
	}

	if info.IsDir() {
		filename = filename + "/"

	}

	fUID := int(info.Sys().(*syscall.Stat_t).Uid) //UID
	fGID := int(info.Sys().(*syscall.Stat_t).Gid) //GID
	//下面处理类型与权限，由于lustre在s3中处理类型与权限与linux一致，例如0100644，代表，首位为0，100代表文件，644代表权限
	modeStr := info.Mode().String()                                                             //提取权限，这时为，-rw-r--r--，这种类型，需要转换成linux内部存储类型，即s3 metadata的类型
	typeMap := map[byte]string{'d': "0040", 'L': "0120", '-': "0100"}                           //文件类型的映射
	permMap := map[byte]int64{'r': 4, 'w': 2, 'x': 1, '-': 0}                                   //权限的映射
	fType := typeMap[modeStr[0]]                                                                //转换类型
	owner := strconv.FormatInt(permMap[modeStr[1]]+permMap[modeStr[2]]+permMap[modeStr[3]], 10) //转换所有者权限
	group := strconv.FormatInt(permMap[modeStr[4]]+permMap[modeStr[5]]+permMap[modeStr[6]], 10) //转换属组权限
	other := strconv.FormatInt(permMap[modeStr[7]]+permMap[modeStr[8]]+permMap[modeStr[9]], 10) //转换其他人权限
	fPerm := owner + group + other                                                              //进行字符串拼接，即完成转换为0100644类型

	faTime := info.Sys().(*syscall.Stat_t).Atim.Sec //s3中存储为unix的纳秒，在进行上传时需要加上+ "000000000ns"， strconv.FormatInt(info.Sys().(*syscall.Stat_t).Mtim.Sec, 10) + "000000000ns"
	fmTime := info.Sys().(*syscall.Stat_t).Mtim.Sec

	//这里go1.18.6和1.19.3不一样， 1.18.6为*syscall.Stat_t).Atim.Sec, *syscall.Stat_t).Mtim.Sec, 而在1.19.3中为info.Sys().(*syscall.Stat_t).Atimespec.Sec,info.Sys().(*syscall.Stat_t).Mtimespec.Sec

	fUserAgent := "admt"

	fSize := info.Size() //这里加了文件大小，是为了迁移后做对比

	return FileInfo{IsMetaExist: true, Filename: filename, FUserAgent: fUserAgent, FUID: fUID, FGID: fGID, FType: fType, FPerm: fPerm, FaTime: faTime, FmTime: fmTime, FSize: fSize}

}

func GetFileMetadataWithoutAttr(srcPath string, fsrcPath string) FileInfo {
	filename, err := filepath.Rel(srcPath, fsrcPath)
	if err != nil {
		log.Println("Unable to get relative path:", srcPath, err)
	}

	info, err := os.Lstat(fsrcPath) //读取文件属性
	if err != nil {
		return FileInfo{Filename: filename, CStatus: CopyInfo{CopyStatus: "notFound"}}
	}

	if info.IsDir() {
		filename = filename + "/"
		return FileInfo{IsMetaExist: false, Filename: filename, FType: "0040", FSize: info.Size(), FaTime: info.Sys().(*syscall.Stat_t).Atim.Sec, FmTime: info.Sys().(*syscall.Stat_t).Mtim.Sec}

	} else {
		//这里不支持指向direcotry的symlink
		return FileInfo{IsMetaExist: false, Filename: filename, FType: "0100", FSize: info.Size(), FaTime: info.Sys().(*syscall.Stat_t).Atim.Sec, FmTime: info.Sys().(*syscall.Stat_t).Mtim.Sec}
	}

}

//函数中，如果是目录，返回的加/， FileType中只会有目录和regular两种，因为没有meta所以没有link文件
//如果没有metadata，返回isMetaExist false, 其他属性也会选用默认或output获取值，无论是否有meta，都会有FileInfo,后续直接使用
func GetObjMetadata(client *s3.Client, srcBucket string, srcPrefix string, key string) FileInfo {
	filename, err := filepath.Rel(srcPrefix, key) //在key上去除掉原来的prefix
	if err != nil {
		log.Fatalln("Unable to get relative path:", key, err)
	}

	isDir, _ := regexp.MatchString("/$", key)
	if isDir {
		filename = filename + "/"
	}
	output, err := client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(srcBucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Println(key, ":", err)
		return FileInfo{Filename: filename, CStatus: CopyInfo{CopyStatus: "notFound"}}
	}

	if len(output.Metadata) != 6 {
		var filetype string
		if isDir {
			filetype = "0040"
			filename = filename + "/"

		} else {
			filetype = "0100"
		}
		return FileInfo{IsMetaExist: false, Filename: filename, FUserAgent: "admt", FUID: 0, FGID: 0, FType: filetype, FPerm: "775", FaTime: output.LastModified.Unix(), FmTime: output.LastModified.Unix(), FSize: output.ContentLength}
	}

	fUserAgent := output.Metadata["user-agent"]
	fUIDInt64, _ := strconv.ParseInt(output.Metadata["file-owner"], 10, 64) //如果是空的话，fUID会返回0
	fUID := int(fUIDInt64)
	fGIDInt64, _ := strconv.ParseInt(output.Metadata["file-group"], 10, 64)
	fGID := int(fGIDInt64)
	fType := ""
	fPerm := ""
	if len(output.Metadata) > 0 { //在attr check的时候，有可能prefix里存在之前传的无metadata的数据，这时进行数组索引时会报错。通过判断长度，防止数组溢出
		fType = output.Metadata["file-permissions"][:4]
		fPerm = output.Metadata["file-permissions"][4:]
	}
	faTime, _ := strconv.ParseInt(output.Metadata["file-atime"], 10, 64)
	fmTime, _ := strconv.ParseInt(output.Metadata["file-mtime"], 10, 64)
	fSize := output.ContentLength //这里加了对象大小，是为了迁移后做对比

	return FileInfo{IsMetaExist: true, Filename: filename, FUserAgent: fUserAgent, FUID: fUID, FGID: fGID, FType: fType, FPerm: fPerm, FaTime: faTime, FmTime: fmTime, FSize: fSize}

}

func GetObjMetadataWithoutAttr(client *s3.Client, srcBucket string, srcPrefix string, key string, lastModified int64, size int64) FileInfo {
	filename, err := filepath.Rel(srcPrefix, key) //在key上去除掉原来的prefix
	if err != nil {
		log.Fatalln("Unable to get relative path:", key, err)
	}
	isDir, _ := regexp.MatchString("/$", key)
	if isDir {
		filename = filename + "/"
		return FileInfo{IsMetaExist: false, Filename: filename, FType: "0040", FmTime: lastModified, FSize: size}
	} else {
		return FileInfo{IsMetaExist: false, Filename: filename, FType: "0100", FmTime: lastModified, FSize: size}
	}

}

func pathJoin(rootPath string, subPath string) string {
	regexpDir, _ := regexp.Compile("/$")
	isRootPathDir := regexpDir.MatchString(rootPath)
	isSubPathDir := regexpDir.MatchString(subPath)

	if isSubPathDir {
		return filepath.Join(rootPath, subPath) + "/"
	} else if subPath == "." {
		if isRootPathDir {
			return rootPath
		} else {
			return rootPath + "/"
		}
	} else {
		return filepath.Join(rootPath, subPath)
	}

}

func Chattr(info FileInfo, fpath string, withTime bool, defaultFileMode Filemod) {

	if info.IsMetaExist {
		fUID := info.FUID
		fGID := info.FGID
		fPermUnit64, _ := strconv.ParseUint(info.FPerm, 8, 64) //这里要用8位的unit，不能用10进制
		fPerm := os.FileMode(fPermUnit64)
		err := os.Chown(fpath, int(fUID), int(fGID)) //chown需要在root才能运行
		if err != nil {
			log.Println("os.Chown need root righst, please execute admt in root mode", err)
		}
		err = os.Chmod(fpath, os.FileMode(fPerm))
		if err != nil {
			log.Println(err)
		}
	} else {
		fUID := defaultFileMode.UID
		fGID := defaultFileMode.GID
		fPermUnit64, _ := strconv.ParseUint(defaultFileMode.Mode, 8, 64) //这里要用8位的unit，不能用10进制
		fPerm := os.FileMode(fPermUnit64)
		err := os.Chown(fpath, fUID, fGID) //chown需要在root才能运行
		if err != nil {
			log.Println("os.Chown need root righst, please execute admt in root mode", err)
		}
		err = os.Chmod(fpath, fPerm)
		if err != nil {
			log.Println(err)
		}
	}
	if withTime {
		faTime := time.Unix(info.FaTime, 0)
		fmTime := time.Unix(info.FmTime, 0)
		os.Chtimes(fpath, faTime, fmTime)
	}

}

func CopyFile(fd *os.File, srcPath string) (int64, error) { //Go里面是以目标文件在前面
	srcfile, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer srcfile.Close()

	nBytes, err := io.Copy(fd, srcfile)

	return nBytes, err

}

func UploadS3(uploader *manager.Uploader, file *os.File, Bucket string, Key string, storageClass string, info FileInfo) {
	if info.IsMetaExist {
		_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket:       aws.String(Bucket),
			StorageClass: types.StorageClass(*aws.String(storageClass)),
			Key:          aws.String(Key),
			Body:         file,
			Metadata: map[string]string{
				"user-agent":       info.FUserAgent,
				"file-owner":       strconv.FormatInt(int64(info.FUID), 10),
				"file-group":       strconv.FormatInt(int64(info.FGID), 10),
				"file-permissions": info.FType + info.FPerm,
				"file-atime":       strconv.FormatInt(info.FaTime, 10),
				"file-mtime":       strconv.FormatInt(info.FmTime, 10),
			},
		})
		if err != nil {
			log.Println("Failed to upload:", info.Filename, err)
		}
	} else {

		_, err := uploader.Upload(context.TODO(), &s3.PutObjectInput{
			Bucket:       aws.String(Bucket),
			StorageClass: types.StorageClass(*aws.String(storageClass)),
			Key:          aws.String(Key),
			Body:         file,
		})
		if err != nil {
			log.Println("Failed to upload:", info.Filename, err)
		}
	}

}

func DownloadS3(downloader *manager.Downloader, file *os.File, Bucket string, Key string) {

	_, err := downloader.Download(context.TODO(), file, &s3.GetObjectInput{
		Bucket: aws.String(Bucket),
		Key:    aws.String(Key),
	})
	if err != nil {
		log.Println("Failed to download", Key, err)
	}
}

func ParseArgs(srcPath string, dstPath string) (string, string, string, string, string) {

	srcBucket := ""
	srcPrefix := ""
	dstBucket := ""
	dstPrefix := ""

	isSrcPathS3, _ := regexp.Match("^(S|s)3://", []byte(srcPath))
	isDstPathS3, _ := regexp.Match("^(S|s)3://", []byte(dstPath))

	if isSrcPathS3 && !isDstPathS3 {
		mode = "o2f"
		srcPath = srcPath[5:]

		srcPath = strings.Trim(srcPath, "/")
		index := strings.Index(srcPath, "/")
		if index == -1 {
			srcBucket = srcPath
			srcPrefix = ""
		} else {
			srcBucket = srcPath[:index]
			srcPrefix = srcPath[index+1:] + "/" //这里要把prefix的开头的/去掉，同时要在末尾加上/，不然listObjectV2会把以这个前缀开头的所有prefix列出来，比如如果是dir1,会更出bucket里面存在的可能是dir1, dir11, dir123等，加上/后就只会列出dir1/
		}

		return mode, srcBucket, srcPrefix, dstBucket, dstPrefix
	} else if isSrcPathS3 && isDstPathS3 {
		mode = "o2o"
		srcPath = srcPath[5:]
		dstPath = dstPath[5:]

		srcPath = strings.Trim(srcPath, "/")
		index := strings.Index(srcPath, "/")
		if index == -1 {
			srcBucket = srcPath
			srcPrefix = ""
		} else {
			srcBucket = srcPath[:index]
			srcPrefix = srcPath[index+1:] + "/" //这里要把prefix的/去掉
		}

		dstPath = strings.Trim(dstPath, "/") //删除前后的/，如//lustre01/这种情况
		index = strings.Index(dstPath, "/")
		if index == -1 { //-1代表没有找到/，是代表只输入了bucket名字
			dstBucket = dstPath
			dstPrefix = ""
		} else {
			dstBucket = dstPath[:index]
			dstPrefix = dstPath[index+1:] + "/" //这里要把prefix的/去掉
		}
		return mode, srcBucket, srcPrefix, dstBucket, dstPrefix

	} else if !isSrcPathS3 && isDstPathS3 {
		mode = "f2o"
		dstPath = dstPath[5:]

		dstPath = strings.Trim(dstPath, "/") //删除前后的/，如//lustre01/这种情况
		index := strings.Index(dstPath, "/")
		if index == -1 { //-1代表没有找到/，是代表只输入了bucket名字
			dstBucket = dstPath
			dstPrefix = ""
		} else {
			dstBucket = dstPath[:index]
			dstPrefix = dstPath[index+1:] + "/" //这里要把prefix的/去掉
		}
		return mode, srcBucket, srcPrefix, dstBucket, dstPrefix

	} else if !isSrcPathS3 && !isDstPathS3 {
		mode = "f2f"
		return mode, srcBucket, srcPrefix, dstBucket, dstPrefix

	}
	return mode, srcBucket, srcPrefix, dstBucket, dstPrefix
}

func CreateTempDir(dataDir string, jobDir string) {
	err := os.MkdirAll(dataDir, 0777)
	if err != nil {
		log.Fatalln("tmpdir create failed:", err)
	}

	err = os.MkdirAll(jobDir, 0777)
	if err != nil {
		log.Fatalln("tmpdir create failed:", err)
	}
}

func CreateTempFile(dataDir string, filename string) *os.File {
	err := os.MkdirAll(dataDir, 0777)
	if err != nil {
		log.Fatalln("tmpdir create failed:", err)
	}
	randSuffix := rand.Int63()
	tmpfilename := pathJoin(dataDir, filename+strconv.FormatInt(randSuffix, 10))
	tmpfile, err := os.Create(tmpfilename) //创建临时文件
	if err != nil {
		log.Println("Create temple link file failed:", err)
	}
	return tmpfile
}

func centerPrint(w int, str string, padding string) {
	fmt.Println("")
	defer fmt.Println("")
	var lpad int
	var rpad int
	lpad = (w - len(str)) / 2
	if (w-len(str))%2 == 0 {
		rpad = (w-len(str))/2 - 1
	} else {
		rpad = (w - len(str)) / 2
	}

	strPrint := strings.Repeat("*", lpad) + str + strings.Repeat("*", rpad)
	fmt.Printf(fmt.Sprintf("%%-%ds", w/2), fmt.Sprintf(fmt.Sprintf("%%%ds\n", w/2), strPrint))

}

func getResult(fileMap *map[string]FileInfo, successFlag string, failFlag string) (int, int) {

	success := 0
	fail := 0
	for filename, info := range *fileMap {
		if info.CStatus.CopyStatus == successFlag {
			success++
		}
		if info.CStatus.CopyStatus == failFlag {
			fmt.Println(filename)
			fail++
		}
	}
	return success, fail
}

func MD5Obj(client *s3.Client, Bucket string, Key string, partSize int64) []byte {

	downloader := manager.NewDownloader(client, func(u *manager.Downloader) {
		u.PartSize = partSize * 1024 * 1024
	})

	fd := CreateTempFile(dataDir, filepath.Base(Key))
	defer fd.Close()
	DownloadS3(downloader, fd, Bucket, Key)

	m := md5.New()
	//	var content = make([]byte,100), fd.Read(content),这里不能用fd.read()函数，是因为fd.read会读指定100个字节的数据，即使对象中没有100，content对象仍然是100， 进行hash计算时是以100为基础计算的
	content, _ := os.ReadFile(fd.Name())
	m.Write(content)
	hash := m.Sum(nil)
	return hash

}

func MD5File(fsrcPath string) []byte {
	m := md5.New()

	content, _ := os.ReadFile(fsrcPath)
	m.Write(content)
	hash := m.Sum(nil)
	return hash

}

func collectCheckInfo(jobFile string, fileMap *map[string]FileInfo) { //在利用json.Marshal进行序列号时，结构体里的变量必须首字母大写

	fd, err := os.Create(jobFile)
	if err != nil {
		log.Println(err)
	}
	defer fd.Close()

	b, err := json.Marshal(fileMap)
	if err != nil {
		log.Println(err)
	}
	_, err = fd.Write(b)
	if err != nil {
		log.Println(err)
	}

}
func collectIncrCheckInfo(jobFile string, fileMap *map[string]FileInfo, resultMap *map[string]FileInfo) { //在利用json.Marshal进行序列号时，结构体里的变量必须首字母大写

	fd, err := os.Create(jobFile)
	if err != nil {
		log.Println(err)
	}
	defer fd.Close()
	for name, value := range *resultMap {
		(*fileMap)[name] = value
	}
	b, err := json.Marshal(fileMap)
	if err != nil {
		log.Println(err)
	}
	_, err = fd.Write(b)
	if err != nil {
		log.Println(err)
	}

}

func readLastTimeCopyInfo(checkfile string) map[string]FileInfo {

	var fileMap = make(map[string]FileInfo)

	content, err := os.ReadFile(checkfile)
	if err != nil {
		return fileMap
	}

	err = json.Unmarshal(content, &fileMap)
	if err != nil {
		log.Println(err)

	}
	return fileMap

}

func CreateS3Client(region string) *s3.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region), config.WithRetryer(func() aws.Retryer {
		return retry.AddWithMaxAttempts(retry.NewStandard(), 10)}) )
	if err != nil {
		log.Fatalln("error:", err)
	}
	return s3.NewFromConfig(cfg)
}

func CheckAttr(SrcCheckMap *map[string]FileInfo, DstCheckMap *map[string]FileInfo, ResultMap *map[string]FileInfo) {

	for name, info := range *SrcCheckMap {

		//在dstPath中没有对应的文件或对象
		if (*DstCheckMap)[name].Filename == "" {
			fmt.Printf("%-23s%s\n", "Attributes check fail: ", info.Filename)

			(*ResultMap)[name] = FileInfo{IsMetaExist: info.IsMetaExist, Filename: info.Filename, FUserAgent: info.FUserAgent, FUID: info.FUID, FGID: info.FGID, FType: info.FType, FPerm: info.FPerm, FaTime: info.FaTime, FmTime: info.FmTime, FSize: info.FSize, CStatus: CopyInfo{CopyStatus: "checkFail", Copytime: time.Now().Unix()}}

			continue
		}

		//找到地应的目标文件或对象
		//如果是目录或symlink，则直接返回checkPass
		if (*SrcCheckMap)[name].FType == "0040" || (*SrcCheckMap)[name].FType == "0120" {
			fmt.Printf("%-23s%s\n", "Attributes check pass: ", info.Filename)
			(*ResultMap)[name] = FileInfo{IsMetaExist: info.IsMetaExist, Filename: info.Filename, FUserAgent: info.FUserAgent, FUID: info.FUID, FGID: info.FGID, FType: info.FType, FPerm: info.FPerm, FaTime: info.FaTime, FmTime: info.FmTime, FSize: info.FSize, CStatus: CopyInfo{CopyStatus: "checkPass", Copytime: time.Now().Unix()}}
			continue
		}

		//如果为文件，则比较大小，和目标对文件或对象的更新时间大于源文件或对象，为什么会出现大于源文件情况，是因为s3上传中生成的文件更新
		if (*SrcCheckMap)[name].FType == "0100" {

			if (*DstCheckMap)[name].FSize == (*SrcCheckMap)[name].FSize && (*DstCheckMap)[name].FmTime >= (*SrcCheckMap)[name].FmTime {
				fmt.Printf("%-23s%s\n", "Attributes check pass: ", info.Filename)

				(*ResultMap)[name] = FileInfo{IsMetaExist: info.IsMetaExist, Filename: info.Filename, FUserAgent: info.FUserAgent, FUID: info.FUID, FGID: info.FGID, FType: info.FType, FPerm: info.FPerm, FaTime: info.FaTime, FmTime: info.FmTime, FSize: info.FSize, CStatus: CopyInfo{CopyStatus: "checkPass", Copytime: time.Now().Unix()}}

			} else {
				fmt.Printf("%-23s%s\n", "Attributes check fail: ", info.Filename)

				(*ResultMap)[name] = FileInfo{IsMetaExist: info.IsMetaExist, Filename: info.Filename, FUserAgent: info.FUserAgent, FUID: info.FUID, FGID: info.FGID, FType: info.FType, FPerm: info.FPerm, FaTime: info.FaTime, FmTime: info.FmTime, FSize: info.FSize, CStatus: CopyInfo{CopyStatus: "checkFail", Copytime: time.Now().Unix()}}
			}

		}

	}

}
