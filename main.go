// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main //go 1.18.6

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

var (
	srcPath   string
	dstPath   string
	srcBucket string
	srcPrefix string
	dstBucket string
	dstPrefix string
	mode      string
)
var ( //参数
	factor          int
	isInitialCopy   bool
	partSize        int64
	storageClass    string
	check           string
	checkMode       string
	withAttr        bool
	defaultFileMode Filemod
	region          string
	dataDir         string
	jobDir          string
)

func init() {

	centerPrint(150, "Written by 王大伟, Welcome any feedback to login:awsdawei@, WeChat: 374727961", "*")
	flag.IntVar(&factor, "f", 10, "Factor of goroutines setting, you will get goroutines with number of factor*numOfCPUs")
	flag.StringVar(&storageClass, "sc", "STANDARD", "Specify one of S3 Storage Classes: 'STANDARD', 'REDUCED_REDUNDANCY', 'STANDARD_IA', 'ONEZONE_IA','INTELLIGENT_TIERING','GLACIER','DEEP_ARCHIVE','GLACIER_IR'")
	flag.StringVar(&region, "region", "", "Specify your region")
	flag.Int64Var(&partSize, "p", 100, "Part size, you will decide how much part when s3 leverages multipart feature to upload or download")
	var isInitialCopyStr string
	flag.StringVar(&isInitialCopyStr, "i", "false", "Do you want initial sync?, Please input 'true' or 'false'") //Go里布尔类型必须要使用--i=true这种方式，所以这里用Int做转换
	var withAttrStr string
	flag.StringVar(&withAttrStr, "a", "false", "'true': copy with file attributes, 'false': copy without file attributes")
	flag.StringVar(&check, "c", "nocheck", "Check mode after copy completion, you can set 'nocheck','attr', 'md5'")
	flag.StringVar(&checkMode, "t", "incr", "'incr': only check the copied files, 'full': check whole dataset")
	flag.IntVar(&(defaultFileMode.UID), "u", os.Getuid(), "You can specify default UID other than current user")
	flag.IntVar(&(defaultFileMode.GID), "g", os.Getgid(), "You can specify default GID other than current group")
	flag.StringVar(&(defaultFileMode.Mode), "m", "775", "You can specify default file mod other than 775")

	flag.Parse() //Parse函数要在参数定义之后解析

	if isInitialCopyStr == "true" {
		isInitialCopy = true
	} else if isInitialCopyStr == "false" {
		isInitialCopy = false
	} else {
		log.Fatalln("For option '-i', only 'true' or 'false' are allowed")
	}

	if withAttrStr == "true" {
		withAttr = true
	} else if withAttrStr == "false" {
		withAttr = false
	} else {
		log.Fatalln("For option '-a', only 'true' or 'false' are allowed")
	}

	if !(check == "nocheck" || check == "attr" || check == "md5") {
		log.Fatalln("For option '-c', only 'nocheck', 'attr', 'md5' are allowed")
	}

	if !(checkMode == "full" || checkMode == "incr") {
		log.Fatalln("For option '-t', only 'incr', 'full' are allowed")
	}

	if flag.NArg() != 2 {
		log.Fatalln("Usage:", os.Args[0], "[options] <Source Path> <Destination Path>")
	}

	srcPath = flag.Arg(0)
	dstPath = flag.Arg(1)

	endingWithDash, _ := regexp.MatchString("/$", srcPath)
	if endingWithDash == false {
		srcPath = srcPath + "/"
	}

	endingWithDash, _ = regexp.MatchString("/$", dstPath)
	if endingWithDash == false {
		dstPath = dstPath + "/"
	}

	mode, srcBucket, srcPrefix, dstBucket, dstPrefix = ParseArgs(srcPath, dstPath)

	dataDir = "/tmp/dataDir/"
	jobDir = "/tmp/jobDir/"
	CreateTempDir(dataDir, jobDir)
}

func main() {
	defer os.RemoveAll(dataDir)

	start := time.Now()
	defer func() {
		centerPrint(100, "Job Completion Summary", "*")
		layout := "2006-01-02 15:04:05"
		fmt.Println("Start time     :", start.Format(layout))
		fmt.Println("Completion time:", time.Now().Format(layout))
		fmt.Printf("Total copy time: %.2f \n", time.Since(start).Seconds())
	}()

	//构造timefile的文件名
	strlist := strings.Split(srcPath, "/")
	srcjob := strings.Join(strlist, "")
	strlist = strings.Split(dstPath, "/")
	dstjob := strings.Join(strlist, "")
	jobFile := "_" + srcjob + "_" + dstjob
	jobFile = filepath.Join(jobDir, jobFile)
	if isInitialCopy {
		os.RemoveAll(jobFile)
	}

	walker := FileWalk{
		make(chan FileInfo, 100000), //注意这里设置缓冲区，不然会死锁
		isInitialCopy,
		readLastTimeCopyInfo(jobFile),
		map[string]FileInfo{},
		map[string]FileInfo{},
		map[string]FileInfo{},
		srcPath,
		dstPath,
		defaultFileMode,
		withAttr,
	}
	if mode == "f2o" || mode == "f2f" {
		go func() {
			// Gather the files to upload by walking the path recursively
			if err := filepath.Walk(srcPath, walker.Walk); err != nil {
				log.Fatalln("Walk failed:", err)
			}
			close(walker.FileList)
		}()
	}
	if mode == "o2f" || mode == "o2o" {

		go func() {
			client := CreateS3Client(region)
			walker.Listobj(client, srcBucket, srcPrefix)
			close(walker.FileList)

		}()
	}

	centerPrint(100, "File Copy is Starting", "*")
	fileCopyStart := time.Now()

	var wg sync.WaitGroup

	procs := factor * runtime.NumCPU()
	runtime.GOMAXPROCS(procs)

	wg.Add(procs)
	if mode == "f2o" {

		for i := 0; i < procs; i++ {

			go func() {
				defer wg.Done()
				client := CreateS3Client(region)

				for info := range walker.FileList {
					if info.FType == "0040" {
						F2O_DirCopy(client, info, srcPath, dstBucket, dstPrefix)
					}
					if info.FType == "0120" {
						F2O_SymCopy(client, info, srcPath, dstBucket, dstPrefix, storageClass)
					}
					if info.FType == "0100" {
						F2O_RegCopy(client, info, srcPath, dstBucket, dstPrefix, storageClass, partSize)
					}
				}
			}()
		}
	}
	if mode == "o2f" {

		for i := 0; i < procs; i++ {

			go func() {
				defer wg.Done()
				client := CreateS3Client(region)

				for info := range walker.FileList {
					if info.FType == "0040" {
						O2F_DirCopy(client, info, srcBucket, srcPrefix, dstPath, defaultFileMode)
					}
					if info.FType == "0120" {
						O2F_SymCopy(client, info, srcBucket, srcPrefix, dstPath)
					}
					if info.FType == "0100" {
						O2F_RegCopy(client, info, srcBucket, srcPrefix, dstPath, partSize, defaultFileMode)
					}

				}
			}()
		}
	}
	if mode == "f2f" {

		for i := 0; i < procs; i++ {
			go func() {
				defer wg.Done()
				for info := range walker.FileList {
					if info.FType == "0040" {
						F2F_DirCopy(info, srcPath, dstPath, defaultFileMode)
					}
					if info.FType == "0120" {
						F2F_SymCopy(info, srcPath, dstPath)
					}
					if info.FType == "0100" {
						F2F_RegCopy(info, srcPath, dstPath, partSize, defaultFileMode)
					}
				}
			}()
		}
	}
	if mode == "o2o" {
		//一个client一个TCP连接，所以要把client放到goroutine里面，这样可以建立多个tcp连接

		for i := 0; i < procs; i++ {

			go func() {
				defer wg.Done()
				client := CreateS3Client(region)

				for info := range walker.FileList {
					if info.FType == "0040" {
						O2O_ObjectCopy(client, info, srcBucket, srcPrefix, dstBucket, storageClass, dstPrefix)
					}
					if info.FType == "0120" {
						O2O_ObjectCopy(client, info, srcBucket, srcPrefix, dstBucket, storageClass, dstPrefix)
					}
					if info.FType == "0100" {
						O2O_ObjectCopy(client, info, srcBucket, srcPrefix, dstBucket, dstPrefix, storageClass)
					}
				}
			}()
		}
	}

	wg.Wait()

	centerPrint(100, "File Copy Completion", "*")
	func() {
		layout := "2006-01-02 15:04:05"
		fmt.Println("File copy start time     :", fileCopyStart.Format(layout))
		fmt.Println("File copy completion time:", time.Now().Format(layout))
		fmt.Printf("Total copy time : %.2f \n", time.Since(fileCopyStart).Seconds())
	}()

	//////////////////////////////////////////////////////////////////////////////////////////////
	//迁移后检查

	if checkMode == "full" && (check == "attr" || check == "md5") {

		// atrributes check检查计时
		centerPrint(100, "Starting Check between Source and Destination", "*")
		checkStart := time.Now()

		checker := FileWalk{
			make(chan FileInfo, 100000), //注意这里设置缓冲区，不然会死锁
			isInitialCopy,
			walker.FileMap,
			map[string]FileInfo{},
			map[string]FileInfo{},
			map[string]FileInfo{},
			srcPath,
			dstPath,
			defaultFileMode,
			withAttr,
		}

		if mode == "f2o" {
			client := CreateS3Client(region)
			checker.F2O_GetCheck(client, srcPath, dstBucket, dstPrefix)

		}
		if mode == "f2f" {
			checker.F2F_GetCheck(srcPath, dstPath)

		}
		if mode == "o2f" {
			client := CreateS3Client(region)
			checker.O2F_GetCheck(client, srcBucket, srcPrefix, dstPath)

		}
		if mode == "o2o" {
			client := CreateS3Client(region)
			checker.O2O_GetCheck(client, srcBucket, srcPrefix, dstBucket, dstPrefix)

		}

		if check == "md5" {

			var wg1 sync.WaitGroup
			wg1.Add(procs)

			var AttrResultList = make(chan FileInfo, 10000000)
			var ResultList = make(chan FileInfo, 10000000)
			var StopSingal = make(chan int, 1)
			StopSingal <- 0

			go func() {
				for _, info := range checker.ResultMap {
					AttrResultList <- info
				}
				close(AttrResultList)
			}()

			for i := 0; i < procs; i++ {
				go func() {
					defer wg1.Done()
					defer func() {
						count := <-StopSingal
						if count == procs-1 {
							close(ResultList)
						} else {
							count++
							StopSingal <- count
						}
					}()
					client := CreateS3Client(region)

					for info := range AttrResultList {

						//如果为目录或软链接，不进行md5比较，直接认为通过
						if info.FType == "0040" || info.FType == "0120" {
							info.CStatus.CopyStatus = "checkPass"
						} else {
							var srcMD5 []byte
							var dstMD5 []byte
							if mode == "f2o" {
								srcMD5 = MD5File(srcPath + info.Filename)
								dstMD5 = MD5Obj(client, dstBucket, pathJoin(dstPrefix, info.Filename), partSize)
							}
							if mode == "o2f" {
								srcMD5 = MD5Obj(client, srcBucket, pathJoin(srcPrefix, info.Filename), partSize)
								dstMD5 = MD5File(dstPath + info.Filename)
							}
							if mode == "f2f" {
								srcMD5 = MD5File(srcPath + info.Filename)
								dstMD5 = MD5File(dstPath + info.Filename)
							}
							if mode == "o2o" {
								srcMD5 = MD5Obj(client, srcBucket, pathJoin(srcPrefix, info.Filename), partSize)
								dstMD5 = MD5Obj(client, dstBucket, pathJoin(dstPrefix, info.Filename), partSize)
							}

							if bytes.Compare(srcMD5, dstMD5) == 0 {
								fmt.Printf("%-23s%s\n", "MD5 check pass: ", info.Filename)
								info.CStatus.CopyStatus = "checkPass"

							} else {
								fmt.Printf("%-23s%s\n", "MD5 check fail: ", info.Filename)
								info.CStatus.CopyStatus = "checkFail"
							}
							ResultList <- info

						}
					}

				}()
			}
			//这里必须要make一个新的临时map，如果直接写checker.ResultMap,会出现concurrent map read and write的问题

			md5ResultMap := make(map[string]FileInfo)
			for info := range ResultList {
				md5ResultMap[info.Filename] = info
			}
			wg1.Wait()
			for filename, info := range md5ResultMap {
				checker.ResultMap[filename] = info
			}

		}

		// atrributes check检查计时
		centerPrint(100, "Full Check between Source and Destination Completion", "*")
		centerPrint(50, "Files which fail to pass check", "+")
		success, fail := getResult(&checker.ResultMap, "checkPass", "checkFail")
		centerPrint(50, "", "+")

		fmt.Printf("File check success: %d, File check fail: %d \n", success, fail)
		func() {
			layout := "2006-01-02 15:04:05"
			fmt.Println("File check start time     :", checkStart.Format(layout))
			fmt.Println("File check completion time:", time.Now().Format(layout))
			fmt.Printf("Total check time : %.2f \n", time.Since(checkStart).Seconds())
		}()
		collectCheckInfo(jobFile, &checker.ResultMap)

	}

	//Incr模式
	if checkMode == "incr" && (check == "attr" || check == "md5") {

		// atrributes check检查计时
		centerPrint(100, "Starting Check between Source and Destination", "*")
		checkStart := time.Now()

		checker := FileWalk{
			make(chan FileInfo, 100000), //注意这里设置缓冲区，不然会死锁
			isInitialCopy,
			walker.FileMap,
			map[string]FileInfo{},
			map[string]FileInfo{},
			map[string]FileInfo{},
			srcPath,
			dstPath,
			defaultFileMode,
			withAttr,
		}

		if mode == "f2o" {
			client := CreateS3Client(region)
			checker.F2O_GetIncrCheck(client, srcPath, dstBucket, dstPrefix)

		}
		if mode == "f2f" {
			checker.F2F_GetIncrCheck(srcPath, dstPath)

		}
		if mode == "o2f" {
			client := CreateS3Client(region)
			checker.O2F_GetIncrCheck(client, srcBucket, srcPrefix, dstPath)

		}
		if mode == "o2o" {
			client := CreateS3Client(region)
			checker.O2O_GetIncrCheck(client, srcBucket, srcPrefix, dstBucket, dstPrefix)

		}

		if check == "md5" {

			var wg1 sync.WaitGroup
			wg1.Add(procs)

			var AttrResultList = make(chan FileInfo, 10000000)
			var ResultList = make(chan FileInfo, 10000000)
			var StopSingal = make(chan int, 1)
			StopSingal <- 0

			go func() {
				for _, info := range checker.ResultMap {
					AttrResultList <- info
				}
				close(AttrResultList)
			}()

			for i := 0; i < procs; i++ {
				go func() {
					defer wg1.Done()
					defer func() {
						count := <-StopSingal
						if count == procs-1 {
							close(ResultList)
						} else {
							count++
							StopSingal <- count
						}
					}()
					client := CreateS3Client(region)

					for info := range AttrResultList {

						//如果为目录或软链接，不进行md5比较，直接认为通过
						if info.FType == "0040" || info.FType == "0120" {
							info.CStatus.CopyStatus = "checkPass"
						} else {
							var srcMD5 []byte
							var dstMD5 []byte
							if mode == "f2o" {
								srcMD5 = MD5File(srcPath + info.Filename)
								dstMD5 = MD5Obj(client, dstBucket, pathJoin(dstPrefix, info.Filename), partSize)
							}
							if mode == "o2f" {
								srcMD5 = MD5Obj(client, srcBucket, pathJoin(srcPrefix, info.Filename), partSize)
								dstMD5 = MD5File(dstPath + info.Filename)
							}
							if mode == "f2f" {
								srcMD5 = MD5File(srcPath + info.Filename)
								dstMD5 = MD5File(dstPath + info.Filename)
							}
							if mode == "o2o" {
								srcMD5 = MD5Obj(client, srcBucket, pathJoin(srcPrefix, info.Filename), partSize)
								dstMD5 = MD5Obj(client, dstBucket, pathJoin(dstPrefix, info.Filename), partSize)
							}

							if bytes.Compare(srcMD5, dstMD5) == 0 {
								fmt.Printf("%-23s%s\n", "MD5 check pass: ", info.Filename)
								info.CStatus.CopyStatus = "checkPass"

							} else {
								fmt.Printf("%-23s%s\n", "MD5 check fail: ", info.Filename)
								info.CStatus.CopyStatus = "checkFail"
							}
							ResultList <- info

						}
					}

				}()
			}
			//这里必须要make一个新的临时map，如果直接写checker.ResultMap,会出现concurrent map read and write的问题

			md5ResultMap := make(map[string]FileInfo)
			for info := range ResultList {
				md5ResultMap[info.Filename] = info
			}
			wg1.Wait()
			for filename, info := range md5ResultMap {
				checker.ResultMap[filename] = info
			}
		}

		// atrributes check检查计时
		centerPrint(100, "Incremental Check between Source and Destination Completion", "*")
		centerPrint(50, "Files which fail to pass check", "+")
		success, fail := getResult(&checker.ResultMap, "checkPass", "checkFail")
		centerPrint(50, "", "+")

		fmt.Printf("File check success: %d, File check fail: %d \n", success, fail)
		func() {
			layout := "2006-01-02 15:04:05"
			fmt.Println("File check start time     :", checkStart.Format(layout))
			fmt.Println("File check completion time:", time.Now().Format(layout))
			fmt.Printf("Total check time : %.2f \n", time.Since(checkStart).Seconds())
		}()
		collectIncrCheckInfo(jobFile, &walker.FileMap, &checker.ResultMap) //这里是full模式用的函数不一样

	}

}
