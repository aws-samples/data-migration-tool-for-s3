// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

//filenmae 指的是相对路径下的文件或对象名
//fsrcPath, fdstPath 指的是绝对路径下的文件或对象名
//srcPath, dstPath指的是拷贝的目录或prefix

func F2O_DirCopy(client *s3.Client, info FileInfo, srcPath string, dstBucket string, dstPrefix string) {
	//这里会先执行，closeCh()再执行wg.Done()，这里使用的栈结构，wg.Done()会先压入栈

	filename := info.Filename
	fdstPath := pathJoin(dstPrefix, filename)
	_, err := client.PutObject(context.TODO(), &s3.PutObjectInput{ //uploadmanager不能上传空目录，所以这里使用client来上传
		Bucket: &dstBucket,
		Key:    aws.String(fdstPath), //这里没有body
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
		log.Fatalln("Failed to upload", fdstPath, err)
	}

	fmt.Println("Copy:", filename)
}

func F2O_RegCopy(client *s3.Client, info FileInfo, srcPath string, dstBucket string, dstPrefix string, storageClass string, partSize int64) {

	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = partSize * 1024 * 1024
	})

	filename := info.Filename
	fsrcPath := pathJoin(srcPath, filename)

	fd, err := os.Open(fsrcPath)
	if err != nil {
		log.Println("Failed opening file", fsrcPath, err)
	}
	defer fd.Close()

	fdstPath := pathJoin(dstPrefix, filename)
	UploadS3(uploader, fd, dstBucket, fdstPath, storageClass, info)
	fmt.Println("Copy:", filename)

}

func F2O_SymCopy(client *s3.Client, info FileInfo, srcPath string, dstBucket string, dstPrefix string, storageClass string) {

	uploader := manager.NewUploader(client)

	filename := info.Filename
	fsrcPath := pathJoin(srcPath, filename)  //path这里是约对路径
	linkFilePath, _ := os.Readlink(fsrcPath) //读取link所指向的文件路径

	tmpfile := CreateTempFile(dataDir, filepath.Base(filename))
	tmpfile.Write([]byte(linkFilePath)) //将指向的目标文件路径写入临时文件
	tmpfname := tmpfile.Name()
	tmpfile.Close() //这里写入没有落盘，所以上传的是空的，所以需要先关闭再打开，后面再更优雅的代码
	fd, _ := os.Open(tmpfname)
	defer fd.Close()

	fdstPath := pathJoin(dstPrefix, filename)
	UploadS3(uploader, fd, dstBucket, fdstPath, storageClass, info)

	fmt.Println("Copy:", filename)

}
