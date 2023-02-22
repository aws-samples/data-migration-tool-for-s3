// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

//filenmae 指的是相对路径下的文件或对象名
//fsrcPath, fdstPath 指的是绝对路径下的文件或对象名
//srcPath, dstPath指的是拷贝的目录或prefix

func O2F_DirCopy(client *s3.Client, info FileInfo, srcBucket string, srcPrefix string, dstPath string, defaultFileMode Filemod) {

	filename := info.Filename
	fdstPath := pathJoin(dstPath, filename)

	//os.MkdirAll，当目录存在时，不做任何事，返回nil，所以这个判断是有必要的，当目录存在时，需要更改目录权限，以保证后续文件能有权限写入（有可能因为某些原因目标目录权限更改使得程序没有写入权限）
	_, err := os.Lstat(fdstPath)
	if errors.Is(err, os.ErrNotExist) { //判断目录是否已存在,不存在的话
		os.MkdirAll(fdstPath, 0775)
	}

	Chattr(info, fdstPath, false, defaultFileMode) //目录会随着目录下的文件更新而更新，所以这里不更新时间

	fmt.Println("Copy:", filename)
}

func O2F_RegCopy(client *s3.Client, info FileInfo, srcBucket string, srcPrefix string, dstPath string, partSize int64, defaultFileMode Filemod) {

	downloader := manager.NewDownloader(client, func(u *manager.Downloader) {
		u.PartSize = partSize * 1024 * 1024

	})

	filename := info.Filename
	fdstPath := pathJoin(dstPath, filename)

	_, err := os.Lstat(filepath.Dir(fdstPath)) //判断目录是否已存在,不存在就新建。
	if errors.Is(err, os.ErrNotExist) {
		os.MkdirAll(filepath.Dir(fdstPath), 0775)
	}
	fd, err := os.Create(fdstPath)
	if err != nil {
		log.Println("Create file failed", err)
	}
	defer fd.Close()

	fsrcPath := pathJoin(srcPrefix, filename)
	DownloadS3(downloader, fd, srcBucket, fsrcPath)

	Chattr(info, fdstPath, true, defaultFileMode) //目录会随着目录下的文件更新而更新，所以这里不更新时间

	fmt.Println("Copy:", filename)

}

func O2F_SymCopy(client *s3.Client, info FileInfo, srcBucket string, srcPrefix string, dstPath string) {

	downloader := manager.NewDownloader(client)

	filename := info.Filename
	fdstPath := pathJoin(dstPath, filename)

	_, err := os.Lstat(filepath.Dir(fdstPath)) //判断目录是否已存在,不存在的话创建
	if errors.Is(err, os.ErrNotExist) {
		os.MkdirAll(filepath.Dir(fdstPath), 0755)
	}

	//os.Symlink()，如果已经Link，则不做任何事，所以为了与源同步，这时如果link已经存在，应该删掉
	_, err = os.Lstat(fdstPath)
	if err == nil { //代表symlink存在，删除现有Link
		os.Remove(fdstPath)
	}

	tmpfile := CreateTempFile(dataDir, filepath.Base(filename))

	fsrcPath := pathJoin(srcPrefix, filename)
	DownloadS3(downloader, tmpfile, srcBucket, fsrcPath)

	b, err := os.ReadFile(tmpfile.Name())
	if err != nil {
		log.Println("Read file failed", err)
	}
	tmpfile.Close()

	err = os.Symlink(string(b), fdstPath)
	if err != nil {
		log.Println("Symbol link failed to create:", err)
	} else { //这里不用Chattr，因为更改Link,实际上只会改变target文件的权限

		fmt.Println("Copy:", filename)
	}

}
