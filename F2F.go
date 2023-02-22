// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

//filenmae 指的是相对路径下的文件或对象名
//fsrcPath, fdstPath 指的是绝对路径下的文件或对象名
//srcPath, dstPath指的是拷贝的目录或prefix

func F2F_DirCopy(info FileInfo, srcPath string, dstpath string, defaultFileMode Filemod) {

	filename := info.Filename
	fdstPath := pathJoin(dstPath, filename)

	_, err := os.Lstat(fdstPath)
	if errors.Is(err, os.ErrNotExist) { //判断目录是否已存在,不存在的话
		os.MkdirAll(fdstPath, 0755)
	}

	Chattr(info, fdstPath, false, defaultFileMode) //目录会随着目录下的文件更新而更新，所以这里不更新时间

	fmt.Println("Copy:", filename)

}

func F2F_RegCopy(info FileInfo, srcPath string, dstpath string, partSize int64, defalutFileMode Filemod) {

	filename := info.Filename
	fdstPath := pathJoin(dstPath, filename)

	_, err := os.Lstat(filepath.Dir(fdstPath)) //判断目录是否已存在,不存在就新建。
	if errors.Is(err, os.ErrNotExist) {
		err = os.MkdirAll(filepath.Dir(fdstPath), 0775)
		if err != nil {
			log.Println(err)
		}
	}

	fd, err := os.Create(fdstPath)
	if err != nil {
		log.Println("Create file failed:", err)
	}

	fsrcPath := pathJoin(srcPath, filename)
	_, err = CopyFile(fd, fsrcPath)
	if err != nil {

		log.Printf("Error %s ocurred in copy %s :\n", err, fsrcPath)
	}
	fd.Close()

	Chattr(info, fdstPath, true, defalutFileMode)

	fmt.Println("Copy:", filename)
}

func F2F_SymCopy( info FileInfo, srcPath string, dstpath string) {

	filename := info.Filename
	fdstPath := pathJoin(dstPath, filename)

	_, err := os.Lstat(filepath.Dir(fdstPath)) //判断目录是否已存在,不存在就新建。
	if errors.Is(err, os.ErrNotExist) {
		os.MkdirAll(filepath.Dir(fdstPath), 0755)
	}

	//os.Symlink()，如果已经Link，则不做任何事，所以为了与源同步，这时如果link已经存在，应该删掉
	_, err = os.Lstat(fdstPath)
	if err == nil { //代表symlink存在，删除现有Link
		os.Remove(fdstPath)
	}

	linkTarget, err := os.Readlink(pathJoin(srcPath, filename))
	if err != nil {
		fmt.Println(err)

	}
	err = os.Symlink(linkTarget, fdstPath)
	if err != nil {
		log.Println("Symbol link failed to create:", fdstPath, err)
	} else {
		fmt.Println("Copy:", filename)
	}
}
