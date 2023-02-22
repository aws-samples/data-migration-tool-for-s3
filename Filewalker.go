// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log"
	"os"
	"path/filepath"
)

//这是filepath.Walk的参数，代表每扫描到一个对象，需要执行这个参数进行操作
//由于这里的Walk参数是固定的，但是因为放到list的最好是相对路径，所以这里的srcPath为全局参数
func (f FileWalk) Walk(fsrcPath string, info os.FileInfo, err error) error {
	if err != nil {
		log.Println(err)
		return err
	}
	if f.IsInitialCopy { //初次拷贝不检查，全部进行待拷贝列表
		var objInfo FileInfo
		if f.withAttr {
			objInfo = GetFileMetadata(f.SrcPath, fsrcPath)

		} else {
			objInfo = GetFileMetadataWithoutAttr(f.SrcPath, fsrcPath)

		}
		if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
			f.FileList <- objInfo

		}
	} else {
		filename, err := filepath.Rel(f.SrcPath, fsrcPath) //在key上去除掉原来的prefix
		if err != nil {
			log.Fatalln("Unable to get relative path:", fsrcPath, err)
		}
		if info.IsDir() {
			filename = filename + "/"
		}
		if f.FileMap[filename].CStatus.CopyStatus != "checkPass" {
			var objInfo FileInfo
			if f.withAttr {
				objInfo = GetFileMetadata(f.SrcPath, fsrcPath)

			} else {
				objInfo = GetFileMetadataWithoutAttr(f.SrcPath, fsrcPath)

			}
			if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
				f.FileList <- objInfo

			}
		}

	}

	return nil
}

func (f FileWalk) WalkforSrcCheck(fsrcPath string, info os.FileInfo, err error) error {
	if err != nil {
		log.Println(err)
		return err
	}
	var objInfo FileInfo
	if f.withAttr {
		objInfo = GetFileMetadata(f.SrcPath, fsrcPath)

	} else {
		objInfo = GetFileMetadataWithoutAttr(f.SrcPath, fsrcPath)

	}

	if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
		f.SrcCheckMap[objInfo.Filename] = objInfo

	}

	return nil
}

func (f FileWalk) WalkforDstCheck(fdstPath string, info os.FileInfo, err error) error {
	if err != nil {
		log.Println(err)
		return err
	}
	var objInfo FileInfo
	if f.withAttr {
		objInfo = GetFileMetadata(f.DstPath, fdstPath) //这里f.SrcPath就是destination目录

	} else {
		objInfo = GetFileMetadataWithoutAttr(f.DstPath, fdstPath)

	}
	if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
		f.DstCheckMap[objInfo.Filename] = objInfo

	}

	return nil
}

func (f FileWalk) WalkforSrcIncrCheck(fsrcPath string, info os.FileInfo, err error) error {
	if err != nil {
		log.Println(err)
		return err
	}

	filename, err := filepath.Rel(f.SrcPath, fsrcPath) //在key上去除掉原来的prefix
	if err != nil {
		log.Fatalln("Unable to get relative path:", fsrcPath, err)
	}
	if info.IsDir() {
		filename = filename + "/"
	}

	if f.FileMap[filename].CStatus.CopyStatus != "checkPass" {
		var objInfo FileInfo
		if f.withAttr {
			objInfo = GetFileMetadata(f.SrcPath, fsrcPath)

		} else {
			objInfo = GetFileMetadataWithoutAttr(f.SrcPath, fsrcPath)

		}
		if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
			f.SrcCheckMap[objInfo.Filename] = objInfo

		}

	}

	return nil
}

func (f FileWalk) WalkforDstIncrCheck(fdstPath string, info os.FileInfo, err error) error {
	if err != nil {
		log.Println(err)
		return err
	}

	filename, err := filepath.Rel(f.DstPath, fdstPath) //在key上去除掉原来的prefix
	if err != nil {
		log.Fatalln("Unable to get relative path:", fdstPath, err)
	}
	if info.IsDir() {
		filename = filename + "/"
	}
	if f.FileMap[filename].CStatus.CopyStatus != "checkPass" {
		var objInfo FileInfo
		if f.withAttr {
			objInfo = GetFileMetadata(f.DstPath, fdstPath)

		} else {
			objInfo = GetFileMetadataWithoutAttr(f.DstPath, fdstPath)

		}
		if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
			f.DstCheckMap[objInfo.Filename] = objInfo

		}
	}

	return nil
}
