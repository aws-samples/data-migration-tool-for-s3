// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main


//定义metadata信息
type FileInfo struct {
	IsMetaExist    bool
	Filename   string
	FUserAgent string
	FUID       int
	FGID       int
	FType      string
	FPerm      string
	FaTime     int64
	FmTime     int64
	FSize      int64
	CStatus CopyInfo
}

type FileWalk struct {
	FileList chan FileInfo
	IsInitialCopy bool
	FileMap map[string]FileInfo
	SrcCheckMap map[string]FileInfo
	DstCheckMap map[string]FileInfo
	ResultMap map[string]FileInfo
	SrcPath    string  	//这里留这个的目的是当F2F,F2O时，Filewarlker函数中无法传递SrcPath的值，需要通过结构体来传参，Listojb因为是自定义的，不需要从这里传参数
	DstPath    string 
	DefaultMod Filemod
	withAttr bool
}

type CopyInfo struct { //定义的Map的值结构

	CopyStatus string //notFound, inCopy, checkPass, checkFail
	Copytime   int64  //time.Unix()时间
}


type Filemod struct {
	UID  int
	GID  int
	Mode string
}