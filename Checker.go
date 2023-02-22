// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (f FileWalk) F2O_GetCheck(client *s3.Client, srcPath string, dstBucket string, dstPrefix string) {
	if err := filepath.Walk(srcPath, f.WalkforSrcCheck); err != nil {
		log.Fatalln("Walk failed:", err)
	}
	close(f.FileList)

	f.ListobjforDstCheck(client, dstBucket, dstPrefix)

	CheckAttr(&f.SrcCheckMap, &f.DstCheckMap, &f.ResultMap)
}

func (f FileWalk) F2F_GetCheck(srcPath string, dstPath string) {
	if err := filepath.Walk(srcPath, f.WalkforSrcCheck); err != nil {
		log.Fatalln("Walk failed:", err)
	}
	close(f.FileList)

	if err := filepath.Walk(dstPath, f.WalkforDstCheck); err != nil {
		log.Fatalln("Walk failed:", err)
	}

	CheckAttr(&f.SrcCheckMap, &f.DstCheckMap, &f.ResultMap)
}

func (f FileWalk) O2F_GetCheck(client *s3.Client, srcBucket string, srcPrefix string, dstPath string) {
	f.ListobjforSrcCheck(client, srcBucket, srcPrefix)

	if err := filepath.Walk(dstPath, f.WalkforDstCheck); err != nil {
		log.Fatalln("Walk failed:", err)
	}
	close(f.FileList)

	CheckAttr(&f.SrcCheckMap, &f.DstCheckMap, &f.ResultMap)
}

func (f FileWalk) O2O_GetCheck(client *s3.Client, srcBucket string, srcPrefix string, dstBucket string, dstPrefix string) {
	f.ListobjforSrcCheck(client, srcBucket, srcPrefix)
	f.ListobjforDstCheck(client, dstBucket, dstPrefix)
	close(f.FileList)

	CheckAttr(&f.SrcCheckMap, &f.DstCheckMap, &f.ResultMap)
}

func (f FileWalk) F2O_GetIncrCheck(client *s3.Client, srcPath string, dstBucket string, dstPrefix string) {
	if err := filepath.Walk(srcPath, f.WalkforSrcIncrCheck); err != nil {
		log.Fatalln("Walk failed:", err)
	}
	close(f.FileList)

	f.ListobjforDstIncrCheck(client, dstBucket, dstPrefix)
	CheckAttr(&f.SrcCheckMap, &f.DstCheckMap, &f.ResultMap)
}

func (f FileWalk) F2F_GetIncrCheck(srcPath string, dstPath string) {
	if err := filepath.Walk(srcPath, f.WalkforSrcIncrCheck); err != nil {
		log.Fatalln("Walk failed:", err)
	}
	close(f.FileList)

	if err := filepath.Walk(dstPath, f.WalkforDstIncrCheck); err != nil {
		log.Fatalln("Walk failed:", err)
	}

	CheckAttr(&f.SrcCheckMap, &f.DstCheckMap, &f.ResultMap)
}

func (f FileWalk) O2F_GetIncrCheck(client *s3.Client, srcPath string, dstBucket string, dstPrefix string) {
	f.ListobjforSrcIncrCheck(client, srcBucket, srcPrefix)

	if err := filepath.Walk(dstPath, f.WalkforDstIncrCheck); err != nil {
		log.Fatalln("Walk failed:", err)
	}
	close(f.FileList)

	CheckAttr(&f.SrcCheckMap, &f.DstCheckMap, &f.ResultMap)
}

func (f FileWalk) O2O_GetIncrCheck(client *s3.Client, srcBucket string, srcPrefix string, dstBucket string, dstPrefix string) {
	f.ListobjforSrcIncrCheck(client, srcBucket, srcPrefix)
	f.ListobjforDstIncrCheck(client, dstBucket, dstPrefix)
	close(f.FileList)

	CheckAttr(&f.SrcCheckMap, &f.DstCheckMap, &f.ResultMap)
}
