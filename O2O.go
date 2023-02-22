// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func O2O_ObjectCopy(client *s3.Client, info FileInfo, srcBucket string, srcPrefix string, dstBucket string, dstPrefix string, storageClass string) {

	filename := info.Filename
	fsrcPath := pathJoin(srcPrefix, filename)
	fdstPath := pathJoin(dstPrefix, filename)
	isDir, _ := regexp.MatchString("/$", filename)
	if isDir {
		//CopyObject如果是directory,不支持storageclass
		_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
			Bucket:       aws.String(dstBucket),
			CopySource:   aws.String(srcBucket + "/" + fsrcPath),
			Key:          aws.String(fdstPath),
		})
		if err != nil {
			log.Println("Error:",fsrcPath, err)
		} else {
			fmt.Println("Copy:", filename)
		}
	}else{
		_, err := client.CopyObject(context.TODO(), &s3.CopyObjectInput{
			Bucket:       aws.String(dstBucket),
			CopySource:   aws.String(srcBucket + "/" + fsrcPath),
			Key:          aws.String(fdstPath),
			StorageClass: types.StorageClass(*aws.String(storageClass)),
		})
		if err != nil {
			log.Println("Error:",fsrcPath, err)
		} else {
			fmt.Println("Copy:", filename)
		}
	}




}
