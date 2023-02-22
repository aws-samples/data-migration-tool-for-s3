// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"log"
	"path/filepath"
	"regexp"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (f FileWalk) Listobj(client *s3.Client, srcBucket string, srcPrefix string) {

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(srcBucket),
		Prefix: aws.String(srcPrefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 10000
	})

	for paginator.HasMorePages() {

		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("error: %v", err)
			return
		}

		for _, value := range output.Contents {

			if f.IsInitialCopy {
				var objInfo FileInfo
				if f.withAttr {
					objInfo = GetObjMetadata(client, srcBucket, srcPrefix, *value.Key)

				} else {
					objInfo = GetObjMetadataWithoutAttr(client, srcBucket, srcPrefix, *value.Key, value.LastModified.Unix(), value.Size)

				}

				if objInfo.CStatus.CopyStatus == "notFound" {
					continue //如果获取Key信息的时候报错，就直接跳过这个对象
				}
				//这里去掉. ..两个目录
				if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
					f.FileList <- objInfo

				}

			} else { //增量拷贝
				filename, err := filepath.Rel(srcPrefix, *value.Key) //在key上去除掉原来的prefix
				if err != nil {
					log.Fatalln("Unable to get relative path:", *value.Key, err)
				}
				isDir, _ := regexp.MatchString("/$", *value.Key)
				if isDir {
					filename = filename + "/"
				}

				if f.FileMap[filename].CStatus.CopyStatus != "checkPass" {
					var objInfo FileInfo
					if f.withAttr {
						objInfo = GetObjMetadata(client, srcBucket, srcPrefix, *value.Key)

					} else {
						objInfo = GetObjMetadataWithoutAttr(client, srcBucket, srcPrefix, *value.Key, value.LastModified.Unix(), value.Size)

					}
					if objInfo.CStatus.CopyStatus == "notFound" {
						continue //如果获取Key信息的时候报错，就直接跳过这个对象
					}

					if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
						f.FileList <- objInfo

					}

				}

			}
		}

	}
}

func (f FileWalk) ListobjforSrcCheck(client *s3.Client, srcBucket string, srcPrefix string) {

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(srcBucket),
		Prefix: aws.String(srcPrefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 10000
	})

	for paginator.HasMorePages() {

		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("error: %v", err)
			return
		}

		for _, value := range output.Contents {

			var objInfo FileInfo
			if f.withAttr {
				objInfo = GetObjMetadata(client, srcBucket, srcPrefix, *value.Key)

			} else {
				objInfo = GetObjMetadataWithoutAttr(client, srcBucket, srcPrefix, *value.Key, value.LastModified.Unix(), value.Size)

			}

			if objInfo.CStatus.CopyStatus == "notFound" {
				continue //如果获取Key信息的时候报错，就直接跳过这个对象
			}

			if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
				f.SrcCheckMap[objInfo.Filename] = objInfo

			}

		}

	}
}

func (f FileWalk) ListobjforDstCheck(client *s3.Client, dstBucket string, dstPrefix string) {

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(dstBucket),
		Prefix: aws.String(dstPrefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 10000
	})

	for paginator.HasMorePages() {

		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("error: %v", err)
			return
		}

		for _, value := range output.Contents {

			var objInfo FileInfo
			if f.withAttr {
				objInfo = GetObjMetadata(client, dstBucket, dstPrefix, *value.Key)

			} else {
				objInfo = GetObjMetadataWithoutAttr(client, dstBucket, dstPrefix, *value.Key, value.LastModified.Unix(), value.Size)

			}

			if objInfo.CStatus.CopyStatus == "notFound" {
				continue //如果获取Key信息的时候报错，就直接跳过这个对象
			}
			if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
				f.DstCheckMap[objInfo.Filename] = objInfo

			}

		}

	}
}

func (f FileWalk) ListobjforSrcIncrCheck(client *s3.Client, srcBucket string, srcPrefix string) {

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(srcBucket),
		Prefix: aws.String(srcPrefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 10000
	})

	for paginator.HasMorePages() {

		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("error: %v", err)
			return
		}

		for _, value := range output.Contents {

			filename, err := filepath.Rel(srcPrefix, *value.Key) //在key上去除掉原来的prefix
			if err != nil {
				log.Fatalln("Unable to get relative path:", *value.Key, err)
			}
			isDir, _ := regexp.MatchString("/$", *value.Key)
			if isDir {
				filename = filename + "/"
			}

			if f.FileMap[filename].CStatus.CopyStatus != "checkPass" {
				var objInfo FileInfo
				if f.withAttr {
					objInfo = GetObjMetadata(client, srcBucket, srcPrefix, *value.Key)

				} else {
					objInfo = GetObjMetadataWithoutAttr(client, srcBucket, srcPrefix, *value.Key, value.LastModified.Unix(), value.Size)

				}
				if objInfo.CStatus.CopyStatus == "notFound" {
					continue //如果获取Key信息的时候报错，就直接跳过这个对象
				}

				if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
					f.SrcCheckMap[objInfo.Filename] = objInfo

				}

			}

		}

	}
}

func (f FileWalk) ListobjforDstIncrCheck(client *s3.Client, dstBucket string, dstPrefix string) {

	params := &s3.ListObjectsV2Input{
		Bucket: aws.String(dstBucket),
		Prefix: aws.String(dstPrefix),
	}
	paginator := s3.NewListObjectsV2Paginator(client, params, func(o *s3.ListObjectsV2PaginatorOptions) {
		o.Limit = 10000
	})

	for paginator.HasMorePages() {

		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			log.Printf("error: %v", err)
			return
		}

		for _, value := range output.Contents {

			filename, err := filepath.Rel(dstPrefix, *value.Key) //在key上去除掉原来的prefix
			if err != nil {
				log.Fatalln("Unable to get relative path:", *value.Key, err)
			}
			isDir, _ := regexp.MatchString("/$", *value.Key)
			if isDir {
				filename = filename + "/"
			}

			if f.FileMap[filename].CStatus.CopyStatus != "checkPass" {
				var objInfo FileInfo
				if f.withAttr {
					objInfo = GetObjMetadata(client, dstBucket, dstPrefix, *value.Key)

				} else {
					objInfo = GetObjMetadataWithoutAttr(client, dstBucket, dstPrefix, *value.Key, value.LastModified.Unix(), value.Size)

				}
				if objInfo.CStatus.CopyStatus == "notFound" {
					continue //如果获取Key信息的时候报错，就直接跳过这个对象
				}

				if !(objInfo.Filename == "./" || objInfo.Filename == "../" || objInfo.Filename == ".." || objInfo.Filename == ".") {
					f.DstCheckMap[objInfo.Filename] = objInfo

				}

			}

		}

	}
}
