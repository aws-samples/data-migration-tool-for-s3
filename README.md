## Data Migration Tool for S3

// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
Data migration tool for S3是使用golang编写的数据迁移程度，支持S3的上传、下载， 文件之间的拷贝，对象之间的拷贝。
特点：
    1. 并行拷贝，拷贝速度最快可以到aws s3 sync的几十倍
    2. 支持增量传输
    3. 支持S3上传、下载过程中的保留文件属性（文件大小、atime、mtime、权限、文件类型等）
    4. 支持symlink, 空目录
    5. 支持迁移后检查（文件属性检查、MD5检查）
    6. 支持上传到指定S3 Storage Class
    7. 与FSx for Lustre, FSx File Cache兼容

应用场景：
    1. 机器学习、Hadoop/Spark
    2. 大规模数据迁移
    3. 本地环境数据备份到S3（保留属性与symlink)

使用说明：
    1. ./admt -h看参数
    2. sudo ./admt [options] <Source Path> <Destination Path>, 请在root模式下运行
        样例： 
            sudo ./admt -f 40  localdir s3://bucketname/prefixname
            sudo ./admt -f 40  s3://bucketname/prefixname localdir
            sudo ./admt -f 40   s3://srcBucketname/srcPrefixname/ s3://dstBucketname/dstPrefix
            sudo ./admt -f 40  srcLocaldir dstLocaldir
    3. 参数说明：
        -f : 代表goroutine factor因子, 所有goroutine数等于factor*numOfCPU
        -a : 代表在迁移过程中是否携带文件属性信息, 及symlink
        -c : 迁移后检查，分为
                nocheck:不需要检查
                attr: 只进行文件大小，修改时间的属性检查
                md5:  进行MD5检查
        -t : 迁移后检查类型，分为
                full: 对源和目进行全面扫描
                incr: 只扫描本次迁移部分
        -sc: 指定上传的storage class, 默认为STANDARD, 支持'STANDARD', 'REDUCED_REDUNDANCY', 'STANDARD_IA', 'ONEZONE_IA','INTELLIGENT_TIERING','GLACIER','DEEP_ARCHIVE','GLACIER_IR' 
        -region: 当使用非默认region时，需要指定
        -u : 当-a为false时，默认文件owner为当前用户，可以使用这个参数指定
        -g : 当-a为false时，默认文件group为当前属组，可以使用这个参数指定
        -m : 当-a为false时，默认文件mode为775，可以使用这个参数指定

注意事项：
    1. 一般-f 指定在40或上以
    2. 如果需要进行增量传输，需要指定-c 为attr或md5, 如果需要检查已传输但源有更改的文件，请使用-t full
    3. 当提示error 301时，请指定-region
    4. 不能将-c false与true混用

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

