## Data Migration Tool for S3

A Data Migration Tool（abbr. ADMT, formally named goSync) is a data migration tool developed by Golang. It can upload data to S3, download  data to local directory, copy data between buckets/prefixes, or between directories. 
It  also supports file attributes retain in S3 uploading and downloading, attribute check, md5 check after data copy, incremental copy.
It is only 10MB size and doesn’t require installation. It works well in k8s, especially in AI/ML, HPC scenarios.

Example of S3 upload:

     admt -f 30  ./localdir s3://bucket1/prefix1 
     
Example of S3 download：

     admt -f 30  s3://bucket1/prefix1 ./localdir 
    
Example of directory copy:

     admt -f 30 ./localdir1 ./localdir2 
   
Example of bueckt/prefix sync:
     admt -f 30  s3://bucket1/prefix1 s3://bucket2/prefix2


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

