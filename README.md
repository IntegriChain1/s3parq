# s3\_parq
Parquet file management in S3 for hive-style partitioned data

## What is this? 
In many ways, parquet standards are still the wild west of data. Depending on your partitioning style, metadata store strategy etc. you can tackle the big data beast in a multitude of different ways. 
This is an AWS-specific solution intended to serve as an interface between python programs and any of the multitude of tools used to access this data. s3\_parq is an end-to-end solution for:
1. writing data from pandas dataframes to s3 as partitioned parquet.
2. reading data from s3 partitioned parquet *that was created by s3_parq* to pandas dataframes.

*NOTE:* s3\_parq writes (and reads) metadata into the s3 objects that is used to filter records _before_ any file i/o; this makes selecting datasets faster, but also means you need to have written data with s3\_parq to read it with s3\_parq. 

*TLDR - to read with s3\_parq, you need to have written with s3\_parq* 
 
## Basic Usage

we get data by dataset name. 

    from s3_parq import S3Parq

    ## writing to s3
    parq = S3Parq(bucket='mybucket',dataset='my_dataset',dataframe=pandas_dataframe_to_write)
    parq.publish()

    ## reading from s3, getting only records with an id >= 150
    parq = S3Parq(bucket='mybucket',dataset='my_dataset',filter={"partition":"id,"values":150, "comparison":>=})
    retrieved_dataframe = parq.fetch()

## Gotchas
- filters can only be applied to partitions; this is because we do not actually  

## Contribution
We welcome pull requests!
Some basic guidelines:
- *test yo' code.* code coverage is important! 
- *be respectful.* in pr comments, code comments etc;
 
