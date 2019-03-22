# s3parq
Parquet file management in S3 for hive-style partitioned data

## What is this? 
In many ways, parquet standards are still the wild west of data. Depending on your partitioning style, metadata store strategy etc. you can tackle the big data beast in a multitude of different ways. 
This is an AWS-specific solution intended to serve as an interface between python programs and any of the multitude of tools used to access this data. s3parq is an end-to-end solution for:
1. writing data from pandas dataframes to s3 as partitioned parquet.
2. reading data from s3 partitioned parquet *that was created by s3parq* to pandas dataframes.

*NOTE:* s3parq writes (and reads) metadata into the s3 objects that is used to filter records _before_ any file i/o; this makes selecting datasets faster, but also means you need to have written data with s3parq to read it with s3parq. 

*TLDR - to read with s3parq, you need to have written with s3parq* 
 
## Basic Usage

we get data by dataset name. 
    
    from s3_parq import S3Parq

    bucket = 'mybucket'
    key = 'path-in-bucket/to/my/dataset'
    dataframe = pd.DataFrame(['some_big_data'])
    
    ## writing to s3
    parq = S3Parq(  bucket=bucket,
                    key=key,
                    dataframe=dataframe, 
                    partitions= ['column1',
                                'column2'])
    parq.publish()

    ## reading from s3, getting only records with an id >= 150
    parq = S3Parq(  bucket=bucket,
                    key=key,
                    dataframe=dataframe, 
                    filter= {"partition":"id,
                            "values":150, 
                            "comparison":'>='})
    retrieved_dataframe = parq.fetch()


## Getting Existing Partition Values 
a lot of pre-filtering involves trimming down your dataset based on the values already in another data set. To make that easier, s3parq provides a few super helpful helper functions: 

    ## max value

    ## partition values not in a list

    ## list values not in partition value list

    ## all values for a partition

## Gotchas
- filters can only be applied to partitions; this is because we do not actually pull down any of the data until after the filtering has happened. This aligns with data best practices; the things you filter on regularly are the things you should partition on!

## Contribution
We welcome pull requests!
Some basic guidelines:
- *test yo' code.* code coverage is important! 
- *be respectful.* in pr comments, code comments etc;
 
