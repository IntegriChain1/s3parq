import pytest
import pandas as pd
from dfmock import DFMock
from s3_parq import S3Parq 

class Test:
    ## requires dataframe
    def test_requires_dataframe(self):
        with pytest.raises(TypeError):
            parq = S3Parq(  bucket="test_bucket",
                            dataset="test_dataset"
                            )
            parq.publish()

    ## only accepts dataframe
    def test_accepts_only_dataframe(self):
        with pytest.raises(TypeError):
            parq = S3Parq(  bucket="test_bucket",
                            dataframe="I am not a dataframe!",
                            dataset="test_dataset"
                            )
            parq.publish()

    ## accepts good dataframe
    def test_accepts_only_dataframe(self):
        df = DFMock()
        df.generate_dataframe() 
        parq = S3Parq(  bucket="test_bucket",
                            dataframe=df.dataframe,
                            dataset="test_dataset"
                            )
        
    ## requires bucket
    def test_requires_bucket(self):
        df = DFMock()
        df.generate_dataframe() 
        with pytest.raises(TypeError):
            parq = S3Parq(  dataset="test_dataset",
                            dataframe=df.dataframe
                            )
            parq.publish()

## requires dataset name

## optionally accepts valid key prefix

## raises not implemented error for timedelta column types

## accepts valid column names as partitions

## only accepts valid column names as partitions

## generates partitions in order

#- TODO: patch pyarrow here to make sure it is called - that's it. 
## generates parquet filesystem hierarchy correctly

## generates files of compressed size <=60mb

## generates valid parquet files

## respects source df schema exactly

## data in == data out

## do_publish returns success tuple

## extra credit: allows 60mb size to be adjusted? 
