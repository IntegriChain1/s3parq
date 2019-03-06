import pytest
from mock import patch
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from moto import mock_s3
from dfmock import DFMock
import s3_parq.publish_parq as pub_parq  
from .mock_helper import MockHelper
import s3fs

@mock_s3
class Test:

    def publish_parq_setup(self, overrides:dict=dict()):
        df = MockHelper(count=100).dataframe 
        defaults ={
        'bucket':'safebucketname',
        'dataset' : 'safedatasetname',
        'key_prefix' : 'safekeyprefixname',
        'dataframe' : df,
        'partitions':[]
        }
        return pub_parq.S3PublishParq(   bucket=overrides.get('bucket',defaults['bucket']),
                                dataset=overrides.get('dataset',defaults['dataset']),
                                key_prefix=overrides.get('key_prefix',defaults['key_prefix']),
                                dataframe=overrides.get('dataframe',defaults['dataframe']),
                                partitions=overrides.get('partitions',defaults['partitions'])
                            )
# accepts valid column names as partitions
    def test_accepts_valid_partitions(self):
        df = DFMock(count=100)
        df.columns = {"text_col":"string","int_cal":"integer","float_col":"float"}
        df.generate_dataframe()
        partitions = df.columns[:1]
        self.publish_parq_setup(overrides={"dataframe":df.dataframe, "partitions":partitions})

    # only accepts valid column names as partitions
    def test_accepts_valid_partitions(self):
        df = DFMock(count=100)
        df.columns = {"text_col":"string","int_cal":"integer","float_col":"float"}
        df.generate_dataframe()
        partitions = df.dataframe.columns[:1]
        partitions += ('banana')
        with pytest.raises(ValueError):
            self.publish_parq_setup(overrides={"dataframe":df.dataframe, "partitions":partitions})
    
    # generates partitions in order
    def test_generates_partitions_in_order(self):
        df = DFMock(count=100)
        df.columns = {"text_col":"string","int_cal":"integer","float_col":"float"}
        df.generate_dataframe()
        partitions = df.dataframe.columns[:1].tolist()
        with patch('s3_parq.publish_parq.pq.write_to_dataset', return_value=None) as mock_method:
            parq = self.publish_parq_setup(overrides={"dataframe":df.dataframe, "partitions":partitions}) 
            arg, kwarg = mock_method.call_args
            assert kwarg['partition_cols'] == partitions
    
    # generates valid parquet files
    @mock_s3
    def test_generates_valid_parquet_files(self):
        bucket = MockHelper().random_name()
        dataset = MockHelper().random_name()
        
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket)
        
        df = DFMock(count=100)
        df.columns={"int_col":"int","str_col":"string","grouped_col":{"option_count":4,"option_type":"string"}}
        df.generate_dataframe()
        parq = pub_parq.S3PublishParq(dataframe=df.dataframe, dataset=dataset, bucket=bucket, partitions=['grouped_col'], key_prefix='')
        
        ## publish pushes directly to s3, so need to pull them down to inspect
        s3_path = f"s3://{bucket}/{dataset}"
        from_s3 = pq.ParquetDataset(s3_path, filesystem=s3fs.S3FileSystem())
        s3pd = from_s3.read().to_pandas()
        pre_df = df.dataframe
        assert set(zip(s3pd.int_col,s3pd.str_col, s3pd.grouped_col)) - set(zip(pre_df.int_col, pre_df.str_col, pre_df.grouped_col)) == set()

# correctly sets s3 metadata

# respects source df schema exactly

# data in == data out

# spectrum register schema

# generates files of compressed size <=60mb
## TODO: this needs to happen in pyarrow actually. :(
