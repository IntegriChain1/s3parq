import pytest
from mock import patch
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from dfmock import DFMock
import s3_parq.publish_parq as pub_parq  
from .mock_helper import MockHelper
import s3fs
from moto import mock_s3

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
    
    def publish_mock(self):
        """ test setup where we do not want s3 records.
            Returns the s3 path components to the dataset."""
        bucket = MockHelper().random_name()
        dataset = MockHelper().random_name()
        
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket)
        
        df = DFMock(count=100)
        df.columns={"int_col":"int","str_col":"string","grouped_col":{"option_count":4,"option_type":"string"}}
        df.generate_dataframe()

        parq = pub_parq.S3PublishParq(dataframe=df.dataframe, dataset=dataset, bucket=bucket, partitions=['grouped_col'], key_prefix='')
        return tuple([bucket,dataset,df.dataframe])
    
    # generates valid parquet files identical to source df
    @mock_s3
    def test_generates_valid_parquet_files(self):

        bucket, dataset, dataframe = self.publish_mock()
        s3_path = f"s3://{bucket}/{dataset}"
        from_s3 = pq.ParquetDataset(s3_path, filesystem=s3fs.S3FileSystem())
        s3pd = from_s3.read().to_pandas()
        pre_df = dataframe
        assert set(zip(s3pd.int_col,s3pd.str_col, s3pd.grouped_col)) - set(zip(pre_df.int_col, pre_df.str_col, pre_df.grouped_col)) == set()

    # generates single partition path files of compressed size ~60mb
   
    def test_parquet_sizes(self):
        bucket = MockHelper().random_name()
        dataset = MockHelper().random_name()
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket)
        df = DFMock(count=1000000)
        df.columns={"int_col":"int","str_col":"string","grouped_col":{"option_count":4,"option_type":"string"}}
        df.generate_dataframe()
        #df.grow_dataframe_to_size(120)
        parq = pub_parq.S3PublishParq(dataframe=df.dataframe, dataset=dataset, bucket=bucket, partitions=['grouped_col'], key_prefix='')

        for obj in s3_client.list_objects(Bucket=bucket)['Contents']:
            if obj['Key'].endswith(".parquet"):
                assert float(obj['Size']) <= 61 * float(1<<20)
        
    # correctly sets s3 metadata

    # spectrum register schema

    
