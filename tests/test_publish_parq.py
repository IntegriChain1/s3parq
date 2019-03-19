import pytest
from mock import patch
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from string import ascii_lowercase
import random
from dfmock import DFMock
from s3_parq.publish_parq import *
import s3fs
from moto import mock_s3
    

@mock_s3
class Test:

    def setup_s3(self):
        def rand_string():
            return ''.join([random.choice(ascii_lowercase) for x in range(0, 10)])
        
        bucket = rand_string()
        key = rand_string()

        s3_client = boto.client('s3')
        s3_client.create_bucket(Bucket= bucket)
        
        return dict("bucket":bucket,"key":key)

    def setup_df(self):
        df = DFMock(count=100)
        df.columns = {"text_col": "string",
                      "int_cal": "integer", "float_col": "float"}
        df.generate_dataframe()
        df.dataframe

        return dict("dataframe":df.dataframe, "columns":df.columns.keys()) 


    # accepts valid column names as partitions

    def test_accepts_valid_partitions(self):
        frame = self.setup_df
        s3 = self.setup_s3

        partitions = frame['columns'][:1]
        
        pub_parq = S3PublishParq(bucket = s3['bucket'],
                                 key = s3['key'],
                                    
        df = DFMock(count=100)
        df.columns = {"text_col": "string",
                      "int_cal": "integer", "float_col": "float"}
        df.generate_dataframe()
        partitions = df.dataframe.columns[:1]
        self.publish_parq_setup(
            overrides={"dataframe": df.dataframe, "partitions": partitions})

    # only accepts valid column names as partitions
    def test_rejects_protected_partitions(self):
        df = DFMock(count=100)
        df.columns = {"text_col": "string",
                      "int_cal": "integer", "float_col": "float"}
        df.generate_dataframe()
        partitions = df.dataframe.columns[:1]
        partitions += ('extract')
        with pytest.raises(ValueError):
            self.publish_parq_setup(
                overrides={"dataframe": df.dataframe, "partitions": partitions})

    # only accepts valid column names as partitions
    def test_only_accepts_valid_partitions(self):
        df = DFMock(count=100)
        df.columns = {"text_col": "string",
                      "int_cal": "integer", "float_col": "float"}
        df.generate_dataframe()
        partitions = df.dataframe.columns[:1]
        partitions += ('banana')
        with pytest.raises(ValueError):
            self.publish_parq_setup(
                overrides={"dataframe": df.dataframe, "partitions": partitions})

    # generates partitions in order
    def test_generates_partitions_in_order(self):
        df = DFMock(count=100)
        df.columns = {"text_col": "string",
                      "int_cal": "integer", "float_col": "float"}
        df.generate_dataframe()
        partitions = df.dataframe.columns[:1].tolist()
        with patch('s3_parq.publish_parq.boto3', return_value=True) as mock_downstream_method:
            with patch('s3_parq.publish_parq.pq.write_to_dataset', return_value=None) as mock_method:
                partitions = df.dataframe.columns[:1]
                parq = pub_parq.S3PublishParq(bucket='stub',dataframe=df.dataframe,prefix='stub',dataset='stub', partitions = partitions)     
                parq.publish()    
                arg, kwarg = mock_method.call_args
                assert kwarg['partition_cols'] == partitions

    def publish_mock(self):
        """ test setup whefre we do not want s3 records.
            Returns the s3 path components to the dataset."""
        bucket = MockHelper().random_name()
        dataset = MockHelper().random_name()

        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket)

        df = DFMock(count=100)
        df.columns = {"str_col": "string", "int_col": "int", "float_col": "float",
                      "bool_col": "boolean", "grouped_col": {"option_count": 4, "option_type": "string"}}
        df.generate_dataframe()

        parq = pub_parq.S3PublishParq(
            dataframe=df.dataframe, dataset=dataset, bucket=bucket, partitions=['grouped_col'], prefix='')

        return tuple([bucket, dataset, df.dataframe])

    # correctly sets s3 metadata
    def test_metadata(self):
        bucket, dataset, dataframe = self.publish_mock()
        s3_client = boto3.client('s3')
        for obj in s3_client.list_objects(Bucket=bucket)['Contents']:
            if obj['Key'].endswith(".parquet"):
                meta = s3_client.get_object(
                    Bucket=bucket, Key=obj['Key'])['Metadata']
                assert meta['partition_data_types'] == str(
                    {"str_col": "string",
                        "int_col": "integer",
                        "float_col": "float",
                        "bool_col": "boolean",
                        # "datetime_col":"datetime",
                        "grouped_col": "string"
                     })

    # generates valid parquet files identical to source df
    def test_generates_valid_parquet_files(self):

        bucket, dataset, dataframe = self.publish_mock()
        s3_path = f"s3://{bucket}/{dataset}"
        s3_client = boto3.client('s3')
        from_s3 = pq.ParquetDataset(s3_path, filesystem=s3fs.S3FileSystem())
        s3pd = from_s3.read().to_pandas()
        pre_df = dataframe
        assert set(zip(s3pd.int_col, s3pd.str_col, s3pd.grouped_col)) - \
            set(zip(pre_df.int_col, pre_df.str_col, pre_df.grouped_col)) == set()

    ## timedeltas no good
    def test_timedeltas_rejected(self):
        bucket = MockHelper().random_name()
        dataset = MockHelper().random_name()

        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket)

        df = DFMock(count=100)
        df.columns = {"timedelta_col": "timedelta", "int_col": "int", "float_col": "float",
                      "bool_col": "boolean", "grouped_col": {"option_count": 4, "option_type": "string"}}
        df.generate_dataframe()

        with pytest.raises(NotImplementedError):
            parq = pub_parq.S3PublishParq(
                dataframe=df.dataframe, dataset=dataset, bucket=bucket, partitions=['grouped_col'], prefix='')
