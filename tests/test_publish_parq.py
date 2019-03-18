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

    def publish_parq_setup(self, overrides: dict = dict()):
        mocker = MockHelper(count=100, s3=True)
        df = mocker.dataframe
        bucket = mocker.s3_bucket
        defaults = {
            'bucket': bucket,
            'dataset': 'safedatasetname',
            'prefix': 'safekeyprefixname',
            'dataframe': df,
            'partitions': []
        }
        return pub_parq.S3PublishParq(bucket=overrides.get('bucket', defaults['bucket']),
                                      dataset=overrides.get(
                                          'dataset', defaults['dataset']),
                                      prefix=overrides.get(
                                          'prefix', defaults['prefix']),
                                      dataframe=overrides.get(
                                          'dataframe', defaults['dataframe']),
                                      partitions=overrides.get(
                                          'partitions', defaults['partitions'])
                                      )
    # accepts valid column names as partitions

    def test_accepts_valid_partitions(self):
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
                parq = self.publish_parq_setup(
                    overrides={"dataframe": df.dataframe, "partitions": partitions})
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
        from_s3 = pq.ParquetDataset(s3_path, filesystem=s3fs.S3FileSystem())
        s3pd = from_s3.read().to_pandas()
        pre_df = dataframe
        assert set(zip(s3pd.int_col, s3pd.str_col, s3pd.grouped_col)) - \
            set(zip(pre_df.int_col, pre_df.str_col, pre_df.grouped_col)) == set()
