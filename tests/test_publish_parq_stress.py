import boto3
from dfmock import DFMock
from moto import mock_s3
import pytest

import s3parq.publish_parq as pub_parq


@pytest.mark.slow
@mock_s3
def test_parquet_sizes():
    """ Running two tests - this needs to work with and without partitions """
    bucket = "testbucket"
    key = "testdataset"
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket)
    df = DFMock(count=100000)
    df.columns = {"int_col": "int", "str_col": "string",
                  "grouped_col": {"option_count": 2, "option_type": "string"}}
    df.generate_dataframe()
    df.grow_dataframe_to_size(500)
    pub_parq.publish(
        dataframe=df.dataframe, key=key, bucket=bucket, partitions=[])

    for obj in s3_client.list_objects(Bucket=bucket)['Contents']:
        if obj['Key'].endswith(".parquet"):
            assert float(obj['Size']) <= 61 * float(1 << 20)


@pytest.mark.slow
@mock_s3
def test_parquet_sizes_with_partition():
    """ Running two tests - this needs to work with and without partitions """
    bucket = "testbucket"
    key = "testdataset"
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket)
    df = DFMock(count=100000)
    df.columns = {"int_col": "int", "str_col": "string",
                  "grouped_col": {"option_count": 2, "option_type": "string"}}
    df.generate_dataframe()
    df.grow_dataframe_to_size(500)
    pub_parq.publish(
        dataframe=df.dataframe, key=key, bucket=bucket, partitions=['grouped_col'])

    for obj in s3_client.list_objects(Bucket=bucket)['Contents']:
        if obj['Key'].endswith(".parquet"):
            assert float(obj['Size']) <= 61 * float(1 << 20)
