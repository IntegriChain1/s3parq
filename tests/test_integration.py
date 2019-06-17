import boto3
import moto
import s3parq
import pytest
import dfmock
from s3parq.testing_helper import df_equal_by_set
from s3parq.publish_parq import publish
from s3parq.fetch_parq import fetch
import pandas as pd


# make a sample DF for all the tests
df = dfmock.DFMock(count=10000)
df.columns = {"string_options": {"option_count": 4, "option_type": "string"},
              "int_options": {"option_count": 4, "option_type": "int"},
              "datetime_options": {"option_count": 5, "option_type": "datetime"},
              "float_options": {"option_count": 2, "option_type": "float"},
              "metrics": "integer"
              }
df.generate_dataframe()


@moto.mock_s3
def test_end_to_end():
    s3_client = boto3.client('s3')

    bucket_name = 'thistestbucket'
    key = 'thisdataset'

    s3_client.create_bucket(Bucket=bucket_name)

    # pub it
    publish(
        bucket=bucket_name,
        key=key,
        dataframe=df.dataframe,
        partitions=['string_options', 'datetime_options', 'float_options']
    )

    # go get it
    fetched_df = fetch(
        bucket=bucket_name,
        key=key,
        parallel=False
    )

    assert fetched_df.shape == df.dataframe.shape
    pd.DataFrame.eq(fetched_df, df.dataframe)
    fetched_df.head()

@moto.mock_s3
def test_via_public_interface():
    s3_client = boto3.client('s3')
    bucket_name = 'another-bucket'
    key = 'testing/is/fun/dataset-name'
    s3_client.create_bucket(Bucket=bucket_name)

    parq.publish(bucket=bucket_name,
                key=key,
                dataframe=df.dataframe,
                partitions=['datetime_options'])

    ## moto explodes when we use parallel :( need to test this with a real boto call
    result = parq.fetch(bucket=bucket_name,
                        key=key,
                        parallel=False
                        )
    assert result.shape == df.dataframe.shape
    assert df_equal_by_set(result, df.dataframe, df.dataframe.columns.tolist())

