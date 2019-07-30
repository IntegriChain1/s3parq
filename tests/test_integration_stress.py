import boto3
import moto
import s3parq
import pytest
import dfmock
from s3parq.publish_parq import publish
from s3parq.fetch_parq import fetch
import pandas as pd


@moto.mock_s3
def test_end_to_end():
    df = dfmock.DFMock(count=1000)
    df.columns = {"string_options": {"option_count": 4, "option_type": "string"},
                "int_options": {"option_count": 4, "option_type": "int"},
                "datetime_options": {"option_count": 5, "option_type": "datetime"},
                "float_options": {"option_count": 2, "option_type": "float"},
                "metrics": "integer"
                }
    df.generate_dataframe()
    df.grow_dataframe_to_size(250)
    
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

