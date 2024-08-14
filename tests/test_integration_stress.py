import boto3
import dfmock
import moto
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from s3parq.fetch_parq import fetch
from s3parq.publish_parq import publish
from s3parq.testing_helper import df_equal_by_set, sorted_dfs_equal_by_pandas_testing


@pytest.mark.slow
@moto.mock_aws
def test_end_to_end():
    df = dfmock.DFMock(count=100000)
    df.columns = {"string_options": {"option_count": 4, "option_type": "string"},
                  "int_options": {"option_count": 4, "option_type": "int"},
                  "datetime_options": {"option_count": 5, "option_type": "datetime"},
                  "float_options": {"option_count": 2, "option_type": "float"},
                  "metrics": "integer"
                  }

    df.generate_dataframe()
    # This is unfortunately big, but getting it to force a partition doesn't work otherwise
    df.grow_dataframe_to_size(500)

    s3_client = boto3.client('s3')
    bucket_name = 'thistestbucket'
    key = 'thisdataset'
    s3_client.create_bucket(Bucket=bucket_name)

    old_df = pd.DataFrame(df.dataframe)
    # pub it
    publish(
        bucket=bucket_name,
        key=key,
        dataframe=old_df,
        partitions=['string_options', 'datetime_options', 'float_options']
    )

    # go get it
    fetched_df = fetch(
        bucket=bucket_name,
        key=key,
        parallel=False
    )

    assert fetched_df.shape == old_df.shape
    assert df_equal_by_set(fetched_df, old_df, old_df.columns)
    sorted_dfs_equal_by_pandas_testing(fetched_df, old_df)
