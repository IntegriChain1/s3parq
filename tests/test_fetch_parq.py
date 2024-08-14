import boto3
import datetime
import dfmock
from mock import patch
import moto
import pandas as pd
import pytest
import random
import contextlib

import s3parq.fetch_parq as fetch_parq
from s3parq.fetch_parq import MissingS3ParqMetadata
from s3parq.testing_helper import (
    sorted_dfs_equal_by_pandas_testing,
    setup_files_list,
    setup_grouped_dataframe,
    setup_nons3parq_parquet,
    setup_partitioned_parquet,
    setup_random_string
)


@contextlib.contextmanager
def get_s3_client():
    with moto.mock_aws():
        yield boto3.client('s3')


""" This test file is meant for testing the "public" iterface of fetch_parq
For the "private" functionality meant for internal testing, please refer
to test_fetch_parq_internal.py
"""

''' Phase 2 tests:
Refer to functionality of fetch_parq that:
    - Compares partitions and filters
    - Creates path(s) that lead to the appropriate files
    - Gets the list of those files to pull down from
'''

# Test that functionality of max pulls correctly


def test_gets_max():
    with get_s3_client() as s3_client:
        key = "safekeyprefixname/safedatasetname"
        bucket = "safebucketname"
        part_types = {
            "int_col": "int",
            "float_col": "float"
        }

        df = setup_grouped_dataframe(count=10, columns=part_types)
        bucket, parquet_paths = setup_partitioned_parquet(
            dataframe=df,
            bucket=bucket,
            key=key,
            partition_data_types={"int_col": "int"},
            s3_client=s3_client
        )

        fetched_max = fetch_parq.get_max_partition_value(
            bucket=bucket, key=key, partition="int_col")

        # Test max of column is max of the fetched partition
        assert df["int_col"].max() == fetched_max

# Test that the max function denies non-numeric partitions
# TODO: add category when it becomes supported


def test_gets_max_denies_text():
    with get_s3_client() as s3_client:
        key = "safekeyprefixname/safedatasetname"
        bucket = "safebucketname"
        part_types = {
            "string_col": "string",
            "bool_col": "bool"
        }
        col_types = dict(part_types)
        col_types["metrics"] = "int"
        df = setup_grouped_dataframe(count=10, columns=col_types)
        bucket, parquet_paths = setup_partitioned_parquet(
            dataframe=df,
            bucket=bucket,
            key=key,
            partition_data_types=part_types,
            s3_client=s3_client
        )

        with pytest.raises(ValueError):
            fetched_max = fetch_parq.get_max_partition_value(
                bucket=bucket, key=key, partition="string_col")

        with pytest.raises(ValueError):
            fetched_max = fetch_parq.get_max_partition_value(
                bucket=bucket, key=key, partition="bool_col")


''' Phase 3 tests:
Refer to functionality of fetch_parq that:
    - Pulls down the parquet files
    - Transforms the files into dataframes
    - Concatenates the dataframes and returns them
'''

# Test that handles full fetch when no files match


def test_fetch_when_none():
    with get_s3_client() as s3_client:
        input_key = "burger-shipment/buns"
        input_bucket = "loadingdock"
        partitions = ["exp-date"]

        part_types = {
            "count": "int",
            "price": "float",
            "exp-date": "str"
        }

        fetched_dtypes = pd.Series(["int64", "float64", "object"], index=[
            "count", "price", "exp-date"])

        input_df = pd.DataFrame({
            "count": [2, 4, 7, 9],
            "price": [2.43, 1.23, 5.76, 3.28],
            "exp-date": ["x", "z", "a", "zz"]
        })

        setup_partitioned_parquet(
            dataframe=input_df,
            bucket=input_bucket,
            key=input_key,
            partition_data_types={"exp-date": "string"},
            s3_client=s3_client)

        filters = [{
            "partition": "exp-date",
            "comparison": "==",
            "values": ["not-there"]
        }]

        fetched = fetch_parq.fetch(
            bucket=input_bucket,
            key=input_key,
            filters=filters,
            parallel=False
        )

        # Testing that DF is empty and has the expected columns+dtypes
        assert fetched.empty
        assert fetched.dtypes.equals(fetched_dtypes)


def test_get_partition_difference_string():
    bucket = 'safebucket'
    key = 'dataset'
    partition = 'hamburger'
    rando_values = [setup_random_string() for x in range(10)]
    s3_paths = [
        f"{key}/{partition}={x}/12345.parquet" for x in rando_values[:-1]]

    with patch("s3parq.fetch_parq.get_all_files_list") as get_all_files_list:
        with patch("s3parq.fetch_parq._get_partitions_and_types") as _get_partitions_and_types:
            get_all_files_list.return_value = s3_paths
            _get_partitions_and_types.return_value = {
                "hamburger": "string"}

            # partition values not in list values
            deltas = fetch_parq.get_diff_partition_values(
                bucket, key, partition, rando_values[:-2])
            assert deltas == [rando_values[-2]]
            # list values not in partition values
            deltas = fetch_parq.get_diff_partition_values(
                bucket, key, partition, rando_values, True)
            assert deltas == [rando_values[-1]]


def test_get_partition_difference_string_when_none():
    bucket = 'safebucket'
    key = 'dataset'
    partition = 'hamburger'
    rando_values = [setup_random_string() for x in range(10)]
    rando_values.sort()
    s3_paths = []

    with patch("s3parq.fetch_parq.get_all_files_list") as get_all_files_list:
        get_all_files_list.return_value = []

        # values when theres no bucket data
        deltas = fetch_parq.get_diff_partition_values(
            bucket, key, partition, rando_values[:-2])
        deltas.sort()
        assert deltas == []
        # values when theres no bucket data and reversed
        deltas = fetch_parq.get_diff_partition_values(
            bucket, key, partition, rando_values[:-2], True)
        deltas.sort()
        assert deltas == rando_values[:-2]


def test_get_partition_difference_int_when_none():
    bucket = 'safebucket'
    key = 'dataset'
    partition = 'hamburger'
    rando_values = [1, 2, 3, 4, 5]
    rando_values.sort()
    s3_paths = []

    with patch("s3parq.fetch_parq.get_all_files_list") as get_all_files_list:
        with patch("s3parq.fetch_parq._get_partitions_and_types") as _get_partitions_and_types:
            get_all_files_list.return_value = []
            _get_partitions_and_types.return_value = 777.09

            # values when there is no bucket data
            deltas = fetch_parq.get_diff_partition_values(
                bucket, key, partition, rando_values[:-2])
            deltas.sort()
            assert deltas == []
            # values when theres no bucket data and reversed
            deltas = fetch_parq.get_diff_partition_values(
                bucket, key, partition, rando_values[:-2], True)
            deltas.sort()
            assert deltas == rando_values[:-2]


def test_get_partition_difference_int_comparison_none():
    bucket = 'safebucket'
    key = 'dataset'
    partition = 'hamburger'
    rando_values = [1, 2, 3, 4, 5]
    rando_values.sort()
    s3_paths = [
        f"{key}/{partition}={x}/12345.parquet" for x in rando_values[:-1]]

    with patch("s3parq.fetch_parq.get_all_files_list") as get_all_files_list:
        with patch("s3parq.fetch_parq._get_partitions_and_types") as _get_partitions_and_types:
            get_all_files_list.return_value = s3_paths
            _get_partitions_and_types.return_value = {
                "hamburger": "integer"}

            # values when there is no sent comparisons
            deltas = fetch_parq.get_diff_partition_values(
                bucket, key, partition, [])
            deltas.sort()
            assert deltas == rando_values[:-1]
            # values when there is no sent comparisons and reversed
            deltas = fetch_parq.get_diff_partition_values(
                bucket, key, partition, [], True)
            deltas.sort()
            assert deltas == []


def test_get_partition_difference_datetime():
    bucket = 'safebucket'
    key = 'dataset'
    partition = 'burgertime'
    rando_values = [(datetime.datetime.now() - datetime.timedelta(
        seconds=random.randrange(100 * 24 * 60 * 60))).replace(microsecond=0) for x in range(5)]
    s3_paths = [
        f"{key}/{partition}={x.strftime('%Y-%m-%d %H:%M:%S')}/12345.parquet" for x in rando_values[:-1]]

    with patch("s3parq.fetch_parq.get_all_files_list") as get_all_files_list:
        with patch("s3parq.fetch_parq._get_partitions_and_types") as _get_partitions_and_types:
            get_all_files_list.return_value = s3_paths
            _get_partitions_and_types.return_value = {
                "burgertime": "datetime"}

            # partition values not in list values
            deltas = fetch_parq.get_diff_partition_values(
                bucket, key, partition, rando_values[:-2])
            assert deltas == [rando_values[-2]]

            # list values not in partition values
            deltas = fetch_parq.get_diff_partition_values(
                bucket, key, partition, rando_values, reverse=True)
            assert deltas == [rando_values[-1]]


def test_get_partition_values():
    bucket = 'safebucket'
    key = 'dataset'
    partition = 'burgertime'
    rando_values = [(datetime.datetime.now() - datetime.timedelta(
        seconds=random.randrange(100 * 24 * 60 * 60))).replace(microsecond=0) for x in range(5)]
    s3_paths = [
        f"{key}/{partition}={x.strftime('%Y-%m-%d %H:%M:%S')}/12345.parquet" for x in rando_values]

    with patch("s3parq.fetch_parq.get_all_files_list") as get_all_files_list:
        with patch("s3parq.fetch_parq._get_partitions_and_types") as _get_partitions_and_types:
            get_all_files_list.return_value = s3_paths
            _get_partitions_and_types.return_value = {
                "burgertime": "datetime"}

            all_values = fetch_parq.get_all_partition_values(
                bucket=bucket, key=key, partition=partition)

            assert set(all_values) == set(rando_values)

# Test that the data in the input but not comparison is fetched


def test_fetches_diff():
    with get_s3_client() as s3_client:
        input_key = "burger-shipment/buns"
        input_bucket = "loadingdock"
        comparison_key = "burger-inventory/buns"
        comparison_bucket = "backroom"
        partitions = ["exp-date"]

        part_types = {
            "count": "int",
            "price": "float",
            "exp-date": "string"
        }

        input_df = pd.DataFrame({
            "count": [2, 4, 7, 9, 9],
            "price": [2.43, 1.23, 5.76, 3.28, 4.44],
            "exp-date": ["x", "z", "a", "zz", "l"]
        })
        comparison_df = pd.DataFrame({
            "count": [2, 3, 4, 9],
            "price": [2.43, 4.35, 1.23, 3.28],
            "exp-date": ["x", "y", "z", "zz"]
        })

        setup_partitioned_parquet(
            dataframe=input_df,
            bucket=input_bucket,
            key=input_key,
            partition_data_types={"exp-date": "string"},
            s3_client=s3_client)

        setup_partitioned_parquet(
            dataframe=comparison_df,
            bucket=comparison_bucket,
            key=comparison_key,
            partition_data_types={"exp-date": "string"},
            s3_client=s3_client)

        test_df = pd.DataFrame({
            "count": [7, 9],
            "price": [5.76, 4.44],
            "exp-date": ["a", "l"]
        })

        fetched_diff = fetch_parq.fetch_diff(
            input_bucket=input_bucket,
            input_key=input_key,
            comparison_bucket=comparison_bucket,
            comparison_key=comparison_key,
            partition=partitions[0],
            parallel=False
        )

        assert fetched_diff.shape == test_df.shape
        sorted_dfs_equal_by_pandas_testing(fetched_diff, test_df)

# Test that the dataframes are fetched properly when one side is empty and the other isnt


def test_fetches_diff_none():
    with get_s3_client() as s3_client:
        input_key = "clay/beads"
        input_bucket = "kiln"
        comparison_key = "new-case"
        comparison_bucket = "storefront"
        partitions = ["price"]

        part_types = {
            "count": "int",
            "price": "float"
        }

        input_df = pd.DataFrame({
            "count": [2, 4, 7, 9],
            "price": [2.43, 1.23, 5.76, 3.28]
        })

        s3_client.create_bucket(Bucket=input_bucket)
        s3_client.create_bucket(Bucket=comparison_bucket)

        setup_partitioned_parquet(
            dataframe=input_df,
            bucket=input_bucket,
            key=input_key,
            partition_data_types={"price": "float"},
            s3_client=s3_client)

        fetched_diff = fetch_parq.fetch_diff(
            input_bucket=input_bucket,
            input_key=input_key,
            comparison_bucket=comparison_bucket,
            comparison_key=comparison_key,
            partition=partitions[0],
            parallel=False
        )

        fetched_diff.sort_values(by=['price'], inplace=True)
        input_df.sort_values(by=['price'], inplace=True)

        sorted_dfs_equal_by_pandas_testing(fetched_diff, input_df)

        fetched_diff_reverse = fetch_parq.fetch_diff(
            input_bucket=input_bucket,
            input_key=input_key,
            comparison_bucket=comparison_bucket,
            comparison_key=comparison_key,
            partition=partitions[0],
            reverse=True,
            parallel=False
        )

        assert fetched_diff_reverse.empty

        fetched_diff_reverse_both = fetch_parq.fetch_diff(
            input_bucket=comparison_bucket,
            input_key=comparison_key,
            comparison_bucket=input_bucket,
            comparison_key=input_key,
            partition=partitions[0],
            reverse=True,
            parallel=False
        )

        sorted_dfs_equal_by_pandas_testing(fetched_diff_reverse_both, input_df)

# Test that all files matching key gets listed out


def test_fetch_files_list():
    with get_s3_client() as s3_client:
        uploaded = setup_files_list(count=100, s3_client=s3_client)

        bucket = uploaded['bucket']
        key = uploaded['key']
        test_files = uploaded['files']

        fetched_files = fetch_parq.get_all_files_list(bucket, key)

        test_files_keyed = list(
            map(lambda x: key + "/" + x + ".parquet", test_files))

        test_files_keyed.sort()
        fetched_files.sort()

        assert (test_files_keyed == fetched_files)

# Test that all files matching key get listed out even with pagination


@pytest.mark.slow
def test_fetch_files_list_more_than_1k():
    with get_s3_client() as s3_client:
        uploaded = setup_files_list(count=1500, s3_client=s3_client)

        bucket = uploaded['bucket']
        key = uploaded['key']
        test_files = uploaded['files']

        fetched_files = fetch_parq.get_all_files_list(bucket, key)

        test_files_keyed = list(
            map(lambda x: key + "/" + x + ".parquet", test_files))

        test_files_keyed.sort()
        fetched_files.sort()

        assert (test_files_keyed == fetched_files)


# Test : fetching without metadata
# Test : fetching when metadata not allowed

def test_fetches_nons3parq():
    with get_s3_client() as s3_client:
        input_key = "burger-shipment/buns"
        input_bucket = "loadingdock"

        input_df = pd.DataFrame({
            "count": [2, 4, 7, 9, 9],
            "price": [2.43, 1.23, 5.76, 3.28, 4.44],
            "exp-date": ["x", "z", "a", "zz", "l"]
        })

        s3_key = "burger-shipment/buns"

        setup_nons3parq_parquet(
            dataframe=input_df,
            bucket=input_bucket,
            key=input_key,
            s3_client=s3_client
        )

        fetched_diff = fetch_parq.fetch(
            bucket=input_bucket,
            key=s3_key,
            parallel=False
        )

        assert fetched_diff.shape == input_df.shape
        sorted_dfs_equal_by_pandas_testing(fetched_diff, input_df)

# test when there's multiple parquet splits


@pytest.mark.slow
def test_fetches_nons3parq_large_parquet():
    input_key = "burger-shipment/buns"
    input_bucket = "loadingdock"

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

    input_df = pd.DataFrame(df.dataframe)

    s3_client = boto3.client('s3')

    s3_key = "burger-shipment/buns"

    setup_nons3parq_parquet(
        dataframe=input_df,
        bucket=input_bucket,
        key=input_key,
        s3_client=s3_client
    )

    fetched_diff = fetch_parq.fetch(
        bucket=input_bucket,
        key=s3_key,
        parallel=False
    )

    assert fetched_diff.shape == input_df.shape
    sorted_dfs_equal_by_pandas_testing(fetched_diff, input_df)


def test_not_fetches_nons3parq():
    with get_s3_client() as s3_client:
        input_key = "burger-shipment/buns"
        input_bucket = "loadingdock"

        input_df = pd.DataFrame({
            "count": [2, 4, 7, 9, 9],
            "price": [2.43, 1.23, 5.76, 3.28, 4.44],
            "exp-date": ["x", "z", "a", "zz", "l"]
        })

        s3_key = "burger-shipment/buns"

        setup_nons3parq_parquet(
            dataframe=input_df,
            bucket=input_bucket,
            key=input_key,
            s3_client=s3_client
        )

        with pytest.raises(MissingS3ParqMetadata):
            fetched_diff = fetch_parq.fetch(
                bucket=input_bucket,
                key=s3_key,
                parallel=False,
                accept_not_s3parq=False
            )
