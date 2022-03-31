import boto3
from collections import OrderedDict
import datetime
from mock import patch
import moto
import pandas as pd
import pytest
import contextlib

import s3parq.fetch_parq as fetch_parq
from s3parq.testing_helper import (
    sorted_dfs_equal_by_pandas_testing,
    setup_grouped_dataframe,
    setup_partitioned_parquet
)


@contextlib.contextmanager
def get_s3_client():
    with moto.mock_s3():
        yield boto3.client('s3')


""" This test file is meant for testing the "private" iterface of fetch_parq
for interal testing. For the "public" functionality testing, please refer
to test_fetch_parq.py
"""

''' Phase 1 tests:
Refer to functionality of fetch_parq that:
    - Takes in parameters and checks for validity
    - Fetches partitions and associated data
'''


def test_filter_validation():
    invalid_filters_list = [
        [{
            "comparison": "==",
            "values": ["fake-value"]
        }],
        [{
            "partition": "fake-part",
            "comparison": "&&",
            "value": "some-string"
        }]
    ]

    for inv_fil in invalid_filters_list:
        with pytest.raises(ValueError):
            fetch_parq._validate_filter_rules(inv_fil)

    valid_filters_list = [
        [{
            "partition": "fake-part",
            "comparison": "==",
            "values": ["some-string"]
        }],
        [{
            "partition": "fake-part",
            "comparison": ">",
            "values": [1]
        }]
    ]

    for val_fil in valid_filters_list:
        fetch_parq._validate_filter_rules(val_fil)


# Test that all valid partitions are correctly parsed


def test_get_partitions():
    parts = [
        "fake-key/fil-1=1/fil-2=2/fil-3=str/rngf1.parquet",
        "fake-key/fil-1=1/fil-2=2/fil-3=rng-str/rngf2.parquet",
        "fake-key/fil-1=1/fil-2=4/fil-3=str_rng/rngf3.parquet",
        "fake-key/fil-1=5/fil-2=2/fil-3=str/rngf4.parquet",
        "fake-key/fil-1=5/fil-2=4/fil-3=99/rngf5.parquet"
    ]
    parsed_parts = OrderedDict({
        "fil-1": {"1", "5"},
        "fil-2": {"2", "4"},
        "fil-3": {"str", "rng-str", "str_rng", "99"}
    })

    key = "fake-key"
    test_parsed_part = fetch_parq._parse_partitions_and_values(parts, key)

    assert parsed_parts == test_parsed_part

# Test that if no partitions are present it'll handle without imploding


def test_get_partitions_none():
    parts = [
        "key/rngf1.parc",
        "key/rngf2.parc",
        "key/rngf3.parc",
        "key/rngf4.parc",
        "key/rngf5.parc"
    ]
    parsed_parts = OrderedDict({})

    key = "key"
    test_parsed_part = fetch_parq._parse_partitions_and_values(parts, key)

    assert parsed_parts == test_parsed_part

# Test it setting the correct partition data types


def test_get_data_types_from_s3():
    with get_s3_client() as s3_client:
        bucket, parquet_paths = setup_partitioned_parquet(s3_client=s3_client)

        s3_client = boto3.client('s3')
        files = s3_client.list_objects_v2(Bucket=bucket)
        first_file_key = files["Contents"][0]["Key"]
        partition_metadata = fetch_parq._get_partitions_and_types(
            first_file_key, bucket)

        assert partition_metadata == {
            "string_col": "string",
            "int_col": "integer",
            "float_col": "float",
            "bool_col": "boolean",
            "datetime_col": "datetime"
        }

    # Test that it errors if data types are mismatched


def test_match_argument_data_types():
    part_types = {"fil-3": "string",
                  "fil-2": "int",
                  "fil-1": "float"
                  }

    parsed_parts = OrderedDict({
        "fil-1": set(["1.23", "5.45"]),
        "fil-2": set(["2", "4"]),
        "fil-3": set(["str", "rng-str", "str_rng", "99"])
    })

    test_typed_parts = OrderedDict({
        "fil-1": set([float(1.23), float(5.45)]),
        "fil-2": set([int(2), int(4)]),
        "fil-3": set(["str", "rng-str", "str_rng", "99"])
    })

    typed_parts = fetch_parq._get_partition_value_data_types(
        parsed_parts=parsed_parts,
        part_types=part_types
    )

    assert typed_parts == test_typed_parts

# Test that it handles correct without false positives


def test_mismatch_argument_data_types():
    part_types = {"fil-2": "int",
                  "fil-1": "int"
                  }

    parsed_parts = OrderedDict({
        "fil-1": set(["1.23", "5.45"]),
        "fil-2": set(["2", "4"])
    })

    with pytest.raises(ValueError):
        fetch_parq._get_partition_value_data_types(
            parsed_parts=parsed_parts,
            part_types=part_types
        )


''' Phase 2 tests:
Refer to functionality of fetch_parq that:
    - Compares partitions and filters
    - Creates path(s) that lead to the appropriate files
    - Gets the list of those files to pull down from
'''

# Test that it errors if filters have no matching partitiion


def test_no_part_for_filter():
    part_types1 = {"fil-2": "int",
                   "fil-1": "int"
                   }

    filters1 = [{
        "partition": "fake-partition",
        "comparison": "==",
        "values": ["fake-value"]
    }]

    with pytest.raises(ValueError):
        fetch_parq._validate_matching_filter_data_type(
            part_types1, filters1)

# Test that it filters partitions fully


def test_filter_all_parts():
    typed_parts = OrderedDict({
        "fil-1": set([float(1.23), float(3.9), float(5.45)]),
        "fil-2": set([int(2), int(4)]),
        "fil-3": set(["str", "rng-str", "str_rng", "99"])
    })

    filters = [{
        "partition": "fil-1",
        "comparison": ">",
        "values": [5]
    }, {
        "partition": "fil-2",
        "comparison": "==",
        "values": [2]
    }, {
        "partition": "fil-3",
        "comparison": "==",
        "values": ["str"]
    }]

    fil_paths = [
        'fake-key/fil-1=5.45/fil-2=2/fil-3=str/'
    ]

    key = "fake-key"
    filters = filters

    filter_paths = fetch_parq._get_filtered_key_list(
        typed_parts=typed_parts, filters=filters, key=key)

    filter_paths.sort()
    fil_paths.sort()

    assert filter_paths == fil_paths

# Test that it filters partitions when only some are filtered


def test_filter_some_parts():
    typed_parts = OrderedDict({
        "fil-1": set([float(1.23), float(3.9), float(5.45)]),
        "fil-2": set([int(2), int(4)]),
        "fil-3": set(["str", "rng-str", "str_rng", "99"])
    })

    filters = [{
        "partition": "fil-1",
        "comparison": ">",
        "values": [5]
    }, {
        "partition": "fil-2",
        "comparison": "==",
        "values": [2]
    }]

    fil_paths = [
        'fake-key/fil-1=5.45/fil-2=2/fil-3=99/',
        'fake-key/fil-1=5.45/fil-2=2/fil-3=str/',
        'fake-key/fil-1=5.45/fil-2=2/fil-3=rng-str/',
        'fake-key/fil-1=5.45/fil-2=2/fil-3=str_rng/'
    ]

    key = "fake-key"
    filters = filters

    filter_paths = fetch_parq._get_filtered_key_list(
        typed_parts=typed_parts, key=key, filters=filters)

    filter_paths.sort()
    fil_paths.sort()

    assert filter_paths == fil_paths

# Test that it handles filters ridding everything


def test_filter_to_none():
    typed_parts = OrderedDict({
        "fil-1": set([float(1.23), float(3.9), float(5.45)]),
        "fil-2": set([int(2), int(4)]),
        "fil-3": set(["str", "rng-str", "str_rng", "99"])
    })

    filters = [{
        "partition": "fil-1",
        "comparison": ">",
        "values": [90]
    }, {
        "partition": "fil-2",
        "comparison": "==",
        "values": [23]
    }]

    fil_paths = []

    key = "fake-key"
    filters = filters

    filter_paths = fetch_parq._get_filtered_key_list(
        typed_parts=typed_parts, key=key, filters=filters)

    filter_paths.sort()
    fil_paths.sort()

    assert filter_paths == fil_paths


''' Phase 3 tests:
Refer to functionality of fetch_parq that:
    - Pulls down the parquet files
    - Transforms the files into dataframes
    - Concatenates the dataframes and returns them
'''

# captures from a single parquet path dataset


def test_s3_parquet_to_dataframe():
    with get_s3_client() as s3_client:
        columns = {"string_col": "string",
                   "int_col": "integer",
                   "float_col": "float",
                   "bool_col": "boolean",
                   "datetime_col": "datetime"}

        bucket = "foobucket"
        key = "fookey"

        df = setup_grouped_dataframe(count=10, columns=columns)
        bucket, parquet_paths = setup_partitioned_parquet(
            dataframe=df,
            bucket=bucket,
            key=key,
            partition_data_types={},
            s3_client=s3_client
        )

        first_published_file = parquet_paths[0]
        response = fetch_parq._s3_parquet_to_dataframe(
            bucket=bucket, key=first_published_file, partition_metadata={})

        assert isinstance(response, pd.DataFrame)
        for col in columns.keys():
            assert (col in response.columns)

        assert response.shape == df.shape
        sorted_dfs_equal_by_pandas_testing(response, df)


def test_s3_partitioned_parquet_to_dataframe():
    with get_s3_client() as s3_client:
        partition_types = {"string_col": "string",
                           "int_col": "integer",
                           "float_col": "float",
                           "bool_col": "boolean",
                           "datetime_col": "datetime"}
        columns = dict(partition_types)
        columns["metrics"] = "int"

        bucket = "foobucket"
        key = "fookey"

        df = setup_grouped_dataframe(count=10, columns=columns)
        bucket, parquet_paths = setup_partitioned_parquet(
            dataframe=df,
            bucket=bucket,
            key=key,
            partition_data_types=partition_types,
            s3_client=s3_client
        )

        first_published_file = parquet_paths[0]
        response = fetch_parq._s3_parquet_to_dataframe(
            bucket=bucket, key=first_published_file, partition_metadata=partition_types)

        assert isinstance(response, pd.DataFrame)
        for col in columns.keys():
            assert (col in response.columns)

        full_response = pd.DataFrame()
        for path in parquet_paths:
            full_response = full_response.append(fetch_parq._s3_parquet_to_dataframe(
                bucket=bucket, key=path, partition_metadata=partition_types))

        assert full_response.shape == df.shape
        sorted_dfs_equal_by_pandas_testing(full_response, df)
