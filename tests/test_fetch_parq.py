import pytest
import pandas as pd
import multiprocessing as mp
import random
import datetime
from string import ascii_lowercase
import moto
import boto3
from mock import patch
from dfmock import DFMock
from collections import OrderedDict

from .mock_helper import MockHelper

import s3parq.fetch_parq as fetch_parq
from s3parq.publish_parq import publish

from typing import Dict


@moto.mock_s3
class Test():
    
    
    def rand_string(self):
        return ''.join([random.choice(ascii_lowercase) for x in range(0, 10)])

    # TODO: refactor to create local functions and cut Mockhelper
    def setup_s3(self):
        bucket = self.rand_string()
        key = self.rand_string()

        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket= bucket)
        
        return bucket, key

    def mock_publish(self, bucket, key, partition_types: Dict[str, str]):
        mocker = MockHelper(count=100, s3=True)
        df = mocker.dataframe
        partitions = partition_types.keys()
        dfmock = DFMock()
        dfmock.count = 10

        # add partition columns
        columns = dict({key: {"option_count": 3, "option_type": value} for key, value in partition_types.items()})

        # add one actual data column, called metrics
        columns["metrics"] = "int"

        dfmock.columns = columns
        dfmock.generate_dataframe()

        # generate dataframe we will write
        df = dfmock.dataframe
        key = 'safekeyprefixname/safedatasetname'
        bucket = mocker.s3_bucket

        defaults = {
            'bucket': bucket,
            'key': key,
            'dataframe': df,
            'partitions': partitions
        }
        published_files = publish(bucket=bucket,
                                    key=key,
                                    dataframe=df,
                                    partitions=partitions)

        return bucket, df, partitions, published_files

    ''' Phase 1 tests:
    Refer to functionality of fetch_parq that:
        - Takes in parameters and checks for validity
        - Fetches partitions and associated data
    '''

    def test_invalid_filters(self):
        inv_fil_params = {
            "bucket": "fake-bucket",
            "key": "fake-key",
            "filters": [{
                "comparison": "==",
                "values": ["fake-value"]
            }]
        }
        with pytest.raises(ValueError):
            fetch_parq.fetch(**inv_fil_params)

        inv_fil_params["filters"] = [{
            "partition": "fake-part",
            "comparison": "&&",
            "value": "some-string"
        }]

        with pytest.raises(ValueError):
            fetch_parq.fetch(**inv_fil_params)

    # Test that all files matching key gets listed out
    def test_fetch_files_list(self):
        mh = MockHelper(count=100, s3=False, files=True)
        uploaded = mh.file_ops

        bucket = uploaded['bucket']
        key = uploaded['key']
        test_files = uploaded['files']

        fetched_files = fetch_parq._get_all_files_list(bucket, key)

        test_files_keyed = list(
            map(lambda x: key + x, test_files))

        assert (test_files_keyed.sort()) == (fetched_files.sort())

    # Test that all files matching key get listed out even with pagination
    def test_fetch_files_list_more_than_1k(self):
        mh = MockHelper(count=1500, s3=False, files=True)
        uploaded = mh.file_ops

        bucket = uploaded['bucket']
        key = uploaded['key']
        test_files = uploaded['files']

        fetched_files = fetch_parq._get_all_files_list(bucket, key)

        test_files_keyed = list(
            map(lambda x: key + x, test_files))

        assert (test_files_keyed.sort()) == (fetched_files.sort())

    # Test that all valid partitions are correctly parsed
    def test_get_partitions(self):
        parts = [
            "fake-key/fil-1=1/fil-2=2/fil-3=str/rngf1.parquet",
            "fake-key/fil-1=1/fil-2=2/fil-3=rng-str/rngf2.parquet",
            "fake-key/fil-1=1/fil-2=4/fil-3=str_rng/rngf3.parquet",
            "fake-key/fil-1=5/fil-2=2/fil-3=str/rngf4.parquet",
            "fake-key/fil-1=5/fil-2=4/fil-3=99/rngf5.parquet"
        ]
        parsed_parts = OrderedDict({
            "fil-1": {
                "1",
                "5"
            },
            "fil-2": {
                "2",
                "4"
            },
            "fil-3": {
                "str",
                "rng-str",
                "str_rng",
                "99"
            }
        })

        key = "key/"
        test_parsed_part = fetch_parq._parse_partitions_and_values(parts, key)

        assert parsed_parts == test_parsed_part

    # Test that if no partitions are present it'll handle without imploding
    def test_get_partitions_none(self):
        parts = [
            "key/rngf1.parc",
            "key/rngf2.parc",
            "key/rngf3.parc",
            "key/rngf4.parc",
            "key/rngf5.parc"
        ]
        parsed_parts = OrderedDict({})

        key = "key/"
        test_parsed_part = fetch_parq._parse_partitions_and_values(parts, key)

        assert parsed_parts == test_parsed_part

    # Test it setting the correct partition data types
    def test_get_data_types_from_s3(self):
        mh = MockHelper(count=10, s3=True, files=False)
        bucket = mh.s3_bucket

        s3_client = boto3.client('s3')
        files = s3_client.list_objects_v2(Bucket=bucket)
        first_file_key = files["Contents"][0]["Key"]
        partition_metadata = fetch_parq._get_partitions_and_types(first_file_key, bucket)

        assert partition_metadata == {"string_col": "string",
                                    "int_col": "integer",
                                    "float_col": "float",
                                    "bool_col": "boolean",
                                    "datetime_col": "datetime"
                                    }

    # Test that it errors if data types are mismatched
    def test_mismatch_argument_data_types(self):
        part_types = {"fil-3": "string",
                      "fil-2": "int",
                      "fil-1": "float"
                      }

        parsed_parts = OrderedDict({
            "fil-1": set([
                "1.23",
                "5.45"
            ]),
            "fil-2": set([
                "2",
                "4"
            ]),
            "fil-3": set([
                "str",
                "rng-str",
                "str_rng",
                "99"
            ])
        })

        test_typed_parts = OrderedDict({
            "fil-1": set([
                float(1.23),
                float(5.45)
            ]),
            "fil-2": set([
                int(2),
                int(4)
            ]),
            "fil-3": set([
                "str",
                "rng-str",
                "str_rng",
                "99"
            ])
        })

        typed_parts = fetch_parq._get_partition_value_data_types(
            parsed_parts=parsed_parts,
            part_types=part_types
        )

        assert typed_parts == test_typed_parts

    # Test that it handles correct without false positives
    def test_match_argument_data_types(self):
        part_types = {"fil-2": "int",
                      "fil-1": "int"
                      }

        parsed_parts = OrderedDict({
            "fil-1": set([
                "1.23",
                "5.45"
            ]),
            "fil-2": set([
                "2",
                "4"
            ])
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

    # Test that functionality of max pulls correctly
    def test_gets_max(self):
        key = "safekeyprefixname/safedatasetname"
        bucket = "safebucketname"
        part_types = {
            "int_col": "int",
            "float_col": "float"
        }
        bucket, df, partitions, published_files = self.mock_publish(
                                                        bucket=bucket,
                                                        key=key,
                                                        partition_types=part_types
                                                    )
        
        fetched_max = fetch_parq.get_max_partition_value(bucket=bucket, key=key, partition="int_col")

        # Test max of column is max of the fetched partition
        assert df["int_col"].max() == fetched_max

    # Test that the max function denies non-numeric partitions
    # TODO: add category when it becomes supported
    def test_gets_max_denies_text(self):
        key = "safekeyprefixname/safedatasetname"
        bucket = "safebucketname"
        part_types = {
            "string_col": "string",
            "bool_col": "bool"
        }
        bucket, df, partitions, published_files = self.mock_publish(
                                                        bucket=bucket,
                                                        key=key,
                                                        partition_types=part_types
                                                    )
        
        with pytest.raises(ValueError):
            fetched_max = fetch_parq.get_max_partition_value(bucket=bucket, key=key, partition="string_col")

        with pytest.raises(ValueError):
            fetched_max = fetch_parq.get_max_partition_value(bucket=bucket, key=key, partition="bool_col")

    # Test that it errors if filters have no matching partitiion
    def test_no_part_for_filter(self):
        part_types1 = {"fil-2": "int",
                      "fil-1": "int"
                      }

        filters1 = [{
            "partition": "fake-partition",
            "comparison": "==",
            "values": ["fake-value"]
        }]

        with pytest.raises(ValueError):
            fetch_parq._validate_matching_filter_data_type(part_types1, filters1)

        filters2 = [{
            "partition": "fil-1",
            "comparison": ">",
            "values": ["fake-value"]
        }]

        part_types2 = {"fil-1": "string"}

        with pytest.raises(ValueError):
            fetch_parq._validate_matching_filter_data_type(part_types2, filters2)

    # Test that it filters partitions fully
    def test_filter_all_parts(self):
        typed_parts = OrderedDict({
            "fil-1": set([
                float(1.23),
                float(3.9),
                float(5.45)
            ]),
            "fil-2": set([
                int(2),
                int(4)
            ]),
            "fil-3": set([
                "str",
                "rng-str",
                "str_rng",
                "99"
            ])
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

        key = "fake-key/"
        filters = filters

        filter_paths = fetch_parq._get_filtered_key_list(
            typed_parts=typed_parts, filters=filters, key=key)

        assert list.sort(filter_paths) == list.sort(fil_paths)

    # Test that it filters partitions when only some are filtered
    def test_filter_some_parts(self):
        typed_parts = OrderedDict({
            "fil-1": set([
                float(1.23),
                float(3.9),
                float(5.45)
            ]),
            "fil-2": set([
                int(2),
                int(4)
            ]),
            "fil-3": set([
                "str",
                "rng-str",
                "str_rng",
                "99"
            ])
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

        key = "fake-key/"
        filters = filters

        filter_paths = fetch_parq._get_filtered_key_list(
            typed_parts=typed_parts, key=key, filters=filters)

        assert list.sort(filter_paths) == list.sort(fil_paths)

    # Test that it handles filters ridding everything
    def test_filter_to_none(self):
        typed_parts = OrderedDict({
            "fil-1": set([
                float(1.23),
                float(3.9),
                float(5.45)
            ]),
            "fil-2": set([
                int(2),
                int(4)
            ]),
            "fil-3": set([
                "str",
                "rng-str"
            ])
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

        assert list.sort(filter_paths) == list.sort(fil_paths)

    # Test getting the file list for all paths
    def test_get_all_file_lists(self):
        pass

    # Test getting the file lists when there are no files
    def test_get_all_file_lists_no_files(self):
        pass

    ''' Phase 3 tests:
    Refer to functionality of fetch_parq that:
        - Pulls down the parquet files
        - Transforms the files into dataframes
        - Concatenates the dataframes and returns them
    '''

    # captures from a single parquet path dataset
    def test_s3_parquet_to_dataframe(self):
        partition_types = {"string_col": "string",
                           "int_col": "integer",
                           "float_col": "float",
                           "bool_col": "boolean",
                           "datetime_col": "datetime"}

        key = f"basekey"
        print(f"Key is: {key}")

        bucket = "foobucket"
        key = "fookey/"
        partitions = partition_types.keys()
        bucket, df, partitions, published_files = self.mock_publish(bucket, key, partition_types)

        # fetch._partition_metadata = mock.partition_metadata
        first_published_file = published_files[0]
        response = fetch_parq._s3_parquet_to_dataframe(
            bucket=bucket, key=first_published_file, partition_metadata=partition_types)

        assert isinstance(response, pd.DataFrame)
        for partition in partition_types.keys():
            assert (partition in response.columns)

        # assert set(fetch._partition_metadata.keys()) <= set(response.columns)

        # captures from multiple paths in dataset

        # paths = set(['/'.join(mock.paths[x].split('/')[:-1])
        #             for x in range(len(mock.paths))])
        # big_df = fetch._get_filtered_data(bucket=mock.s3_bucket, paths=paths)

        # assert isinstance(big_df, pd.DataFrame)
        # assert set(mock.dataframe.columns) == set(big_df.columns)
        # assert mock.dataframe.shape == big_df.shape

    def test_get_partition_difference_string(self):
        bucket = 'safebucket'
        key = 'dataset'
        partition='hamburger'
        rando_values = [self.rand_string() for x in range(5)]
        s3_paths = [f"{key}/{partition}={x}/12345.parquet" for x in rando_values]

        with patch("s3parq.fetch_parq._get_all_files_list") as _get_all_files_list:
            with patch("s3parq.fetch_parq._get_partitions_and_types") as _get_partitions_and_types:
                _get_all_files_list.return_value = s3_paths
                _get_partitions_and_types.return_value = {"hamburger":"string"}

                deltas = fetch_parq.get_diff_partition_values(bucket,key,partition,rando_values[:-1])
                assert deltas == [rando_values[-1]]

    def test_get_partition_difference_datetime(self):
        bucket = 'safebucket'
        key = 'dataset'
        partition='burgertime'
        rando_values = [(datetime.datetime.now() - datetime.timedelta(seconds = random.randrange(100 * 24 * 60 * 60))).replace(microsecond=0) for x in range(5)]
        s3_paths = [f"{key}/{partition}={x.strftime('%Y-%m-%d %H:%M:%S')}/12345.parquet" for x in rando_values]

        with patch("s3parq.fetch_parq._get_all_files_list") as _get_all_files_list:
            with patch("s3parq.fetch_parq._get_partitions_and_types") as _get_partitions_and_types:
                _get_all_files_list.return_value = s3_paths
                _get_partitions_and_types.return_value = {"burgertime":"datetime"}

                deltas = fetch_parq.get_diff_partition_values(bucket,key,partition,rando_values[:-1])
                assert deltas == [rando_values[-1]]
