import pytest
import pandas as pd
import multiprocessing as mp
import moto
import boto3
from mock import patch
from dfmock import DFMock
from collections import OrderedDict

from .mock_helper import MockHelper
from s3_parq.fetch_parq import _get_all_files_list, _parse_partitions_and_values, _get_partitions_and_types, _get_partition_value_data_types
from s3_parq.fetch_parq import _validate_matching_filter_data_type, _get_filtered_key_list, _s3_parquet_to_dataframe
from s3_parq.fetch_parq import *
from s3_parq import publish_parq

from typing import Dict


@moto.mock_s3
class Test():

    # TODO: refactor to use MockHelper in places for more complete coverage
    def setup_dummy_params(self):
        dummy_init_params = {
            "bucket": "fake-bucket",
            "key": "fake-key",
            "filters": [{
                "partition": "fake-partition",
                "comparison": "==",
                "values": ["fake-value"]
            }]
        }

        return dummy_init_params

    ''' Phase 1 tests:
    Refer to functionality of fetch_parq that:
        - Takes in parameters and checks for validity
        - Fetches partitions and associated data
    '''

    # Test requires bucket, key, filters
    # def test_requires_params(self):
    #    good_fetch = S3FetchParq(**self.setup_dummy_params())

    #    with pytest.raises(TypeError):
    #        bad_fetch = S3FetchParq(bucket="just-a-bucket")

    # Test that if inapropriate filters are passed it'll be denied
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
            fetch(**inv_fil_params)

        inv_fil_params["filters"] = [{
            "partition": "fake-part",
            "comparison": "&&",
            "value": "some-string"
        }]

        with pytest.raises(ValueError):
            fetch(**inv_fil_params)

    # Test that all files matching key gets listed out
    def test_fetch_files_list(self):
        mh = MockHelper(count=100, s3=False, files=True)
        uploaded = mh.file_ops

        #fetcher = S3FetchParq(**self.setup_dummy_params())
        bucket = uploaded['bucket']
        key = uploaded['key']
        test_files = uploaded['files']

        fetched_files = _get_all_files_list(bucket, key)

        test_files_keyed = list(
            map(lambda x: key + x, test_files))

        assert (test_files_keyed.sort()) == (fetched_files.sort())

    # Test that all files matching key get listed out even with pagination
    # def test_fetch_files_list_more_than_1k(self):
    #     mh = MockHelper(count=1500, s3=False, files=True)
    #     uploaded = mh.file_ops

    #     fetcher = S3FetchParq(**self.setup_dummy_params())
    #     fetcher.s3_bucket = uploaded['bucket']
    #     fetcher.s3_key = uploaded['key']
    #     test_files = uploaded['files']

    #     fetched_files = fetcher._get_all_files_list()
    #     test_files_keyed = list(map(lambda x: fetcher.s3_key + x, test_files))

    #     assert (test_files_keyed.sort()) == (fetched_files.sort())

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

        dummy_params = self.setup_dummy_params()

        test_parsed_part = _parse_partitions_and_values(parts, dummy_params['key'])

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

        dummy_params = self.setup_dummy_params()
        #fetcher = S3FetchParq(**dummy_params)
        key = "key/"
        test_parsed_part = _parse_partitions_and_values(parts, key)

        assert parsed_parts == test_parsed_part

    # Test it setting the correct partition data types
    def test_get_data_types_from_s3(self):
        mh = MockHelper(count=10, s3=True, files=False)
        bucket = mh.s3_bucket
        dummy_params = self.setup_dummy_params()

        s3_client = boto3.client('s3')
        files = s3_client.list_objects_v2(Bucket=bucket)
        first_file_key = files["Contents"][0]["Key"]
        partition_metadata = _get_partitions_and_types(first_file_key, bucket)

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

        dummy_params = self.setup_dummy_params()
        typed_parts = _get_partition_value_data_types(
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
            _get_partition_value_data_types(
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
            _validate_matching_filter_data_type(part_types1, filters1)

        filters2 = [{
            "partition": "fil-1",
            "comparison": ">",
            "values": ["fake-value"]
        }]

        part_types2 = {"fil-1": "string"}

        with pytest.raises(ValueError):
            _validate_matching_filter_data_type(part_types2, filters2)

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

        filter_paths = _get_filtered_key_list(
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

        params = self.setup_dummy_params()
        key = "fake-key/"
        filters = filters

        filter_paths = _get_filtered_key_list(
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

        filter_paths = _get_filtered_key_list(
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
        publish = publish_parq.S3PublishParq(bucket=bucket,
                                             key=key,
                                             dataframe=df,
                                             partitions=partitions)

        published_files = publish.publish()
        #        partition_key_suffix = "/".join([f"{key}={value}" for key, value in test_partitions])
        return bucket, df, partitions, published_files

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
        response = _s3_parquet_to_dataframe(
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

    # Test pulling down a list of parquet files
    def test_fetch_parquet_list(self):
        pass
