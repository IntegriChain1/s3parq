import pytest
import pandas as pd
import multiprocessing as mp
import moto
import boto3
from mock import patch
from dfmock import DFMock
from collections import OrderedDict

from .mock_helper import MockHelper
from s3_parq.fetch_parq import S3FetchParq


@moto.mock_s3
class Test():

    # TODO: refactor to use MockHelper in places for more complete coverage
    def setup_dummy_params(self):
        dummy_init_params = {
            "bucket": "fake-bucket",
            "prefix": "fake-prefix",
            "dataset": "fake-dataset",
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

    # Test requires bucket, prefix, filters
    def test_requires_params(self):
        good_fetch = S3FetchParq(**self.setup_dummy_params())

        with pytest.raises(TypeError):
            bad_fetch = S3FetchParq(bucket="just-a-bucket")

    # Test that dataset is just popped onto the prefix
    def test_dataset_addition(self):
        good_fetch = S3FetchParq(**self.setup_dummy_params())

        assert good_fetch._key_path() == "fake-prefix/fake-dataset"

        good_fetch.dataset = "new-fake-dataset"

        assert good_fetch._key_path() == "fake-prefix/new-fake-dataset"

    # Test that if inapropriate filters are passed it'll be denied
    def test_invalid_filters(self):
        inv_fil_params = {
            "bucket": "fake-bucket",
            "prefix": "fake-prefix",
            "dataset": "dake-dataset",
            "filters": [{
                "comparison": "==",
                "values": ["fake-value"]
            }]
        }

        with pytest.raises(ValueError):
            bad_fetch = S3FetchParq(**inv_fil_params)

        inv_fil_params["filters"] = [{
            "partition": "fake-part",
            "comparison": "&&",
            "value": "some-string"
        }]

        with pytest.raises(ValueError):
            bad_fetch = S3FetchParq(**inv_fil_params)

    # Test that all files matching prefix gets listed out

    def test_fetch_files_list(self):
        mh = MockHelper(count=100, s3=False, files=True)
        uploaded = mh.file_ops

        fetcher = S3FetchParq(**self.setup_dummy_params())
        fetcher.bucket = uploaded['bucket']
        fetcher.dataset = uploaded['dataset']
        fetcher.prefix = uploaded['prefix']
        test_files = uploaded['files']

        fetched_files = fetcher._get_all_files_list()
       
        test_files_prefixed = list(
            map(lambda x: fetcher._key_path() + x, test_files))

        assert (test_files_prefixed.sort()) == (fetched_files.sort())

    # Test that all files matching prefix get listed out even with pagination
    # def test_fetch_files_list_more_than_1k(self):
    #     mh = MockHelper(count=1500, s3=False, files=True)
    #     uploaded = mh.file_ops

    #     fetcher = S3FetchParq(**self.setup_dummy_params())
    #     fetcher.s3_bucket = uploaded['bucket']
    #     fetcher.s3_prefix = uploaded['prefix']
    #     test_files = uploaded['files']

    #     fetched_files = fetcher._get_all_files_list()
    #     test_files_prefixed = list(map(lambda x: fetcher.s3_prefix + x, test_files))

    #     assert (test_files_prefixed.sort()) == (fetched_files.sort())

    # Test that all valid partitions are correctly parsed
    def test_get_partitions(self):
        parts = [
            "fake-prefix/fake-dataset/fil-1=1/fil-2=2/fil-3=str/rngf1.parquet",
            "fake-prefix/fake-dataset/fil-1=1/fil-2=2/fil-3=rng-str/rngf2.parquet",
            "fake-prefix/fake-dataset/fil-1=1/fil-2=4/fil-3=str_rng/rngf3.parquet",
            "fake-prefix/fake-dataset/fil-1=5/fil-2=2/fil-3=str/rngf4.parquet",
            "fake-prefix/fake-dataset/fil-1=5/fil-2=4/fil-3=99/rngf5.parquet"
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
        fetcher = S3FetchParq(**dummy_params)

        test_parsed_part = fetcher._parse_partitions_and_values(parts)

        assert parsed_parts == test_parsed_part

    # Test that if no partitions are present it'll handle without imploding
    def test_get_partitions_none(self):
        parts = [
            "prefix/rngf1.parc",
            "prefix/rngf2.parc",
            "prefix/rngf3.parc",
            "prefix/rngf4.parc",
            "prefix/rngf5.parc"
        ]
        parsed_parts = OrderedDict({})

        dummy_params = self.setup_dummy_params()
        fetcher = S3FetchParq(**dummy_params)
        fetcher.s3_prefix = "prefix/"

        test_parsed_part = fetcher._parse_partitions_and_values(parts)

        assert parsed_parts == test_parsed_part

    # Test it setting the correct partition data types
    def test_get_data_types_from_s3(self):
        mh = MockHelper(count=10, s3=True, files=False)
        bucket = mh.s3_bucket
        dummy_params = self.setup_dummy_params()
        fetcher = S3FetchParq(**dummy_params)
        fetcher.bucket = bucket

        s3_client = boto3.client('s3')
        files = s3_client.list_objects_v2(Bucket=bucket)
        first_file_key = files["Contents"][0]["Key"]
        parsed_part_args = fetcher._get_partitions_and_types(first_file_key)

        assert parsed_part_args == {"string_col": "string",
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
        fetcher = S3FetchParq(**dummy_params)

        typed_parts = fetcher._set_partition_value_data_types(
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

        dummy_params = self.setup_dummy_params()
        fetcher = S3FetchParq(**dummy_params)

        with pytest.raises(ValueError):
            typed_parts = fetcher._set_partition_value_data_types(
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
        part_types = {"fil-2": "int",
                      "fil-1": "int"
                      }

        inv_fil_params = self.setup_dummy_params()
        inv_fil_params["filters"] = [{
            "partition": "fake-partition",
            "comparison": "==",
            "values": ["fake-value"]
        }]

        with pytest.raises(ValueError):
            bad_fetch = S3FetchParq(**inv_fil_params)
            bad_fetch._validate_matching_filter_data_type(part_types)

        inv_fil_params["filters"] = [{
            "partition": "fil-1",
            "comparison": ">",
            "values": ["fake-value"]
        }]

        part_types = {"fil-1": "string"}

        with pytest.raises(ValueError):
            bad_fetch = S3FetchParq(**inv_fil_params)
            bad_fetch._validate_matching_filter_data_type(part_types)

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
            'fake-prefix/fil-1=5.45/fil-2=2/fil-3=str/'
        ]

        params = self.setup_dummy_params()
        params["prefix"] = "fake-prefix/"
        params["filters"] = filters

        fetcher = S3FetchParq(**params)
        filter_paths = fetcher._set_filtered_prefix_list(
            typed_parts=typed_parts)

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
            'fake-prefix/fil-1=5.45/fil-2=2/fil-3=99/',
            'fake-prefix/fil-1=5.45/fil-2=2/fil-3=str/',
            'fake-prefix/fil-1=5.45/fil-2=2/fil-3=rng-str/',
            'fake-prefix/fil-1=5.45/fil-2=2/fil-3=str_rng/'
        ]

        params = self.setup_dummy_params()
        params["prefix"] = "fake-prefix/"
        params["filters"] = filters

        fetcher = S3FetchParq(**params)
        filter_paths = fetcher._set_filtered_prefix_list(
            typed_parts=typed_parts)

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

        params = self.setup_dummy_params()
        params["prefix"] = "fake-prefix"
        params["filters"] = filters

        fetcher = S3FetchParq(**params)
        filter_paths = fetcher._set_filtered_prefix_list(
            typed_parts=typed_parts)

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
        mock = MockHelper(count=500, s3=True)
        fetch = S3FetchParq(bucket=mock.s3_bucket,
                            prefix='', dataset='', filters={})
        fetch._partition_metadata = mock.partition_metadata
        path = '/'.join(mock.paths[0].split('/')[:-1])
        response = fetch._s3_parquet_to_dataframe(
            bucket=mock.s3_bucket, path=path)

        assert isinstance(response, pd.DataFrame)
        assert set(fetch._partition_metadata.keys()) <= set(response.columns)

    # captures from multiple paths in dataset
        
        paths = set(['/'.join(mock.paths[x].split('/')[:-1])
                     for x in range(len(mock.paths))])
        big_df = fetch._get_filtered_data(bucket=mock.s3_bucket, paths=paths)

        assert isinstance(big_df, pd.DataFrame)
        assert set(mock.dataframe.columns) == set(big_df.columns)
        assert mock.dataframe.shape == big_df.shape
        
    # Test pulling down a list of parquet files
    def test_fetch_parquet_list(self):
        pass
