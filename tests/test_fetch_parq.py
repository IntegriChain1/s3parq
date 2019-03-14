import pytest
import pandas as pd
import boto3
import moto
from s3_parq.fetch_parq import S3FetchParq
from .mock_helper import MockHelper

@moto.mock_s3
class Test():

    ''' Phase 1 tests:
    Refer to functionality of fetch_parq that:
        - Takes in parameters and checks for validity
        - Fetches partitions and associated data
    '''

    dummy_init_params = {
        "bucket": "fake-bucket",
        "prefix": "fake-prefix",
        "filters": [
            {"partition": "fake-partition",
            "comparison": "==",
            "value": "fake-value"}
        ]
    }

    # Test requires bucket, prefix, filters
    def test_requires_params(self):
        pass

    # Test that if an inapropriate bucket is passed it'll be denied
    def test_invalid_bucket(self):
        pass

    # Test that it does take a valid bucket without a false negative
    def test_valid_bucket(self):
        pass

    # Test that if an inapropriate prefix is passed it'll be denied
    def test_invalid_prefix(self):
        pass

    # Test that it does take a valid prefix without a false negative
    def test_valid_prefix(self):
        pass

    # Test that if inapropriate filters are passed it'll be denied
    def test_invalid_filters(self):
        pass

    # Test that it does take valid filters without a false negative
    def test_valid_filters(self):
        pass

    # Test that all files matching prefix gets listed out
    def test_fetch(self):
        pass

    # Test that all files matching prefix get listed out even with pagination
    def test_fetch_more_than_1k(self):
        pass

    # Test that all valid partitions are correctly parsed
    def test_get_partitions(self):
        pass

    # Test that if no partitions are present it'll handle without imploding
    def test_get_partitions_none(self):
        pass

    # Test it setting the correct partition data types
    def test_get_data_types(self):
        pass

    # Test that it errors if data types are mismatched
    def test_mismatch_data_types(self):
        pass

    # Test that it handles correct without false positives
    def test_match_data_types(self):
        pass

    ''' Phase 2 tests:
    Refer to functionality of fetch_parq that:
        - Compares partitions and filters
        - Creates path(s) that lead to the appropriate files
        - Gets the list of those files to pull down from
    '''

    # Test that it errors if filters have no matching partitiion
    def test_no_part_for_filter(self):
        pass

    # Test that it filters partitions fully
    def test_filter_all_parts(self):
        pass
    
    # Test that it handles when only some partitions are filtered
    def test_filter_partial(self):
        pass

    # Test that it handles filters ridding everything
    def test_filter_to_none(self):
        pass

    # Test create paths from successful filters
    def test_find_paths(self):
        pass

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

    ## captures from a single parquet path dataset
    def test_s3_parquet_to_dataframe(self):
        mock = MockHelper(count=500, s3 = True)
        fetch = S3FetchParq(bucket=mock.s3_bucket, prefix=mock.dataset, filters={})
        
        dest = []
        fetch._s3_parquet_to_dataframe(bucket = mock.s3_bucket, prefix ='/'.join(mock.paths[0].split('/')[:-1]), destination = dest)
        
        assert len(dest) == 1 
        assert isinstance(dest[0],pd.DataFrame)    

    ## captures from multiple paths in dataset

    
    ## borks when different datasets


    ## borks when not found




    # Test pulling down a parquet file
    def test_fetch_parquet_file(self):
        pass

    # Test pulling down a list of parquet files
    def test_fetch_parquet_list(self):
        pass

    # Test turning a parquet file into a pandas dataframe
    def test_parquet_to_df(self):
        pass

    # Test concatenating pandas dataframes
    def test_concat_dfs(self):
        pass

    # Test concatenating pandas dataframes when only one exists
    def test_concat_dfs_one(self):
        pass
