import pytest
import boto3
from moto import mock_s3
from tests.mock_helper import MockHelper

""" HOW TO USE MOCK HELPER 
    mock helper builds a dataframe, builds partitioned parquet, and pushes to S3 for you. 
    - use moto to mock it for unit tests
    - don't use moto when you want to push to a sandbox (for integration-ish tests)
      NOTE: this will use your real IAM / config AWS creds, so make sure all your stuff points to sandbox and not production! 
    
    EXAMPLE:

    # mocked
    @mock_s3
    def test_a_thing():
        badass_mock = MockHelper()
        
        ## here's a dataframe!
        df = badass_mock.dataframe
    
        ## here's the s3 bucket with the partitioned parquet files in it.
        ## Since we used moto, this is a fake S3 bucket that actually lives in memory!    
        bucket = badass_mock.s3_bucket

    you can now "download" the files from s3, check to make sure they convert back to a matching dataframe,
    and other cool stuff. 

    Since this is dynamically generated data, no 2 runs will have the same dataframes / parquet files / bucket names. 
""" 

