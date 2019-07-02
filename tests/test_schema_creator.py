import pytest
import boto3
import s3parq.schema_creator as SC
from moto import mock_redshift


@mock_redshift
class Test():
    # Given a correctly formatted string make sure output is correct SQL
    def test_sql_output():
        verified_string = "my_string"
        assert SC.schema_generator(verified_string) == f"create schema if not exists {verified_string};"

    # Test that the function is called with the schema name
    def test
