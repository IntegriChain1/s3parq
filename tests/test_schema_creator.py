import pytest
import boto3
import s3parq.schema_creator as SC


# Given a correctly formatted string make sure output is correct SQL
def test_sql_output():
    verified_string = "my_string"
    assert SC.schema_generator(verified_string) == f"create schema if not exists {verified_string};"
