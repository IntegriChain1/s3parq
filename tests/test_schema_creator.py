import pytest
import boto3
from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from moto import mock_redshift


@mock_redshift
class Test():

    SH = SessionHelper(
        'us-east-1',
        'core-sandbox-cluster-1',
        'core-sandbox-cluster-1.c3swieqn0nz0.us-east-1.redshift.amazonaws.com',
        '5439',
        'ichain_core'
    )

    SH.configure_session_helper()

    # Given a correctly formatted string make sure output is correct SQL
    def test_sql_output():
        verified_string = "my_string"
        assert SC.schema_generator(verified_string) == f"create schema if not exists {verified_string};"

    # Test that the function is called with the schema name
    def test_create_schema(schema_name: str):
        with self.SH.db_session_scope() as scope:
            scope.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')

