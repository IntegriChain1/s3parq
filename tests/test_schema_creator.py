import pytest
import boto3
from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from moto import mock_redshift
from unittest.mock import patch
from s3parq.schema_creator import schema_generator, create_schema


@mock_redshift
class Test():

    # Given a correctly formatted string make sure output is correct SQL
    def test_sql_output(self):
        schema_name = "my_string"
        assert schema_generator(schema_name) == f"create schema if not exists {schema_name};"

    # Test that the function is called with the schema name
    # def test_create_schema(self):
    #     schema_name = "my_string"
    #     with self.SH.db_session_scope() as scope:
    #         scope.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
