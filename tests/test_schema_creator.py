import pytest
import boto3
from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from moto import mock_redshift, mock_iam
from unittest import mock
from mock import patch
from s3parq.schema_creator import schema_generator, create_schema

class MockScopeObj():

    def execute(self, schema_string: str):
        print(schema_string)

def scope_execute_mock(mock_session_helper):
        return MockScopeObj()  

@mock_redshift
class Test():

    # Given a correctly formatted string make sure output is correct SQL
    def test_sql_output(self):
        schema_name = "my_string"
        assert schema_generator(schema_name) == f"create schema if not exists {schema_name};"

    # Test that the function is called with the schema name
    @patch('s3parq.schema_creator.SessionHelper')
    @patch('tests.test_schema_creator.scope_execute_mock')
    def test_create_schema(self, mock_session_helper, mock_execute):

        # mock_session_helper = msh(
        #     'region',
        #     'cluster',
        #     'host',
        #     'port',
        #     'dbname'
        #     )
        # mock_session_helper.configure_session_helper()
        print(mock_session_helper.db_session_scope.return_value)
        # mock_execute = scope_execute_mock
        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock


        schema_name = "my_string"
        # with mock_session_helper.db_session_scope() as scope:
        # with mock.patch('mock_session_helper.db_session_scope') as mock_scope:
       

        with mock_session_helper.db_session_scope as mock_scope:
            create_schema(schema_name, mock_session_helper)
            mock_scope.execute.assert_called_once_with(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
