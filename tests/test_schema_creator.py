import pytest
import boto3
from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from moto import mock_redshift, mock_iam
from unittest import mock
from mock import patch
from s3parq.schema_creator import schema_name_validator, create_schema

class MockScopeObj():

    def execute(self, schema_string: str):
        pass

def scope_execute_mock(mock_session_helper):
    pass
    
class Test():

    # Given a correctly formatted string make sure output is correct SQL
    def test_validator(self):
        schema_name_good = "my_string"
        schema_name_bad = "my string"
        schema_name_validator(schema_name_good)
        with pytest.raises(ValueError):
            schema_name_validator(schema_name_bad)

    # Test that the function is called with the schema name
    @patch('s3parq.schema_creator.SessionHelper')
    @patch('tests.test_schema_creator.scope_execute_mock')
    def test_create_schema(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        schema_name = "my_string"     
        with mock_session_helper.db_session_scope() as mock_scope:
            create_schema(schema_name, mock_session_helper)
            mock_scope.execute.assert_called_once_with(f'CREATE SCHEMA IF NOT EXISTS {schema_name}')
