import pytest
from mock import patch
from s3parq.table_creator import create_table, table_name_validator
from s3parq.session_helper import SessionHelper

class MockScopeObj():

    def execute(self, schema_string: str):
        pass

def scope_execute_mock(mock_session_helper):
    pass

class Test():
        
    # Given a correctly formatted string make sure output is correct SQL
    def test_validator(self):
        table_name_good = "my_string"
        table_name_bad = "my string"
        table_name_validator(table_name_good)
        with pytest.raises(ValueError):
            table_name_validator(table_name_bad)

    # Test that the function is called with the table name
    @patch('s3parq.schema_creator.SessionHelper')
    @patch('tests.test_table_creator.scope_execute_mock')
    def test_create_table(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        table_name = "my_string"
        cols = ["testid integer not null", "testname varchar(100) not null", "testabrev char(2) not null"]  
        with mock_session_helper.db_session_scope() as mock_scope:
            create_table(table_name, cols, mock_session_helper)
            mock_scope.execute.assert_called_once_with(f'CREATE TABLE IF NOT EXISTS {table_name} ( {cols.values});')
    