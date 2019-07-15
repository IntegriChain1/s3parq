import pytest
from mock import patch
from s3parq.table_creator import create_table, table_name_validator,datatype_to_sql, datatype_mapper
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
        # print(cols)
        # raise NotImplementedError 
        with mock_session_helper.db_session_scope() as mock_scope:
            create_table(table_name, cols, mock_session_helper)
            mock_scope.execute.assert_called_once_with(f'CREATE EXTERNAL TABLE IF NOT EXISTS {schema_name}.{table_name} ( {cols});')

    #Test to check that the passed in datatype maps correctly
    def test_datatype_mapper(self):
        included_type = 'object'
        excluded_type = 'yarn'
        assert datatype_mapper(included_type) == 'VARCHAR'
        with pytest.raises(KeyError):
            datatype_mapper(excluded_type)
        

    #Test to check that passed in a dictionary outputs, returns proper sql query
    def test_dtype_to_sql(self):
        test_dict = {'grouped_col': 'object', 'text_col': 'object', 'int_col': 'int64', 'float_col': 'float64', 'bool_col': 'bool'}

        assert datatype_to_sql(test_dict) == 'grouped_col as VARCHAR, text_col as VARCHAR, int_col as integer, float_col as float, bool_col as BOOLEAN'