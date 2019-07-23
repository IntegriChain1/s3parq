import pytest
from mock import patch
import s3parq.publish_redshift as rs
from s3parq.session_helper import SessionHelper

class MockScopeObj():

    def execute(self, schema_string: str):
        pass

def scope_execute_mock(mock_session_helper):
    pass

class Test():

    # Test that the function is called with the table name
    @patch('s3parq.publish_redshift.SessionHelper')
    @patch('tests.test_table_creator.scope_execute_mock')
    def test_create_table(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        table_name = "my_string"
        schema_name = "my_schema"
        path = "s3://lol"
        columns = {'grouped_col': 'object', 'text_col': 'object', 'int_col': 'int64', 'float_col': 'float64'}
        partitions = {'fish': 'object'}
        
        expected_sql = f'CREATE EXTERNAL TABLE {schema_name}.{table_name} {columns} \
            PARTITIONED BY {partitions} STORED AS PARQUET \
            LOCATION "{path}";'
        with mock_session_helper.db_session_scope() as mock_scope:
            rs.create_table(table_name, schema_name, columns, partitions, path, mock_session_helper)
            assert mock_scope.execute.called_once_with(expected_sql)
    
    # Test that the function is called with the table name without partitions
    @patch('s3parq.publish_redshift.SessionHelper')
    @patch('tests.test_table_creator.scope_execute_mock')
    def test_create_table_without_partitions(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        table_name = "my_string"
        schema_name = "my_schema"
        path = "s3://lol"
        columns = {'grouped_col': 'object', 'text_col': 'object', 'int_col': 'int64', 'float_col': 'float64'}
        partitions = {}
        
        expected_sql = f'CREATE EXTERNAL TABLE {schema_name}.{table_name} {columns} \
            STORED AS PARQUET \
            LOCATION "{path}";'
        with mock_session_helper.db_session_scope() as mock_scope:
            rs.create_table(table_name, schema_name, columns, partitions, path, mock_session_helper)
            assert mock_scope.execute.called_once_with(expected_sql)
    

    #Test to check that the passed in datatype maps correctly
    def test_datatype_mapper(self):
        columns = {'grouped_col': 'object', 'text_col': 'object', 'int_col': 'int64', 'float_col': 'float64'}
        expected = {'grouped_col': 'VARCHAR', 'text_col': 'VARCHAR', 'int_col': 'BIGINT', 'float_col': 'FLOAT'}
        sql = ""
        for key, val in expected.items():
            sql += f'{key} {val}, '
        sql = "(" + sql[:-2] + ")"
        actual = rs._datatype_mapper(columns)
        assert actual == sql
        