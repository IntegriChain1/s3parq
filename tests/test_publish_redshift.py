from mock import patch
import pytest

from s3parq import publish_redshift


class MockScopeObj():

    def execute(self, schema_string: str):
        pass


def scope_execute_mock(mock_session_helper):
    pass


class Test():

    # Given a correctly formatted string make sure output is correct SQL
    def test_validator(self):
        schema_name_good = "my_string"
        bad_schema_names = ["my string", "#t3rr1bl13_n4m3", "", "select"]
        database_name_good = 'my_database'
        bad_database_names = ['my database',
                              "#t3rr1bl13_n4m3", "", ".", "select"]
        publish_redshift._redshift_name_validator(
            schema_name_good, database_name_good)
        for bad_schema in bad_schema_names:
            with pytest.raises(ValueError):
                publish_redshift._redshift_name_validator(
                    bad_schema, database_name_good)
        for bad_db in bad_database_names:
            with pytest.raises(ValueError):
                publish_redshift._redshift_name_validator(
                    schema_name_good, bad_db)

    # Test that the function is called with the schema name
    @patch('s3parq.publish_redshift.SessionHelper')
    @patch('tests.test_publish_redshift.scope_execute_mock')
    def test_create_schema(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        schema_name = "my_string"
        db_name = "my_database"
        iam_role = "my_iam_role"
        with mock_session_helper.db_session_scope() as mock_scope:
            publish_redshift.create_schema(
                schema_name, db_name, iam_role, mock_session_helper)
            mock_scope.execute.assert_called_once_with(f"CREATE EXTERNAL SCHEMA IF NOT EXISTS {schema_name} \
                FROM DATA CATALOG \
                database '{db_name}' \
                iam_role '{iam_role}';")

    # Test that the function is called with the table name
    @patch('s3parq.publish_redshift.SessionHelper')
    @patch('tests.test_publish_redshift.scope_execute_mock')
    def test_create_table(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        table_name = "my_string"
        schema_name = "my_schema"
        path = "s3://lol"
        columns = {'grouped_col': 'object', 'text_col': 'object',
                   'int_col': 'int64', 'float_col': 'float64'}
        partitions = {'fish': 'object'}

        expected_sql = f'CREATE EXTERNAL TABLE IF NOT EXISTS {schema_name}.{table_name} {columns} \
            PARTITIONED BY {partitions} STORED AS PARQUET \
            LOCATION "{path}";'
        with mock_session_helper.db_session_scope() as mock_scope:
            publish_redshift.create_table(table_name, schema_name, columns,
                                          partitions, path, mock_session_helper)
            assert mock_scope.execute.called_once_with(expected_sql)

    # Test that the function is called with the table name without partitions
    @patch('s3parq.publish_redshift.SessionHelper')
    @patch('tests.test_publish_redshift.scope_execute_mock')
    def test_create_table_without_partitions(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        table_name = "my_string"
        schema_name = "my_schema"
        path = "s3://lol"
        columns = {'grouped_col': 'object', 'text_col': 'object',
                   'int_col': 'int64', 'float_col': 'float64'}
        partitions = {}

        expected_sql = f'CREATE EXTERNAL TABLE IF NOT EXISTS {schema_name}.{table_name} {columns} \
            STORED AS PARQUET \
            LOCATION "{path}";'
        with mock_session_helper.db_session_scope() as mock_scope:
            publish_redshift.create_table(table_name, schema_name, columns,
                                          partitions, path, mock_session_helper)
            assert mock_scope.execute.called_once_with(expected_sql)

    # Test that the function is called with the table name without partitions
    @patch('s3parq.publish_redshift.SessionHelper')
    @patch('tests.test_publish_redshift.scope_execute_mock')
    def test_create_partitions(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        table_name = "my_table"
        schema_name = "my_schema"
        bucket = "test"
        partitions = ["version", "time"]
        filepath = "something_overgeneric/dataset/version=v2_final_new/time=01-01-69  23:59:07/keysmash.parquet"
        sql_partitions = "(version='v2_final_new', time='01-01-69  23:59:07')"
        path_for_sql = "'s3://test/something_overgeneric/dataset/version=v2_final_new'"

        expected_sql = f"ALTER TABLE {schema_name}.{table_name} \
#             ADD IF NOT EXISTS PARTITION {sql_partitions} \
#             LOCATION {path_for_sql};"
        with mock_session_helper.db_session_scope() as mock_scope:
            publish_redshift.create_partitions(bucket, schema_name, table_name, filepath, mock_session_helper)
            assert mock_scope.execute.called_once_with(expected_sql)
   
    # Test to check that the passed in datatype maps correctly
    def test_datatype_mapper(self):
        columns = {'grouped_col': 'object', 'text_col': 'object',
                   'int_col': 'int64', 'float_col': 'float64'}
        expected = {'grouped_col': 'VARCHAR', 'text_col': 'VARCHAR',
                    'int_col': 'BIGINT', 'float_col': 'FLOAT'}
        sql = ""
        for key, val in expected.items():
            sql += f'{key} {val}, '
        sql = "(" + sql[:-2] + ")"
        actual = publish_redshift._datatype_mapper(columns)
        assert actual == sql
