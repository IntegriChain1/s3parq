from mock import patch
import pytest

from s3parq import publish_redshift


class MockScopeObj():

    def execute(self, schema_string: str):
        pass


def scope_execute_mock(mock_session_helper):
    pass


class Test():

    # Make sure that naming validator throws expected errors
    def test_naming_validator(self):
        response = publish_redshift._validate_name('my string')
        assert not response[0], 'Allowed name to contain spaces'

        response = publish_redshift._validate_name('my_string')
        assert response[0]

        response = publish_redshift._validate_name('WHERE')
        assert not response[0], 'Allowed name to be a reserved SQL keyword'

        response = publish_redshift._validate_name('@my_string')
        assert not response[0], 'Allowed name to start as not an alphanumeric or an underscore'

        response = publish_redshift._validate_name(
            "asdffdsaasdffdsaasdfasdffdsaasdffdsaasd\
            fasdffdsaasdffdsaasdfasdffdsaasdffdsaasdsd\
            ffdsaasdffdsaasdfasdffdsaasdffdsaasdfasdffdsaasdffdsaasdf"
        )
        assert not response[0], f'Allowed name as too long string'

    # Make sure that the reshift-specific validator throes the right errors
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

    def test_gets_proper_partitions(self):
        test_str = '/some/path/to/data/banana=33/orange=65/apple=abcd/xyz.parquet'
        final_partitions = publish_redshift._get_partitions_for_spectrum(
            test_str)
        assert final_partitions == ['banana=33', 'orange=65', 'apple=abcd']

    def test_gets_no_partitions(self):
        test_str = '/some/path/to/data/xyz.parquet'
        final_partitions = publish_redshift._get_partitions_for_spectrum(
            test_str)
        assert final_partitions == []

    def test_gets_proper_partitions_multiple_slashes(self):
        test_str = '/some/path/to/data//banana=33/orange=65/apple=abcd/xyz.parquet'
        final_partitions = publish_redshift._get_partitions_for_spectrum(
            test_str)
        assert final_partitions == ['banana=33', 'orange=65', 'apple=abcd']

    def test_format_partition_strings(self):
        test_partitions = ['banana=33', 'orange=65', 'apple=abcd']
        final_partitions = publish_redshift._format_partition_strings_for_sql(
            test_partitions)
        assert final_partitions == [
            "banana='33'", "orange='65'", "apple='abcd'"]

    def test_format_partition_strings_no_partitions(self):
        test_partitions = []
        final_partitions = publish_redshift._format_partition_strings_for_sql(
            test_partitions)
        assert final_partitions == []

    def test_index_containing_substring(self):
        test_list = ['abcd', 'efgh=1234', 'ijkl=5678', 'xyz.parquet']
        index = publish_redshift._last_index_containing_substring(
            test_list, '=')
        assert index == 2

    def test_index_containing_substring_no_match(self):
        test_list = ['abcd', 'efgh=1234', 'ijkl=5678']
        index = publish_redshift._last_index_containing_substring(
            test_list, '&')
        assert index == 4

    def test_get_partition_location(self):
        test_filepath = 'path/to/data/apple=abcd/orange=1234/abcd1234.parquet'
        partition_path = publish_redshift._get_partition_location(
            test_filepath)
        assert partition_path == 'path/to/data/apple=abcd/orange=1234'

    def test_get_partition_location_no_partition(self):
        test_filepath = 'path/to/data/abcd1234.parquet'
        with pytest.raises(ValueError):
            partition_path = publish_redshift._get_partition_location(
                test_filepath)

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
            publish_redshift.create_partitions(
                bucket, schema_name, table_name, filepath, mock_session_helper)
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
