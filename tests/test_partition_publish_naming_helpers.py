import pytest
import s3parq.publish_redshift as parq
from mock import patch

class MockScopeObj():

    def execute(self, schema_string: str):
        pass

def scope_execute_mock(mock_session_helper):
    pass

class Test:

    def test_gets_proper_partitions(self):
        test_str = '/some/path/to/data/banana=33/orange=65/apple=abcd/xyz.parquet'
        final_partitions = parq._get_partitions_for_spectrum(test_str)
        assert final_partitions == ['banana=33', 'orange=65', 'apple=abcd']

    def test_gets_no_partitions(self):
        test_str = '/some/path/to/data/xyz.parquet'
        final_partitions = parq._get_partitions_for_spectrum(test_str)
        assert final_partitions == []

    def test_gets_proper_partitions_multiple_slashes(self):
        test_str = '/some/path/to/data//banana=33/orange=65/apple=abcd/xyz.parquet'
        final_partitions = parq._get_partitions_for_spectrum(test_str)
        assert final_partitions == ['banana=33', 'orange=65', 'apple=abcd']

    def test_format_partition_strings(self):
        test_partitions = ['banana=33', 'orange=65', 'apple=abcd']
        final_partitions = parq._format_partition_strings_for_sql(test_partitions)
        assert final_partitions == ["banana='33'", "orange='65'", "apple='abcd'"]

    def test_format_partition_strings_no_partitions(self):
        test_partitions = []
        final_partitions = parq._format_partition_strings_for_sql(test_partitions)
        assert final_partitions == []

    def test_index_containing_substring(self):
        test_list = ['abcd', 'efgh=1234', 'ijkl=5678', 'xyz.parquet']
        index = parq._last_index_containing_substring(test_list, '=')
        assert index == 2

    def test_index_containing_substring_no_match(self):
        test_list = ['abcd', 'efgh=1234', 'ijkl=5678']
        index = parq._last_index_containing_substring(test_list, '&')
        assert index == 4

    def test_get_partition_location(self):
        test_filepath = 'path/to/data/apple=abcd/orange=1234/abcd1234.parquet'
        partition_path = parq._get_partition_location(test_filepath)
        assert partition_path == 'path/to/data/apple=abcd/orange=1234'

    def test_get_partition_location_no_partition(self):
        test_filepath = 'path/to/data/abcd1234.parquet'
        with pytest.raises(ValueError):
            partition_path = parq._get_partition_location(test_filepath)

    @patch('s3parq.publish_parq.SessionHelper')
    @patch('tests.test_partition_publish_naming_helpers.scope_execute_mock')
    def test_create_partitions(self, mock_session_helper, mock_execute):
        bucket, schema, table, filepath = 'MyBucket', 'MySchema', 'MyTable', 'path/to/data/apple=abcd/banana=1234/abcd1234.parquet'
        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        partitions = parq._get_partitions_for_spectrum(filepath)
        formatted_partitions = parq._format_partition_strings_for_sql(partitions)
        path_to_data = parq._get_partition_location(filepath)

        with mock_session_helper.db_session_scope() as mock_scope:
            generated_sql = parq.create_partitions(bucket, schema, table, filepath, mock_session_helper)
            expected_sql = f"ALTER TABLE {schema}.{table} \
            ADD PARTITION ({' ,'.join(formatted_partitions)}) \
            LOCATION 's3://{bucket}/{path_to_data}';"
            assert mock_scope.execute.called_once_with(expected_sql)
