import pytest
from mock import patch
from s3parq import publish_spectrum
from s3parq.session_helper import SessionHelper

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
        database_name_good = 'my_database'
        database_name_bad = 'my database'
        publish_spectrum._redshift_name_validator(schema_name_good, database_name_good)
        with pytest.raises(ValueError):
            publish_spectrum._redshift_name_validator(schema_name_bad, database_name_bad)

    # Test that the function is called with the schema name
    @patch('s3parq.publish_spectrum.SessionHelper')
    @patch('tests.test_schema_creator.scope_execute_mock')
    def test_create_schema(self, mock_session_helper, mock_execute):

        mock_execute.return_value = MockScopeObj()
        mock_session_helper.db_session_scope.return_value.__enter__ = scope_execute_mock

        schema_name = "my_string"
        db_name = "my_database"
        iam_role = "my_iam_role"     
        with mock_session_helper.db_session_scope() as mock_scope:
            publish_spectrum.create_schema(schema_name, db_name, iam_role, mock_session_helper)
            mock_scope.execute.assert_called_once_with(f"CREATE EXTERNAL SCHEMA IF NOT EXISTS {schema_name} \
                FROM DATA CATALOG \
                database '{db_name}' \
                iam_role '{iam_role}';")
