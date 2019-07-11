from s3parq.create_table import create_table
from s3parq.session_helper import SessionHelper


class Test():
        
    # Given a correctly formatted string make sure output is correct SQL
    def test_validator(self):
        table_name_good = "my_string"
        table_name_bad = "my string"
        table_name_validator(table_name_good)
        with pytest.raises(ValueError):
            schema_name_validator(table_name_bad)
    