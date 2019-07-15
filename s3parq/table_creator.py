from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from s3parq.redshift_naming_helper import RedshiftNamingHelper
import logging

logger = logging.getLogger(__name__)

#write a function for the switcher
def datatype_mapper(dtype: str):
    switcher = {
        'object': 'VARCHAR',
        'int64': 'int',
        'float64': 'float',
        'bool': 'BOOLEAN'
    }
    if switcher.get(dtype) is None:
            raise KeyError()
    return switcher.get(dtype)

def datatype_to_sql(dtypes: dict):
    cols = dtypes.keys
    types = dtypes.values


def table_name_validator(table_name: str):
    name_validated = RedshiftNamingHelper().validate_name(table_name)
    if not name_validated[0]:
        raise ValueError(name_validated[1])


def create_table(table_name: str, schema_name: str, columns: dict, session_helper: SessionHelper):
    table_name_validator(table_name)
    columns_string = ', '.join(columns)
    with session_helper.db_session_scope() as scope:
        new_schema_query = (
            f'CREATE TABLE IF NOT EXISTS {table_name}( {columns_string});'
        )
        logger.info(f'Running query to create table: {new_schema_query}')
        scope.execute(new_schema_query)