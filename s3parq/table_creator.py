from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from s3parq.redshift_naming_helper import RedshiftNamingHelper
import logging

logger = logging.getLogger(__name__)

#write a function for the switcher
def _datatype_mapper(dataframe: pd.DataFrame, partitions: list) -> dict:
    """ Returns a dict with the column names as keys, the data types (in strings) as values."""
    logger.debug("Determining write metadata for publish...")
    dataframe = dataframe[partitions]
    dtypes = {}
    sql_statement = ""
    for col, dtype in dataframe.dtypes.items():
        dtype = str(dtype)
        if dtype == 'object':
            dtypes[col] = 'VARCHAR'
        elif dtype.startswith('int'):
            dtypes[col] = 'INTEGER'
        elif dtype.startswith('float'):
            dtypes[col] = 'REAL'
        elif dtype.startswith('date'):
            dtypes[col] = 'TIMESTAMP'
        elif dtype.startswith('category'):
            dtypes[col] = 'VARCHAR'
        elif dtype == 'bool':
            dtypes[col] = 'BOOLEAN'
        sql_statement += f'{col} {dtypes[col]} ,'
    
        

def datatype_to_sql(dtypes: dict):
    cols = dtypes.keys
    types = dtypes.values
    sql_statement = []
    for key, value in dtypes.items():
            sql_statement.append(f'{key} as {datatype_mapper(value)}')
    return ', '.join(sql_statement)


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
