from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
import logging, re
import pandas as pd

logger = logging.getLogger(__name__)

def _check_reserved_keyword(name: str):
    reserved = "AES128, AES256, ALL, ALLOWOVERWRITE, ANALYSE, ANALYZE, AND, ANY, ARRAY, AS, ASC, AUTHORIZATION, BACKUP, BETWEEN, BINARY, BLANKSASNULL, BOTH, BYTEDICT, BZIP2, CASE, CAST, CHECK, COLLATE, COLUMN, CONSTRAINT, CREATE, CREDENTIALS, CROSS, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_USER_ID, DEFAULT, DEFERRABLE, DEFLATE, DEFRAG, DELTA, DELTA32K, DESC, DISABLE, DISTINCT, DO, ELSE, EMPTYASNULL, ENABLE, ENCODE, ENCRYPT, ENCRYPTION, END, EXCEPT, EXPLICIT, FALSE, FOR, FOREIGN, FREEZE, FROM, FULL, GLOBALDICT256, GLOBALDICT64K, GRANT, GROUP, GZIP, HAVING, IDENTITY, IGNORE, ILIKE, IN, INITIALLY, INNER, INTERSECT, INTO, IS, ISNULL, JOIN, LANGUAGE, LEADING, LEFT, LIKE, LIMIT, LOCALTIME, LOCALTIMESTAMP, LUN, LUNS, LZO, LZOP, MINUS, MOSTLY13, MOSTLY32, MOSTLY8, NATURAL, NEW, NOT, NOTNULL, NULL, NULLS, OFF, OFFLINE, OFFSET, OID, OLD, ON, ONLY, OPEN, OR, ORDER, OUTER, OVERLAPS, PARALLEL, PARTITION, PERCENT, PERMISSIONS, PLACING, PRIMARY, RAW, READRATIO, RECOVER, REFERENCES, RESPECT, REJECTLOG, RESORT, RESTORE, RIGHT, SELECT, SESSION_USER, SIMILAR, SNAPSHOT , SOME, SYSDATE, SYSTEM, TABLE, TAG, TDES, TEXT255, TEXT32K, THEN, TIMESTAMP, TO, TOP, TRAILING, TRUE, TRUNCATECOLUMNS, UNION, UNIQUE, USER, USING, VERBOSE, WALLET, WHEN, WHERE, WITH, WITHOUT".split()

    reserved = [x.strip(',') for x in reserved]
    return (name.upper() in reserved)

def _validate_name(name: str):
    if _check_reserved_keyword(name):
        return tuple([False, f'name: {name} cannot be a reserved SQL keyword'])
    elif not bool(re.match(r"^[a-zA-Z0-9_]", name)):
        return tuple([False, f'name: {name} can only start with an alphanumeric or an underscore'])
    elif bool(re.search("([ '\"])", name)):
        return tuple([False, f'name: {name} cannot contain spaces or quotations'])
    elif len(name) < 1 or len(name) > 127:
        return tuple([False, f'name: {name} must be between 1 and 127 characters'])
    else:
        return tuple([True, None])

def redshift_name_validator(*args):
    for arg in args:
        response = _validate_name(arg)
        if not response[0]:
            raise ValueError(response[1])

def _datatype_mapper(columns: dict) -> dict:
    """ Takes a dict of column names and pandas dtypes and returns a redshift create table statement of column names/dtypes."""
    logger.debug("Determining write metadata for publish...")
    dtypes = {}
    sql_statement = ""
    for col, dtype in columns.items():
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
    return "(" + sql_statement[:-2] + ")"

def create_schema(schema_name: str, db_name: str, iam_role: str, session_helper: SessionHelper):
    redshift_name_validator(schema_name, db_name)
    with session_helper.db_session_scope() as scope:
        new_schema_query = f"CREATE EXTERNAL SCHEMA IF NOT EXISTS {schema_name} \
                FROM DATA CATALOG \
                database '{db_name}' \
                iam_role '{iam_role}' \
                CREATE EXTERNAL DATABASE IF NOT EXISTS;"

        logger.info(f'Running query to create schema: {new_schema_query}')
        scope.execute(new_schema_query)

def create_table(table_name: str, schema_name: str, columns: dict, partitions: dict, path: str, session_helper: SessionHelper):
    redshift_name_validator(table_name)
    redshift_columns = _datatype_mapper(columns)
    redshift_partitions = _datatype_mapper(partitions)
    with session_helper.db_session_scope() as scope:
        new_schema_query = (
            f'CREATE EXTERNAL TABLE {schema_name}.{table_name} {redshift_columns} \
            PARTITIONED BY {redshift_partitions} STORED AS PARQUET \
            LOCATION "{path}";'
        )
        logger.info(f'Running query to create table: {new_schema_query}')
        scope.execute(new_schema_query)
