from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
import logging
import re
import pandas as pd

logger = logging.getLogger(__name__)


def _is_reserved_keyword(name: str) -> bool:
    """ Returns bool of whether the uppercased name is reserved in Redshift
    NOTE: This is backwards to expected by name (if _is_reserved_keyword , its fine)
    """
    reserved = "AES128 AES256 ALL ALLOWOVERWRITE ANALYSE ANALYZE AND ANY ARRAY AS ASC AUTHORIZATION BACKUP BETWEEN BINARY BLANKSASNULL BOTH BYTEDICT BZIP2 CASE CAST CHECK COLLATE COLUMN CONSTRAINT CREATE CREDENTIALS CROSS CURRENT_DATE CURRENT_TIME CURRENT_TIMESTAMP CURRENT_USER CURRENT_USER_ID DEFAULT DEFERRABLE DEFLATE DEFRAG DELTA DELTA32K DESC DISABLE DISTINCT DO ELSE EMPTYASNULL ENABLE ENCODE ENCRYPT ENCRYPTION END EXCEPT EXPLICIT FALSE FOR FOREIGN FREEZE FROM FULL GLOBALDICT256 GLOBALDICT64K GRANT GROUP GZIP HAVING IDENTITY IGNORE ILIKE IN INITIALLY INNER INTERSECT INTO IS ISNULL JOIN LANGUAGE LEADING LEFT LIKE LIMIT LOCALTIME LOCALTIMESTAMP LUN LUNS LZO LZOP MINUS MOSTLY13 MOSTLY32 MOSTLY8 NATURAL NEW NOT NOTNULL NULL NULLS OFF OFFLINE OFFSET OID OLD ON ONLY OPEN OR ORDER OUTER OVERLAPS PARALLEL PARTITION PERCENT PERMISSIONS PLACING PRIMARY RAW READRATIO RECOVER REFERENCES RESPECT REJECTLOG RESORT RESTORE RIGHT SELECT SESSION_USER SIMILAR SNAPSHOT SOME SYSDATE SYSTEM TABLE TAG TDES TEXT255 TEXT32K THEN TIMESTAMP TO TOP TRAILING TRUE TRUNCATECOLUMNS UNION UNIQUE USER USING VERBOSE WALLET WHEN WHERE WITH WITHOUT".split()
    return not(name.upper() in reserved)


def _validate_name(name: str) -> tuple:
    """ Returns tuple, of which the first index indicates if the name is valid,
    and the second is the error message of why it is invalid if so
    """
    if not _is_reserved_keyword(name):
        return tuple([False, f'name: {name} cannot be a reserved SQL keyword'])
    elif not bool(re.match(r"^[a-zA-Z0-9_]", name)):
        return tuple([False, f'name: {name} can only start with an alphanumeric or an underscore'])
    elif bool(re.search("([ '\"])", name)):
        return tuple([False, f'name: {name} cannot contain spaces or quotations'])
    elif len(name) < 1 or len(name) > 127:
        return tuple([False, f'name: {name} must be between 1 and 127 characters'])
    else:
        return tuple([True, None])


def _redshift_name_validator(*args) -> None:
    """ Passes a list of args through name validation

    Returns:
        None

    Raises:
        ValueError: Uses internal validate_name function and raises error based
            on failure reasons
    """
    for arg in args:
        response = _validate_name(arg)
        if not response[0]:
            raise ValueError(response[1])


def _get_partitions_for_spectrum(filename: str) -> [str]:
    '''
    Turns S3 filepath with partitions into list of only those partitions as strings
    Args:
        filename (str): entire filepath for a single file
    ----
    Returns:
        final_partitions (list of strings): these are the partitions for that file
    --------
    Example:
        Args:
            filename = 'some-path/to/data/zipcode=12345/birth_month=january/final_data_set.parquet
        ----
        Returns:
            final_partitions = ['zipcode=12345', 'birth_month=january']
    '''
    filepath = filename.split('/')
    final_partitions = [_dir for _dir in filepath if '=' in _dir]
    return final_partitions


def _format_partition_strings_for_sql(partitions: [str]) -> [str]:
    '''
    Formats a list of S3 partition strings for SQL statements
    Args:
        partitions ([str]): list of strings representing the partitions in S3
    ----
    Returns:
        formatted_partitions ([str]): list of the same partitions with quotes
        to use for SQL
    --------
    Example:
        Args:
            partitions = ['hamburger=abcd', 'hot_dog=1234']
        ----
        Returns:
            formatted_partitions = ["hamburger='abcd'", "hot_dog='1234'"]
    '''
    formatted_partitions = []
    for p in partitions:
        key, value = p.split('=')
        value = f"'{value}'"
        formatted_partitions.append(f'{key}={value}')
    return formatted_partitions


def _last_index_containing_substring(the_list: [str], substring: str) -> int:
    '''
    Returns index of last string that contains a substring within a list.  If there's no match, it returns
    the length of the list + 1 because we want the function that uses it "_get_partition_location"
    to not identify any partitions if none exists
    Args:
        the_list ([str]): list of strings, probably representing the partitions
        substring (str): string to look for in the_list
    ----
    Returns:
        i (int): The index in the list that contains the first instance of the substring
        returns -1 if no matches
    --------
    Example:
        Args: 
            the_list = ['path', 'to', 'data', 'hamburger=abcd', 'hot_dog=1234', 'the_file.parquet]
            substring = '='
        ----
        Returns:
            i = 3

    '''
    for s in reversed(the_list):
        if substring in s:
            return len(the_list) - the_list.index(s)
    return len(the_list) + 1


def _get_partition_location(filepath: str):
    '''
    Gets location of data in S3 for partitioning in S3.  You need to know the path to 
    the first partition in order to make a proper Spectrum partition w/ SQL-Redshift
    Args:
        filepath (str): path to a file in S3 that is partitioned
    Returns:
        final_path (str): path within S3 bucket that has the last partition of a filepath
    Example:
        Args: 
            filepath = 'path/to/data/hamburger=abcd/hot_dog=1234/abcd1234.parquet'
        Returns:
            final_path = 'path/to/data/hamburger=abcd/hot_dog=1234'
    '''
    separate_dirs = filepath.split('/')
    last_partition = _last_index_containing_substring(separate_dirs, "=")
    # I think this is more confusing than it has to be
    final_set = separate_dirs[:-last_partition + 1]
    final_path = '/'.join(final_set)
    if final_path == '':
        raise ValueError(f'No partitions in this filepath {filepath}')
    return final_path


def _datatype_mapper(columns: dict) -> dict:
    """ Takes a dict of column names and pandas dtypes and returns a redshift create table statement of column names/dtypes."""
    logger.debug("Determining write metadata for publish...")
    dtypes = {}
    sql_statement = ""
    for col, dtype in columns.items():
        if dtype == 'object':
            dtypes[col] = 'VARCHAR'
        elif dtype.startswith('int8'):
            dtypes[col] = 'INTEGER'
        elif dtype.startswith('int32'):
            dtypes[col] = 'INTEGER'
        elif dtype.startswith('int64'):
            dtypes[col] = 'BIGINT'
        elif dtype.startswith('float16'):
            dtypes[col] = 'REAL'
        elif dtype.startswith('float32'):
            dtypes[col] = 'REAL'
        elif dtype.startswith('float64'):
            dtypes[col] = 'FLOAT'
        elif dtype.startswith('date'):
            dtypes[col] = 'TIMESTAMP'
        elif dtype.startswith('category'):
            dtypes[col] = 'VARCHAR'
        elif dtype == 'bool':
            dtypes[col] = 'BOOLEAN'
        else:
            raise ValueError(
                f"Error: {dtype} is not a datatype which can be mapped to Redshift.")
        sql_statement += f'{col} {dtypes[col]}, '
    return f"({sql_statement[:-2]})"  # Slice off the last space and comma


def create_schema(schema_name: str, db_name: str, iam_role: str, session_helper: SessionHelper) -> None:
    """ Creates a schema in AWS redshift using a given iam_role

    Args:
        schema_name (str): Name of the schema to create in Redshift Spectrum
        db_name (str): (Existing) database that the schema should belong to
        iam_role (str): link to an existing AWS IAM Role with Redshift Spectrum 
            write permissions
        session_helper (str): Active and configured session_helper session to use
    """
    _redshift_name_validator(schema_name, db_name)
    with session_helper.db_session_scope() as scope:
        new_schema_query = f"CREATE EXTERNAL SCHEMA IF NOT EXISTS {schema_name} \
                FROM DATA CATALOG \
                database '{db_name}' \
                iam_role '{iam_role}';"

        logger.info(f'Running query to create schema: {new_schema_query}')
        scope.execute(new_schema_query)


def create_table(table_name: str, schema_name: str, columns: dict, partitions: dict, path: str, session_helper: SessionHelper) -> None:
    """ Creates a table in AWS redshift. The table will be named 
    schema_name.table_name and belong to the (existing) Redshift db db_name

    Args:
        table_name (str): name of created table
            NOTE: THIS WILL ERROR IF table_name ALREADY EXISTS IN REDSHIFT
        schema_name (str): name of schema in redshift; Schema must be external 
            and already exist!
        columns (dict): Dictionary with keys corresponding to column names and 
            values corresponding to pandas dtypes, excluding partition columns
        partitions (dict): Dict similar to columns, except ONLY with partition columns
        path (str): Path to published dataset in s3 (excluding partitions)
        session_helper (SessionHelper): Instance of Redshift s3parq.session_helper
    """
    _redshift_name_validator(table_name)
    redshift_columns = _datatype_mapper(columns)
    redshift_partitions = _datatype_mapper(partitions)
    with session_helper.db_session_scope() as scope:
        if_exists_query = f'SELECT EXISTS(SELECT schemaname, tablename FROM SVV_EXTERNAL_TABLES WHERE tablename=\'{table_name}\' AND schemaname=\'{schema_name}\');'
        table_exists = scope.execute(if_exists_query).first()[0]
        if table_exists:
            return

        if not partitions:
            new_schema_query = (
                f'CREATE EXTERNAL TABLE {schema_name}.{table_name} {redshift_columns} \
                STORED AS PARQUET \
                LOCATION \'{path}\';'
            )
        else:
            new_schema_query = (
                f'CREATE EXTERNAL TABLE {schema_name}.{table_name} {redshift_columns} \
                PARTITIONED BY {redshift_partitions} STORED AS PARQUET \
                LOCATION \'{path}\';'
            )
        logger.info(f'Running query to create table: {new_schema_query}')
        scope.execute(new_schema_query)

def create_custom_table(table_name: str, schema_name: str, partitions: dict, path: str, custom_redshift_columns: dict, session_helper: SessionHelper) -> None:
    """ Creates a table in AWS redshift. The table will be named 
    schema_name.table_name and belong to the (existing) Redshift db db_name.
    The created table will use the CUSTOM redshift column data types defined 
    in custom_redshift_columns.

    Args:
        table_name (str): name of created table
            NOTE: THIS WILL ERROR IF table_name ALREADY EXISTS IN REDSHIFT
        schema_name (str): name of schema in redshift; Schema must be external 
            and already exist!
        partitions (dict): Dict similar to columns, except ONLY with partition columns
        path (str): Path to published dataset in s3 (excluding partitions)
        custom_redshift_columns (dict): 
            This dictionary contains custom column data type definitions for redshift.
            The params should be formatted as follows:
                - column name (str)
                - data type (str)
        session_helper (SessionHelper): Instance of Redshift s3parq.session_helper
    """

    logger.info("Running create_custom_table...")

    _redshift_name_validator(table_name)

    logger.info("Generating create columns sql statement with custom redshift columns...")
    logger.info("Generating create partitions sql statement with custom redshift columns...")
    redshift_columns_sql = ""
    redshift_partitions_sql = ""
    for k, v in custom_redshift_columns.items():
        if k in partitions:
            redshift_partitions_sql += f'{k} {v}, '
        else:
            redshift_columns_sql += f'{k} {v}, '
    redshift_columns = f"({redshift_columns_sql[:-2]})"  # Slice off the last space and comma
    redshift_partitions = f"({redshift_partitions_sql[:-2]})"  # Slice off the last space and comma

    with session_helper.db_session_scope() as scope:
        if_exists_query = f'SELECT EXISTS(SELECT schemaname, tablename FROM SVV_EXTERNAL_TABLES WHERE tablename=\'{table_name}\' AND schemaname=\'{schema_name}\');'
        table_exists = scope.execute(if_exists_query).first()[0]
        if table_exists:
            return

        if not partitions:
            new_schema_query = (
                f'CREATE EXTERNAL TABLE {schema_name}.{table_name} {redshift_columns} \
                STORED AS PARQUET \
                LOCATION \'{path}\';'
            )
        else:
            new_schema_query = (
                f'CREATE EXTERNAL TABLE {schema_name}.{table_name} {redshift_columns} \
                PARTITIONED BY {redshift_partitions} STORED AS PARQUET \
                LOCATION \'{path}\';'
            )
        logger.info(f'Running query to create table: {new_schema_query}')
        scope.execute(new_schema_query)


def create_partitions(bucket: str, schema: str, table: str, filepath: str, session_helper: SessionHelper) -> None:
    ''' Executes the SQL that creates partitions on the given table for an 
    individual file

    Args:
        bucket (str): S3 bucket where data is stored
        schema (str): name of redshift schema (must already exist)
        table (str): name of table in schema.  Must have partitions scoped out in `CREATE TABLE ...`
        filepath (str): path to data in S3 that will be queryable by it's partitions
            NOTE: This is to the single parquet file, including the partitions
        session_helper (SessionHelper): a configured s3parq.session_helper.SessionHelper session

    Returns:
        None

    Example:
        Args:
            bucket = 'MyBucket'
            schema = 'MySchema'
            table = 'MyTable'
            filepath = 'path/to/data/apple=abcd/banana=1234/abcd1234.parquet'
            session_helper = some_configured_session
    '''
    partitions = _get_partitions_for_spectrum(filepath)
    formatted_partitions = _format_partition_strings_for_sql(partitions)
    path_to_data = _get_partition_location(filepath)

    with session_helper.db_session_scope() as scope:
        partitions_query = f"ALTER TABLE {schema}.{table} \
            ADD IF NOT EXISTS PARTITION ({', '.join(formatted_partitions)}) \
            LOCATION 's3://{bucket}/{path_to_data}';"
        logger.info(f'Running query to create: {partitions_query}')
        scope.execute(partitions_query)
