import boto3
import s3fs
import re
import sys
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List
from s3parq.session_helper import SessionHelper
from s3parq import publish_redshift
from sqlalchemy import Column, Integer, String

logger = logging.getLogger(__name__)


def check_empty_dataframe(dataframe: pd.DataFrame) -> None:
    """ Make sure that the dataframe being published is not empty

    Args:
        dataframe (pd.DataFrame): Dataframe to verify

    Returns:
        None

    Raises:
        ValueError: If dataframe is empty
    """
    if dataframe.empty:
        no_data_message = "Empty dataframes cannot be published."
        raise ValueError(no_data_message)


def check_dataframe_for_timedelta(dataframe: pd.DataFrame) -> None:
    """ Pyarrow can't do timedelta, this verifies one is not present

    Args:
        dataframe (pd.DataFrame): Dataframe to verify

    Returns:
        None

    Raises:
        NotImplementedError: If the dataframe contains a timedelta dtype
    """
    dtypes = set(dataframe.columns)
    for dtype in dtypes:
        if 'timedelta' in dtype:
            timedelta_message = "Pyarrow does not support parquet conversion of timedelta columns at this time."
            logger.debug(timedelta_message)
            raise NotImplementedError(timedelta_message)


def _check_partition_compatibility(partition: str) -> bool:
    """ Make sure each partition value is hive-allowed.

    Args:
        partition (str): Partition to verify

    Returns:
        bool of whether the partition is not a hive reserved one
    """
    reserved = "ALL ALTER AND ARRAY AS AUTHORIZATION BETWEEN BIGINT BINARY BOOLEAN BOTH BY CASE CAST CHAR COLUMN CONF CREATE CROSS CUBE CURRENT CURRENT_DATE CURRENT_TIMESTAMP CURSOR DATABASE DATE DECIMAL DELETE DESCRIBE DISTINCT DOUBLE DROP ELSE END EXCHANGE EXISTS EXTENDED EXTERNAL FALSE FETCH FLOAT FOLLOWING FOR FROM FULL FUNCTION GRANT GROUP GROUPING HAVING IF IMPORT IN INNER INSERT INT INTERSECT INTERVAL INTO IS JOIN LATERAL LEFT LESS LIKE LOCAL MACRO MAP MORE NONE NOT NULL OF ON OR ORDER OUT OUTER OVER PARTIALSCAN PARTITION PERCENT PRECEDING PRESERVE PROCEDURE RANGE READS REDUCE REVOKE RIGHT ROLLUP ROW ROWS SELECT SET SMALLINT TABLE TABLESAMPLE THEN TIMESTAMP TO TRANSFORM TRIGGER TRUE TRUNCATE UNBOUNDED UNION UNIQUEJOIN UPDATE USER USING UTC_TMESTAMP VALUES VARCHAR WHEN WHERE WINDOW WITH COMMIT ONLY REGEXP RLIKE ROLLBACK START CACHE CONSTRAINT FOREIGN PRIMARY REFERENCES DAYOFWEEK EXTRACT FLOOR INTEGER PRECISION VIEWS TIME NUMERIC SYNC".split()
    return not (partition.upper() in reserved)


def check_partitions(partitions: iter, dataframe: pd.DataFrame) -> None:
    """ Make sure all given partitions are in the dataframe and HIVE-allowed

    Args:
        partitions (iter): Partitions to verify
        dataframe (pd.DataFrame): Dataframe the partitions should be in

    Returns:
        None

    Raises:
        ValueError: If the partition is not present in the dataframe's columns
        ValueError: If the partition is a HIVE reserved word
    """
    logger.debug("Checking partition args...")
    for partition in partitions:
        if partition not in dataframe.columns.tolist():
            partition_message = f"Cannot set {partition} as a partition; this is not a valid column header for the supplied dataframe."
            logger.critical(partition_message)
            raise ValueError(partition_message)
        if not _check_partition_compatibility(partition):
            partition_message = f"{partition} is a reserved word in hive that cannot be used as a partition."
            logger.critical(partition_message)
            raise ValueError(partition_message)

    logger.debug("Done checking partitions.")


def validate_redshift_params(redshift_params: dict) -> dict:
    """ Make sure redshift params are the expected format with all needed values
    Will also lowercase table and schema names.

    Args:
        redshift_params (dict):
            Dictionary for Spectrum, should be provided in the following format
            The params should be formatted as follows:

                - schema_name (str): Name of the Spectrum schema to publish to
                - table_name (str): Name of the table to write the dataset as
                - iam_role (str): Role to take while writing data to Spectrum
                - region (str): AWS region for Spectrum
                - cluster_id (str): Spectrum cluster id
                - host (str): Redshift Spectrum host name
                - port (str): Redshift Spectrum port to use
                - db_name (str): Redshift Spectrum database name to use
                - ec2_user (str): If on ec2, the user that should be used

    Returns:
        The given redshift_params, with table and schema names lowercase

    Raises:
        ValueError: If the length of redshift_params is less than expected
        ValueError: If redshift_params other than ec2_user have no value
        ValueError: If redshift_params is missing any of the above attributes
    """
    expected_params = ["schema_name", "table_name", "iam_role",
                       "region", "cluster_id", "host", "port", "db_name", "ec2_user"]
    logger.debug("Checking redshift params are correctly formatted")
    if len(redshift_params) != len(expected_params):
        params_length_message = f"Expected parameters: {len(expected_params)}. Received: {len(redshift_params)}"
        raise ValueError(params_length_message)
    for key, item in redshift_params.items():
        if not item and key != "ec2_user":
            params_type_message = f"No value assigned for param {key}."
            raise ValueError(params_type_message)
    for param in expected_params:
        if param not in redshift_params.keys():
            params_key_message = f"Error: Required parameter {param} not found in passed redshift_params."
            raise KeyError(params_key_message)

     # Ensure redshift table name is lowercase
    if redshift_params['table_name'] != redshift_params['table_name'].lower():
        logger.warning(
            f"Table name {redshift_params['table_name']} contains uppercase letters. Converting to lowercase...")
        redshift_params['table_name'] = redshift_params['table_name'].lower()

    # Ensure redshift schema name is lowercase
    if redshift_params['schema_name'] != redshift_params['schema_name'].lower():
        logger.warning(
            f"Schema name {redshift_params['schema_name']} contains uppercase letters. Converting to lowercase...")
        redshift_params['schema_name'] = redshift_params['schema_name'].lower()

    logger.debug('Done validating redshift params')
    return redshift_params


def s3_url(bucket: str, key: str) -> str:
    """ Turn a bucket + key into a S3 url """
    return '/'.join(["s3:/", bucket, key])


def _gen_parquet_to_s3(bucket: str, key: str, dataframe: pd.DataFrame,
                       partitions: list, custom_redshift_columns: dict = None) -> None:
    """ Converts the dataframe into a PyArrow table then writes it into S3 as
    parquet. Partitions are done with PyArrow parquet's write.
    NOTE: Timestamps are coerced to ms , this is due to a constraint of parquet
        with pandas's datetime64

    Args:
        bucket (str): S3 bucket to publish to
        key (str): S3 key to the root of where the dataset should be published
        dataframe (pd.DataFrame): Dataframe that's being published
        partitions (list): List of partition columns
        custom_redshift_columns (dict): 
            This dictionary contains custom column data type definitions for redshift.
            The params should be formatted as follows:
                - column name (str)
                - data type (str)

    Returns:
        None
    """
    logger.info("Writing to S3...")

    try:
        table = pa.Table.from_pandas(
            df=dataframe, schema=_parquet_schema(dataframe, custom_redshift_columns=custom_redshift_columns), preserve_index=False)
    except pa.lib.ArrowTypeError:
        logger.warn(
            "Dataframe conversion to pyarrow table failed, checking object columns for mixed types")
        dataframe_dtypes = dataframe.dtypes.to_dict()
        object_columns = [
            # Skip conversion to strings if the column contains decimal objects. Otherwise, decimal objects will be improperly converted.
            col for col, col_type in dataframe_dtypes.items() if col_type == "object" and "[Decimal(" not in str(dataframe[col].values)[:9]]
        for object_col in object_columns:
            if not dataframe[object_col].apply(isinstance, args=[str]).all():
                logger.warn(
                    f"Dataframe column : {object_col} : in this chunk is type object but contains non-strings, converting to all-string column")
                dataframe[object_col] = dataframe[object_col].astype(str)

        logger.info("Retrying conversion to pyarrow table")
        table = pa.Table.from_pandas(
            df=dataframe, schema=_parquet_schema(dataframe, custom_redshift_columns=custom_redshift_columns), preserve_index=False)

    uri = s3_url(bucket, key)
    logger.debug(f"Writing to s3 location: {uri}...")
    pq.write_to_dataset(table, compression="snappy", root_path=uri,
                        partition_cols=partitions, filesystem=s3fs.S3FileSystem(), coerce_timestamps='ms', allow_truncated_timestamps=True)
    logger.debug("Done writing to location.")


def _assign_partition_meta(bucket: str, key: str, dataframe: pd.DataFrame, partitions: List['str'], session_helper: SessionHelper, redshift_params=None, custom_redshift_columns: dict = None) -> List[str]:
    """ Assigns the dataset partition meta to all object keys in the dataset.
    Keys are found by listing all files under the given key and then filtering
    to only those that end in '.parquet' and then further filtering to those
    that do not already have partition_data_types metadata.

    Args:
        bucket (str): S3 bucket to publish to
        key (str): S3 key to the root of where the dataset is published
        dataframe (pd.DataFrame): Dataframe that has been published
        partitions (list): List of partition columns
        session_helper (SessionHelper): Current session, if not using Spectrum this should be None
        redshift_params (dict, Optional):
            Dictionary for Spectrum, should be in the following format
            The params should be formatted as follows:

                - schema_name (str): Name of the Spectrum schema to publish to
                - table_name (str): Name of the table to write the dataset as
                - iam_role (str): Role to take while writing data to Spectrum
                - region (str): AWS region for Spectrum
                - cluster_id (str): Spectrum cluster id
                - host (str): Redshift Spectrum host name
                - port (str): Redshift Spectrum port to use
                - db_name (str): Redshift Spectrum database name to use
                - ec2_user (str): If on ec2, the user that should be used
        custom_redshift_columns (dict, Optional): 
            This dictionary contains custom column data type definitions for redshift.
            The params should be formatted as follows:
                - column name (str)
                - data type (str)

    Returns:
        A str list of object keys of the objects that got metadata added
    """
    s3_client = boto3.client('s3')
    all_files_without_meta = []
    paginator = s3_client.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=key)
    for page in page_iterator:
        for obj in page['Contents']:
            if obj['Key'].endswith(".parquet"):
                head_obj = s3_client.head_object(Bucket=bucket, Key=obj['Key'])
                if not 'partition_data_types' in head_obj['Metadata']:
                    all_files_without_meta.append(obj['Key'])
                    if redshift_params and partitions:
                        sql_command = publish_redshift.create_partitions(
                            bucket, redshift_params['schema_name'], redshift_params['table_name'], obj['Key'], session_helper)

    for obj in all_files_without_meta:
        logger.debug(f"Appending metadata to file {obj}..")
        s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': obj}, Key=obj,
                              Metadata={'partition_data_types': str(
                                  _parse_dataframe_col_types(
                                      dataframe=dataframe, partitions=partitions, custom_redshift_columns=custom_redshift_columns)
                              )}, MetadataDirective='REPLACE')
        logger.debug("Done appending metadata.")
    return all_files_without_meta


def _get_dataframe_datatypes(dataframe: pd.DataFrame, partitions=[], use_parts=False) -> dict:
    """ Gets the dtypes of the columns in the dataframe

    Args:
        dataframe (pd.DataFrame): Dataframe that has been published
        partitions (list, Optional): List of partition columns
        use_parts (bool, Optional): bool to determine if only partition datatypes
            should be returned

    Returns:
        Dictionary of the column names as keys and dtypes as values,
            if use_parts is true then only limited to partition columns
    """
    types = dict()
    if use_parts:
        columns = partitions
    else:
        columns = dataframe.drop(labels=partitions, axis="columns").columns
    for col in columns:
        type_string = dataframe[col].dtype.name
        types[col] = type_string
    return types


def _parquet_schema(dataframe: pd.DataFrame, custom_redshift_columns: dict = None) -> pa.Schema:
    """ Translates pandas dtypes to PyArrow types and creates a Schema from them

    Args:
        dataframe (pd.DataFrame): Dataframe to pull the schema of
        custom_redshift_columns (dict, Optional): 
            This dictionary contains custom column data type definitions for redshift.
            The params should be formatted as follows:
                - column name (str)
                - data type (str)

    Returns:
        PyArrow Schema of the given dataframe
    """
    fields = []
    for col, dtype in dataframe.dtypes.items():
        dtype = dtype.name
        if dtype == 'object':
            if custom_redshift_columns:
                # Detect if the Pandas object column contains Python decimal objects. 
                if "[Decimal(" in str(dataframe[col].values)[:9]:
                    # If Python decimal objects are present, parse out the precision and scale 
                    # from the custom_redshift_columns dictionary to use when converting 
                    # to PyArrow's decimal128 data type.
                    s = custom_redshift_columns[col]
                    precision = int(s[s.find('DECIMAL(')+len('DECIMAL('):s.rfind(',')].strip())
                    scale = int(s[s.find(',')+len(','):s.rfind(')')].strip())
                    pa_type = pa.decimal128(precision=precision, scale=scale)
                else:
                    pa_type = pa.string()
            else:
                pa_type = pa.string()
        elif dtype.startswith('int32'):
            pa_type = pa.int32()
        elif dtype.startswith('int64'):
            pa_type = pa.int64()
        elif dtype.startswith('int8'):
            pa_type = pa.int8()  
        elif dtype.startswith('Int32'):
            dataframe[col] = dataframe.astype({col: 'object'})
            pa_type = pa.int32()
        elif dtype.startswith('Int64'):
            dataframe[col] = dataframe.astype({col: 'object'})
            pa_type = pa.int64()
        elif dtype.startswith('float32'):
            pa_type = pa.float32()
        elif dtype.startswith('float64'):
            pa_type = pa.float64()
        elif dtype.startswith('float16'):
            pa_type = pa.float16()
        elif dtype.startswith('datetime'):
            pa_type = pa.timestamp('ns')
        elif dtype.startswith('date'):
            pa_type = pa.date64()
        elif dtype.startswith('category'):
            pa_type = pa.string()
        elif dtype == 'bool':
            pa_type = pa.bool_()
        else:
            raise NotImplementedError(
                f"Error: {dtype} is not a datatype which can be mapped to Parquet using s3parq.")
        fields.append(pa.field(col, pa_type))

    return pa.schema(fields=fields)


def _parse_dataframe_col_types(dataframe: pd.DataFrame, partitions: list, custom_redshift_columns: dict = None) -> dict:
    """ Determines the metadata of the partition columns based on dataframe dtypes

    Args:
        dataframe (pd.DataFrame): Dataframe to parse the types of
        partitions (list): Partitions in the dataframe to get the type of
        custom_redshift_columns (dict, Optional): 
            This dictionary contains custom column data type definitions for redshift.
            The params should be formatted as follows:
                - column name (str)
                - data type (str)

    Returns:
        Dictionary of column names as keys with their datatypes (string form) as values
    """
    logger.debug("Determining write metadata for publish...")
    dataframe = dataframe[partitions]
    dtypes = {}
    for col, dtype in dataframe.dtypes.items():
        dtype = dtype.name
        if dtype == 'object':
            dtypes[col] = 'string'
        elif dtype.startswith('int'):
            dtypes[col] = 'integer'
        elif dtype.startswith('float'):
            dtypes[col] = 'float'
        elif dtype.startswith('date'):
            dtypes[col] = 'datetime'
        elif dtype.startswith('category'):
            dtypes[col] = 'category'
        elif dtype == 'bool':
            dtypes[col] = 'boolean'

        if custom_redshift_columns:
            if 'DECIMAL' in custom_redshift_columns[col]:
                dtypes[col] = 'decimal'

    logger.debug(f"Done. Metadata determined as {dtypes}")
    return dtypes


def _sized_dataframes(dataframe: pd.DataFrame) -> tuple:
    """ Determines optimal chunks to publish the dataframe in. In smaller dataframes
    this may be the whole dataframe.
    This is determined by the hard values of 60MB being ideal for Spectrum, and 
    assumed compression ratio for dataframes to parquet being ~4; the bytes per
    row are then estimated and then chunks are determined.

    Args:
        dataframe (pd.DataFrame): Dataframe to size out

    Yields:
        A dictionary containing the upper and lower index of the dataframe to chunk
    """

    def log_size_estimate(num_bytes):
        if num_bytes == 0:
            return "0MB"
        elif round(num_bytes / float(1 << 20), 2) == 0.0:
            return "<0.1MB"
        else:
            return str(round(num_bytes / float(1 << 20), 2)) + "MB"

    # get first row size
    row_size_est = sys.getsizeof(dataframe.head(1))
    # get number of rows
    num_rows = int(dataframe.shape[0])
    frame_size_est = row_size_est * num_rows
    # at scale dataframes seem to compress around 3.5-4.5 times as parquet.
    # TODO: should build a real regression to calc this!
    compression_ratio = 4
    # 60MB compressed is ideal for Spectrum
    ideal_size = compression_ratio * (60 * float(1 << 20))
    # short circut if < ideal size
    batch_log_message = f"""row size estimate: {row_size_est} bytes.
number of rows: {num_rows} rows
frame size estimate: {frame_size_est} bytes
compression ratio: {compression_ratio}:1
ideal size: {ideal_size} bytes
"""
    logger.debug(batch_log_message)
    if ideal_size > frame_size_est:
        yield {'lower': 0, 'upper': len(dataframe)}
        return

    # math the number of estimated partitions
    sized_frames = []
    num_partitions = int(row_size_est * num_rows / ideal_size)
    rows_per_partition = int(num_rows / num_partitions)
    # for each partition do the thing
    logger.info(
        f"Sized out {len(range(0, num_rows, rows_per_partition))} dataframes.")
    for index, lower in enumerate(range(0, num_rows, rows_per_partition)):
        lower = lower if lower == 0 else lower + 1
        if index == num_partitions:
            upper = num_rows
        else:
            if lower == 0:
                upper = lower + rows_per_partition + 1
            else:
                upper = lower + rows_per_partition

        yield {'lower': lower, 'upper': upper}


def publish(bucket: str, key: str, partitions: List[str], dataframe: pd.DataFrame, redshift_params: dict = None) -> List[str]:
    """ Dataframe to S3 Parquet Publisher
    This function handles the portion of work that will see a dataframe converted
    to parquet and then published to the given S3 location.
    It supports partitions and will preserve the datatypes on s3parq fetch.
    It also has the option to automatically publish up to Redshift Spectrum for
    the newly published parquet files.

    Args:
        bucket (str): S3 Bucket name
        key (str): S3 key to lead to the desired dataset
        partitions (List[str]): List of columns that should be partitioned on
        dataframe (pd.DataFrame): Dataframe to be published
        redshift_params (dict, Optional):
            This dictionary should be provided in the following format in order
            for data to be published to Spectrum. Leave out entirely to avoid
            publishing to Spectrum.
            The params should be formatted as follows:

                - schema_name (str): Name of the Spectrum schema to publish to
                - table_name (str): Name of the table to write the dataset as
                - iam_role (str): Role to take while writing data to Spectrum
                - region (str): AWS region for Spectrum
                - cluster_id (str): Spectrum cluster id
                - host (str): Redshift Spectrum host name
                - port (str): Redshift Spectrum port to use
                - db_name (str): Redshift Spectrum database name to use
                - ec2_user (str): If on ec2, the user that should be used

    Returns:
        A str list of all the newly published object keys
    """
    session_helper = None

    if redshift_params:
        if "index" in dataframe.columns:
            raise ValueError(
                "'index' is a reserved keyword in Redshift. Please remove or rename your DataFrame's 'index' column.")

        logger.debug(
            "Found redshift parameters. Checking validity of params...")
        redshift_params = validate_redshift_params(redshift_params)
        logger.debug("Redshift parameters valid. Opening Session helper.")
        session_helper = SessionHelper(
            region=redshift_params['region'],
            cluster_id=redshift_params['cluster_id'],
            host=redshift_params['host'],
            port=redshift_params['port'],
            db_name=redshift_params['db_name'],
            ec2_user=redshift_params['ec2_user']
        )

        session_helper.configure_session_helper()
        publish_redshift.create_schema(
            redshift_params['schema_name'], redshift_params['db_name'], redshift_params['iam_role'], session_helper)
        logger.debug(
            f"Schema {redshift_params['schema_name']} created. Creating table {redshift_params['table_name']}...")

        df_types = _get_dataframe_datatypes(dataframe, partitions)
        partition_types = _get_dataframe_datatypes(dataframe, partitions, True)
        publish_redshift.create_table(
            redshift_params['table_name'], redshift_params['schema_name'], df_types, partition_types, s3_url(bucket, key), session_helper)
        logger.debug(f"Table {redshift_params['table_name']} created.")

    logger.debug("Checking publish params...")
    check_empty_dataframe(dataframe)
    check_dataframe_for_timedelta(dataframe)
    check_partitions(partitions, dataframe)
    logger.debug("Publish params valid.")
    logger.debug("Begin writing to S3..")

    files = []
    for frame_params in _sized_dataframes(dataframe):
        logger.info(
            f"Publishing dataframe chunk : {frame_params['lower']} to {frame_params['upper']}")
        frame = pd.DataFrame(
            dataframe[frame_params['lower']:frame_params['upper']])
        _gen_parquet_to_s3(bucket=bucket,
                           key=key,
                           dataframe=frame,
                           partitions=partitions)

        published_files = _assign_partition_meta(bucket=bucket,
                                                 key=key,
                                                 dataframe=frame,
                                                 partitions=partitions,
                                                 session_helper=session_helper,
                                                 redshift_params=redshift_params)
        files = files + published_files

    logger.info("Done writing to S3.")

    return files

def custom_publish(bucket: str, key: str, partitions: List[str], dataframe: pd.DataFrame, custom_redshift_columns: dict, redshift_params: dict = None) -> List[str]:
    """ Dataframe to S3 Parquet Publisher with a CUSTOM redshift column definition.
    Custom publish allows custom defined redshift column definitions to be used and 
    enables support for Redshift's decimal data type. 
    This function handles the portion of work that will see a dataframe converted
    to parquet and then published to the given S3 location.
    It supports partitions and will use the custom redshift columns defined in the
    custom_redshift_columns dictionary when creating the table schema for the parquet file. 
    View the Custom Publishes section of s3parq's readme file for more guidance on formatting
    the custom_redshift_columns dictionary. It also has the option to automatically publish up 
    to Redshift Spectrum for the newly published parquet files. 

    Args:
        bucket (str): S3 Bucket name
        key (str): S3 key to lead to the desired dataset
        partitions (List[str]): List of columns that should be partitioned on
        dataframe (pd.DataFrame): Dataframe to be published
        custom_redshift_columns (dict): 
            This dictionary contains custom column data type definitions for redshift.
            The params should be formatted as follows:
                - column name (str)
                - data type (str)
        redshift_params (dict, Optional):
            This dictionary should be provided in the following format in order
            for data to be published to Spectrum. Leave out entirely to avoid
            publishing to Spectrum.
            The params should be formatted as follows:
                - schema_name (str): Name of the Spectrum schema to publish to
                - table_name (str): Name of the table to write the dataset as
                - iam_role (str): Role to take while writing data to Spectrum
                - region (str): AWS region for Spectrum
                - cluster_id (str): Spectrum cluster id
                - host (str): Redshift Spectrum host name
                - port (str): Redshift Spectrum port to use
                - db_name (str): Redshift Spectrum database name to use
                - ec2_user (str): If on ec2, the user that should be used

    Returns:
        A str list of all the newly published object keys
    """
    logger.debug(
        "Running custom publish...")

    session_helper = None

    if redshift_params:
        if "index" in dataframe.columns:
            raise ValueError(
                "'index' is a reserved keyword in Redshift. Please remove or rename your DataFrame's 'index' column.")

        logger.debug(
            "Found redshift parameters. Checking validity of params...")
        redshift_params = validate_redshift_params(redshift_params)
        logger.debug("Redshift parameters valid. Opening Session helper.")
        session_helper = SessionHelper(
            region=redshift_params['region'],
            cluster_id=redshift_params['cluster_id'],
            host=redshift_params['host'],
            port=redshift_params['port'],
            db_name=redshift_params['db_name'],
            ec2_user=redshift_params['ec2_user']
        )

        session_helper.configure_session_helper()
        publish_redshift.create_schema(
            redshift_params['schema_name'], redshift_params['db_name'], redshift_params['iam_role'], session_helper)
        logger.debug(
            f"Schema {redshift_params['schema_name']} created. Creating table {redshift_params['table_name']}...")

        publish_redshift.create_custom_table(
            redshift_params['table_name'], redshift_params['schema_name'], partitions, s3_url(bucket, key), custom_redshift_columns, session_helper)
        logger.debug(f"Custom table {redshift_params['table_name']} created.")

    logger.debug("Checking publish params...")
    check_empty_dataframe(dataframe)
    check_dataframe_for_timedelta(dataframe)
    check_partitions(partitions, dataframe)
    logger.debug("Publish params valid.")
    logger.debug("Begin writing to S3..")

    files = []
    for frame_params in _sized_dataframes(dataframe):
        logger.info(
            f"Publishing dataframe chunk : {frame_params['lower']} to {frame_params['upper']}")
        frame = pd.DataFrame(
            dataframe[frame_params['lower']:frame_params['upper']])
        _gen_parquet_to_s3(bucket=bucket,
                           key=key,
                           dataframe=frame,
                           partitions=partitions,
                           custom_redshift_columns=custom_redshift_columns)

        published_files = _assign_partition_meta(bucket=bucket,
                                                 key=key,
                                                 dataframe=frame,
                                                 partitions=partitions,
                                                 session_helper=session_helper,
                                                 redshift_params=redshift_params,
                                                 custom_redshift_columns=custom_redshift_columns)
        files = files + published_files

    logger.info("Done writing to S3.")

    return files
