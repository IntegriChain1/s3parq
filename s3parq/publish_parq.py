import boto3, s3fs, re, sys, logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List
from s3parq.session_helper import SessionHelper
from s3parq import publish_redshift
from sqlalchemy import Column, Integer, String

logger = logging.getLogger(__name__)


def check_empty_dataframe(dataframe: pd.DataFrame) -> None:
    """ Make sure that the dataframe being published is not empty. """
    if dataframe.empty:
        no_data_message = "Empty dataframes cannot be published."
        raise ValueError(no_data_message)


def check_dataframe_for_timedelta(dataframe: pd.DataFrame)->None:
    """Pyarrow can't do timedelta."""
    dtypes = set(dataframe.columns)
    for dtype in dtypes:
        if 'timedelta' in dtype:
            timedelta_message = "Pyarrow does not support parquet conversion of timedelta columns at this time."
            logger.debug(timedelta_message)
            raise NotImplementedError(timedelta_message)

def _check_partition_compatibility(partition: str) -> bool:
    """ Make sure each partition value is hive-allowed."""
    reserved = "ALL ALTER AND ARRAY AS AUTHORIZATION BETWEEN BIGINT BINARY BOOLEAN BOTH BY CASE CAST CHAR COLUMN CONF CREATE CROSS CUBE CURRENT CURRENT_DATE CURRENT_TIMESTAMP CURSOR DATABASE DATE DECIMAL DELETE DESCRIBE DISTINCT DOUBLE DROP ELSE END EXCHANGE EXISTS EXTENDED EXTERNAL FALSE FETCH FLOAT FOLLOWING FOR FROM FULL FUNCTION GRANT GROUP GROUPING HAVING IF IMPORT IN INNER INSERT INT INTERSECT INTERVAL INTO IS JOIN LATERAL LEFT LESS LIKE LOCAL MACRO MAP MORE NONE NOT NULL OF ON OR ORDER OUT OUTER OVER PARTIALSCAN PARTITION PERCENT PRECEDING PRESERVE PROCEDURE RANGE READS REDUCE REVOKE RIGHT ROLLUP ROW ROWS SELECT SET SMALLINT TABLE TABLESAMPLE THEN TIMESTAMP TO TRANSFORM TRIGGER TRUE TRUNCATE UNBOUNDED UNION UNIQUEJOIN UPDATE USER USING UTC_TMESTAMP VALUES VARCHAR WHEN WHERE WINDOW WITH COMMIT ONLY REGEXP RLIKE ROLLBACK START CACHE CONSTRAINT FOREIGN PRIMARY REFERENCES DAYOFWEEK EXTRACT FLOOR INTEGER PRECISION VIEWS TIME NUMERIC SYNC".split()
    return not (partition.upper() in reserved)

def check_partitions(partitions: iter, dataframe: pd.DataFrame)->None:
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

def check_redshift_params(redshift_params: dict):
    expected_params = ["schema_name", "table_name", "iam_role", "region", "cluster_id", "host", "port", "db_name", "ec2_user"]
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
        

    logger.debug('Done checking redshift params')


def s3_url(bucket: str, key: str):
    return '/'.join(["s3:/", bucket, key])

def _gen_parquet_to_s3(bucket: str, key: str, dataframe: pd.DataFrame,
                       partitions: list) -> None:
    """ pushes the parquet dataset directly to s3. """
    logger.info("Writing to S3...")
    table = pa.Table.from_pandas(df=dataframe, schema=_parquet_schema(dataframe), preserve_index=False)

    uri = s3_url(bucket, key)
    logger.debug(f"Writing to s3 location: {uri}...")
    pq.write_to_dataset(table, compression="snappy", root_path=uri,
                        partition_cols=partitions, filesystem=s3fs.S3FileSystem(), coerce_timestamps='ms', allow_truncated_timestamps=True)
    logger.debug("Done writing to location.")


def _assign_partition_meta(bucket: str, key: str, dataframe: pd.DataFrame, partitions: List['str'], session_helper: SessionHelper, redshift_params=None) -> List[str]:
    """ assigns the dataset partition meta to all keys in the dataset"""
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
                        sql_command = publish_redshift.create_partitions(bucket, redshift_params['schema_name'], redshift_params['table_name'], obj['Key'], session_helper)

    for obj in all_files_without_meta:
        logger.debug(f"Appending metadata to file {obj}..")
        s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': obj}, Key=obj,
                              Metadata={'partition_data_types': str(
                                  _parse_dataframe_col_types(
                                      dataframe=dataframe, partitions=partitions)
                              )}, MetadataDirective='REPLACE')
        logger.debug("Done appending metadata.")
    return all_files_without_meta

def _get_dataframe_datatypes(dataframe: pd.DataFrame, partitions=[], use_parts=False) -> dict:
    """ returns key/value paired dictionary of a dataframe's column names and column datatypes.
        if partitions is included, only return datatypes for partition columns. """
    types = dict()
    if use_parts:
        columns = partitions
    else:
        columns = dataframe.drop(labels=partitions, axis="columns").columns
    for col in columns:
        type_string = dataframe[col].dtype.name
        types[col] = type_string
    return types

def _parquet_schema(dataframe: pd.DataFrame)->pa.Schema:
    """returns a parquet schema corresponding to a passed pandas dataframe. This schema is used for the construction of
        a parquet Table from pandas."""

    fields = []
    for col, dtype in dataframe.dtypes.items():
        dtype = dtype.name
        if dtype == 'object':
            pa_type = pa.string()
        elif dtype.startswith('int32'):
            pa_type = pa.int32()
        elif dtype.startswith('int64'):
            pa_type = pa.int64()
        elif dtype.startswith('float32'):
            pa_type = pa.float32()
        elif dtype.startswith('float64'):
            pa_type = pa.float64()
        elif dtype.startswith('datetime'):
            pa_type = pa.timestamp('ns')
        elif dtype.startswith('date'):
            pa_type = pa.date64()
        elif dtype.startswith('category'):
            pa_type = pa.string()
        elif dtype == 'bool':
            pa_type = pa.bool_()
        else:
            raise NotImplementedError(f"Error: {dtype} is not a datatype which can be mapped to Parquet using s3parq.")
        fields.append(pa.field(col, pa_type))

    return pa.schema(fields=fields)
        

def _parse_dataframe_col_types(dataframe: pd.DataFrame, partitions: list) -> dict:
    """ Returns a dict with the column names as keys, the data types (in strings) as values."""
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
    logger.debug(f"Done.Metadata set as {dtypes}")
    return dtypes

def _sized_dataframes(dataframe: pd.DataFrame) -> tuple:
    """Takes a dataframe and slices it into sized dataframes for optimal parquet sizes in S3.
        RETURNS a tuple of dataframes.         
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
        return tuple([dataframe])

    # math the number of estimated partitions
    sized_frames = []
    num_partitions = int(row_size_est * num_rows / ideal_size)
    rows_per_partition = int(num_rows / num_partitions)
    # for each partition do the thing
    for index, lower in enumerate(range(0, num_rows, rows_per_partition)):
        lower = lower if lower == 0 else lower + 1
        if index + 1 == num_partitions:
            upper = num_rows
        else:
            if lower == 0:
                upper = lower + rows_per_partition + 1
            else:
                upper = lower + rows_per_partition
        
        logger.debug(f"Appending dataframe chunk : df[{lower}:{upper}]")
        sized_frames.append(dataframe[lower:upper])
    logger.info(f"sized out {len(sized_frames)} dataframes.")
    return tuple(sized_frames)


def publish(bucket: str, key: str, partitions: List['str'], dataframe: pd.DataFrame, redshift_params = None) -> None:
    """Redshift Params:
        ARGS: 
            schema_name: str
            table_name: str
            iam_role: str
            region: str
            cluster_id: str
            host: str 
            port: str 
            db_name: str
    """
    session_helper = None

    if redshift_params:
        if "index" in dataframe.columns:
            raise ValueError("'index' is a reserved keyword in Redshift. Please remove or rename your DataFrame's 'index' column.")

        logger.debug("Found redshift parameters. Checking validity of params...")
        check_redshift_params(redshift_params)
        logger.debug("Redshift parameters valid. Opening Session helper.")
        session_helper = SessionHelper(
            region = redshift_params['region'],
            cluster_id = redshift_params['cluster_id'],
            host = redshift_params['host'],
            port = redshift_params['port'],
            db_name = redshift_params['db_name'],
            ec2_user = redshift_params['ec2_user']
        )
        session_helper.configure_session_helper()
        publish_redshift.create_schema(redshift_params['schema_name'], redshift_params['db_name'], redshift_params['iam_role'], session_helper)
        logger.debug(f"Schema {redshift_params['schema_name']} created. Creating table {redshift_params['table_name']}...")

        df_types = _get_dataframe_datatypes(dataframe, partitions)
        partition_types = _get_dataframe_datatypes(dataframe, partitions, True)
        publish_redshift.create_table(redshift_params['table_name'], redshift_params['schema_name'], df_types, partition_types, s3_url(bucket, key), session_helper)
        logger.debug(f"Table {redshift_params['table_name']} created.")

    logger.info("Checking params...")
    check_empty_dataframe(dataframe)
    check_dataframe_for_timedelta(dataframe)
    check_partitions(partitions, dataframe) 
    logger.info("Params valid.")
    logger.debug("Begin writing to S3..")

    files = []
    for frame in _sized_dataframes(dataframe):
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

    logger.debug("Done writing to S3.")

    return files
