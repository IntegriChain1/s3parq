import boto3
from contextlib import ExitStack
from dfmock import DFMock
import logging
import moto
import os
import pandas as pd
from pandas.util.testing import assert_frame_equal
import pyarrow as pa
import pyarrow.parquet as pq
import random
from string import ascii_lowercase
import tempfile
from typing import Dict

logger = logging.getLogger(__name__)
logger.warning(
    "s3parq.testing_helper is made for internal use and may change unpredictably")


"""
Assertion-based helper functions:
- df_equal_by_set
- sorted_dfs_equal_by_pandas_testing

Setup-based helper functions:
- setup_files_list
- setup_grouped_dataframe
- setup_partitioned_parquet
- setup_random_string
"""


# START ASSERTION BASED


def df_equal_by_set(df1: pd.DataFrame, df2: pd.DataFrame, cols: iter) -> bool:
    """ This checks over whether 2 dataframes are equal by comparing zips of
    the given columns

    Args:
        df1 (pd.DataFrame): The first dataframe to compare against
        df2 (pd.DataFrame): The second dataframe to compare against
        cols (iter): The column to compare on the dataframes

    Returns:
        bool of whether the dataframe columns are equal
    """
    set1, set2 = list(), list()
    for col in cols:
        set1.append(df1[col])
        set2.append(df2[col])
    zipped1 = set(zip(*set1))
    zipped2 = set(zip(*set2))

    return (zipped1 == zipped2)


def sorted_dfs_equal_by_pandas_testing(df1: pd.DataFrame, df2: pd.DataFrame) -> None:
    """ Wraps the pandas.util.testing assert_frame_equal with the boilerplate
    of fixing any irregularities from the publish/fetch process - this
    includes index and sorting, as those will almost never match
    Args:
        df1 (pd.DataFrame): The first dataframe to compare
        df2 (pd.DataFrame): The second dataframe to compare
    Raises:
        AttributeError is the two dataframes are not equivalent
    """
    # make the column order match
    df2 = df2[df1.columns]
    # make them the same format, remerging partitions changes it all around
    df2 = df2.sort_values(
        by=df1.columns.tolist()).reset_index(drop=True)
    df1 = df1.sort_values(
        by=df1.columns.tolist()).reset_index(drop=True)
    # The setup on this part is a pain but it's the most specific check
    assert_frame_equal(df2, df1)


# START SETUP BASED


@moto.mock_s3
def setup_files_list(count: int = 100, bucket: str = None, key: str = None):
    """ Creates temporary files and publishes them to the mocked S3 to test fetches,
    will fill in unsupplied parameters with random values or defaults

    Args:
        count (int, Optional): How many files to create, defaults to 100
        bucket (str, Optional): bucket to create to put parquet files in,
            will be a random string if not supplied
        key (str, Optional): S3 key to put parquet files in, will be a random
            string if not supplied
        partition_data_types (Dict, Optional): partition columns and datatypes,
            will be the default if not supplied

    Returns:
        A dict of retrieval informations (bucket, key, files)
    """
    if not key:
        key = setup_random_string()
    if not bucket:
        bucket = setup_random_string()
    temp_file_names = []
    s3_client = boto3.client('s3')
    s3_client.create_bucket(Bucket=bucket)

    with tempfile.TemporaryDirectory() as tmp_dir:
        for x in range(count):
            with tempfile.NamedTemporaryFile(dir=tmp_dir) as t:
                head, tail = os.path.split(t.name)
                temp_file_names.append(str(tail))
                with open(t.name, 'rb') as data:
                    s3_client.upload_fileobj(
                        data, Bucket=bucket, Key=(key+"/"+tail + ".parquet"))

    retrieval_ops = {
        "bucket": bucket,
        "key": key,
        "files": temp_file_names
    }

    return retrieval_ops


def setup_grouped_dataframe(count: int = 100, columns: Dict = None):
    """ Creates mock dataframe using dfmock

    Args:
        count (int): Row length to generate on the dataframe
        columns (Dict): dictionary of columns and types, following dfmock guides

    Returns:
        A freshly mocked dataframe
    """
    df = DFMock()
    df.count = count
    if not columns:
        columns = {"string_col": {"option_count": 3, "option_type": "string"},
                   "int_col": {"option_count": 3, "option_type": "int"},
                   "float_col": {"option_count": 3, "option_type": "float"},
                   "bool_col": {"option_count": 3, "option_type": "bool"},
                   "datetime_col": {"option_count": 3, "option_type": "datetime"},
                   "text_col": "string",
                   "metrics": "int"
                   }
    df.columns = columns
    df.generate_dataframe()
    return df.dataframe


def setup_partitioned_parquet(
    dataframe: pd.DataFrame = None,
    bucket: str = None,
    key: str = None,
    partition_data_types: Dict = None,
    s3_client=None
):
    """ Creates temporary files and publishes them to the mocked S3 to test fetches,
    will fill in unsupplied parameters with random values or defaults

    Args:
        dataframe (pd.DataFrame): dataframe to split into parquet files
        bucket (str, Optional): bucket to create to put parquet files in,
            will be a random string if not supplied
        key (str, Optional): S3 key to put parquet files in, will be a random
            string if not supplied
        partition_data_types (Dict, Optional): partition columns and datatypes,
            will be the default if not supplied
        s3_client (boto3 S3 client, Optional): The started S3 client that boto
            uses - NOTE: this should be made under a moto S3 mock!
            If it is not provided, a session is crafted under moto.mock_s3

    Returns:
        A tuple of the bucket and the published parquet file paths
    """
    if dataframe is None:
        dataframe = setup_grouped_dataframe()
    if not key:
        key = setup_random_string()
    if not bucket:
        bucket = setup_random_string()
    if partition_data_types is None:
        partition_data_types = {
            "string_col": "string",
            "int_col": "integer",
            "float_col": "float",
            "bool_col": "boolean",
            "datetime_col": "datetime"
        }

    with ExitStack() as stack:
        tmp_dir = stack.enter_context(tempfile.TemporaryDirectory())
        if not s3_client:
            stack.enter_context(moto.mock_s3())
            s3_client = boto3.client('s3')

        s3_client.create_bucket(Bucket=bucket)

        # generate the local parquet tree
        table = pa.Table.from_pandas(dataframe)
        pq.write_to_dataset(table,
                            root_path=str(tmp_dir),
                            partition_cols=list(
                                partition_data_types.keys())
                            )

        parquet_paths = []

        # traverse the local parquet tree
        extra_args = {'partition_data_types': str(
            partition_data_types)}
        for subdir, dirs, files in os.walk(str(tmp_dir)):
            for file in files:
                full_path = os.path.join(subdir, file)
                full_key = '/'.join([key,
                                     subdir.replace(f"{tmp_dir}/", ''), file])
                with open(full_path, 'rb') as data:
                    s3_client.upload_fileobj(data, Bucket=bucket, Key=full_key, ExtraArgs={
                        "Metadata": extra_args})
                    parquet_paths.append(full_key)

    return bucket, parquet_paths


def setup_nons3parq_parquet(
    dataframe: pd.DataFrame = None,
    bucket: str = None,
    key: str = None,
    s3_client=None
):
    """ Creates temporary files and publishes them to the mocked S3 to test fetches,
    will fill in unsupplied parameters with random values or defaults

    Args:
        dataframe (pd.DataFrame): dataframe to split into parquet files
        bucket (str, Optional): bucket to create to put parquet files in,
            will be a random string if not supplied
        key (str, Optional): S3 key to put parquet files in, will be a random
            string if not supplied
        s3_client (boto3 S3 client, Optional): The started S3 client that boto
            uses - NOTE: this should be made under a moto S3 mock!
            If it is not provided, a session is crafted under moto.mock_s3

    Returns:
        A tuple of the bucket and the published parquet file paths
    """
    if dataframe is None:
        dataframe = setup_grouped_dataframe()
    if not key:
        key = setup_random_string()
    if not bucket:
        bucket = setup_random_string()

    with ExitStack() as stack:
        tmp_dir = stack.enter_context(tempfile.TemporaryDirectory())
        if not s3_client:
            stack.enter_context(moto.mock_s3())
            s3_client = boto3.client('s3')

        s3_client.create_bucket(Bucket=bucket)

        # generate the local parquet tree
        table = pa.Table.from_pandas(dataframe)
        pq.write_to_dataset(table,root_path=str(tmp_dir), partition_cols=[])

        parquet_paths = []

        # traverse the local parquet tree
        for subdir, dirs, files in os.walk(str(tmp_dir)):
            for file in files:
                full_path = os.path.join(subdir, file)
                full_key = '/'.join([key,
                                     subdir.replace(f"{tmp_dir}/", ''), file])
                with open(full_path, 'rb') as data:
                    s3_client.upload_fileobj(data, Bucket=bucket, Key=full_key)
                    parquet_paths.append(full_key)

    return bucket, parquet_paths


def setup_random_string(min_len: int = 0, max_len: int = 10):
    """ Create a random string of either given min_lento max_len or default 0 to 10 """
    return ''.join([random.choice(ascii_lowercase) for x in range(min_len, max_len)])

def setup_custom_redshift_columns_and_dataframe():
    """ Create a custom_redshift_columns dictionary that contains redshift column definitions and corresponding mock dataframe """ 
    sample_data = {'colA': ["A","B","C"], 'colB': [4,5,6], 'colC': [4.12,5.22,6.57], 'colD': [4.1289,5.22,6.577], 'colE': ["test1","test2","test3"], 'colF': [True,False,True]}
    dataframe = pd.DataFrame(data=sample_data)

    custom_redshift_columns = {"colA":"VARCHAR(1000)", 
                            "colB":"BIGINT",
                            "colC":"REAL",
                            "colD":"DECIMAL(5,4)",
                            "colE":"VARCHAR",
                            "colF":"BOOLEAN"}

    return (dataframe, custom_redshift_columns)
