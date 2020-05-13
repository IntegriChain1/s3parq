import ast
import boto3
from collections import OrderedDict
import datetime
from distutils.util import strtobool
import logging
import operator
from typing import Dict, List, Any
import multiprocessing as mp
from multiprocessing import get_context
import s3fs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from .s3_naming_helper import S3NamingHelper

# Filter
# class Filter(NamedTuple):
#    partition: str
#    comparison: str
#    values: List[any]

Filter = {
    "partition": str,
    "comparison": str,
    "values": List[any]
}

OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt
}

NON_NUM_TYPES = [
    "string",
    "category",
    "bool",
    "boolean"
]


logger = logging.getLogger(__name__)


def get_all_partition_values(bucket: str, key: str, partition: str) -> iter:
    """ Returns all values, correctly typed, for a given partition IN NO ORDER

    Args:
        bucket (str): S3 Bucket name
        key (str): S3 key that leads to the desired dataset
        partition (str): The partition in the dataset to parse the values out of

    Returns:
        An iterable of all the partition values
    """

    all_files = get_all_files_list(bucket, key)

    if not all_files:
        return []

    partition_dtype = _get_partitions_and_types(
        first_file_key=all_files[0], bucket=bucket)[partition]
    partition_values = _parse_partitions_and_values(all_files, key=key)[
        partition]
    return [convert_type(val, partition_dtype) for val in partition_values]


def get_diff_partition_values(bucket: str, key: str, partition: str, values_to_diff: iter, reverse: bool = False) -> iter:
    """ Returns all the partition values in the dataset at the bucket/key
    that are not in values_to_diff

    Args:
        bucket (str): S3 Bucket name
        key (str): S3 key that leads to the desired dataset
        partition (str): The partition in the dataset to parse the values out of
        values_to_diff (iter): The iterable set of values to compare the partition values to
        reverse (bool, Optional): Determines if the operation should be inversed,
            if True it will look for the values in values_to_diff that are not
            in the partition values (basically backwards). Defaults to False

    Returns:
        An iterable of all the partition values that are not in values_to_diff
            and vice-versa if reverse is True
    """
    all_files = get_all_files_list(bucket, key)

    if not all_files:
        if reverse:
            # Keeping  ->set->list fuctionality to have consistent lack of duplicates
            diff = list(set(values_to_diff))
            return diff
        else:
            return []

    partition_dtype = _get_partitions_and_types(
        first_file_key=all_files[0], bucket=bucket)[partition]
    partition_values = _parse_partitions_and_values(all_files, key=key)[
        partition]

    partition_set = set(partition_values)
    values_to_diff_set = set([str(val) for val in values_to_diff])

    if not values_to_diff:
        if reverse:
            return []
        else:
            return [convert_type(val, partition_dtype) for val in partition_set]

    if reverse:
        diff = values_to_diff_set - partition_set
    else:
        diff = partition_set - values_to_diff_set
    return [convert_type(val, partition_dtype) for val in diff]


def get_max_partition_value(bucket: str, key: str, partition: str) -> any:
    """ Returns the max partition value in the dataset at the bucket/key
    NOTE: This functionality cannot occur on non-numerical datatypes!

    Args:
        bucket (str): S3 Bucket name
        key (str): S3 key that leads to the desired dataset
        partition (str): The partition in the dataset to parse the values out of

    Returns:
        The max partition value, of the matching partition datatype
    """
    S3NamingHelper().validate_bucket_name(bucket_name=bucket)

    all_files = get_all_files_list(bucket=bucket, key=key)

    if not all_files:
        return None

    partition_dtype = _get_partitions_and_types(
        first_file_key=all_files[0], bucket=bucket)[partition]
    partition_values = _parse_partitions_and_values(
        file_paths=all_files, key=key)[partition]

    if partition_dtype in NON_NUM_TYPES:
        raise ValueError(
            f"Max cannot be used on partition types of {partition_dtype}")

    return max([convert_type(val, partition_dtype) for val in partition_values])


def fetch(bucket: str, key: str, filters: List[type(Filter)] = {}, parallel: bool = True, accept_not_s3parq: bool = True) -> pd.DataFrame:
    """ S3 Parquet to Dataframe Fetcher.
    This function handles the portion of work that will return a concatenated 
    dataframe based on the partition filters of the specified dataset.

    Args:
        bucket (str): S3 Bucket name
        key (str): S3 key that leads to the desired dataset
        filters (List(Filter), Optional): filters to be applied to the Dataset Partitions
            Filters have the fields:

                - partition (str):
                    The published partition to be applied to
                - comparison (str):
                    Comparison function - one of: [ == , != , > , < , >= , <= ]
                - values (List(any)): 
                    Values to compare to - must match partition data type
                    May not use multiple values with the '<', '>' comparisons
        parallel (bool, Optional):
            Determines if multiprocessing should be used, defaults to True
            Whether this is more or less efficient is dataset dependent,
                smaller datasets run much quicker without parallel
        accept_not_s3parq (bool, Optional):
            Determines whether it will fetch dataframes that have not been published
                with S3Parq and are thus lacking metadata. This does not support
                partitioned parquet.

    Returns:
        A pandas dataframe of the filtered results from S3
    """

    _validate_filter_rules(filters)
    S3NamingHelper().validate_bucket_name(bucket)

    all_files = get_all_files_list(bucket, key)

    if not all_files:
        logger.debug(f"No files present under : {key} :, returning empty DataFrame")
        return pd.DataFrame()

    partition_metadata = _get_partitions_and_types(all_files[0], bucket)

    if partition_metadata is None:
        if accept_not_s3parq:
            logger.info("Parquet files do not have S3Parq metadata, fetching anyways.")
            return _get_filtered_data(bucket=bucket, paths=all_files, partition_metadata={},
                              parallel=parallel)
        else:
            raise MissingS3ParqMetadata("Parquet files are missing s3parq metadata, enable 'accept_not_s3parq' if you'd like this to pass.")

    _validate_matching_filter_data_type(partition_metadata, filters)

    # strip out the filenames (which we need)
    partition_values = _parse_partitions_and_values(all_files, key)

    typed_values = _get_partition_value_data_types(
        partition_values, partition_metadata)

    # filtered_paths is a list of the S3 prefixes from which we want to load data
    filtered_paths = _get_filtered_key_list(typed_values, filters, key)

    files_to_load = []

    for file in all_files:
        for prefix in filtered_paths:
            if file.startswith(prefix):
                files_to_load.append(file)

    # if there is no data matching the filters, return an empty DataFrame
    # with correct headers and type
    if len(files_to_load) < 1:
        logger.debug(f"Dataset filters left no matching values, returning empty dataframe with matching headers")
        sacrifical_files = [all_files[0]]
        sacrifical_frame = _get_filtered_data(
            bucket=bucket, paths=sacrifical_files, partition_metadata=partition_metadata, parallel=parallel)
        return sacrifical_frame.head(0)

    return _get_filtered_data(bucket=bucket, paths=files_to_load, partition_metadata=partition_metadata,
                              parallel=parallel)


def fetch_diff(input_bucket: str, input_key: str, comparison_bucket: str, comparison_key: str, partition: str, reverse: bool = False, parallel: bool = True) -> pd.DataFrame:
    """ Returns a dataframe of whats in the input dataset but not the comparison 
    dataset by the specified partition.

    Args:
        input_bucket (str): The bucket of the dataset to start from
        input_key (str): The key to the dataset to start from
        comparison_bucket (str): The bucket of the dataset to compare against
        comparison_key (str): The key to the dataset to compare against
        partition (str): The partition in the dataset to compare the values of
        reverse (bool, Optional): Determines if the operation should be inversed,
            if True it will look for the values in comparison that are not 
            in the input (basically backwards). Defaults to False
        parallel (bool, Optional):
            Determines if multiprocessing should be used, defaults to True.
            Whether this is more or less efficient is dataset dependent,
            smaller datasets run much quicker without parallel

    Returns:
        A dataframe of all values in the input but not the comparison,
            if reverse=True then vice-versa
    """
    S3NamingHelper().validate_bucket_name(input_bucket)
    S3NamingHelper().validate_bucket_name(comparison_bucket)

    comparison_values = get_all_partition_values(
        bucket=comparison_bucket, key=comparison_key, partition=partition)

    diff_values = get_diff_partition_values(
        bucket=input_bucket,
        key=input_key,
        partition=partition,
        values_to_diff=comparison_values,
        reverse=reverse
    )

    filters = [{
        "partition": partition,
        "comparison": "==",
        "values": diff_values
    }]

    logger.debug(f"Fetching difference, looking for values : {diff_values} : under partition : {partition} ")

    if reverse:
        return fetch(bucket=comparison_bucket, key=comparison_key, filters=filters, parallel=parallel)
    else:
        return fetch(bucket=input_bucket, key=input_key, filters=filters, parallel=parallel)


def convert_type(val: Any, dtype: str) -> Any:
    """ Converts the given value to the given datatype

    Args:
        val (Any): The value to convert
        dtype (str): The type to attempt to convert to

    Returns:
        The value parsed into the new dtype
    """
    if dtype == 'string' or dtype == 'str':
        return str(val)
    elif dtype == 'integer' or dtype == 'int':
        return int(val)
    elif dtype == 'float':
        return float(val)
    elif dtype == 'datetime':
        return datetime.datetime.strptime(
            val, '%Y-%m-%d %H:%M:%S')
    elif dtype == 'category':
        return pd.Category(val)
    elif dtype == 'bool' or dtype == 'boolean':
        return bool(strtobool(val))


def dtype_to_pandas_dtype(dtype: str) -> str:
    """ Matches the given dtype to its pandas equivalent, if one exists

    Args:
        dtype (str): The type to attempt to match

    Returns:
        The pandas version of the dtype if one exists, otherwise the original
    """
    if dtype == "integer":
        dtype = "int"
    elif dtype == "string":
        dtype = "str"
    elif dtype == "boolean":
        dtype = "bool"

    return dtype


def get_all_files_list(bucket: str, key: str) -> list:
    """ Get a list of all files to get all partitions values.
    Necesarry to catch all partition values for non-filtered partiions.
    NOTE: This paginates across as many as are matching keys in the bucket;
        be mindful of any costs in extra large buckets without a specific key.

    Args:
        bucket (str): S3 bucket to search in
        key (str): S3 key to the dataset to check

    Returns:
        A list of the keys of all objects in the bucket/key that end with .parquet
    """
    objects_in_bucket = []
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket,
                            'Prefix': key}
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        if not "Contents" in page.keys():
            break

        for item in page['Contents']:
            if item['Key'].endswith('.parquet'):
                objects_in_bucket.append(item['Key'])

    return objects_in_bucket


def _get_partitions_and_types(first_file_key: str, bucket: str) -> dict:
    """ Fetch a list of all the partitions actually there and their 
    datatypes. List may be different than passed list if not being used
    for filtering on.
    NOTE: This is pulled from the metadata. It is assumed that this package
        is being used with its own publish method.

    Args:
        first_file_key (str): The key to the first file in the dataset, to get
            the metadata off of
        bucket (str): The S3 bucket to run in

    Returns:
        The parsed metadata from heading the first file
    TODO
    """
    s3_client = boto3.client('s3')

    first_file = s3_client.head_object(
        Bucket=bucket,
        Key=first_file_key
    )

    metadata = first_file['Metadata']
    partition_metadata_raw = metadata.get("partition_data_types", None)

    if partition_metadata_raw is None:
        return None

    partition_metadata = ast.literal_eval(partition_metadata_raw)

    return partition_metadata


def _parse_partitions_and_values(file_paths: List[str], key: str) -> dict:
    """ Parses the string keys into a usable dict of the parts and values

    Args:
        file_paths (List[str]): The list of all files to parse out
        key (str): S3 key to the root of the dataset of the files

    Returns:
        A dictionary of all partitions with their values
    """
    # TODO: find more neat/efficient way to do this
    parts = OrderedDict()
    key_len = len(key)
    for file_path in file_paths:
        # Delete key, split the parts out, delete the file name
        file_path = file_path[key_len:]
        unparsed_parts = file_path.split("/")
        del unparsed_parts[-1]

        for part in unparsed_parts:
            if "=" in part:
                key, value = part.split("=")
                if key not in parts:
                    parts.update({key: set([value])})
                else:
                    parts[key].add(value)

    return parts


def _get_partition_value_data_types(parsed_parts: dict, part_types: dict) -> dict:
    """ Uses the partitions with their known types to parse them out

    Args:
        parsed_parts (dict): A dictionary of all partitions with their values
        part_types (dict): A dictionary of all partitions to their datatypes

    Returns:
        A dictionary of all partitions with their values parsed into the correct datatype
    """
    for part, values in parsed_parts.items():
        part_type = part_types[part]
        if (part_type == 'string') or (part_type == 'category'):
            continue
        elif (part_type == 'int') or (part_type == 'integer'):
            parsed_parts[part] = set(map(int, values))
        elif part_type == 'float':
            parsed_parts[part] = set(map(float, values))
        elif part_type == 'datetime':
            parsed_parts[part] = set(
                map(lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"), values))
        elif (part_type == 'bool') or (part_type == 'boolean'):
            parsed_parts[part] = set(map(bool, values))
        else:
            logger.debug(f"Unknown partition type : {part_type} :, leaving as a string")

    return parsed_parts


def _get_filtered_key_list(typed_parts: dict, filters: List[type(Filter)], key: str) -> List[str]:
    """ Create list of all "paths" to files after the filtered partitions
    are set ie all non-matching partitions are excluded.

    Args:
        typed_parts (dict): A dictionary of all partitions with their values
        filters (List[type(Filter)]): A dictionary of all partitions to their datatypes
        key (str): S3 key to the base root of the dataset

    Returns:
        A list of object keys that have been filtered by partition values
    """
    filter_keys = []
    matched_parts = OrderedDict()
    matched_parts.keys = typed_parts.keys()
    matched_part_vals = set()

    for part, part_values in typed_parts.items():
        matched_part_vals.clear()
        fil = next((f for f in filters if f['partition'] == part), False)
        if fil:
            comparison = OPS[fil['comparison']]
            for v in fil['values']:
                for x in part_values:
                    if comparison(x, v):
                        matched_part_vals.add(x)
            matched_parts[part] = matched_part_vals.copy()
        else:
            matched_parts[part] = part_values

    def construct_paths(matched_parts, previous_fil_keys: List[str]) -> None:
        if len(matched_parts) > 0:
            part = matched_parts.popitem(last=False)
            new_filter_keys = list()
            for value in part[1]:
                mapped_keys = list(map(
                    (lambda x: str(x) +
                     str(part[0]) + "=" + str(value) + "/"),
                    previous_fil_keys
                ))
                new_filter_keys = new_filter_keys + mapped_keys

            construct_paths(matched_parts, new_filter_keys)
        else:
            filter_keys.append(previous_fil_keys)

    construct_paths(matched_parts, [f"{key}/"])
    # TODO: fix the below mess with random array
    return filter_keys[0]


def _get_filtered_data(bucket: str, paths: List[str], partition_metadata: dict, parallel: bool = True) -> pd.DataFrame:
    """ Gets the data based on the filtered object key list. Concatenates all 
    the separate parquet files.

    Args:
        bucket (str): S3 bucket to fetch from
        paths (List[str]): A list of all the object keys of the parquet files
        partition_metadata (dict): A dictionary of all partitions to their datatypes
        parallel (bool, Optional):
            Determines if multiprocessing should be used, defaults to True.
            Whether this is more or less efficient is dataset dependent,
            smaller datasets run much quicker without parallel

    Returns:
        The dataframe of all the parquet files from the given keys
    """

    temp_frames = []

    def append_to_temp(frame: pd.DataFrame):
        temp_frames.append(frame)

    if parallel:
        with get_context("spawn").Pool() as pool:
            for path in paths:
                logger.debug(f"[Parallel] Fetching parquet file to append from : {path} ")
                append_to_temp(
                    pool.apply_async(_s3_parquet_to_dataframe, args=(bucket, path, partition_metadata)).get())
            pool.close()
            pool.join()
    else:
        for path in paths:
            logger.debug(f"Fetching parquet file to append from : {path} ")
            append_to_temp(_s3_parquet_to_dataframe(
                bucket, path, partition_metadata))

    return pd.concat(temp_frames, ignore_index=True)


def _s3_parquet_to_dataframe(bucket: str, key: str, partition_metadata: dict) -> pd.DataFrame:
    """ Grab a parquet file from s3 and convert to pandas df, add it to the destination

    Args:
        bucket (str): S3 bucket to fetch from
        key (key): The full object path of the parquet file to read
        partition_metadata (dict): A dictionary of all partitions to their datatypes

    Returns:
        Dataframe from the key parquet, with partitions repopulated
    """
    s3 = s3fs.S3FileSystem()
    uri = f"{bucket}/{key}"
    table = pq.ParquetDataset(uri, filesystem=s3)
    frame = table.read_pandas().to_pandas()

    if partition_metadata != {}:
        partitions = _repopulate_partitions(uri, partition_metadata)
        for k, v in partitions.items():
            frame[k] = v

    return frame


def _repopulate_partitions(partition_string: str, partition_metadata: dict) -> tuple:
    """ For each val in the partition string creates a list that can be added back into the dataframe

    Args:
        partition_string (str): S3 bucket to fetch from
        partition_metadata (dict): A dictionary of all partitions to their datatypes

    Returns:
        Dataframe from the key parquet, with partitions repopulated
    """
    raw = partition_string.split('/')
    partitions = {}
    for string in raw:
        if '=' in string:
            k, v = string.split('=')
            partitions[k] = v

    if partition_metadata:
        for key, val in partitions.items():
            partitions[key] = convert_type(val, partition_metadata[key])
    
    return partitions


def _validate_filter_rules(filters: List[type(Filter)]) -> None:
    """ Validate that the filters are the correct format and follow basic
    comparison rules, otherwise throw a ValueError

    Args:
        filters (List[type(Filter)]): List of filters to validate

    Returns:
        None
    """

    single_value_comparisons = [
        ">",
        "<",
        "<=",
        ">="
    ]
    for f in filters:
        if not all(key in f for key in ("partition", "comparison", "values")):
            raise ValueError(
                "Filters require partition, comparison, and values.")
        elif f["comparison"] not in OPS:
            raise ValueError(
                f"Comparison {f['comparison']} is not supported.")
        elif (f["comparison"] in single_value_comparisons) & (len(f["values"]) != 1):
            raise ValueError(
                f"Comparison {f['comparison']} can only be used with one filter value.")


def _validate_matching_filter_data_type(part_types, filters: List[type(Filter)]) -> None:
    """ Validate that the filters passed are matching to the partitions'
    listed datatypes, otherwise throw a ValueError. 
    This includes validating comparisons too.

    Args:
        part_types (dict): A dictionary of all partitions to their datatypes
        filters (List[type(Filter)]): List of filters to validate

    Returns:
        None
    """
    num_comparisons = [
        ">",
        "<",
        "<=",
        ">="
    ]

    for f in filters:
        try:
            fil_part = part_types[f["partition"]]
        except:
            raise ValueError("Filter does not have a matching partition.")

        if (f["comparison"] in num_comparisons):
            if fil_part in NON_NUM_TYPES:
                raise ValueError(
                    f"Comparison {f['comparison']} cannot be used on partition types of {fil_part}")


# Custom error for catching
class MissingS3ParqMetadata(Exception):
    """ This class is meant to be an Exception thrown only when parquet files
    being fetched are not S3Parq compatible and the option to fetch anyways is
    disabled.
    """
    pass
