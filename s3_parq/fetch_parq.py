import ast
import boto3
from collections import OrderedDict
import datetime
import operator
from typing import List
import multiprocessing as mp
from multiprocessing import get_context
import s3fs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from .s3_naming_helper import S3NamingHelper


''' S3 Parquet to Dataframe Fetcher.
This class handles the portion of work that will return a concatenated 
dataframe based on the partition filters the specified dataset.

Required kwargs:
    bucket (str): S3 Bucket name
    key (str): S3 key that leads to the desired dataset
    filters (List(Filter)): filters to be applied to the Dataset Partitions
        Filters have the fields:
            partition (str):
                Partition to be applied to
            comparison (str):
                Comparison function - one of: [ == , != , > , < , >= , <= ]
            values (List(any)): 
                Values to compare to - must match partition data type
                May not use multiple values with the '<', '>' comparisons
'''
# TODO: Future add-ons : get max value of partition

Filter = {
    "partition": str,
    "comparison": str,
    "values": List[any]
}

ops = {
    "==": operator.eq,
    "!=": operator.ne,
    ">=": operator.ge,
    "<=": operator.le,
    ">": operator.gt,
    "<": operator.lt
}

''' TODO: development notes, remove after
Internal attributes:
    List of keys for the filtered dataset
    List of local files pulled from above
    List of dataframes

Phase 1:
    Take and validate input
    Get partitions
Phase 2:
    Compare partitions to filters
    Create file paths to filtered parquets
Phase 3:
    Pull down files
    Transform files to dataframes
    Concat dataframes and return
'''

def fetch(bucket: str, key: str, filters: List[type(Filter)]):
    ''' Access function to kick off all bits and return result. '''
    _validate_filter_rules(filters)
    S3NamingHelper().validate_bucket_name(bucket)

    all_files = _get_all_files_list(bucket, key)

    partition_types, partition_metadata = _get_partitions_and_types(all_files[0], bucket)

    _validate_matching_filter_data_type(partition_types, filters)

    partition_values = _parse_partitions_and_values(all_files, key)

    typed_values = _get_partition_value_data_types(
        partition_values, partition_types)

    filtered_paths = _get_filtered_key_list(typed_values, filters)
    return _get_filtered_data(bucket=bucket, paths=filtered_paths, partition_metadata=partition_metadata)

def _get_partitions_and_types(first_file_key: str, bucket):
    ''' Fetch a list of all the partitions actually there and their 
    datatypes. List may be different than passed list if not being used
    for filtering on.
    - This is pulled from the metadata. It is assumed that this package
        is being used with its own publish method.
    '''
    parts_and_types = []
    s3_client = boto3.client('s3')

    first_file = s3_client.head_object(
        Bucket=bucket,
        Key=first_file_key
    )

    # save for repopulating parquet later
    partition_metadata = first_file['Metadata']['partition_data_types']

    part_data_types = ast.literal_eval(partition_metadata)

    return part_data_types, partition_metadata

def _get_all_files_list(bucket, key) -> list:
    ''' Get a list of all files to get all partitions values.
    Necesarry to catch all partition values for non-filtered partiions.
    '''
    objects_in_bucket = []
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects')
    operation_parameters = {'Bucket': bucket,
                            'Prefix': key}
    page_iterator = paginator.paginate(**operation_parameters)
    for page in page_iterator:
        for item in page['Contents']:
            if item['Key'].endswith('.parquet'):
                objects_in_bucket.append(item['Key'])

    return objects_in_bucket

def _parse_partitions_and_values(file_paths: List[str], key: str) -> dict:
    ''' Take the list of all the file keys and return a dict of the
    partitions with arrays of their values.
    '''
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

def _get_partition_value_data_types(parsed_parts: dict, part_types: dict):
    ''' Convert the collected values to their python data types for use.
    '''
    for part, values in parsed_parts.items():
        part_type = part_types[part]
        try:
            if (part_type == 'string') or (part_type == 'category'):
                continue
            elif part_type == 'int':
                parsed_parts[part] = set(map(int, values))
            elif part_type == 'float':
                parsed_parts[part] = set(map(float, values))
            elif part_type == 'datetime':
                parsed_parts[part] = set(
                    map(datetime.datetime.fromisoformat, values))
            elif part_type == 'bool':
                parsed_parts[part] = set(map(bool, values))
        except:
            raise ValueError(
                f"Invalid partition type: {part_type} does not match partition {part}")

    return parsed_parts

# TODO: Neaten up?
def _get_filtered_key_list(typed_parts: dict, filters, key) -> List[str]:
    ''' Create list of all "paths" to files after the filtered partitions
    are set ie all non-matching partitions are excluded.
    '''
    filter_keys = []

    for part, part_values in typed_parts.items():
        for f in filters:
            if (f['partition'] == part):
                comparison = self.ops[f['comparison']]
                for v in f['values']:
                    typed_parts[part] = set(filter(
                        lambda x: comparison(x, v),
                        typed_parts[part]
                    )
                    )

    def construct_paths(typed_parts, previous_fil_keys: List[str]) -> None:
        if len(typed_parts) > 0:
            part = typed_parts.popitem(last=False)
            new_filter_keys = list()
            for value in part[1]:
                mapped_keys = list(map(
                    (lambda x: str(x) +
                               str(part[0]) + "=" + str(value) + "/"),
                    previous_fil_keys
                ))
                new_filter_keys = new_filter_keys + mapped_keys

            construct_paths(typed_parts, new_filter_keys)
        else:
            filter_keys.append(previous_fil_keys)

    construct_paths(typed_parts, [key])

    # TODO: fix the below mess with random array
    return filter_keys[0]

def _get_filtered_data(bucket: str, paths: list, partition_metadata) -> pd.DataFrame:
    ''' Pull all filtered parquets down and return a dataframe.
    '''

    temp_frames = []

    def append_to_temp(frame):
        temp_frames.append(frame)

    with get_context("spawn").Pool() as pool:
        for path in paths:
            append_to_temp(
                pool.apply_async(s3_parquet_to_dataframe, args=(bucket, path, partition_metadata)).get())
        pool.close()
        pool.join()
    return pd.concat(temp_frames)

def _s3_parquet_to_dataframe(bucket: str, key: str, partition_metadata) -> None:
    """ grab a parquet file from s3 and convert to pandas df, add it to the destination"""
    s3 = s3fs.S3FileSystem()
    uri = f"{bucket}/{key}"
    table = pq.ParquetDataset(uri, filesystem=s3)
    frame = table.read_pandas().to_pandas()
    partitions = repopulate_partitions(uri, partition_metadata)
    for k, v in partitions.items():
        frame[k] = v
    return frame

def _repopulate_partitions(partition_string: str, partition_metadata) -> tuple:
    """ for each val in the partition string creates a list that can be added back into the dataframe"""
    raw = partition_string.split('/')
    partitions = {}
    for string in raw:
        if '=' in string:
            k, v = string.split('=')
            partitions[k] = v

    for key, val in partitions.items():
        try:
            dtype = partition_metadata[key]
        except:
            raise ValueError(
                f"{key} is not a recognized partition in the current s3 meta.")
        if dtype == 'string':
            partitions[key] = str(val)
        elif dtype == 'integer':
            partitions[key] = int(val)
        elif dtype == 'float':
            partitions[key] = float(val)
        elif dtype == 'datetime':
            partitions[key] = datetime.datetime.strptime(
                val, '%Y-%m-%d %H:%M:%S')
        elif dtype == 'category':
            partitions[key] = pd.Category(val)
        elif dtype == 'bool':
            partitions[key] = bool(val)
    return partitions

def _validate_filter_rules(filters: List[type(Filter)]) -> None:
    ''' Validate that the filters are the correct format and follow basic
    comparison rules.
    '''

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
        elif f["comparison"] not in self.ops:
            raise ValueError(
                f"Comparison {f['comparison']} is not supported.")
        elif (f["comparison"] in single_value_comparisons) & (len(f["values"]) != 1):
            raise ValueError(
                f"Comparison {f['comparison']} can only be used with one filter value.")

def _validate_matching_filter_data_type(part_types, filters) -> None:
    ''' Validate that the filters passed are matching to the partitions'
    listed datatypes. This includes validating comparisons too.
    '''
    num_comparisons = [
        ">",
        "<",
        "<=",
        ">="
    ]

    non_num_types = [
        "string",
        "category",
        "bool"
    ]

    for f in filters:
        try:
            fil_part = part_types[f["partition"]]
        except:
            raise ValueError("Filter does not have a matching partition.")

        if (f["comparison"] in num_comparisons):
            if fil_part in non_num_types:
                raise ValueError(
                    f"Comparison {f['comparison']} cannot be used on partition types of {fil_part}")
