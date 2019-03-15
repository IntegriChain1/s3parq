import ast
import boto3
import operator
from typing import List
from collections import OrderedDict

from .s3_naming_helper import S3NamingHelper


class S3FetchParq():
    ''' S3 Parquet to Dataframe Fetcher.
    This class handles the portion of work that will return a concatenated 
    dataframe based on the partition filters the specified dataset.

    Required kwargs:
        s3_bucket (str): S3 Bucket name
        prefix (str): Prefix that leads to the desired Dataset
        dataset (str): The actual Dataset
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
        List of prefixes for the filtered dataset
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

    def __init__(self, bucket: str, prefix: str, dataset: str, filters: List[type(Filter)]) -> None:
        self.s3_bucket = bucket
        self.s3_prefix = prefix
        self.dataset = dataset
        self.filters = filters

    @property
    def s3_bucket(self) -> str:
        return self._bucket

    @s3_bucket.setter
    def s3_bucket(self, bucket: str) -> None:
        validater = S3NamingHelper().validate_bucket_name(bucket)
        if validater[0]:
            self._bucket = bucket
        else:
            raise ValueError(val[1])

    @property
    def s3_prefix(self) -> str:
        return self._prefix

    @s3_prefix.setter
    def s3_prefix(self, prefix: str) -> None:
        self._prefix = prefix

    @property
    def filters(self):
        return self._filters

    @filters.setter
    def filters(self, filters: List[type(Filter)]):
        # if any(f["comparison"] not in ops for f in filters):
        #     raise ValueError(
        #         "Filter comparison must be one of:  >, >=, <, <=, ==, !="
        #     )
        self._validate_filter_rules(filters)

        self._filters = filters

    def fetch(self):
        ''' Access function to kick off all bits and return result. '''
        pass

    def _get_partitions_and_types(self, first_file_key: str):
        ''' Fetch a list of all the partitions actually there and their 
        datatypes. List may be different than passed list if not being used
        for filtering on.
        - This is pulled from the metadata. It is assumed that this package
            is being used with its own publish method.
        '''
        parts_and_types = []
        s3_client = boto3.client('s3')
        bucket = self.s3_bucket

        first_file = s3_client.head_object(
            Bucket=self.s3_bucket,
            Key=first_file_key
        )

        part_data_types = first_file['Metadata']['partition_data_types']
        part_data_types = ast.literal_eval(part_data_types)

        return part_data_types

    def _get_all_files_list(self):
        ''' Get a list of all files to get all partitions values.
        Necesarry to catch all partition values for non-filtered partiions.
        '''
        objects_in_bucket = []
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects')
        operation_parameters = {'Bucket': self._bucket,
                                'Prefix': self._prefix}
                                # 'Prefix': 'foo/baz'}
        page_iterator = paginator.paginate(**operation_parameters)
        for page in page_iterator:
            for item in page['Contents']:
                objects_in_bucket.append(item['Key'])

        return objects_in_bucket

    def _parse_partitions_and_values(self, file_paths: List[str]):
        ''' Take the list of all the file keys and return a dict of the
        partitions with arrays of their values.
        '''
        # TODO: find more neat/efficient way to do this
        parts = OrderedDict()
        prefix_len = len(self.s3_prefix)
        for file_path in file_paths:
            # Delete prefix, split the parts out, delete the file name
            file_path = file_path[prefix_len:]
            unparsed_parts = file_path.split("/")
            del unparsed_parts[-1]
            
            for part in unparsed_parts:
                key, value = part.split("=")
                if key not in parts:
                    parts.update({key: set([value])})
                else:
                    parts[key].add(value)
        
        return parts
     
    def _set_partition_value_data_types(self, parsed_parts, part_types):
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
                    parsed_parts[part] = set(map(datetime.fromisoformat, values))
                elif part_type == 'bool':
                    parsed_parts[part] = set(map(bool, values))
            except:
                raise ValueError(f"Invalid partition type: {part_type} does not match partition {part}")

        return parsed_parts

    # TODO: Neaten up?
    def _set_filtered_prefix_list(self, typed_parts):
        ''' Create list of all "paths" to files after the filtered partitions
        are set ie all non-matching partitions are excluded.
        '''
        filters = self.filters
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

        def construct_paths(typed_parts, previous_fil_keys):
            if len(typed_parts)>0:
                old_filter_keys = previous_fil_keys
                part = typed_parts.popitem(last=False)
                new_filter_keys = list()
                for value in part[1]:
                    mapped_keys = list(map(
                        (lambda x: str(x) + str(part[0]) + "=" + str(value) + "/"),
                        previous_fil_keys
                    ))
                    new_filter_keys = new_filter_keys + mapped_keys

                construct_paths(typed_parts, new_filter_keys)
            else:
                filter_keys.append(previous_fil_keys)

        construct_paths(typed_parts, [self.s3_prefix])

        #TODO: fix the below mess with random array
        return filter_keys[0]

    def _get_filtered_files(self):
        ''' Pull all filtered parquets down to local temp directory.
        '''
        pass

    def _turn_files_into_dataframes(self):
        ''' Turn the local files into pandas dataframes.
        TODO: Level of handling?
        '''
        pass

    def _concat_dataframes(self):
        ''' Concatenate dataframes from the local files to return.
        '''
        pass

    def _validate_filter_rules(self, filters: List[type(Filter)]):
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
            if not all(key in f for key in ("partition","comparison","values")):
                raise ValueError("Filters require partition, comparison, and values.")
            elif f["comparison"] not in self.ops:
                raise ValueError(f"Comparison {f['comparison']} is not supported.")
            elif (f["comparison"] in single_value_comparisons) & (len(f["values"])!=1):
                raise ValueError(f"Comparison {f['comparison']} can only be used with one filter value.")

    def _validate_matching_filter_data_type(self, part_types):
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

        filters = self.filters

        for f in filters:
            try:
                fil_part = part_types[f["partition"]]
            except:
                raise ValueError("Filter does not have a matching partition.")

            if (f["comparison"] in num_comparisons):
                if fil_part in non_num_types:
                    raise ValueError(f"Comparison {f['comparison']} cannot be used on partition types of {fil_part}")


