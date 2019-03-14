from typing import List
import boto3
import operator
import multiprocessing as mp
import s3fs
import pandas as pd
import pyarrow as pa
import datetime
import pyarrow.parquet as pq
from .s3_naming_helper import S3NamingHelper

class S3FetchParq:
    ''' S3 Parquet to Dataframe Fetcher.
    This class handles the portion of work that will return a concatenated 
    dataframe based on the partition filters the specified dataset.

    Required kwargs:
        s3_bucket (str): S3 Bucket name
        prefix (str): Prefix that leads to the desired Dataset
        filters (List(Filter)): filters to be applied to the Dataset Partitions
            Filters have the fields:
                partition (str):
                    Partition to be applied to
                comparison (str):
                    Comparison function - one of: [ == , != , > , < ]
                values (List(any)): 
                    Values to compare to - must match partition data type
                    May not use multiple values with the '<', '>' comparisons
    '''

    ## STUB! REMOVE ME!
    _partition_metadata = {"string_col":"string",
                        "int_col":"integer",
                        "float_col":"float",
                        "bool_col":"boolean",
                        "datetime_col":"datetime"
                        }

    Filter = {
        "partition": str,
        "comparison": str,
        "values": List
    }

    ops = {
        "==": operator.eq,
        "!=": operator.ne,
        ">": operator.ge,
        "<": operator.le
    }

    ''' TODO: development notes, remove after
    Internal attributes:
        List of prefixes for the filtered dataset
        List of local files pulled from above
        List of dataframes

    Phase 1:
        Take and validate input
    Phase 2:
        Get partitions
        Compare to filters
        Create file paths to filtered parquets
    Phase 3:
        Pull down files
        Transform files to dataframes
        Concat dataframes and return
    '''

    def __init__(self, bucket:str, prefix: str, filters: dict) -> None:
        pass

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
    def filters(self, filters: dict):
        if any(f["comparison"] not in ops for f in filters):
            raise ValueError("Filter comparison must be one of:  >, <, ==, !=")

        self._filters = filters


    def fetch(self):
        ''' Access function to kick off all bits and return result. '''
        pass

    def _get_partitions_and_types(self):
        ''' Fetch a list of all the partitions actually there and their 
        datatypes. List may be different than passed list if not being used
        for filtering on.
        '''
        pass

    def _get_all_files_list(self):
        ''' Get a list of all files to get all partitions values.
        Necesarry to catch all partition values for non-filtered partiions.
        '''
        # TODO: Ensure handlers for pagination!
        pass

    def _set_filtered_prefix_list(self):
        ''' Create list of all "paths" to files after the filtered partitions
        are set ie all non-matching partitions are excluded.
        '''
        pass

    def _get_filtered_data(self, bucket:str, paths:list)->pd.DataFrame:
        ''' Pull all filtered parquets down and return a dataframe.
        '''
        temp_queue = mp.Queue()
        temp_frames = []
        threads = [mp.Process(target=self._s3_parquet_to_dataframe, args=(bucket, path, temp_queue,)) for path in paths]
        for thread in threads:
            thread.start()
            temp_frames.append(temp_queue.get())
           
        for thread in threads:
            thread.join()

        return pd.concat(temp_frames)


    def _s3_parquet_to_dataframe(self, bucket:str, path:str, destination:list)->None:
        """ grab a parquet file from s3 and convert to pandas df, add it to the destination"""
        s3 = s3fs.S3FileSystem()
        uri = f"{bucket}/{path}"
        table = pq.ParquetDataset(uri, filesystem= s3)
        frame = table.read_pandas().to_pandas()
        partitions = self._repopulate_partitions(uri)
        for k,v in partitions.items():
            frame[k] = v
        destination.put(frame)


    def _repopulate_partitions(self,partition_string:str)->tuple:
        """ for each val in the partition string creates a list that can be added back into the dataframe"""
        raw = partition_string.split('/')
        partitions = {}
        for string in raw:
            if '=' in string:
                k,v = string.split('=')
                partitions[k] = v

        for key, val in partitions.items():
            try:
                dtype = self._partition_metadata[key] 
            except:
                raise ValueError(f"{key} is not a recognized partition in the current s3 meta.")
            if dtype == 'string':
                partitions[key] = str(val)    
            elif dtype == 'integer':
                partitions[key] = int(val)
            elif dtype == 'float':
                partitions[key] = float(val)
            elif dtype == 'datetime':
                partitions[key] = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
            elif dtype == 'category':
                partitions[key] = pd.Category(val)
            elif dtype == 'bool':
                partitions[key] = bool(val)
        return partitions

    def _validate_matching_filter_data_type(self):
        ''' Validate that the filters passed are matching to the partitions'
        listed datatypes. This includes validating comparisons too.
        '''
        pass
