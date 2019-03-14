import boto3
import operator
import multiprocessing as mp
import s3fs

from s3_naming_helper import S3NamingHelper

class S3FetchParq():
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

    Filter = {
        "partition": str,
        "comparison": str,
        "values": List(any)
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

    def __init__(self, bucket:str, prefix: str, filters: List(self.Filter)) -> None:
        self._s3fs = s3fs    

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
    def filters(self, filters: List(self.Filter)):
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
        temp_frames = []
        threads = [mp.Process() for path in paths]
        
    def _s3_parquet_to_dataframe(self, key:str, file_system, destination:list)->None:
        """ grab a parquet file from s3 and convert to pandas df, add it to the destination"""
        table = pq.read_table(key, filesystem= file_system)
        destination.append(table.to_pandas())

    def _turn_files_into_dataframes(self):
        ''' Turn the local files into pandas dataframes.
        TODO: Level of handling?
        '''
        pass

    def _concat_dataframes(self):
        ''' Concatenate dataframes from the local files to return.
        '''
        pass

    def _validate_matching_filter_data_type(self):
        ''' Validate that the filters passed are matching to the partitions'
        listed datatypes. This includes validating comparisons too.
        '''
        pass
