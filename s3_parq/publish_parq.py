import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import sys
import logging

class S3PublishParq:

    def __init__(   self,
                    dataframe:pd.DataFrame,
                    dataset:str,
                    bucket:str,
                    key_prefix:str,
                    partitions:iter)->None:
        self.logger = logging.getLogger(__name__)
        for partition in partitions:
            if partition not in dataframe.columns.tolist():
                raise ValueError(f"Cannot set {partition} as a partition; this is not a valid column header for the supplied dataframe.")
        for frame in self._sized_dataframes(dataframe):
            self._gen_parquet_to_s3(dataset=dataset, bucket=bucket,dataframe=frame, key_prefix=key_prefix, partitions=partitions)
            

    def _check_partition_compatibility(self):
        """ Make sure each partition value is hive-allowed."""
        pass

    def _sized_dataframes(self,dataframe:pd.DataFrame)->tuple:
        """Takes a dataframe and slices it into ~100mb dataframes for optimal parquet sizes in S3.
            RETURNS a tuple of dataframes.         
        """
        def log_size_estimate(num_bytes):
            if num_bytes == 0:
                return "0MB"
            elif round(num_bytes/float(1<<20),2) == 0.0:
                return "<0.1MB"
            else:
                return str(round(num_bytes/float(1<<20),2)) +"MB"

        sized_frames = []
        ## get first row size
        row_size_est = sys.getsizeof(dataframe.head)
        ## get number of rows
        num_rows = dataframe.size()
        ##TODO: need the compression ratio from dataframe to parquet
        compression_ratio = raise ValueError("you gotta get compression ratio!")
        ## math the number of estimated partitions 
        num_partitions = floor(row_size*num_rows / (compression_ratio * (60*float(1<<20))))
        ## for each partition do the thing
        
        ## do the leftovers 
        
        #temp_df = pd.DataFrame()
        for row in dataframe.itertuples(index=False):
            num_bytes = sys.getsizeof(temp_list)  
            ## the parquet files come out around 3.9 times the size of the same data in a list of tuples. This
            if 1<<20 % num_bytes== 0:
                self.logger.info(f"Tuple size == {log_size_estimate(num_bytes)}")
            if num_bytes < (60*4)*float(1<<20):
                temp_list.append(row)
                #temp_df = pd.concat([temp_df, pd.DataFrame.from_records([row], columns=row._fields)])
            else:
                self.logger.info(f"Created dataframe of size {log_size_estimate(num_bytes)}")
                sized_frames.append(pd.DataFrame.from_records(temp_list, columns=temp_list[0]._fields))
                temp_list = []
                #temp_df = pd.DataFrame()
        
        ## get the leftover frame
        if len(temp_list) > 0:
            self.logger.info(f"Created final dataframe of size {log_size_estimate(num_bytes)}")
            sized_frames.append(pd.DataFrame.from_records(temp_list, columns=temp_list[0]._fields))
        return tuple(sized_frames)

    def _gen_parquet_to_s3(self, dataset:str, bucket:str, dataframe:pd.DataFrame, key_prefix:str, partitions:list)->None:
        """ pushes the parquet dataset directly to s3. """
        table = pa.Table.from_pandas(dataframe, preserve_index=False)
        uri = '/'.join(["s3:/",bucket,dataset])
        pq.write_to_dataset(table, compression = "snappy", root_path = uri, partition_cols=partitions, filesystem=s3fs.S3FileSystem())

