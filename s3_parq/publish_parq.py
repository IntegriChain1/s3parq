import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from collections import namedtuple


class S3PublishParq:

    def __init__(   self,
                    dataframe:pd.DataFrame,
                    dataset:str,
                    bucket:str,
                    key_prefix:str,
                    partitions:iter)->None:
        for partition in partitions:
            if partition not in dataframe.columns:
                raise ValueError(f"Cannot set {partition} as a partition; this is not a valid column header for the supplied dataframe.")
        self._gen_parquet_to_s3(dataset=dataset, bucket=bucket,dataframe=dataframe, key_prefix=key_prefix, partitions=partitions)
            

    def _check_partition_compatibility(self):
        """ Make sure each partition value is hive-allowed."""
        pass

        
    def _gen_parquet_to_s3(self, dataset:str, bucket:str, dataframe:pd.DataFrame, key_prefix:str, partitions:list)->None:
        """ pushes the parquet dataset directly to s3. """
        table = pa.Table.from_pandas(dataframe, preserve_index=False)
        pq.write_to_dataset(table, root_path = '/'.join([key_prefix,dataset]), partition_cols=partitions)

    def do_publish(self)->None:
        ##TODO: feels like this should log or something
        #       response = namedtouple("response", ["rows", "files"])
        #if True:
        #    return response._make(45, 50)
        pass
