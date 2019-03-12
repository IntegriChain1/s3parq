import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import sys
import logging


class S3PublishParq:

    def __init__(self,
                 dataframe: pd.DataFrame,
                 dataset: str,
                 bucket: str,
                 key_prefix: str,
                 partitions: iter)->None:
        self.logger = logging.getLogger(__name__)
        for partition in partitions:
            if partition not in dataframe.columns.tolist():
                raise ValueError(
                    f"Cannot set {partition} as a partition; this is not a valid column header for the supplied dataframe.")
        for frame in self._sized_dataframes(dataframe):
            self._gen_parquet_to_s3(dataset=dataset, bucket=bucket,
                                    dataframe=frame, key_prefix=key_prefix, partitions=partitions)
            self._assign_partition_meta(bucket=bucket, dataset=dataset, dataframe=frame)

    def _check_partition_compatibility(self):
        """ Make sure each partition value is hive-allowed."""
        pass

    def _sized_dataframes(self, dataframe: pd.DataFrame)->tuple:
        """Takes a dataframe and slices it into ~100mb dataframes for optimal parquet sizes in S3.
            RETURNS a tuple of dataframes.         
        """
        def log_size_estimate(num_bytes):
            if num_bytes == 0:
                return "0MB"
            elif round(num_bytes/float(1 << 20), 2) == 0.0:
                return "<0.1MB"
            else:
                return str(round(num_bytes/float(1 << 20), 2)) + "MB"

        # get first row size
        row_size_est = sys.getsizeof(dataframe.head(1))
        # get number of rows
        num_rows = int(dataframe.size.item())
        frame_size_est = row_size_est * num_rows
        # TODO: need the compression ratio from dataframe to parquet
        compression_ratio = 4
        # 60MB compressed is ideal for Spectrum
        ideal_size = compression_ratio * (60*float(1 << 20))
        # short circut if < ideal size
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
                upper = lower + rows_per_partition
            sized_frames.append(dataframe[lower:upper])

        return tuple(sized_frames)

    def _gen_parquet_to_s3(self, dataset: str, bucket: str, dataframe: pd.DataFrame, key_prefix: str, partitions: list)->None:
        """ pushes the parquet dataset directly to s3. """
        table = pa.Table.from_pandas(dataframe, preserve_index=False)
        uri = '/'.join(["s3:/", bucket, dataset])
        pq.write_to_dataset(table, compression="snappy", root_path=uri,
                            partition_cols=partitions, filesystem=s3fs.S3FileSystem())

    def _assign_partition_meta(self, bucket: str, dataset: str, dataframe:pd.DataFrame)->None:
        """ assigns the dataset partition meta to all keys in the dataset"""
        s3_client = boto3.client('s3')
        all_files = []
        for obj in s3_client.list_objects(Bucket=bucket, Prefix=dataset)['Contents']:
            if obj['Key'].endswith(".parquet"):
                all_files.append(obj['Key'])

        for obj in all_files:
            self.logger.info(f"Appending metadata to file {obj}")
            s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': obj}, Key=obj, Metadata={'partition_data_types': str(
            self._parse_dataframe_col_types(dataframe=dataframe)     
            )}, MetadataDirective='REPLACE')

    def _parse_dataframe_col_types(self,dataframe: pd.DataFrame)-> dict:
        """ Returns a dict with the column names as keys, the data types (in strings) as values."""
        dtypes = {}
        for col, dtype in dataframe.dtypes.items():
            dtype = str(dtype)
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
        return dtypes
