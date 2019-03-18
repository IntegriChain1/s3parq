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
                 prefix: str,
                 partitions: iter)->None:
        self.logger = logging.getLogger(__name__)
        self.dataset = dataset
        self.dataframe = dataframe
        self.bucket = bucket
        self.prefix = prefix
        self.partitions = partitions

        self.publish(bucket=self._bucket, dataset=self._dataset,
                     prefix=self._prefix, dataframe=self._dataframe, partitions=self._partitions)

    @property
    def dataset(self)->str:
        return self._dataset

    @dataset.setter
    def dataset(self, dataset: str)->None:
        self._dataset = dataset

    @property
    def dataframe(self)->pd.DataFrame:
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe: pd.DataFrame)->None:
        self._dataframe = dataframe

    @property
    def bucket(self)->str:
        return self._bucket

    @bucket.setter
    def bucket(self, bucket: str)->None:
        self._bucket = bucket

    @property
    def prefix(self)->str:
        return self._prefix

    @prefix.setter
    def prefix(self, prefix: str)->None:
        self._prefix = prefix

    @property
    def partitions(self)->iter:
        return self._partitions

    @partitions.setter
    def partitions(self, partitions: iter):
        self._partitions = partitions

    def publish(self, bucket: str, prefix: str, dataset: str, dataframe: pd.DataFrame, partitions: iter)->None:
        self.logger.debug("Checking partition args...")
        for partition in partitions:
            if partition not in dataframe.columns.tolist():
                partition_message = f"Cannot set {partition} as a partition; this is not a valid column header for the supplied dataframe."
                self.logger.critical(partition_message)
                raise ValueError(partition_message)
            if not self._check_partition_compatibility(partition):
                partition_message = f"{partition} is a reserved word in hive that cannot be used as a partition."
                self.logger.critical(partition_message)
                raise ValueError(partition_message)
        self.logger.debug("Done checking partitions.")
        self.logger.debug("Begin writing to S3..")
        for frame in self._sized_dataframes(dataframe):
            self._gen_parquet_to_s3(dataset=dataset, bucket=bucket,
                                    dataframe=frame, prefix=prefix, partitions=partitions)
            self._assign_partition_meta(
                bucket=bucket, dataset=dataset, prefix=prefix, dataframe=frame)
        self.logger.debug("Done writing to S3.")

    def _check_partition_compatibility(self, partition: str)->bool:
        """ Make sure each partition value is hive-allowed."""
        reserved = "ALL, ALTER, AND, ARRAY, AS, AUTHORIZATION, BETWEEN, BIGINT, BINARY, BOOLEAN, BOTH, BY, CASE, CAST, CHAR, COLUMN, CONF, CREATE, CROSS, CUBE, CURRENT, CURRENT_DATE, CURRENT_TIMESTAMP, CURSOR, DATABASE, DATE, DECIMAL, DELETE, DESCRIBE, DISTINCT, DOUBLE, DROP, ELSE, END, EXCHANGE, EXISTS, EXTENDED, EXTERNAL, FALSE, FETCH, FLOAT, FOLLOWING, FOR, FROM, FULL, FUNCTION, GRANT, GROUP, GROUPING, HAVING, IF, IMPORT, IN, INNER, INSERT, INT, INTERSECT, INTERVAL, INTO, IS, JOIN, LATERAL, LEFT, LESS, LIKE, LOCAL, MACRO, MAP, MORE, NONE, NOT, NULL, OF, ON, OR, ORDER, OUT, OUTER, OVER, PARTIALSCAN, PARTITION, PERCENT, PRECEDING, PRESERVE, PROCEDURE, RANGE, READS, REDUCE, REVOKE, RIGHT, ROLLUP, ROW, ROWS, SELECT, SET, SMALLINT, TABLE, TABLESAMPLE, THEN, TIMESTAMP, TO, TRANSFORM, TRIGGER, TRUE, TRUNCATE, UNBOUNDED, UNION, UNIQUEJOIN, UPDATE, USER, USING, UTC_TMESTAMP, VALUES, VARCHAR, WHEN, WHERE, WINDOW, WITH, COMMIT, ONLY, REGEXP, RLIKE, ROLLBACK, START, CACHE, CONSTRAINT, FOREIGN, PRIMARY, REFERENCES, DAYOFWEEK, EXTRACT, FLOOR, INTEGER, PRECISION, VIEWS, TIME, NUMERIC, SYNC".split()

        return not partition.upper() in reserved

    def _sized_dataframes(self, dataframe: pd.DataFrame)->tuple:
        """Takes a dataframe and slices it into sized dataframes for optimal parquet sizes in S3.
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
        num_rows = int(dataframe.shape[0])
        frame_size_est = row_size_est * num_rows
        # at scale dataframes seem to compress around 3.5-4.5 times as parquet.        ## TODO: should build a real regression to calc this!
        compression_ratio = 4
        # 60MB compressed is ideal for Spectrum
        ideal_size = compression_ratio * (60*float(1 << 20))
        # short circut if < ideal size
        batch_log_message = """row size estimate: {row_size_est} bytes.
number of rows: {num_rows} rows
frame size estimate: {frame_size_est} bytes
compression ratio: {compression_ratio}:1
ideal size: {ideal_size} bytes
"""
        self.logger.debug(batch_log_message)
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
        self.logger.info(f"sized out {len(sized_frames)} dataframes.")
        return tuple(sized_frames)

    def _gen_parquet_to_s3(self, dataset: str, bucket: str, dataframe: pd.DataFrame, prefix: str, partitions: list)->None:
        """ pushes the parquet dataset directly to s3. """
        self.logger.info("Writing to S3...")
        table = pa.Table.from_pandas(dataframe, preserve_index=False)
        uri = 's3://' + \
            ('/'.join([bucket, prefix, dataset]).replace("//", "/"))
        self.logger.debug(f"Writing to s3 location: {uri}...")
        pq.write_to_dataset(table, compression="snappy", root_path=uri,
                            partition_cols=partitions, filesystem=s3fs.S3FileSystem())
        self.logger.debug("Done writing to location.")

    def _assign_partition_meta(self, bucket: str, prefix: str, dataset: str, dataframe: pd.DataFrame)->None:
        """ assigns the dataset partition meta to all keys in the dataset"""
        s3_client = boto3.client('s3')
        all_files = []
        prefix = '/'.join([prefix, dataset]).strip('/')
        for obj in s3_client.list_objects(Bucket=bucket, Prefix=prefix)['Contents']:
            if obj['Key'].endswith(".parquet"):
                all_files.append(obj['Key'])

        for obj in all_files:
            self.logger.debug(f"Appending metadata to file {obj}..")
            s3_client.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': obj}, Key=obj, Metadata={'partition_data_types': str(
                self._parse_dataframe_col_types(dataframe=dataframe)
            )}, MetadataDirective='REPLACE')
            self.logger.debug("Done appending metadata.")

    def _parse_dataframe_col_types(self, dataframe: pd.DataFrame)-> dict:
        """ Returns a dict with the column names as keys, the data types (in strings) as values."""
        self.logger.debug("Determining write metadata for publish...")
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
        self.logger.debug(f"Done.Metadata set as {dtypes}")
        return dtypes
