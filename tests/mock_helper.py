import os
import pytest
import boto3
from moto import mock_s3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dfmock import DFMock
from string import ascii_lowercase
import random
import tempfile


@mock_s3
class MockHelper:
    """ creates some helpful dataset stuff.
        - dataframe is the frame created
        - s3_bucket is the test bucket that has been populated 
        be sure to wrap this with moto @mock_s3 if you want to mock it
    """

    def __init__(self, count=1000000, s3=False, files=False):
        """ If s3 then will populate the s3 bucket with partitioned parquet. """
        self._dataframe = self.setup_grouped_dataframe(count=count)
        self._s3_bucket = ''
        self._dataset = ''
        self._paths = []
        self._partition_metadata = {"string_col": "string",
                                    "int_col": "integer",
                                    "float_col": "float",
                                    "bool_col": "boolean",
                                    "datetime_col": "datetime"}

        if s3:
            self._s3_bucket = self.setup_partitioned_parquet()
        if files:
            self._file_ops = self.setup_files_list(
                count)

    @property
    def partition_metadata(self):
        return self._partition_metadata

    @property
    def paths(self):
        return self._paths

    @property
    def dataset(self):
        return self._dataset

    @property
    def dataframe(self):
        return self._dataframe

    @property
    def file_ops(self):
        return self._file_ops

    @property
    def s3_bucket(self):
        return self._s3_bucket

    def setup_grouped_dataframe(self, count):
        df = DFMock()
        df.count = count
        df.columns = {"string_col": {"option_count": 3, "option_type": "string"},
                      "int_col": {"option_count": 3, "option_type": "int"},
                      "float_col": {"option_count": 3, "option_type": "float"},
                      "bool_col": {"option_count": 3, "option_type": "bool"},
                      "datetime_col": {"option_count": 3, "option_type": "datetime"},
                      "metrics": "int"}
        df.generate_dataframe()
        return df.dataframe

    def random_name(self):
        return ''.join([random.choice(ascii_lowercase) for x in range(0, 10)])

    def setup_partitioned_parquet(self):
        bucket_name = self.random_name()
        t = tempfile.mkdtemp()
        self.tmpdir = t
        self._s3_bucket = bucket_name

        s3_client = boto3.client('s3')
        df = self._dataframe
        s3_client.create_bucket(Bucket=bucket_name)

        # generate the local parquet tree
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table,
                            root_path=str(t),
                            partition_cols=['string_col', 'int_col', 'float_col', 'bool_col', 'datetime_col'])

        # traverse the local parquet tree
        extra_args = {'partition_data_types': str(self._partition_metadata)}
        for subdir, dirs, files in os.walk(str(t)):
            for file in files:
                full_path = os.path.join(subdir, file)
                keysub = subdir.split(os.path.sep)[
                    subdir.split(os.path.sep).index('T')+1:]
                self._dataset = keysub[0]
                keysub.append(file)
                key = '/'.join(keysub)
                with open(full_path, 'rb') as data:
                    s3_client.upload_fileobj(data, Bucket=bucket_name, Key=key, ExtraArgs={
                                             "Metadata": extra_args})
                    self._paths.append(key)
        return self._s3_bucket

    def setup_files_list(self, count=1500):
        key = self.random_name()
        bucket_name = self.random_name()
        temp_file_names = []
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket_name)

        with tempfile.TemporaryDirectory() as tmp_dir:
            for x in range(count):
                with tempfile.NamedTemporaryFile(dir=tmp_dir) as t:
                    head, tail = os.path.split(t.name)
                    temp_file_names.append(str(tail))
                    with open(t.name, 'rb') as data:
                        s3_client.upload_fileobj(
                            data, Bucket=bucket_name, Key=(key+"/"+tail + ".parquet"))

        retrieval_ops = {
            "bucket": bucket_name,
            "key": key,
            "files": temp_file_names
        }

        return retrieval_ops
