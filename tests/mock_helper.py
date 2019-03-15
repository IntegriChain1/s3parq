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


class MockHelper:
    """ creates some helpful dataset stuff.
        - dataframe is the frame created
        - s3_bucket is the test bucket that has been populated 
        be sure to wrap this with moto @mock_s3 if you want to mock it
    """

    def __init__(self, count=1000000, s3=False):
        """ If s3 then will populate the s3 bucket with partitioned parquet. """
        self._dataframe = self.setup_grouped_dataframe(count=count)
        if s3:
            self._s3_bucket = self.setup_partitioned_parquet()

    @property
    def dataframe(self):
        return self._dataframe

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
        s3_client = boto3.client('s3')
        df = self._dataframe
        s3_client.create_bucket(Bucket=bucket_name)

        # generate the local parquet tree
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table,
                            root_path=str(t),
                            partition_cols=['string_col', 'int_col', 'float_col', 'bool_col', 'datetime_col'])

        # traverse the local parquet tree
        paths = ''
        extra_args = {'partition_data_types': str(
            {"string_col": "string",
             "int_col": "integer",
                        "float_col": "float",
                        "bool_col": "boolean",
                        "datetime_col": "datetime"
             })}
        for subdir, dirs, files in os.walk(str(t)):
            for file in files:
                full_path = os.path.join(subdir, file)
                with open(full_path, 'rb') as data:
                    path = full_path[1:]  # [len(str(1))+1:]
                    s3_client.upload_fileobj(data, Bucket=bucket_name, Key=path, ExtraArgs={
                                             "Metadata": extra_args})
        return bucket_name
