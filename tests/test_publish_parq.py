import pytest
from mock import patch
import pandas as pd
from moto import mock_s3
from dfmock import DFMock
import s3_parq.publish_parq as pub_parq  
from .mock_helper import MockHelper

@mock_s3
class Test:

    def publish_parq_setup(self, overrides:dict=dict()):
        df = MockHelper(count=100).dataframe 
        defaults ={
        'bucket':'safebucketname',
        'dataset' : 'safedatasetname',
        'key_prefix' : 'safekeyprefixname',
        'dataframe' : df,
        'partitions':[]
        }
        return pub_parq.S3PublishParq(   bucket=overrides.get('bucket',defaults['bucket']),
                                dataset=overrides.get('dataset',defaults['dataset']),
                                key_prefix=overrides.get('key_prefix',defaults['key_prefix']),
                                dataframe=overrides.get('dataframe',defaults['dataframe']),
                                partitions=overrides.get('partitions',defaults['partitions'])
                            )
# accepts valid column names as partitions
    def test_accepts_valid_partitions(self):
        df = DFMock(count=100)
        df.columns = {"text_col":"string","int_cal":"integer","float_col":"float"}
        df.generate_dataframe()
        partitions = df.columns[:1]
        self.publish_parq_setup(overrides={"dataframe":df.dataframe, "partitions":partitions})

    # only accepts valid column names as partitions
    def test_accepts_valid_partitions(self):
        df = DFMock(count=100)
        df.columns = {"text_col":"string","int_cal":"integer","float_col":"float"}
        df.generate_dataframe()
        partitions = df.dataframe.columns[:1]
        partitions += ('banana')
        with pytest.raises(ValueError):
            self.publish_parq_setup(overrides={"dataframe":df.dataframe, "partitions":partitions})
    
    # generates partitions in order
    
    def test_generates_partitions_in_order(self):
        df = DFMock(count=100)
        df.columns = {"text_col":"string","int_cal":"integer","float_col":"float"}
        df.generate_dataframe()
        partitions = df.dataframe.columns[:1].tolist()
        with patch('s3_parq.publish_parq.pq.write_to_dataset', return_value=None) as mock_method:
            parq = self.publish_parq_setup(overrides={"dataframe":df.dataframe, "partitions":partitions}) 
            arg, kwarg = mock_method.call_args
            assert kwarg['partition_cols'] == partitions
    
# generates files of compressed size <=60mb
## TODO: this needs to happen in pyarrow actually. :(

# generates valid parquet files

# respects source df schema exactly

# data in == data out

# do_publish returns success tuple

# extra credit: allows 60mb size to be adjusted?
