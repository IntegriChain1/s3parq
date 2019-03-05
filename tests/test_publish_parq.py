import pytest
import pandas as pd
from dfmock import DFMock
from s3_parq import S3Parq


class Test:

    strings = ("bucket","dataset","key_prefix",)

    
    def parq_setup_exclude(self, to_exclude):
        df = DFMock()
        df.generate_dataframe()
        defaults ={
        'bucket':'safebucketname',
        'dataset' : 'safedatasetname',
        'key_prefix' : 'safekeyprefixname',
        'filters' :{"filtered_thing":"filter_val"},
        'dataframe' : df.dataframe
        }
        parq = S3Parq()
        for a in defaults.keys():
            if a not in to_exclude:
                setattr(parq,a,defaults[a])
        return parq

    # requires dataframe
    def test_requires_dataframe(self):
        parq = self.parq_setup_exclude(('dataframe',))
        with pytest.raises(ValueError):
            parq.publish()

    # only accepts dataframe
    def test_accepts_only_dataframe(self):
        with pytest.raises(TypeError):
            parq = self.parq_setup_exclude(('dataframe',))
            parq.dataframe = 'not a dataframe'

    # accepts good dataframe
    def test_accepts_valid_dataframe(self):
            parq = self.parq_setup_exclude([])
            parq.publish()

    # accepts good string
    def test_accepts_valid_dataframe(self):
        parq = self.parq_setup_exclude([])
        parq.publish()

    # requires string
    def test_requires_string(self):
        for s in self.strings:
            if s != 'key_prefix':
                with pytest.raises(ValueError):
                    parq = self.parq_setup_exclude((s,))
                    parq.publish()

    # only accepts string
    def test_accepts_only_string(self):
        for s in self.strings:
            parq = self.parq_setup_exclude([])
            with pytest.raises(TypeError):
                setattr(parq,s,['this is not a string'])


# raises not implemented error for timedelta column types
    def test_not_implemented_timedelta(self):
        df = DFMock(count=100)
        df.columns = {"time":"timedelta","stringer":"string"}
        df.generate_dataframe()
        parq = S3Parq()
        with pytest.raises(NotImplementedError):
            parq.dataframe = df.dataframe

# accepts valid column names as partitions

# only accepts valid column names as partitions

# generates partitions in order

# - TODO: patch pyarrow here to make sure it is called - that's it.
# generates parquet filesystem hierarchy correctly

# generates files of compressed size <=60mb

# generates valid parquet files

# respects source df schema exactly

# data in == data out

# do_publish returns success tuple

# extra credit: allows 60mb size to be adjusted?
