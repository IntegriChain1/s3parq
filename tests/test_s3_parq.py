import pytest
from mock import patch
import pandas as pd
from dfmock import DFMock
from s3_parq import S3Parq
from s3_parq.publish_parq import S3PublishParq
import boto3
from moto import mock_s3


@mock_s3
class Test:

    strings = ("bucket", "dataset", "prefix",)

    def parq_setup_exclude(self, to_exclude):
        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket='safebucketname')
        df = DFMock(count=100)
        df.columns = {"stringer": "string", "intcol": "int",
                      "groupcol": {"option_count": 5, "option_type": "string"}}
        df.generate_dataframe()
        defaults = {
            'bucket': 'safebucketname',
            'dataset': 'safedatasetname',
            'prefix': 'safekeyprefixname',
            'filters': {"filtered_thing": "filter_val"},
            'dataframe': df.dataframe
        }
        parq = S3Parq()
        for a in defaults.keys():
            if a not in to_exclude:
                setattr(parq, a, defaults[a])
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

    # raises not implemented error for timedelta column types
    def test_not_implemented_timedelta(self):
        df = DFMock(count=100)
        df.columns = {"time": "timedelta", "stringer": "string"}
        df.generate_dataframe()
        parq = S3Parq()
        with pytest.raises(NotImplementedError):
            parq.dataframe = df.dataframe

    # requires string
    def test_requires_string(self):
        for s in self.strings:
            if s != 'prefix':
                with pytest.raises(ValueError):
                    parq = self.parq_setup_exclude((s,))
                    parq.publish()

    # only accepts string
    def test_accepts_only_string(self):
        for s in self.strings:
            parq = self.parq_setup_exclude([])
            with pytest.raises(TypeError):
                setattr(parq, s, ['this is not a string'])
