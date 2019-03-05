import pytest
from dfmock import DFMock
from s3_parq import S3Parq


class Test:
    def tests_check_types_sensitivity(self):
        parq = S3Parq()
        df = DFMock(count=100)
        df.generate_dataframe()

        parq.dataset = 'dataset_string'
        parq.bucket = 'bucket_string'
        parq.dataframe = df.dataframe
        parq.filters = {'filter_key': 'filter_value'}
        parq.key_prefix = 'key_prefix'

    def test_check_types_specificity(self):
        parq = S3Parq()
        with pytest.raises(TypeError):
            parq.dataset = ['not a string']
        with pytest.raises(TypeError):
            parq.bucket = ['not a string']
        with pytest.raises(TypeError):
            parq.key_prefix = ['not a string']
        with pytest.raises(TypeError):
            parq.dataframe = ['not a dataframe']
        with pytest.raises(TypeError):
            parq.filters = ['not a dict']
