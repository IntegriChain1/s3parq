import boto3
from .publish_parq import S3PublishParq
import pandas as pd
import sys


class S3Parq:

    def __init__(self, **kwargs):
        self._set_kwargs_as_attrs(**kwargs)

    def publish(self, partitions:iter=set())->None:
        required_attributes = ('dataset', 'bucket', 'dataframe',)
        self._check_required_attr(required_attributes)

        publish = S3PublishParq(dataset=self.dataset,
                                bucket=self.bucket,
                                dataframe=self.dataframe,
                                key_prefix=getattr(self,"key_prefix",''),
                                partitions=partitions
                                )
        return publish.do_publish()

    def fetch(self)->None:
        required_attributes = ('dataset', 'bucket',)
        self._check_required_attr(required_attributes)
        # DO fetch here

    @property
    def dataset(self)->str:
        return self._dataset

    @dataset.setter
    def dataset(self, dataset: str)->None:
        self._type_check_attr('dataset', dataset)
        self._dataset = dataset

    @property
    def bucket(self)->str:
        return self._bucket

    @bucket.setter
    def bucket(self, bucket: str)->None:
        self._type_check_attr('bucket', bucket)
        self._bucket = bucket

    @property
    def dataframe(self)->pd.DataFrame:
        return self._dataframe

    @dataframe.setter
    def dataframe(self, dataframe: pd.DataFrame)->None:
        self._type_check_attr('dataframe', dataframe)
        ## cannot support timedelta at this time
        for tp in dataframe.dtypes:
            if tp.name.startswith('timedelta'):
                raise NotImplementedError("Sorry, pyarrow does not support parquet conversion of timedelta columns to parquet.")

        self._dataframe = dataframe

    @property
    def filters(self)->dict:
        return self._filters

    @filters.setter
    def filters(self, filters: dict)->None:
        self._type_check_attr('filters', filters)
        self._filters = filters

    @property
    def key_prefix(self)->str:
        return self._key_prefix

    @key_prefix.setter
    def key_prefix(self, key_prefix: str)->None:
        self._type_check_attr('key_prefix', key_prefix)
        self._key_prefix = key_prefix

    def _check_required_attr(self, attributes: iter)->None:
        """ make sure all required attributes are set before running.
            The sys._getframe bit is the name of the calling function (in this case S3Parq.fetch / S3Parq.publish).
        """
        for required_attr in attributes:
            if not hasattr(self, required_attr):
                raise ValueError(
                    f"Unable to call S3Parq.{sys._getframe(1).f_code.co_name}; missing required attribute {required_attr}")

    def _type_check_attr(self, attr: str, value)->None:
        """ checks typing of attribute and throws error if it is incorrect."""
        for k in [('dataset', str,),
                  ('bucket', str,),
                  ('key_prefix', str,),
                  ('dataframe', pd.DataFrame,),
                  ('filters', dict,)]:
            if attr == k[0]:
                if not isinstance(value, k[1]):
                    raise TypeError(
                        f"Bad value for {attr}; {value} is not an instance of {k[1]}")

    def _set_kwargs_as_attrs(self, **kwargs)->None:
        """ type check and set instance attributes."""
        for key in kwargs.keys():
                self._type_check_attr(key, kwargs[key])
                self.__dict__[key] = kwargs[key]
