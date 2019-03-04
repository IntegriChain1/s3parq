import boto3
from .s3_parq_publish import S3ParqPublish
import pandas as pd

class S3Parq:

    def __init__(self, **kwargs):
        self._set_kwargs_as_attrs(**kwargs)
        

    def publish(self,**kwargs)->None:
        for required_attr in ('dataset','bucket','dataframe'):
            if not hasattr(self,required_attr):
                raise TypeError(f"Unable to call S3Parq.publish(); missing required attribute {required_attr}")
    
        publish = S3ParqPublish(dataset=self.dataset,
                                bucket=self.bucket,
                                key_prefix=self.key_prefix,
                                dataframe=self.dataframe)
        return publish.do_publish()

    def _set_kwargs_as_attrs(self, **kwargs)->None:       
        for k in [  ('dataset',str,),
                    ('bucket',str,),
                    ('key_prefix',str,),
                    ('dataframe',pd.DataFrame,),
                    ('filters',dict,)]:
            if k[0] in kwargs.keys():
                key, typeof = k
                if not isinstance(kwargs[key],typeof):
                    raise TypeError(f"Bad value for {key}; {kwargs[key]} is not an instance of {typeof}")
                self.__dict__[key] = kwargs[key]
