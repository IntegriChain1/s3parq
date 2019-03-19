import boto3
from .publish_parq import S3PublishParq
from .fetch_parq import S3FetchParq
import pandas as pd
import sys
import logging


class S3Parq:

    def publish(self,
                bucket:str, 
                dataset:str,
                dataframe:pd.DataFrame,
                **kwargs
                    )->None:
        pub = S3PulishParq( dataset= dataset,
                            dataframe= dataframe,
                            bucket = bucket,
                            prefix = kwargs.get('prefix',''),
                            partitions = kwargs.get('partitions','')
                          )
        pub.publish()                     
    

    def fetch(  bucket:str, 
                dataset:str,
                **kwargs
                    )->None:
        fetch = S3FetchParq( dataset= dataset,
                            dataframe= dataframe,
                            bucket = bucket,
                            prefix = kwargs.get('prefix',''),
                            filters = kwargs.get('partitions',dict())
                          )
        fetch.fetch()


