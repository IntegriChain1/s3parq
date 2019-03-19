import boto3
from .publish_parq import S3PublishParq
from .fetch_parq import S3FetchParq
import pandas as pd
import sys
import logging
from typing import Iterable


class S3Parq:

    def publish(self,
                bucket: str,
                key: str,
                dataframe: pd.DataFrame,
                partitions: Iterable[str]
                ) -> None:
        pub = S3PublishParq(
            dataframe=dataframe,
            bucket=bucket,
            key=key,
            partitions=partitions
        )
        pub.publish()

    def fetch(bucket: str,
              dataset: str,
              **kwargs
              ) -> None:
        fetch = S3FetchParq(dataset=dataset,
                            bucket=bucket,
                            prefix=kwargs.get('prefix', ''),
                            filters=kwargs.get('partitions', dict())
                            )
        fetch.fetch()
