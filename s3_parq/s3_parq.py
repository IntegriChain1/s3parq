import boto3
from .publish_parq import S3PublishParq
from s3_parq.fetch_parq import fetch

import pandas as pd
import sys
import logging
from typing import Iterable


class S3Parq:

    def publish(self,
                bucket: str,
                key: str,
                dataframe: pd.DataFrame,
                partitions: Iterable[str]) -> None:
        pub = S3PublishParq(
            dataframe=dataframe,
            bucket=bucket,
            key=key,
            partitions=partitions
        )
        pub.publish()

    def fetch(bucket: str,
              key: str,
              **kwargs
              ) -> None:
        return fetch(key=key,
                     bucket=bucket,
                     filters=kwargs.get('partitions', dict())
                     )
