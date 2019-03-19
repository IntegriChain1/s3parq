import boto3
from s3parq.publish_parq import publish
from s3parq.fetch_parq import fetch

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
        pub = publish(
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
