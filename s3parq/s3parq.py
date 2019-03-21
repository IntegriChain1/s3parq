import boto3
from s3parq.publish_parq import publish
from s3parq.fetch_parq import fetch, get_max_partition_value

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

    def fetch(self,
              bucket: str,
              key: str,
              **kwargs
              ) -> None:

        return fetch(key=key,
                     bucket=bucket,
                     filters=kwargs.get('partitions', dict())
                     )

    def get_max_partition_value(self, bucket: str, key: str, partition: str) -> any:
        return get_max_partition_value(
            bucket=bucket,
            key=key,
            partition=partition
        )
