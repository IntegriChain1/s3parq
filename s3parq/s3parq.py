import boto3
from s3parq.publish_parq import publish
from s3parq.fetch_parq import fetch, get_max_partition_value, fetch_diff

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

    def fetch_diff(self,
        input_bucket: str, 
        input_key: str, 
        comparison_bucket: str, 
        comparison_key: str, 
        partition: str, 
        parallel: bool = True
    ) -> pd.DataFrame:
        return fetch_diff(
            input_bucket = input_bucket, 
            input_key = input_key, 
            comparison_bucket = comparison_bucket, 
            comparison_key = comparison_key, 
            partition = partition, 
            parallel = parallel
        )

    def get_max_partition_value(self, bucket: str, key: str, partition: str) -> any:
        return get_max_partition_value(
            bucket=bucket,
            key=key,
            partition=partition
        )
