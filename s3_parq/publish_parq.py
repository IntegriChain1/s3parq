import boto3
import pandas as pd
from collections import namedtuple


class S3PublishParq:

    def __init__(self, dataset: str, dataframe: pd.DataFrame, bucket: str, key_prefix: str = '')->None:
        pass

    def do_publish(self):
        response = namedtouple("response", ["rows", "files"])
        if True:
            return response._make(45, 50)
