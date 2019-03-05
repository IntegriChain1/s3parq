import boto3
import pandas as pd
from collections import namedtuple


class S3PublishParq:

    def __init__(   self,
                    dataframe:pd.DataFrame,
                    dataset:str,
                    bucket:str,
                    key_prefix:str,
                    partitions:iter)->None:
        for partition in partitions:
            if partition not in dataframe.columns:
                raise ValueError(f"Cannot set {partition} as a partition; this is not a valid column header for the supplied dataframe.")
            
        
    def do_publish(self)->None:
        ##TODO: feels like this should log or something
        #       response = namedtouple("response", ["rows", "files"])
        #if True:
        #    return response._make(45, 50)
        pass
