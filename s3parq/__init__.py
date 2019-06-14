__version__ = "1.0.3"
from .s3parq import S3Parq

from s3parq.fetch_parq import fetch
from s3parq.fetch_parq import fetch_diff
from s3parq.fetch_parq import get_max_partition_value
from s3parq.publish_parq import publish
