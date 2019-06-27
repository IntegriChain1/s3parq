# Publish
# -------
from dfmock import dfmock
from moto import mock_s3
import boto3
import s3parq as parq



@mock_s3
class Test():

  def test_pub(self):
    cols = { "hamburger":"string",
          "hot_dog":"integer",
          "shoelace":"string"
        }
    client = boto3.client('s3')
    client.create_bucket(Bucket='sweet-bucket')
    dfmocker = dfmock.DFMock(count=100, columns=cols)
    dfmocker.generate_dataframe()
    my_mocked_dataframe = dfmocker.dataframe
    bucket = 'sweet-bucket'
    key='alec'
    parq.publish(bucket=bucket, key=key, dataframe=my_mocked_dataframe, partitions=['hamburger','hot_dog'])

  # Fetch
  # -------
  def test_fetch(self):
    import s3parq
    bucket = 'sweet-bucket'
    key='dc51-contract-spectrum-support/derp/hamberder'

    df = s3parq.fetch( bucket=bucket, key=key,filters=[{"partition":'hot_dog',"values":[90000], "comparison":'>='}])
    return df
