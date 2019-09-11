from dfmock import dfmock
import s3parq.publish_parq as parq
import logging

logging.basicConfig(level=logging.DEBUG)

columns = { "hamburger":{"option_count":3, "option_type": "string"},
            "hot_dog":{"option_count":5, "option_type": "integer"},
            "shoelace":"string"
          }

dfmocker = dfmock.DFMock(count=100, columns=columns, )
dfmocker.generate_dataframe()
my_mocked_dataframe = dfmocker.dataframe
bucket = 'ichain-dev'
key = 'thanks/justin'
print(key)
parq.publish(bucket=bucket, key=key, dataframe=my_mocked_dataframe, partitions=['hamburger','hot_dog'], redshift_params = {
  'schema_name': 'happy_schema',
  'table_name': 'happy_table',
  'iam_role': 'arn:aws:iam::265991248033:role/mySpectrumRole',
  'region': 'us-east-1',
  'cluster_id': 'core-sandbox-cluster-1',
  'host': 'core-sandbox-cluster-1.c3swieqn0nz0.us-east-1.redshift.amazonaws.com',
  'port': '5439',
  'db_name': 'ichain_core'})