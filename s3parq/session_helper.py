
import boto3
import urllib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging


class SessionHelper:
    '''
    Helper class for establishing a connection with a Redshift database.  It assumes your IAM
    role has been configured to access the cluster _and_ has privileges on the database.

    Example usage:
    SH = SessionHelper('us-east-1', 'some-cluster-1', 'some-host','5439','some-database')
    SH.configure_session_helper()
    '''

    def __init__(self, region: str, cluster_id: str, host: str, port: str, db_name: str):
        self.region = region
        self.cluster_id = cluster_id
        self.host = host
        self.port = port
        self.db_name = db_name

    def set_boto_session(self):
        self.boto_session = boto3.Session(region_name=self.region)

    def set_iam_user(self):
        iam_client = self.boto_session.client('iam')
        iam_user = iam_client.get_user()
        self.iam_user = iam_user['User']['UserName']

    def set_aws_credentials(self, session):
        credentials = session.get_credentials()
        self.aws_credentials = credentials.get_frozen_credentials()

    def make_db_session(self, **kwargs):
        user, pwd = kwargs['user'], kwargs['pwd']
        self.engine = create_engine(
            f'postgresql://{user}:{pwd}@{self.host}:{self.port}/{self.db_name}')
        self.Session = sessionmaker(bind=self.engine)

    def get_redshift_credentials(self):
        client = self.boto_session.client(
            'redshift',
            region_name=self.region,
            aws_access_key_id=self.aws_credentials.access_key,
            aws_secret_access_key=self.aws_credentials.secret_key
        )
        temp_redshift_credentials = client.get_cluster_credentials(
            DbUser=self.iam_user,
            ClusterIdentifier=self.cluster_id,
            AutoCreate=True,
        )
        return temp_redshift_credentials

    def parse_temp_redshift_credentials(self, rs_creds):
        username = rs_creds['DbUser']
        username = urllib.parse.quote_plus(username)
        pwd = rs_creds['DbPassword']
        return username, pwd

    def configure_session_helper(self):
        try:
            self.set_boto_session()
            self.set_iam_user()
            self.set_aws_credentials(self.boto_session)
            temp_creds = self.get_redshift_credentials()
            user, pwd = self.parse_temp_redshift_credentials(temp_creds)
            self.make_db_session(user=user, pwd=pwd)
        except Exception as e:
            logging.error('Failed to set up the SessionHelper')
            logging.error(e)
            raise

    @contextmanager
    def db_session_scope(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
        finally:
            session.close()
