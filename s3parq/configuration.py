
import boto3
import urllib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import logging


class SessionHelper:

    def __init__(self, region, cluster_id, host, port, db_name, iam_user):
        self.region = region
        self.cluster_id = cluster_id
        self.host = host
        self.port = port
        self.db_name = db_name
        self.iam_user = iam_user
        self.boto_session = None
        self.rs_client = None  # Redshift Client
        self.aws_credentials = None
        self.Session = None
        self.engine = None

    def get_boto_session(self):
        try:
            self.boto_session = boto3.Session(region_name=self.region)
            return True
        except Exception as e:  # TODO: which specific errors can occur?
            logging.warn('Failed to create a boto3 session')
            logging.warn(e)
            return False

    def make_db_session(self, **kwargs):
        if not self.engine:
            user, pwd = kwargs['user'], kwargs['pwd']
            self.engine = create_engine(f'postgresql://{user}:{pwd}@{self.host}:{self.port}/{self.db_name}')
        self.Session = sessionmaker(bind=self.engine)

    def set_aws_credentials(self, session):
        try:
            credentials = session.get_credentials()
            self.aws_credentials = credentials.get_frozen_credentials()
            return True
        except Exception as e:  # TODO: which specific errors can occur?
            logging.warn('Failed to generate a redshift client')
            logging.warn(e)
            return False

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
