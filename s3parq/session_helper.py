import boto3
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
import urllib
import urllib.request
import logging


class SessionHelper:
    """
    Helper class for establishing a connection with a Redshift database.  It assumes your IAM
    role has been configured to access the cluster _and_ has privileges on the database.

    Args:
        region (str): AWS region the session should be using for boto
        cluster_id (str): AWS Redshift cluser_id to connect to
        host (str): Host for Redshift connection
        port (str): port for Redshift connection
        db_name (str): Database name to connect to in Redshift
        ec2_user (str): User to use for ec2, can be blank if using iam of local

    Example:
        SH = SessionHelper('us-east-1', 'some-cluster-1', 'some-host','5439','some-database')
        SH.configure_session_helper()
    """

    def __init__(self, region: str, cluster_id: str, host: str, port: str, db_name: str, ec2_user: str):
        self.region = region
        self.cluster_id = cluster_id
        self.host = host
        self.port = port
        self.db_name = db_name
        self.ec2_user = ec2_user
        self.is_ec2_flag = self._is_ec2()

    def _is_ec2(self):
        """ Determines if the session is running on an ec2 server.
        Uses urllib to determine this by attempting to find the instance id;
        if there is one then its on ec2, otherwise the command will hang and
        timeout.
        """
        try:
            # Timeout set to 2 seconds to avoid prolonged hang if the url cannot be reached
            self.instance_id = urllib.request.urlopen(
                'http://169.254.169.254/latest/meta-data/instance-id', timeout=2).read().decode()
            return True
        except urllib.error.URLError:
            return False

    def set_boto_session(self) -> None:
        """ Adds the boto3 session to the class, with the region set here """
        self.boto_session = boto3.Session(region_name=self.region)

    def set_iam_user(self) -> None:
        """ Adds the iam user to the SessionHelper, has a switch for ec2 or local """
        if self.is_ec2_flag:
            # On ec2, the cluster user is set from the redshift configuration dictionary
            self.iam_user = self.ec2_user
        else:
            iam_client = self.boto_session.client('iam')
            iam_user = iam_client.get_user()
            self.iam_user = iam_user['User']['UserName']

    def set_aws_credentials(self, session: boto3.Session) -> None:
        """ Sets the aws_credentials based on what's in the boto session """
        credentials = session.get_credentials()
        self.aws_credentials = credentials.get_frozen_credentials()

    def make_db_session(self, **kwargs) -> None:
        """ Creates the sqlalchemy Session to Redshift based on the class params
        NOTE: this session is set to autocommit

        Args:
            **kwargs : Arbitrary length keyword params
                Must include:
                    user (str): The user to connect to Redshift under
                    pwd (str): The password to connect to Redshift with

        Returns:
            None
        """
        user, pwd = kwargs['user'], kwargs['pwd']
        self.engine = sqlalchemy.create_engine(
            f'postgresql://{user}:{pwd}@{self.host}:{self.port}/{self.db_name}', isolation_level="AUTOCOMMIT")
        self.Session = sessionmaker(bind=self.engine)

    def get_redshift_credentials(self) -> dict:
        """ Uses the boto session client to get temporary credentials to
        connect to Redshift with.
        If this is in ec2, it will just use the local access stuff, otherwise
        it will use the boto session's credentials.

        Returns:
            This returns a dict of the cluster_credentials per boto's Redshift
            Format is:
                DbUser (str): Database user for Redshift
                DbPassword (str): Password for the database user for Redshift
                Expiration (datetime): The date and time when the password expires
        """
        if self.is_ec2_flag:
            client = self.boto_session.client(
                'redshift', region_name=self.region)

            temp_redshift_credentials = client.get_cluster_credentials(
                DbUser=self.iam_user,
                ClusterIdentifier=self.cluster_id,
                AutoCreate=True,
            )
        else:
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

    def parse_temp_redshift_credentials(self, rs_creds: dict) -> tuple:
        """ Return the user credentials as a tuple, with the username html safe
        by changing any spaces the +'s

        Args:
            rs_creds (dict): The Redshift credentials returned by boto
                Format is:
                    DbUser (str): Database user for Redshift
                    DbPassword (str): Password for the database user for Redshift
                    Expiration (datetime): The date and time when the password expires

        Returns:
            Tuple of creds, format of:
                DbUser (str): Database user for Redshift, html escaped
                DbPassword (str): Password for the database user for Redshift
        """
        username = rs_creds['DbUser']
        username = urllib.parse.quote_plus(username)
        pwd = rs_creds['DbPassword']
        return username, pwd

    def configure_session_helper(self) -> None:
        """ Runs all of the configuration steps for SessionHelper in one go.
        Steps are:
            1. Creates boto session with the initialized region
            2. Sets the iam user
            3. Sets the full aws credentials from the boto session
            4. Gets temporary Redshift credentials
            5. Creates the sqlalchemy database session for use based on above
        """
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
    def db_session_scope(self) -> sessionmaker:
        """ Wraps the sqlalchemy session so that it can used a try-except-finally
        to ensure there are rollbacks and that the session is always closed.

        Yields:
            The sqlalchemy database session, configured by class attributes

        Excepts:
            Exception: This triggers a session rollback, after which it is
                raised immediately
        """
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
