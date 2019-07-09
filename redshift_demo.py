from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from moto import mock_redshift


@mock_redshift
def test_connect():
    # Connect
    # -------
    SH = SessionHelper(
      'us-east-1',
      'core-sandbox-cluster-1',
      'core-sandbox-cluster-1.c3swieqn0nz0.us-east-1.redshift.amazonaws.com',
      '5439',
      'ichain_core'
    )

    SH.configure_session_helper()

    Base = declarative_base()

    class Hamburger(Base):
        __table_args__ = {'schema': 'spectrum'}
        __tablename__ = 'test11'
        shoelace = Column(String, primary_key=True)
        hot_dog = Column(Integer)
        hamburger = Column(String)

    with SH.db_session_scope() as scope:
        data = scope.query(Hamburger).limit(2)
        for d in data:
            print(d.hamburger, d.hot_dog, d.shoelace)

# When building the cluster, make sure you set the role of the cluster
# to have access to S3

# create external schema spectrum
# from data catalog
# database 'spectrumdb'

# iam_role 'arn:aws:iam::265991248033:role/mySpectrumRole'
# create external database if not exists;

# create external table spectrum.test11 (shoelace varchar)
# partitioned by (hamburger varchar, hot_dog int)

# stored as parquet
# location 's3://ichain-dev/dc51-contract-spectrum-support/derp/hamberder/'

# alter table spectrum.test11

# add partition (hamburger='abnvoejvaspabwgbjinwcr', hot_dog=926099)

# location
# 's3://ichain-dev/dc51-contract-spectrum-support/derp/hamberder/hamburger=abnvoejvaspabwgbjinwcr';

# select * from spectrum.test11# SQLAlchemy will treat redshift table same
# way as a postgres table
