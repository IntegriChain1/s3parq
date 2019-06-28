from configuration import SessionHelper
from sqlalchemy import Column, Integer, String
SH = SessionHelper('us-east-1', 'core-sandbox-cluster-1', 'core-sandbox-cluster-1.c3swieqn0nz0.us-east-1.redshift.amazonaws.com','5439','ichain_core', 'tobiasj-dev')

SH.get_boto_session()
SH.set_aws_credentials(SH.boto_session)
rmp_creds = SH.get_redshift_credentials()
un, pwd = SH.parse_temp_redshift_credentials(rmp_creds)
SH.make_db_session(user=un, pwd=pwd)

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Test11(Base):
    __table_args__ = {'schema': 'spectrum'}
    __tablename__ = 'test11'
    shoelace = Column(String, primary_key=True)
    hot_dog = Column(Integer)
    hamburger = Column(String)

with SH.db_session_scope() as scope:
    import pdb; pdb.set_trace()
    derp = scope.query(Test11)
    for x in derp.values():
        print(x)