from sqlalchemy import engine, create_engine, Column, Integer, String, Boolean, TIMESTAMP, text, ForeignKey, func, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import session, sessionmaker, relationship, validates
import json
import yaml
from types import SimpleNamespace
from datetime import datetime
from typing import Any
import os
Base = declarative_base()

## add sqlalchemy 

from sqlalchemy import engine, create_engine

import os
# region_name = os.environ['AWS_REGION']
region_name = 'us-east-1'

import boto3
session = boto3.Session(region_name=region_name)
credentials = session.get_credentials()

credentials = credentials.get_frozen_credentials()
access_key = credentials.access_key
secret_key = credentials.secret_key

redshift = session.client('redshift')

def get_redshift_credentials(region, access_key, secret_key, redshift_user, cluster_id):
    client = boto3.client(
        'redshift',
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    response = client.get_cluster_credentials(
        DbUser=redshift_user,
        ClusterIdentifier=cluster_id,
        AutoCreate=True
    )
    return response

creds = get_redshift_credentials(region_name, access_key, secret_key, 'tobiasj-dev', 'core-sandbox-cluster-1')
host = 'core-sandbox-cluster-1.c3swieqn0nz0.us-east-1.redshift.amazonaws.com'
port='5439'
db='ichain_dev'
user = creds['DbUser']
import urllib
user = urllib.parse.quote_plus(user)
pwd = creds['DbPassword']
engine = create_engine(f'postgresql://{user}:{pwd}@{host}:{port}/{db}')

cxn = engine.connect() # This breaks