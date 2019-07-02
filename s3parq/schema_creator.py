

def schema_string_validator(schema_query: str):
    return "jejkg"


def schema_generator(verified_string: str):
    return "djklfjs"

from session_helper import SessionHelper
from sqlalchemy import Column, Integer, String

SH = SessionHelper('us-east-1', 'core-sandbox-cluster-1', 'core-sandbox-cluster-1.c3swieqn0nz0.us-east-1.redshift.amazonaws.com','5439','ichain_core')
SH.configure_session_helper()

with SH.db_session_scope() as scope:
    scope.execute('CREATE SCHEMA IF NOT EXISTS justins_schema')