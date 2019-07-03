
from session_helper import SessionHelper
from sqlalchemy import Column, Integer, String


SH = SessionHelper('us-east-1', 'core-sandbox-cluster-1', 'core-sandbox-cluster-1.c3swieqn0nz0.us-east-1.redshift.amazonaws.com','5439','ichain_core')
SH.configure_session_helper()


def schema_string_validator(schema_query: str):
    return "jejkg"


def schema_generator(verified_string: str):
    return schema_string_validator(verified_string)


def create_schema(schema_name: str):
    schema_n = schema_generator(schema_name)
    with self.SH.db_session_scope() as scope:
        scope.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_n}')
