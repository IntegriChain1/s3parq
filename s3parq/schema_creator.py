from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from s3parq.redshift_naming_helper import RedshiftNamingHelper

def schema_generator(schema_name: str):
    name_validated = RedshiftNamingHelper().validate_name(schema_name)
    if name_validated[0]:
        return f"create schema if not exists {schema_name};"
    else:
        raise ValueError(name_validated[1])


def create_schema(schema_name: str, session_helper: SessionHelper):
    schema_n = schema_generator(schema_name)
    session_helper.configure_session_helper()
    with session_helper.db_session_scope() as scope:
        scope.execute(f'CREATE SCHEMA IF NOT EXISTS {schema_n}')
