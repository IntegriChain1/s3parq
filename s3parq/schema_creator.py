from s3parq.session_helper import SessionHelper
from sqlalchemy import Column, Integer, String
from s3parq.redshift_naming_helper import RedshiftNamingHelper
import logging

logger = logging.getLogger(__name__)


def schema_name_validator(schema_name: str):
    name_validated = RedshiftNamingHelper().validate_name(schema_name)
    if not name_validated[0]:
        raise ValueError(name_validated[1])


def create_schema(schema_name: str, session_helper: SessionHelper):
    schema_name_validator(schema_name)
    with session_helper.db_session_scope() as scope:
        new_schema_query = f'CREATE SCHEMA IF NOT EXISTS {schema_name}'
        logger.info(f'Running query to create schema: {new_schema_query}')
        scope.execute(new_schema_query)
