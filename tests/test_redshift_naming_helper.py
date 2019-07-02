import pytest
from s3parq.redshift_naming_helper import RedshiftNamingHelper


def test_naming_validator():
    helper = RedshiftNamingHelper
    # Makes sure the schema name meets best practices

    response = helper.name_validator('my string')
    assert not response, f'allowed name with space'

    response = helper.name_validator('my_string')
    assert response, f'disallowed valid schema name'

    response = helper.name_validator('WHERE')
    assert not response, f'allowed SQL keyword as name'

    response = helper.name_validator('@my_string')
    assert not response, f'allowed invalid character'

    response = helper.name_validator(
        "asdffdsaasdffdsaasdfasdffdsaasdffdsaasd\
        fasdffdsaasdffdsaasdfasdffdsaasdffdsaasdsd\
        ffdsaasdffdsaasdfasdffdsaasdffdsaasdfasdffdsaasdffdsaasdf"
    )
    assert not response[0], f'allowed too long string'
