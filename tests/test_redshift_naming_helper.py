import pytest
from s3parq.redshift_naming_helper import RedshiftNamingHelper


def test_naming_validator():
    helper = RedshiftNamingHelper()
    # Makes sure the schema name meets best practices

    response = helper.validate_chars('my string')
    assert not response[0], response[1]

    response = helper.validate_chars('my_string')
    assert response[0], response[1]

    response = helper.validate_chars('WHERE')
    assert not response[0], response[1]

    response = helper.validate_chars('@my_string')
    assert not response[0], response[1]

    response = helper.validate_chars(
        "asdffdsaasdffdsaasdfasdffdsaasdffdsaasd\
        fasdffdsaasdffdsaasdfasdffdsaasdffdsaasdsd\
        ffdsaasdffdsaasdfasdffdsaasdffdsaasdfasdffdsaasdffdsaasdf"
    )
    assert not response[0], f'allowed too long string'
