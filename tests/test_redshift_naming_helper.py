import pytest
from s3parq import publish_spectrum as rs


def test_naming_validator():
    # Makes sure the schema name meets best practices

    response = rs._validate_name('my string')
    assert not response[0], response[1]

    response = rs._validate_name('my_string')
    assert response[0], response[1]

    response = rs._validate_name('WHERE')
    assert not response[0], response[1]

    response = rs._validate_name('@my_string')
    assert not response[0], response[1]

    response = rs._validate_name(
        "asdffdsaasdffdsaasdfasdffdsaasdffdsaasd\
        fasdffdsaasdffdsaasdfasdffdsaasdffdsaasdsd\
        ffdsaasdffdsaasdfasdffdsaasdffdsaasdfasdffdsaasdffdsaasdf"
    )
    assert not response[0], f'allowed too long string'
