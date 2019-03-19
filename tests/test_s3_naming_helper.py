import pytest
from s3_parq.s3_naming_helper import S3NamingHelper
from typing import NamedTuple

class TestCase(NamedTuple):
    name: str
    test_name: str


def test_validate_bucket_name():
    helper = S3NamingHelper()

    invalid_cases = [
        # must be between 3-63 chars
        TestCase(name="ab", test_name="allowed bucket name that was too short"),
        TestCase(name=''.join([str(x) for x in range(0, 65)]), test_name='allowed bucket name that was too long'),

        # lower case chars, numbers, periods, dashes
        TestCase(name='_$Bucket', test_name='allowed bucket name with invalid chars'),

        # cannot end with dash
        TestCase(name="bucket-", test_name='allowed bucket name with dash ending'),

        # cannot end with consecutive periods
        TestCase(name="bucket..", test_name="allowed bucket name with double periods"),

        # dashes next to periods
        TestCase(name="bucket-.", test_name="allowed bucket name with dash next to period"),

        # char or number after period
        TestCase(name="bucket.", test_name="allowed bucket name without a letter or number after period)"),

        TestCase(name="_bucket", test_name="allowed bucket name without a letter or number to start")
    ]

    valid_names = ["bucket"]
    for invalid_case in invalid_cases:
        with pytest.raises(ValueError) as excinfo:
            helper.validate_bucket_name(invalid_case.name)

    for valid_name in valid_names:
        helper.validate_bucket_name(valid_name)

def test_validate_part():
    helper = S3NamingHelper()
    response = helper.validate_part('this/is/invalid', allow_prefix=False)

    assert not response[0], f'allowed prefix when prefix was disallowed'

    response = helper.validate_part('')
    assert not response[0], f'allowed blank part'

    response = helper.validate_part('/abc/$$!badval/def')
    assert not response[0], f'allowed bad compound part'


def test_validate_s3_path():
    helper = S3NamingHelper()

    response = helper.validate_s3_path('abc/not/valid')
    assert not response[0], f'allowed s3 path without arn prefix'

    response = helper.validate_s3_path('s3://%%$Bucket_name/is/bad')
    assert not response[0], f'allowed bad bucket name'

    response = helper.validate_s3_path('s3://bucket/path/B#)$_ad/dataset')
    assert not response[0], f'allowed bad bucket prefix'

    response = helper.validate_s3_path('s3://bucket/path/all/good')
    assert response[0], f'disallowed good s3 path'
