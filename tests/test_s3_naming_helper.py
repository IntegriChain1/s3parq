import pytest
from s3_parq.s3_naming_helper import S3NamingHelper

def test_validate_bucket_name():
    helper = S3NamingHelper()

    # must be between 3-63 chars
    response = helper.validate_bucket_name('ab')
    assert not response[0], 'allowed bucket name that was too short'

    response = helper.validate_bucket_name(
        ''.join([str(x) for x in range(0, 65)]))
    assert not response[0], 'allowed bucket name that was too long'

    # lower case chars, numbers, periods, dashes

    response = helper.validate_bucket_name('_$Bucket')
    assert not response[0], 'allowed bucket name with invalid chars'

    # cannot end with dash

    response = helper.validate_bucket_name('bucket-')
    assert not response[0], 'allowed bucket name with dash ending'

    # cannot consecutive periods
    response = helper.validate_bucket_name('bucket..')
    assert not response[0], 'allowed bucket name with double periods'

    # dashes next to periods
    response = helper.validate_bucket_name('bucket-.')
    assert not response[0], 'allowed bucket name with dash next to period'

    # char or number after period
    response = helper.validate_bucket_name('bucket.')
    assert not response[0], 'allowed bucket name without a letter or number after period'

    # char or number at start
    response = helper.validate_bucket_name('_bucket')
    assert not response[0], 'allowed bucket name without a letter or number to start'

    # valid
    response = helper.validate_bucket_name('bucket')
    assert response[0], f'failed to validate valid name - message {response[1]}'


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
