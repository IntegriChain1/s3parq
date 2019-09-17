import pytest
from s3parq.session_helper import SessionHelper

class Test:
    
    def test_no_ec2(self):
        '''Verify that False is returned if not run on an ec2 server'''
        is_ec2 = SessionHelper._is_ec2(self)
        assert is_ec2 is False

        '''Verify that ec2 flag evaluates to false if not run on an ec2 server'''
        sh = SessionHelper(
            region = 'cheeseburger',
            cluster_id = 'cheeseburger_id',
            host = 'cheeseburger_host',
            port = 'cheeseburger_port',
            db_name = 'cheeseburger_params',
            ec2_user = 'cheeseburger_params')
        assert sh.is_ec2_flag is False

    def test_iam_user(self):
        '''Verify that iam_user will assume the value of ec2_user when run on an ec2 server'''
        sh = SessionHelper(
            region = 'cheeseburger',
            cluster_id = 'cheeseburger_id',
            host = 'cheeseburger_host',
            port = 'cheeseburger_port',
            db_name = 'cheeseburger_params',
            ec2_user = 'cheeseburger_user')
        # Fake is_ec2_flag to be True
        sh.is_ec2_flag = True
        sh.set_iam_user()
        assert sh.iam_user is 'cheeseburger_user'

    def test_no_ec2_user(self):
        '''Verify that SessionHelper can accept None as an argument for ec2 user'''
        sh = SessionHelper(
            region = 'cheeseburger',
            cluster_id = 'cheeseburger_id',
            host = 'cheeseburger_host',
            port = 'cheeseburger_port',
            db_name = 'cheeseburger_params',
            ec2_user = None)
        assert(type(sh.ec2_user)) is type(None)

    def test_ec2_user(self):
        '''Verify that SessionHelper can accept a string as an argument for ec2 user'''
        sh = SessionHelper(
            region = 'cheeseburger',
            cluster_id = 'cheeseburger_id',
            host = 'cheeseburger_host',
            port = 'cheeseburger_port',
            db_name = 'cheeseburger_params',
            ec2_user = 'cheeseburger_user')
        assert(isinstance(sh.ec2_user, str)) is True




