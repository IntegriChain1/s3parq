import boto3
from dfmock import DFMock
from mock import patch
from moto import mock_s3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import s3fs

import s3parq.publish_parq as parq
from s3parq.testing_helper import (
    setup_grouped_dataframe,
    setup_random_string,
    sorted_dfs_equal_by_pandas_testing,
    setup_custom_redshift_columns_and_dataframe
)


@mock_s3
class Test:
    """ Test class for testing out publish_parq
    Unlike fetch, this functionality is encompassible in one file, with just a 
    few sections.
    """

    '''
    Setup helpers
    '''

    def setup_s3(self):
        bucket = setup_random_string()
        key = setup_random_string()

        s3_client = boto3.client('s3')
        s3_client.create_bucket(Bucket=bucket)

        return bucket, key

    def setup_redshift_params(self):
        redshift_params = {
            'schema_name': 'hamburger_schema',
            'table_name': 'hamburger_table',
            'iam_role': 'hamburger_arn',
            'region': 'us-east-1',
            'cluster_id': 'hamburger_cluster',
            'host': 'hamburger_host',
            'port': '9999',
            'db_name': 'hamburger_db',
            'ec2_user': 'hamburger_aws',
            'read_access_user': 'some_read_only_user'
        }

        return redshift_params

    '''
    Internal-facing publish tests
    '''

    def test_reject_protected_name_partitions(self):
        assert parq._check_partition_compatibility("shoe")
        assert parq._check_partition_compatibility("all") is False

    def test_generates_partitions_in_order(self):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        partitions = dataframe.columns[:2]
        with patch('s3parq.publish_parq.pq.write_to_dataset', return_value=None) as mock_method:
            parq._gen_parquet_to_s3(bucket, key, dataframe, partitions)
            arg, kwarg = mock_method.call_args
            assert (kwarg['partition_cols'] == partitions).all()

    def test_df_datatypes(self):
        dataframe = setup_grouped_dataframe()
        assert parq._get_dataframe_datatypes(
            dataframe) == dataframe.dtypes.to_dict()

    def test_partition_datatypes(self):
        dataframe = setup_grouped_dataframe()
        partitions = ["text_col", "int_col", "float_col"]
        dataframe_dtypes = dataframe.dtypes.to_dict()
        part_dtypes = {}
        for part in partitions:
            part_dtypes[part] = dataframe_dtypes.pop(part, None)
        assert parq._get_dataframe_datatypes(
            dataframe, partitions, True) == part_dtypes

    def test_dataframe_sans_partitions(self):
        dataframe = setup_grouped_dataframe()
        partitions = ["text_col", "int_col", "float_col"]
        dataframe_dtypes = dataframe.dtypes.to_dict()
        for part in partitions:
            dataframe_dtypes.pop(part, None)
        assert parq._get_dataframe_datatypes(
            dataframe, partitions) == dataframe_dtypes

    '''
    External publish functionality tests
    '''

    def test_accepts_valid_partitions(self):
        dataframe = setup_grouped_dataframe()
        parq.check_partitions(dataframe.columns, dataframe)

    def test_reject_non_column_partitions(self):
        dataframe = setup_grouped_dataframe()
        with pytest.raises(ValueError):
            parq.check_partitions(('banana',), dataframe)

    def test_reject_empty_dataframe(self):
        dataframe = pd.DataFrame()
        bucket, key = self.setup_s3()
        s3_path = f"s3://{bucket}/{key}"

        with pytest.raises(ValueError):
            parq.publish(bucket=bucket, key=key,
                         dataframe=dataframe, partitions=[])

    def test_reject_timedelta_dataframes(self):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        partitions = ['text_col']
        dataframe['time_col'] = pd.Timedelta('1 days')
        with pytest.raises(NotImplementedError):
            parq.publish(bucket=bucket, key=key,
                         dataframe=dataframe, partitions=partitions)

    def test_works_without_partitions(self):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        partitions = []
        parq.publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions)

    def test_input_equals_output(self):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        s3_path = f"s3://{bucket}/{key}"
        partitions = [dataframe.columns[0]]
        parq.publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions)

        from_s3 = pq.ParquetDataset(s3_path, filesystem=s3fs.S3FileSystem())
        s3pd = from_s3.read().to_pandas()
        # Switch partition type back -> by default it gets set to a category
        s3pd[partitions[0]] = s3pd[partitions[0]].astype(
            dataframe[partitions[0]].dtype)

        sorted_dfs_equal_by_pandas_testing(dataframe, s3pd)

    def test_set_metadata_correctly(self):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        s3_client = boto3.client('s3')
        partitions = ['string_col', 'int_col', 'bool_col']
        parq.publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions)
        for obj in s3_client.list_objects(Bucket=bucket)['Contents']:
            if obj['Key'].endswith(".parquet"):
                meta = s3_client.get_object(
                    Bucket=bucket, Key=obj['Key'])['Metadata']
                assert meta['partition_data_types'] == str(
                    {"string_col": "string", "int_col": "integer", "bool_col": "boolean"})

    '''
    External custom publish functionality tests
    '''
    def test_custom_publish_reject_empty_dataframe(self):
        dataframe = pd.DataFrame()
        custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()[1]
        bucket, key = self.setup_s3()
        s3_path = f"s3://{bucket}/{key}"

        with pytest.raises(ValueError):
            parq.custom_publish(bucket=bucket, key=key,
                         dataframe=dataframe, partitions=[], 
                         custom_redshift_columns=custom_redshift_columns)

    def test_custom_publish_reject_timedelta_dataframes(self):
        dataframe, custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()
        bucket, key = self.setup_s3()
        partitions = ['colA']
        dataframe['time_col'] = pd.Timedelta('1 days')
        with pytest.raises(NotImplementedError):
            parq.custom_publish(bucket=bucket, key=key,
                         dataframe=dataframe, partitions=partitions,
                         custom_redshift_columns=custom_redshift_columns)


    def test_custom_publish_works_without_partitions(self):
        dataframe, custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()
        bucket, key = self.setup_s3()
        partitions = []
        parq.custom_publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions,
                     custom_redshift_columns=custom_redshift_columns)

    def test_custom_publish_input_equals_output(self):
        dataframe, custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()
        bucket, key = self.setup_s3()
        s3_path = f"s3://{bucket}/{key}"
        partitions = [dataframe.columns[0]]
        parq.custom_publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions,
                     custom_redshift_columns=custom_redshift_columns)

        from_s3 = pq.ParquetDataset(s3_path, filesystem=s3fs.S3FileSystem())
        s3pd = from_s3.read().to_pandas()
        # Switch partition type back -> by default it gets set to a category
        s3pd[partitions[0]] = s3pd[partitions[0]].astype(
            dataframe[partitions[0]].dtype)

        sorted_dfs_equal_by_pandas_testing(dataframe, s3pd)

    def test_custom_publish_set_metadata_correctly(self):
        dataframe, custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()
        bucket, key = self.setup_s3()
        s3_client = boto3.client('s3')
        partitions = ['colA', 'colB', 'colC', 'colD', 'colF']
        parq.custom_publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions,
                     custom_redshift_columns=custom_redshift_columns)
        for obj in s3_client.list_objects(Bucket=bucket)['Contents']:
            if obj['Key'].endswith(".parquet"):
                meta = s3_client.get_object(
                    Bucket=bucket, Key=obj['Key'])['Metadata']
                assert meta['partition_data_types'] == str(
                    {"colA": "string", "colB": "integer", "colC": "float", "colD": "decimal", "colF": "boolean"})

    '''
    Tests related to Redshift Spectrum
    '''

    # Test to ensure that exception are raised on missing/wrong params
    def test_validate_redshift_params_wrong_params(self):
        redshift_params = self.setup_redshift_params()
        redshift_params.pop("table_name", None)

        with pytest.raises(ValueError):
            parq.validate_redshift_params(redshift_params)

        redshift_params['tbale_name'] = "some_typoed_mess"
        with pytest.raises(KeyError):
            parq.validate_redshift_params(redshift_params)

    # Test to ensure that redshift parameter validation checks and addresses table name caseing
    def test_validate_redshift_params_table_case(self):
        redshift_params = self.setup_redshift_params()
        redshift_params['table_name'] = 'hAMbURgeR_tAbLE'
        assert parq.validate_redshift_params(redshift_params)[
            'table_name'] == 'hamburger_table'

    # Test to ensure that redshift parameter validation checks and addresses schema name caseing
    def test_validate_redshift_params_schema_case(self):
        redshift_params = self.setup_redshift_params()
        redshift_params['schema_name'] = 'HAmbURger_sCHemA'
        assert parq.validate_redshift_params(redshift_params)[
            'schema_name'] == 'hamburger_schema'

    # Test Schema Creator on Publish
    # TODO : is this necesarry?
    def test_no_redshift_publish(self):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        partitions = []
        parq.publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions)

    @patch('s3parq.publish_redshift.create_schema')
    @patch('s3parq.publish_parq.SessionHelper')
    def test_schema_publish(self, mock_session_helper, mock_create_schema):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        partitions = [dataframe.columns[0]]
        redshift_params = self.setup_redshift_params()
        msh = mock_session_helper(
            region=redshift_params['region'],
            cluster_id=redshift_params['cluster_id'],
            host=redshift_params['host'],
            port=redshift_params['port'],
            db_name=redshift_params['db_name']
        )

        msh.configure_session_helper()
        parq.publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions, redshift_params=redshift_params)

        mock_create_schema.assert_called_once_with(
            redshift_params['schema_name'], redshift_params['db_name'], redshift_params['iam_role'], msh, redshift_params['read_access_user'])

    @patch('s3parq.publish_redshift.create_table')
    @patch('s3parq.publish_parq.SessionHelper')
    def test_table_publish(self, mock_session_helper, mock_create_table):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        partitions = ["text_col", "int_col", "float_col"]
        redshift_params = self.setup_redshift_params()
        msh = mock_session_helper(
            region=redshift_params['region'],
            cluster_id=redshift_params['cluster_id'],
            host=redshift_params['host'],
            port=redshift_params['port'],
            db_name=redshift_params['db_name']
        )

        msh.configure_session_helper()
        parq.publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions, redshift_params=redshift_params)

        df_types = parq._get_dataframe_datatypes(dataframe, partitions)
        partition_types = parq._get_dataframe_datatypes(
            dataframe, partitions, True)

        mock_create_table.assert_called_once_with(
            redshift_params['table_name'], redshift_params['schema_name'], df_types, partition_types, parq.s3_url(bucket, key), msh)

    @patch('s3parq.publish_redshift.create_table')
    @patch('s3parq.publish_parq.SessionHelper')
    def test_table_publish_mixed_type_column(self, mock_session_helper, mock_create_table):
        dataframe = setup_grouped_dataframe()
        bucket, key = self.setup_s3()
        partitions = []
        redshift_params = self.setup_redshift_params()
        msh = mock_session_helper(
            region=redshift_params['region'],
            cluster_id=redshift_params['cluster_id'],
            host=redshift_params['host'],
            port=redshift_params['port'],
            db_name=redshift_params['db_name']
        )

        msh.configure_session_helper()

        dataframe.iat[5, dataframe.columns.get_loc("text_col")] = 45

        parq.publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions, redshift_params=redshift_params)

        df_types = parq._get_dataframe_datatypes(dataframe, partitions)
        partition_types = parq._get_dataframe_datatypes(
            dataframe, partitions, True)

        mock_create_table.assert_called_once_with(
            redshift_params['table_name'], redshift_params['schema_name'], df_types, partition_types, parq.s3_url(bucket, key), msh)

    '''
    Tests related to Redshift Spectrum with custom publish
    '''
    # Test Schema Creator on Publish
    def test_custom_publish_no_redshift_publish(self):
        dataframe, custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()
        bucket, key = self.setup_s3()
        partitions = []
        parq.custom_publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions,
                     custom_redshift_columns=custom_redshift_columns)

    @patch('s3parq.publish_redshift.create_schema')
    @patch('s3parq.publish_parq.SessionHelper')
    def test_custom_publish_schema_publish(self, mock_session_helper, mock_create_schema):
        dataframe, custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()
        bucket, key = self.setup_s3()
        partitions = [dataframe.columns[0]]
        redshift_params = self.setup_redshift_params()
        msh = mock_session_helper(
            region=redshift_params['region'],
            cluster_id=redshift_params['cluster_id'],
            host=redshift_params['host'],
            port=redshift_params['port'],
            db_name=redshift_params['db_name']
        )

        msh.configure_session_helper()
        parq.custom_publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions, redshift_params=redshift_params,
                     custom_redshift_columns=custom_redshift_columns)

        mock_create_schema.assert_called_once_with(
            redshift_params['schema_name'], redshift_params['db_name'], redshift_params['iam_role'], msh, redshift_params['read_access_user'])

    @patch('s3parq.publish_redshift.create_custom_table')
    @patch('s3parq.publish_parq.SessionHelper')
    def test_custom_publish_table_publish(self, mock_session_helper, mock_create_custom_table):
        dataframe, custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()
        bucket, key = self.setup_s3()
        partitions = ["colA", "colB", "colC"]
        redshift_params = self.setup_redshift_params()
        msh = mock_session_helper(
            region=redshift_params['region'],
            cluster_id=redshift_params['cluster_id'],
            host=redshift_params['host'],
            port=redshift_params['port'],
            db_name=redshift_params['db_name']
        )

        msh.configure_session_helper()
        parq.custom_publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions, redshift_params=redshift_params,
                     custom_redshift_columns=custom_redshift_columns)

        mock_create_custom_table.assert_called_once_with(
            redshift_params['table_name'], redshift_params['schema_name'], partitions, parq.s3_url(bucket, key), custom_redshift_columns, msh)

    @patch('s3parq.publish_redshift.create_custom_table')
    @patch('s3parq.publish_parq.SessionHelper')
    def test_custom_table_publish_mixed_type_column(self, mock_session_helper, mock_create_custom_table):
        dataframe, custom_redshift_columns = setup_custom_redshift_columns_and_dataframe()
        bucket, key = self.setup_s3()
        partitions = []
        redshift_params = self.setup_redshift_params()
        msh = mock_session_helper(
            region=redshift_params['region'],
            cluster_id=redshift_params['cluster_id'],
            host=redshift_params['host'],
            port=redshift_params['port'],
            db_name=redshift_params['db_name']
        )

        msh.configure_session_helper()

        dataframe.iat[1, dataframe.columns.get_loc("colA")] = 45

        parq.custom_publish(bucket=bucket, key=key,
                     dataframe=dataframe, partitions=partitions, redshift_params=redshift_params,
                     custom_redshift_columns=custom_redshift_columns)

        mock_create_custom_table.assert_called_once_with(
            redshift_params['table_name'], redshift_params['schema_name'], partitions, parq.s3_url(bucket, key), custom_redshift_columns, msh)
