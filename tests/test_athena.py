"""Tests for AthenaClient"""

import pytest
import boto3
import polars as pl
from unittest.mock import Mock, patch, MagicMock
from botocore.config import Config
from botocore.exceptions import ClientError

from owlbear import AthenaClient


@pytest.fixture
def mock_session():
    """Mock boto3 session"""
    session = Mock(spec=boto3.Session)
    client = Mock()
    session.client.return_value = client
    return session, client


@pytest.fixture
def athena_client():
    """Create AthenaClient with mocked AWS client"""
    with patch('boto3.client') as mock_client:
        client = AthenaClient(
            database="test_db",
            output_location="s3://test-bucket/results/",
            region="us-east-1"
        )
        return client, mock_client.return_value


class TestAthenaClientInit:
    """Test AthenaClient initialization"""

    def test_init_default(self):
        """Test default initialization"""
        with patch('boto3.client') as mock_client:
            client = AthenaClient(
                database="test_db",
                output_location="s3://test-bucket/results/"
            )
            assert client.database == "test_db"
            assert client.output_location == "s3://test-bucket/results/"
            mock_client.assert_called_once()

    def test_init_with_session(self, mock_session):
        """Test initialization with provided session"""
        session, mock_client = mock_session

        client = AthenaClient(
            database="test_db",
            output_location="s3://test-bucket/results/",
            session=session
        )

        assert client.database == "test_db"
        assert client.output_location == "s3://test-bucket/results/"
        session.client.assert_called_once_with('athena', config=None)

    def test_init_with_config(self):
        """Test initialization with custom config"""
        config = Config(region_name='us-west-2')

        with patch('boto3.client') as mock_client:
            client = AthenaClient(
                database="test_db",
                output_location="s3://test-bucket/results/",
                config=config
            )

            mock_client.assert_called_once()
            args, kwargs = mock_client.call_args
            assert kwargs['config'] == config

    def test_from_session(self, mock_session):
        """Test from_session class method"""
        session, mock_client = mock_session

        client = AthenaClient.from_session(
            session=session,
            database="test_db",
            output_location="s3://test-bucket/results/"
        )

        assert client.database == "test_db"
        session.client.assert_called_once()


class TestExecuteQuery:
    """Test query execution"""

    def test_query_basic(self, athena_client):
        """Test basic query execution"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.return_value = {'QueryExecutionId': 'test-id'}
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}
        }

        result = client.query("SELECT * FROM test_table")

        assert result == "test-id"
        mock_athena.start_query_execution.assert_called_once()
        call_args = mock_athena.start_query_execution.call_args[1]
        assert call_args['QueryString'] == "SELECT * FROM test_table"
        assert call_args['QueryExecutionContext']['Database'] == "test_db"
        assert call_args['ResultConfiguration']['OutputLocation'] == "s3://test-bucket/results/"

    def test_query_no_wait(self, athena_client):
        """Test query execution without waiting"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.return_value = {'QueryExecutionId': 'test-id'}

        result = client.query("SELECT * FROM test_table", wait_for_completion=False)

        assert result == "test-id"
        mock_athena.get_query_execution.assert_not_called()

    def test_query_with_workgroup(self, athena_client):
        """Test query execution with work group"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.return_value = {'QueryExecutionId': 'test-id'}
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}
        }

        client.query("SELECT * FROM test_table", work_group="test-workgroup")

        call_args = mock_athena.start_query_execution.call_args[1]
        assert call_args['WorkGroup'] == "test-workgroup"

    def test_query_failure(self, athena_client):
        """Test query execution failure"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to execute query: AWS Error"):
            client.query("SELECT * FROM test_table")


class TestWaitForCompletion:
    """Test _wait_for_completion method"""

    def test_wait_success(self, athena_client):
        """Test successful wait"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {'Status': {'State': 'SUCCEEDED'}}
        }

        result = client._wait_for_completion('test-id')
        assert result == 'SUCCEEDED'

    def test_wait_failed_query(self, athena_client):
        """Test wait for failed query"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {
                'Status': {
                    'State': 'FAILED',
                    'StateChangeReason': 'Syntax error',
                    'AthenaError': {'ErrorMessage': 'Invalid SQL'}
                }
            }
        }

        with pytest.raises(Exception, match="Query failed: Syntax error. Error: Invalid SQL"):
            client._wait_for_completion('test-id')

    def test_wait_cancelled_query(self, athena_client):
        """Test wait for cancelled query"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {'Status': {'State': 'CANCELLED'}}
        }

        with pytest.raises(Exception, match="Query was cancelled"):
            client._wait_for_completion('test-id')

    @patch('time.sleep')
    def test_wait_timeout(self, mock_sleep, athena_client):
        """Test wait timeout"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.return_value = {
            'QueryExecution': {'Status': {'State': 'RUNNING'}}
        }

        with pytest.raises(TimeoutError, match="Query did not complete within 5 seconds"):
            client._wait_for_completion('test-id', max_wait_time=5)


class TestGetResultsPolars:
    """Test results method"""

    def test_get_results_basic(self, athena_client):
        """Test basic results retrieval with PyArrow type handling"""
        client, mock_athena = athena_client

        # Mock response with typed data
        mock_athena.get_query_results.return_value = {
            'ResultSet': {
                'ResultSetMetadata': {
                    'ColumnInfo': [
                        {'Name': 'id', 'Type': 'integer'},
                        {'Name': 'name', 'Type': 'varchar'},
                        {'Name': 'age', 'Type': 'integer'}
                    ]
                },
                'Rows': [
                    # Header row (skipped)
                    {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'name'}, {'VarCharValue': 'age'}]},
                    # Data rows
                    {'Data': [{'VarCharValue': '1'}, {'VarCharValue': 'Alice'}, {'VarCharValue': '25'}]},
                    {'Data': [{'VarCharValue': '2'}, {'VarCharValue': 'Bob'}, {'VarCharValue': '30'}]}
                ]
            }
        }

        df = client.results('test-id')

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ['id', 'name', 'age']
        assert len(df) == 2

        # Check that types are properly inferred
        assert df.dtypes[0] == pl.Int32  # id should be integer
        assert df.dtypes[1] == pl.Utf8   # name should be string
        assert df.dtypes[2] == pl.Int32  # age should be integer

        # Check actual values
        assert df.item(0, 'id') == 1
        assert df.item(0, 'name') == 'Alice'
        assert df.item(0, 'age') == 25

    def test_get_results_empty(self, athena_client):
        """Test empty results"""
        client, mock_athena = athena_client

        mock_athena.get_query_results.return_value = {
            'ResultSet': {
                'ResultSetMetadata': {
                    'ColumnInfo': [
                        {'Name': 'id', 'Type': 'integer'},
                        {'Name': 'name', 'Type': 'varchar'}
                    ]
                },
                'Rows': [
                    # Only header row
                    {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'name'}]}
                ]
            }
        }

        df = client.results('test-id')

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ['id', 'name']
        assert len(df) == 0

    def test_get_results_with_nulls(self, athena_client):
        """Test results with null values"""
        client, mock_athena = athena_client

        mock_athena.get_query_results.return_value = {
            'ResultSet': {
                'ResultSetMetadata': {
                    'ColumnInfo': [
                        {'Name': 'id', 'Type': 'integer'},
                        {'Name': 'value', 'Type': 'varchar'}
                    ]
                },
                'Rows': [
                    {'Data': [{'VarCharValue': 'id'}, {'VarCharValue': 'value'}]},
                    {'Data': [{'VarCharValue': '1'}, {'NullValue': True}]},
                    {'Data': [{'VarCharValue': '2'}, {'VarCharValue': 'test'}]}
                ]
            }
        }

        df = client.results('test-id')

        assert len(df) == 2
        assert df.item(0, 'id') == 1  # Should be converted to integer
        assert df.item(0, 'value') is None  # Should handle null
        assert df.item(1, 'value') == 'test'

    def test_get_results_pagination(self, athena_client):
        """Test results with pagination"""
        client, mock_athena = athena_client

        # First page
        first_response = {
            'ResultSet': {
                'ResultSetMetadata': {
                    'ColumnInfo': [{'Name': 'id', 'Type': 'integer'}]
                },
                'Rows': [
                    {'Data': [{'VarCharValue': 'id'}]},
                    {'Data': [{'VarCharValue': '1'}]}
                ]
            },
            'NextToken': 'token123'
        }

        # Second page
        second_response = {
            'ResultSet': {
                'Rows': [
                    {'Data': [{'VarCharValue': '2'}]}
                ]
            }
        }

        mock_athena.get_query_results.side_effect = [first_response, second_response]

        df = client.results('test-id')

        assert len(df) == 2
        assert df.item(0, 'id') == 1  # Should be converted to integer
        assert df.item(1, 'id') == 2
        assert mock_athena.get_query_results.call_count == 2

    def test_get_results_error(self, athena_client):
        """Test results retrieval error"""
        client, mock_athena = athena_client
        mock_athena.get_query_results.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to retrieve query results: AWS Error"):
            client.results('test-id')


class TestQueryInfo:
    """Test get_query_info method"""

    def test_get_query_info(self, athena_client):
        """Test query info retrieval"""
        client, mock_athena = athena_client

        expected_info = {
            'QueryExecutionId': 'test-id',
            'Query': 'SELECT * FROM test',
            'Status': {'State': 'SUCCEEDED'}
        }
        mock_athena.get_query_execution.return_value = {'QueryExecution': expected_info}

        info = client.get_query_info('test-id')
        assert info == expected_info

    def test_get_query_info_error(self, athena_client):
        """Test query info error"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to get query info: AWS Error"):
            client.get_query_info('test-id')


class TestCancelQuery:
    """Test cancel_query method"""

    def test_cancel_query(self, athena_client):
        """Test query cancellation"""
        client, mock_athena = athena_client

        result = client.cancel_query('test-id')
        assert result is True
        mock_athena.stop_query_execution.assert_called_once_with(QueryExecutionId='test-id')

    def test_cancel_query_error(self, athena_client):
        """Test query cancellation error"""
        client, mock_athena = athena_client
        mock_athena.stop_query_execution.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to cancel query: AWS Error"):
            client.cancel_query('test-id')


class TestListWorkGroups:
    """Test list_work_groups method"""

    def test_list_work_groups(self, athena_client):
        """Test work groups listing"""
        client, mock_athena = athena_client

        mock_athena.list_work_groups.return_value = {
            'WorkGroups': [
                {'Name': 'primary'},
                {'Name': 'analytics'},
                {'Name': 'test'}
            ]
        }

        groups = client.list_work_groups()
        assert groups == ['primary', 'analytics', 'test']

    def test_list_work_groups_error(self, athena_client):
        """Test work groups listing error"""
        client, mock_athena = athena_client
        mock_athena.list_work_groups.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to list work groups: AWS Error"):
            client.list_work_groups()
