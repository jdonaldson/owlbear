"""Tests for AthenaClient"""

import io

import pytest
import boto3
import polars as pl
import pyarrow.parquet as pq
from unittest.mock import Mock, patch
from botocore.config import Config

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
    with patch("boto3.client") as mock_client:
        client = AthenaClient(
            database="test_db",
            output_location="s3://test-bucket/results/",
            region="us-east-1",
        )
        return client, mock_client.return_value


class TestAthenaClientInit:
    """Test AthenaClient initialization"""

    def test_init_default(self):
        """Test default initialization"""
        with patch("boto3.client") as mock_client:
            client = AthenaClient(
                database="test_db", output_location="s3://test-bucket/results/"
            )
            assert client.database == "test_db"
            assert client.output_location == "s3://test-bucket/results/"
            mock_client.assert_called_once()

    def test_init_stores_params_for_s3(self):
        """Verify __init__ stores session/region/config/kwargs for lazy S3 client"""
        config = Config(region_name="us-west-2")
        with patch("boto3.client"):
            client = AthenaClient(
                database="test_db",
                output_location="s3://test-bucket/results/",
                region="eu-west-1",
                config=config,
                endpoint_url="http://localhost:4566",
            )
            assert client._session is None
            assert client._region == "eu-west-1"
            assert client._config is config
            assert client._client_kwargs == {"endpoint_url": "http://localhost:4566"}
            assert client._s3_client is None
            assert client._last_query_execution is None

    def test_init_with_session(self, mock_session):
        """Test initialization with provided session"""
        session, mock_client = mock_session

        client = AthenaClient(
            database="test_db",
            output_location="s3://test-bucket/results/",
            session=session,
        )

        assert client.database == "test_db"
        assert client.output_location == "s3://test-bucket/results/"
        session.client.assert_called_once_with("athena", config=None)

    def test_init_with_config(self):
        """Test initialization with custom config"""
        config = Config(region_name="us-west-2")

        with patch("boto3.client") as mock_client:
            AthenaClient(
                database="test_db",
                output_location="s3://test-bucket/results/",
                config=config,
            )

            mock_client.assert_called_once()
            _, kwargs = mock_client.call_args
            assert kwargs["config"] == config

    def test_from_session(self, mock_session):
        """Test from_session class method"""
        session, mock_client = mock_session

        client = AthenaClient.from_session(
            session=session,
            database="test_db",
            output_location="s3://test-bucket/results/",
        )

        assert client.database == "test_db"
        session.client.assert_called_once()


class TestExecuteQuery:
    """Test query execution"""

    def test_query_basic(self, athena_client):
        """Test basic query execution"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "test-id"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        result = client.query("SELECT * FROM test_table")

        assert result == "test-id"
        mock_athena.start_query_execution.assert_called_once()
        call_args = mock_athena.start_query_execution.call_args[1]
        assert call_args["QueryString"] == "SELECT * FROM test_table"
        assert call_args["QueryExecutionContext"]["Database"] == "test_db"
        assert (
            call_args["ResultConfiguration"]["OutputLocation"]
            == "s3://test-bucket/results/"
        )

    def test_query_no_wait(self, athena_client):
        """Test query execution without waiting"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "test-id"}

        result = client.query("SELECT * FROM test_table", wait_for_completion=False)

        assert result == "test-id"
        mock_athena.get_query_execution.assert_not_called()

    def test_query_with_workgroup(self, athena_client):
        """Test query execution with work group"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "test-id"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        client.query("SELECT * FROM test_table", work_group="test-workgroup")

        call_args = mock_athena.start_query_execution.call_args[1]
        assert call_args["WorkGroup"] == "test-workgroup"

    def test_query_failure(self, athena_client):
        """Test query execution failure"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to execute query"):
            client.query("SELECT * FROM test_table")

    def test_query_with_parameters(self, athena_client):
        """Test parameterized query execution"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "test-id"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        client.query(
            "SELECT * FROM t WHERE id = ?",
            parameters=["42"],
        )

        call_args = mock_athena.start_query_execution.call_args[1]
        assert call_args["ExecutionParameters"] == ["42"]

    def test_query_with_result_reuse(self, athena_client):
        """Test query with result reuse configuration"""
        client, mock_athena = athena_client
        mock_athena.start_query_execution.return_value = {"QueryExecutionId": "test-id"}
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        client.query("SELECT * FROM t", result_reuse_max_age=60)

        call_args = mock_athena.start_query_execution.call_args[1]
        reuse_config = call_args["ResultReuseConfiguration"]
        assert reuse_config == {
            "ResultReuseByAgeConfiguration": {
                "Enabled": True,
                "MaxAgeInMinutes": 60,
            }
        }

    def test_query_exception_chaining(self, athena_client):
        """Test that exceptions are chained with 'from e'"""
        client, mock_athena = athena_client
        original = ValueError("original error")
        mock_athena.start_query_execution.side_effect = original

        with pytest.raises(Exception) as exc_info:
            client.query("SELECT 1")

        assert exc_info.value.__cause__ is original


class TestWaitForCompletion:
    """Test _wait_for_completion method"""

    def test_wait_success(self, athena_client):
        """Test successful wait"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
        }

        result = client._wait_for_completion("test-id")
        assert result == "SUCCEEDED"

    def test_wait_failed_query(self, athena_client):
        """Test wait for failed query"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {
                "Status": {
                    "State": "FAILED",
                    "StateChangeReason": "Syntax error",
                    "AthenaError": {"ErrorMessage": "Invalid SQL"},
                }
            }
        }

        with pytest.raises(
            Exception, match="Query failed: Syntax error. Error: Invalid SQL"
        ):
            client._wait_for_completion("test-id")

    def test_wait_cancelled_query(self, athena_client):
        """Test wait for cancelled query"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "CANCELLED"}}
        }

        with pytest.raises(Exception, match="Query was cancelled"):
            client._wait_for_completion("test-id")

    @patch("time.sleep")
    def test_wait_timeout(self, mock_sleep, athena_client):
        """Test wait timeout"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.return_value = {
            "QueryExecution": {"Status": {"State": "RUNNING"}}
        }

        with pytest.raises(
            TimeoutError, match="Query did not complete within 5 seconds"
        ):
            client._wait_for_completion("test-id", max_wait_time=5)


class TestGetResultsPolars:
    """Test results method"""

    def test_get_results_basic(self, athena_client):
        """Test basic results retrieval with PyArrow type handling"""
        client, mock_athena = athena_client

        # Mock response with typed data
        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "id", "Type": "integer"},
                        {"Name": "name", "Type": "varchar"},
                        {"Name": "age", "Type": "integer"},
                    ]
                },
                "Rows": [
                    # Header row (skipped)
                    {
                        "Data": [
                            {"VarCharValue": "id"},
                            {"VarCharValue": "name"},
                            {"VarCharValue": "age"},
                        ]
                    },
                    # Data rows
                    {
                        "Data": [
                            {"VarCharValue": "1"},
                            {"VarCharValue": "Alice"},
                            {"VarCharValue": "25"},
                        ]
                    },
                    {
                        "Data": [
                            {"VarCharValue": "2"},
                            {"VarCharValue": "Bob"},
                            {"VarCharValue": "30"},
                        ]
                    },
                ],
            }
        }

        df = client.results("test-id")

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["id", "name", "age"]
        assert len(df) == 2

        # Check that types are properly inferred
        assert df.dtypes[0] == pl.Int32  # id should be integer
        assert df.dtypes[1] == pl.Utf8  # name should be string
        assert df.dtypes[2] == pl.Int32  # age should be integer

        # Check actual values
        assert df.item(0, "id") == 1
        assert df.item(0, "name") == "Alice"
        assert df.item(0, "age") == 25

    def test_get_results_empty(self, athena_client):
        """Test empty results"""
        client, mock_athena = athena_client

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "id", "Type": "integer"},
                        {"Name": "name", "Type": "varchar"},
                    ]
                },
                "Rows": [
                    # Only header row
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]}
                ],
            }
        }

        df = client.results("test-id")

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["id", "name"]
        assert len(df) == 0

    def test_get_results_with_nulls(self, athena_client):
        """Test results with null values"""
        client, mock_athena = athena_client

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "id", "Type": "integer"},
                        {"Name": "value", "Type": "varchar"},
                    ]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "value"}]},
                    {"Data": [{"VarCharValue": "1"}, {"NullValue": True}]},
                    {"Data": [{"VarCharValue": "2"}, {"VarCharValue": "test"}]},
                ],
            }
        }

        df = client.results("test-id")

        assert len(df) == 2
        assert df.item(0, "id") == 1  # Should be converted to integer
        assert df.item(0, "value") is None  # Should handle null
        assert df.item(1, "value") == "test"

    def test_get_results_pagination(self, athena_client):
        """Test results with pagination"""
        client, mock_athena = athena_client

        # First page
        first_response = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "id", "Type": "integer"}]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "1"}]},
                ],
            },
            "NextToken": "token123",
        }

        # Second page
        second_response = {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "2"}]}]}}

        mock_athena.get_query_results.side_effect = [first_response, second_response]

        df = client.results("test-id")

        assert len(df) == 2
        assert df.item(0, "id") == 1  # Should be converted to integer
        assert df.item(1, "id") == 2
        assert mock_athena.get_query_results.call_count == 2

    def test_get_results_error(self, athena_client):
        """Test results retrieval error"""
        client, mock_athena = athena_client
        mock_athena.get_query_results.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to retrieve query results"):
            client.results("test-id")

    def test_results_exception_chaining(self, athena_client):
        """Test that results exceptions are chained"""
        client, mock_athena = athena_client
        original = RuntimeError("network error")
        mock_athena.get_query_results.side_effect = original

        with pytest.raises(Exception) as exc_info:
            client.results("test-id")

        assert exc_info.value.__cause__ is original


class TestExtractTypedValue:
    """Test _extract_typed_value preserves empty strings for string types"""

    def test_empty_string_preserved_for_varchar(self, athena_client):
        client, _ = athena_client
        result = client._extract_typed_value({"VarCharValue": ""}, "varchar")
        assert result == ""

    def test_empty_string_preserved_for_string(self, athena_client):
        client, _ = athena_client
        result = client._extract_typed_value({"VarCharValue": ""}, "string")
        assert result == ""

    def test_empty_string_preserved_for_char(self, athena_client):
        client, _ = athena_client
        result = client._extract_typed_value({"VarCharValue": ""}, "char(10)")
        assert result == ""

    def test_empty_string_null_for_integer(self, athena_client):
        client, _ = athena_client
        result = client._extract_typed_value({"VarCharValue": ""}, "integer")
        assert result is None

    def test_empty_string_null_for_double(self, athena_client):
        client, _ = athena_client
        result = client._extract_typed_value({"VarCharValue": ""}, "double")
        assert result is None


class TestResultsIter:
    """Test results_iter method"""

    def test_single_page(self, athena_client):
        """Single page, no NextToken — yields one DataFrame"""
        client, mock_athena = athena_client

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [
                        {"Name": "id", "Type": "integer"},
                        {"Name": "name", "Type": "varchar"},
                    ]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]},
                    {"Data": [{"VarCharValue": "1"}, {"VarCharValue": "Alice"}]},
                    {"Data": [{"VarCharValue": "2"}, {"VarCharValue": "Bob"}]},
                ],
            }
        }

        pages = list(client.results_iter("test-id"))

        assert len(pages) == 1
        df = pages[0]
        assert df.columns == ["id", "name"]
        assert len(df) == 2
        assert df.item(0, "id") == 1
        assert df.item(1, "name") == "Bob"

    def test_multi_page(self, athena_client):
        """Multiple pages via NextToken"""
        client, mock_athena = athena_client

        first_response = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "id", "Type": "integer"}]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "1"}]},
                ],
            },
            "NextToken": "token123",
        }
        second_response = {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "2"}]},
                ]
            },
        }

        mock_athena.get_query_results.side_effect = [first_response, second_response]

        pages = list(client.results_iter("test-id"))

        assert len(pages) == 2
        assert pages[0].item(0, "id") == 1
        assert pages[1].item(0, "id") == 2
        assert mock_athena.get_query_results.call_count == 2

    def test_page_size_capped_at_1000(self, athena_client):
        """page_size > 1000 is capped to 1000"""
        client, mock_athena = athena_client

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "id", "Type": "integer"}]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "1"}]},
                ],
            }
        }

        list(client.results_iter("test-id", page_size=5000))

        call_args = mock_athena.get_query_results.call_args[1]
        assert call_args["MaxResults"] == 1000

    def test_empty_results(self, athena_client):
        """Only header row — yields nothing"""
        client, mock_athena = athena_client

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "id", "Type": "integer"}]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                ],
            }
        }

        pages = list(client.results_iter("test-id"))
        assert len(pages) == 0

    def test_error_handling(self, athena_client):
        """Errors are wrapped consistently"""
        client, mock_athena = athena_client
        mock_athena.get_query_results.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to retrieve query results"):
            list(client.results_iter("test-id"))


class TestQueryInfo:
    """Test get_query_info method"""

    def test_get_query_info(self, athena_client):
        """Test query info retrieval"""
        client, mock_athena = athena_client

        expected_info = {
            "QueryExecutionId": "test-id",
            "Query": "SELECT * FROM test",
            "Status": {"State": "SUCCEEDED"},
        }
        mock_athena.get_query_execution.return_value = {"QueryExecution": expected_info}

        info = client.get_query_info("test-id")
        assert info == expected_info

    def test_get_query_info_error(self, athena_client):
        """Test query info error"""
        client, mock_athena = athena_client
        mock_athena.get_query_execution.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to get query info"):
            client.get_query_info("test-id")


class TestCancelQuery:
    """Test cancel_query method"""

    def test_cancel_query(self, athena_client):
        """Test query cancellation"""
        client, mock_athena = athena_client

        result = client.cancel_query("test-id")
        assert result is True
        mock_athena.stop_query_execution.assert_called_once_with(
            QueryExecutionId="test-id"
        )

    def test_cancel_query_error(self, athena_client):
        """Test query cancellation error"""
        client, mock_athena = athena_client
        mock_athena.stop_query_execution.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to cancel query"):
            client.cancel_query("test-id")


class TestListWorkGroups:
    """Test list_work_groups method"""

    def test_list_work_groups(self, athena_client):
        """Test work groups listing"""
        client, mock_athena = athena_client

        mock_athena.list_work_groups.return_value = {
            "WorkGroups": [{"Name": "primary"}, {"Name": "analytics"}, {"Name": "test"}]
        }

        groups = client.list_work_groups()
        assert groups == ["primary", "analytics", "test"]

    def test_list_work_groups_error(self, athena_client):
        """Test work groups listing error"""
        client, mock_athena = athena_client
        mock_athena.list_work_groups.side_effect = Exception("AWS Error")

        with pytest.raises(Exception, match="Failed to list work groups"):
            client.list_work_groups()


# ---------------------------------------------------------------------------
# Helpers for S3 direct-read tests
# ---------------------------------------------------------------------------


def _make_parquet_bytes(df: pl.DataFrame) -> bytes:
    """Serialize a Polars DataFrame to Parquet bytes via PyArrow."""
    table = df.to_arrow()
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def _make_csv_bytes(df: pl.DataFrame) -> bytes:
    """Serialize a Polars DataFrame to CSV bytes."""
    return df.write_csv().encode("utf-8")


def _sample_df() -> pl.DataFrame:
    """Return a small DataFrame for testing."""
    return pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})


# Column metadata response for _get_column_schema (id: integer, name: varchar)
SAMPLE_SCHEMA_RESPONSE = {
    "ResultSet": {
        "ResultSetMetadata": {
            "ColumnInfo": [
                {"Name": "id", "Type": "integer"},
                {"Name": "name", "Type": "varchar"},
            ]
        },
        "Rows": [{"Data": [{"VarCharValue": "id"}, {"VarCharValue": "name"}]}],
    }
}


def _dml_query_execution(execution_id: str, output_loc: str) -> dict:
    """Build a QueryExecution dict for a DML statement."""
    return {
        "QueryExecutionId": execution_id,
        "StatementType": "DML",
        "ResultConfiguration": {"OutputLocation": output_loc},
        "Status": {"State": "SUCCEEDED"},
    }


def _ddl_query_execution(execution_id: str) -> dict:
    """Build a QueryExecution dict for a DDL statement (skips S3 path)."""
    return {
        "QueryExecutionId": execution_id,
        "StatementType": "DDL",
        "ResultConfiguration": {"OutputLocation": "s3://bucket/results/abc.txt"},
        "Status": {"State": "SUCCEEDED"},
    }


# ---------------------------------------------------------------------------
# TestParseS3Uri
# ---------------------------------------------------------------------------


class TestParseS3Uri:
    """Test _parse_s3_uri static method"""

    def test_basic(self):
        bucket, key = AthenaClient._parse_s3_uri("s3://my-bucket/path/to/file.csv")
        assert bucket == "my-bucket"
        assert key == "path/to/file.csv"

    def test_no_key(self):
        bucket, key = AthenaClient._parse_s3_uri("s3://my-bucket/")
        assert bucket == "my-bucket"
        assert key == ""

    def test_bare_bucket(self):
        bucket, key = AthenaClient._parse_s3_uri("s3://my-bucket")
        assert bucket == "my-bucket"
        assert key == ""

    def test_not_s3_raises(self):
        with pytest.raises(ValueError, match="Not an S3 URI"):
            AthenaClient._parse_s3_uri("https://example.com/file")

    def test_empty_bucket_raises(self):
        with pytest.raises(ValueError, match="no bucket"):
            AthenaClient._parse_s3_uri("s3:///key")


# ---------------------------------------------------------------------------
# TestLazyS3
# ---------------------------------------------------------------------------


class TestLazyS3:
    """Test lazy _s3 property"""

    def test_creates_s3_client_without_session(self):
        with patch("boto3.client") as mock_boto:
            client = AthenaClient(
                database="db",
                output_location="s3://bucket/out/",
                region="us-west-2",
            )
            mock_boto.reset_mock()

            _ = client._s3

            mock_boto.assert_called_once()
            args, kwargs = mock_boto.call_args
            assert args[0] == "s3"

    def test_creates_s3_client_with_session(self):
        session = Mock(spec=boto3.Session)
        s3_mock = Mock()
        session.client.return_value = s3_mock

        client = AthenaClient(
            database="db",
            output_location="s3://bucket/out/",
            session=session,
        )
        session.reset_mock()

        result = client._s3
        session.client.assert_called_once_with("s3", config=None)
        assert result is s3_mock

    def test_caches_s3_client(self):
        with patch("boto3.client") as mock_boto:
            client = AthenaClient(
                database="db",
                output_location="s3://bucket/out/",
            )
            mock_boto.reset_mock()

            first = client._s3
            second = client._s3
            assert first is second
            assert mock_boto.call_count == 1


# ---------------------------------------------------------------------------
# TestWaitForCompletionCache
# ---------------------------------------------------------------------------


class TestWaitForCompletionCache:
    """Test that _wait_for_completion caches the QueryExecution."""

    def test_caches_on_success(self, athena_client):
        client, mock_athena = athena_client
        qe = {
            "QueryExecutionId": "exec-1",
            "Status": {"State": "SUCCEEDED"},
            "StatementType": "DML",
            "ResultConfiguration": {"OutputLocation": "s3://b/k.csv"},
        }
        mock_athena.get_query_execution.return_value = {"QueryExecution": qe}

        client._wait_for_completion("exec-1")
        assert client._last_query_execution is qe


# ---------------------------------------------------------------------------
# TestResultsFromS3
# ---------------------------------------------------------------------------


class TestResultsFromParquet:
    """Test _results_from_parquet private method"""

    def test_reads_parquet(self, athena_client):
        client, _ = athena_client
        parquet_bytes = _make_parquet_bytes(_sample_df())

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=parquet_bytes))
        }
        client._s3_client = mock_s3

        df = client._results_from_parquet("s3://bucket/results/output.parquet")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3
        assert df.columns == ["id", "name"]
        assert df.item(0, "id") == 1
        mock_s3.get_object.assert_called_once_with(
            Bucket="bucket", Key="results/output.parquet"
        )

    def test_max_rows_slices(self, athena_client):
        client, _ = athena_client
        parquet_bytes = _make_parquet_bytes(_sample_df())

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=parquet_bytes))
        }
        client._s3_client = mock_s3

        df = client._results_from_parquet(
            "s3://bucket/results/output.parquet", max_rows=2
        )
        assert len(df) == 2

    def test_empty_parquet(self, athena_client):
        client, _ = athena_client
        empty_df = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})
        parquet_bytes = _make_parquet_bytes(empty_df)

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=parquet_bytes))
        }
        client._s3_client = mock_s3

        df = client._results_from_parquet("s3://bucket/results/empty.parquet")
        assert len(df) == 0
        assert df.columns == ["id"]


# ---------------------------------------------------------------------------
# TestResultsIterFromParquet
# ---------------------------------------------------------------------------


class TestResultsIterFromParquet:
    """Test _results_iter_from_parquet private method"""

    def test_batched_reads(self, athena_client):
        client, _ = athena_client
        parquet_bytes = _make_parquet_bytes(_sample_df())  # 3 rows

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=parquet_bytes))
        }
        client._s3_client = mock_s3

        pages = list(
            client._results_iter_from_parquet("s3://bucket/out.parquet", page_size=2)
        )

        total_rows = sum(len(p) for p in pages)
        assert total_rows == 3
        assert len(pages) == 2  # 2 + 1
        assert all(isinstance(p, pl.DataFrame) for p in pages)

    def test_empty_parquet_yields_nothing(self, athena_client):
        client, _ = athena_client
        empty_df = pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)})
        parquet_bytes = _make_parquet_bytes(empty_df)

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=parquet_bytes))
        }
        client._s3_client = mock_s3

        pages = list(client._results_iter_from_parquet("s3://bucket/out.parquet"))
        assert len(pages) == 0


# ---------------------------------------------------------------------------
# TestResultsFromCsv / TestResultsIterFromCsv
# ---------------------------------------------------------------------------


class TestResultsFromCsv:
    """Test _results_from_csv private method"""

    def test_reads_csv_with_types(self, athena_client):
        client, mock_athena = athena_client
        csv_bytes = _make_csv_bytes(_sample_df())

        mock_athena.get_query_results.return_value = SAMPLE_SCHEMA_RESPONSE
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=csv_bytes))
        }
        client._s3_client = mock_s3

        df = client._results_from_csv("exec-1", "s3://bucket/results/output.csv")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 3
        assert df.columns == ["id", "name"]
        assert df.item(0, "id") == 1
        assert df.dtypes[0] == pl.Int32  # typed via schema

    def test_max_rows_slices(self, athena_client):
        client, mock_athena = athena_client
        csv_bytes = _make_csv_bytes(_sample_df())

        mock_athena.get_query_results.return_value = SAMPLE_SCHEMA_RESPONSE
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=csv_bytes))
        }
        client._s3_client = mock_s3

        df = client._results_from_csv(
            "exec-1", "s3://bucket/results/output.csv", max_rows=2
        )
        assert len(df) == 2

    def test_empty_csv(self, athena_client):
        client, mock_athena = athena_client
        empty_df = pl.DataFrame({"id": pl.Series([], dtype=pl.Int64)})
        csv_bytes = _make_csv_bytes(empty_df)

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "id", "Type": "integer"}]
                },
                "Rows": [{"Data": [{"VarCharValue": "id"}]}],
            }
        }
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=csv_bytes))
        }
        client._s3_client = mock_s3

        df = client._results_from_csv("exec-1", "s3://bucket/results/empty.csv")
        assert len(df) == 0
        assert df.columns == ["id"]


class TestResultsIterFromCsv:
    """Test _results_iter_from_csv private method"""

    def test_batched_reads(self, athena_client):
        client, mock_athena = athena_client
        csv_bytes = _make_csv_bytes(_sample_df())  # 3 rows

        mock_athena.get_query_results.return_value = SAMPLE_SCHEMA_RESPONSE
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=csv_bytes))
        }
        client._s3_client = mock_s3

        pages = list(
            client._results_iter_from_csv("exec-1", "s3://bucket/out.csv", page_size=2)
        )

        total_rows = sum(len(p) for p in pages)
        assert total_rows == 3
        assert len(pages) == 2  # 2 + 1

    def test_empty_csv_yields_nothing(self, athena_client):
        client, mock_athena = athena_client
        empty_df = pl.DataFrame({"x": pl.Series([], dtype=pl.Int64)})
        csv_bytes = _make_csv_bytes(empty_df)

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {"ColumnInfo": [{"Name": "x", "Type": "integer"}]},
                "Rows": [{"Data": [{"VarCharValue": "x"}]}],
            }
        }
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=csv_bytes))
        }
        client._s3_client = mock_s3

        pages = list(client._results_iter_from_csv("exec-1", "s3://bucket/out.csv"))
        assert len(pages) == 0


# ---------------------------------------------------------------------------
# TestResultsS3Path
# ---------------------------------------------------------------------------


class TestResultsS3Path:
    """Integration tests for S3 direct-read waterfall in results()"""

    def test_parquet_read_for_dml(self, athena_client):
        """DML queries with .parquet output read from S3 (no schema lookup)."""
        client, mock_athena = athena_client
        parquet_bytes = _make_parquet_bytes(_sample_df())

        qe = _dml_query_execution("exec-1", "s3://bucket/results/abc.parquet")
        client._last_query_execution = qe

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=parquet_bytes))
        }
        client._s3_client = mock_s3

        df = client.results("exec-1")

        assert len(df) == 3
        assert df.columns == ["id", "name"]
        mock_athena.get_query_results.assert_not_called()

    def test_csv_read_for_dml(self, athena_client):
        """DML queries with CSV output read from S3 (with schema lookup)."""
        client, mock_athena = athena_client
        csv_bytes = _make_csv_bytes(_sample_df())

        qe = _dml_query_execution("exec-csv", "s3://bucket/results/abc.csv")
        client._last_query_execution = qe

        mock_athena.get_query_results.return_value = SAMPLE_SCHEMA_RESPONSE
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=csv_bytes))
        }
        client._s3_client = mock_s3

        df = client.results("exec-csv")

        assert len(df) == 3
        assert df.columns == ["id", "name"]
        # Only the schema lookup call — no paginated data fetch
        mock_athena.get_query_results.assert_called_once_with(
            QueryExecutionId="exec-csv", MaxResults=1
        )

    def test_s3_error_falls_back(self, athena_client):
        """If S3 read fails, fall back to JSON API transparently."""
        client, mock_athena = athena_client

        qe = _dml_query_execution("exec-3", "s3://bucket/results/abc.parquet")
        client._last_query_execution = qe

        mock_s3 = Mock()
        mock_s3.get_object.side_effect = Exception("S3 access denied")
        client._s3_client = mock_s3

        json_response = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "id", "Type": "integer"}]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "99"}]},
                ],
            }
        }
        mock_athena.get_query_results.return_value = json_response

        df = client.results("exec-3")

        assert df.item(0, "id") == 99
        mock_athena.get_query_results.assert_called_once()

    def test_ddl_skips_s3(self, athena_client):
        """DDL statements always use JSON API."""
        client, mock_athena = athena_client

        qe = _ddl_query_execution("exec-4")
        client._last_query_execution = qe

        mock_athena.get_query_results.return_value = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "col", "Type": "varchar"}]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "col"}]},
                    {"Data": [{"VarCharValue": "hello"}]},
                ],
            }
        }

        df = client.results("exec-4")

        assert df.item(0, "col") == "hello"
        mock_athena.get_query_results.assert_called_once()

    def test_fetches_query_info_when_not_cached(self, athena_client):
        """When _last_query_execution is None, fetches via get_query_info."""
        client, mock_athena = athena_client
        parquet_bytes = _make_parquet_bytes(_sample_df())

        qe = _dml_query_execution("exec-5", "s3://bucket/results/abc.parquet")
        mock_athena.get_query_execution.return_value = {"QueryExecution": qe}

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=parquet_bytes))
        }
        client._s3_client = mock_s3

        df = client.results("exec-5")

        assert len(df) == 3
        mock_athena.get_query_execution.assert_called_once()
        mock_athena.get_query_results.assert_not_called()


# ---------------------------------------------------------------------------
# TestResultsIterS3Path
# ---------------------------------------------------------------------------


class TestResultsIterS3Path:
    """Integration tests for S3 direct-read waterfall in results_iter()"""

    def test_parquet_read_for_dml(self, athena_client):
        """Reads Parquet from S3 and yields batches."""
        client, mock_athena = athena_client
        parquet_bytes = _make_parquet_bytes(_sample_df())

        qe = _dml_query_execution("exec-1", "s3://bucket/results/abc.parquet")
        client._last_query_execution = qe

        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=parquet_bytes))
        }
        client._s3_client = mock_s3

        pages = list(client.results_iter("exec-1", page_size=2))

        total = sum(len(p) for p in pages)
        assert total == 3
        mock_athena.get_query_results.assert_not_called()

    def test_csv_read_for_dml(self, athena_client):
        """Reads CSV from S3 and yields batches."""
        client, mock_athena = athena_client
        csv_bytes = _make_csv_bytes(_sample_df())

        qe = _dml_query_execution("exec-csv", "s3://bucket/results/abc.csv")
        client._last_query_execution = qe

        mock_athena.get_query_results.return_value = SAMPLE_SCHEMA_RESPONSE
        mock_s3 = Mock()
        mock_s3.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=csv_bytes))
        }
        client._s3_client = mock_s3

        pages = list(client.results_iter("exec-csv", page_size=2))

        total = sum(len(p) for p in pages)
        assert total == 3
        # Only schema lookup
        mock_athena.get_query_results.assert_called_once_with(
            QueryExecutionId="exec-csv", MaxResults=1
        )

    def test_s3_error_falls_back(self, athena_client):
        """Falls back to JSON API if S3 read fails."""
        client, mock_athena = athena_client

        qe = _dml_query_execution("exec-3", "s3://bucket/results/abc.parquet")
        client._last_query_execution = qe

        mock_s3 = Mock()
        mock_s3.get_object.side_effect = Exception("S3 error")
        client._s3_client = mock_s3

        json_response = {
            "ResultSet": {
                "ResultSetMetadata": {
                    "ColumnInfo": [{"Name": "id", "Type": "integer"}]
                },
                "Rows": [
                    {"Data": [{"VarCharValue": "id"}]},
                    {"Data": [{"VarCharValue": "7"}]},
                ],
            }
        }
        mock_athena.get_query_results.return_value = json_response

        pages = list(client.results_iter("exec-3"))

        assert pages[0].item(0, "id") == 7
        mock_athena.get_query_results.assert_called_once()
