"""Tests for the owlbear MCP server."""

import json
import pytest
import polars as pl
from unittest.mock import patch, MagicMock

from owlbear.mcp_server import (
    _get_client,
    _query_to_json,
    _MAX_ROWS_CAP,
    execute_query,
    list_databases,
    list_tables,
    describe_table,
)
import owlbear.mcp_server as mcp_mod


@pytest.fixture(autouse=True)
def _reset_client():
    """Reset the lazy singleton before each test."""
    mcp_mod._client = None
    yield
    mcp_mod._client = None


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------


class TestGetClient:
    def test_athena_default(self):
        env = {
            "OWLBEAR_S3_OUTPUT_LOCATION": "s3://bucket/results/",
            "OWLBEAR_DATABASE": "mydb",
        }
        with patch.dict("os.environ", env, clear=False), patch(
            "owlbear.mcp_server.AthenaClient"
        ) as MockAthena:
            client = _get_client()
            MockAthena.assert_called_once_with(
                database="mydb",
                output_location="s3://bucket/results/",
                region="us-east-1",
            )
            assert client is MockAthena.return_value

    def test_athena_explicit(self):
        env = {
            "OWLBEAR_BACKEND": "athena",
            "OWLBEAR_S3_OUTPUT_LOCATION": "s3://b/r/",
            "AWS_REGION": "eu-west-1",
        }
        with patch.dict("os.environ", env, clear=False), patch(
            "owlbear.mcp_server.AthenaClient"
        ) as MockAthena:
            client = _get_client()
            MockAthena.assert_called_once_with(
                database="default",
                output_location="s3://b/r/",
                region="eu-west-1",
            )
            assert client is MockAthena.return_value

    def test_trino_backend(self):
        env = {
            "OWLBEAR_BACKEND": "trino",
            "OWLBEAR_TRINO_HOST": "trino.example.com",
            "OWLBEAR_TRINO_PORT": "8080",
            "OWLBEAR_TRINO_USER": "analyst",
            "OWLBEAR_TRINO_CATALOG": "hive",
            "OWLBEAR_DATABASE": "default",
        }
        with patch.dict("os.environ", env, clear=False), patch(
            "owlbear.mcp_server.TrinoClient"
        ) as MockTrino:
            client = _get_client()
            MockTrino.assert_called_once_with(
                host="trino.example.com",
                port=8080,
                user="analyst",
                catalog="hive",
                schema="default",
            )
            assert client is MockTrino.return_value

    def test_unknown_backend(self):
        env = {"OWLBEAR_BACKEND": "spark"}
        with patch.dict("os.environ", env, clear=False):
            with pytest.raises(ValueError, match="Unknown OWLBEAR_BACKEND"):
                _get_client()

    def test_lazy_singleton(self):
        """Second call returns the same object without re-creating."""
        sentinel = object()
        mcp_mod._client = sentinel
        assert _get_client() is sentinel


# ---------------------------------------------------------------------------
# _query_to_json helper
# ---------------------------------------------------------------------------


class TestQueryToJson:
    def test_athena_path(self):
        mock_client = MagicMock(spec_set=["query", "results"])
        # Make isinstance(client, AthenaClient) return True
        with patch("owlbear.mcp_server._get_client", return_value=mock_client), patch(
            "owlbear.mcp_server.isinstance", side_effect=lambda obj, cls: cls.__name__ == "AthenaClient"
        ):
            # Simpler approach: just set the class
            pass

        # Use a real approach: patch _get_client and check behaviour
        from owlbear.athena import AthenaClient

        mock_athena = MagicMock(spec=AthenaClient)
        mock_athena.query.return_value = "eid-123"
        mock_athena.results.return_value = pl.DataFrame({"x": [1, 2]})

        with patch("owlbear.mcp_server._get_client", return_value=mock_athena):
            result = _query_to_json("SELECT 1", 100)

        mock_athena.query.assert_called_once_with("SELECT 1")
        mock_athena.results.assert_called_once_with("eid-123", max_rows=100)
        assert json.loads(result) == [{"x": 1}, {"x": 2}]

    def test_trino_path(self):
        from owlbear.trino import TrinoClient

        mock_trino = MagicMock(spec=TrinoClient)
        mock_trino.query.return_value = pl.DataFrame({"y": [3]})

        with patch("owlbear.mcp_server._get_client", return_value=mock_trino):
            result = _query_to_json("SELECT 1", 50)

        mock_trino.query.assert_called_once_with("SELECT 1", max_rows=50)
        assert json.loads(result) == [{"y": 3}]

    def test_max_rows_capped(self):
        from owlbear.athena import AthenaClient

        mock_athena = MagicMock(spec=AthenaClient)
        mock_athena.query.return_value = "eid"
        mock_athena.results.return_value = pl.DataFrame({"x": [1]})

        with patch("owlbear.mcp_server._get_client", return_value=mock_athena):
            _query_to_json("SELECT 1", 999_999)

        mock_athena.results.assert_called_once_with("eid", max_rows=_MAX_ROWS_CAP)


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


class TestExecuteQuery:
    def test_success(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value='[{"a":1}]'
        ) as mock_q:
            result = execute_query("SELECT 1", max_rows=10)
            mock_q.assert_called_once_with("SELECT 1", 10)
            assert result == '[{"a":1}]'

    def test_default_max_rows(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value="[]"
        ) as mock_q:
            execute_query("SELECT 1")
            mock_q.assert_called_once_with("SELECT 1", 500)

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._query_to_json", side_effect=RuntimeError("boom")
        ):
            result = execute_query("BAD SQL")
            parsed = json.loads(result)
            assert "error" in parsed
            assert "boom" in parsed["error"]


class TestListDatabases:
    def test_success(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value='[{"database":"db1"}]'
        ) as mock_q:
            result = list_databases()
            mock_q.assert_called_once_with("SHOW DATABASES", max_rows=_MAX_ROWS_CAP)
            assert json.loads(result) == [{"database": "db1"}]

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._query_to_json", side_effect=Exception("fail")
        ):
            result = list_databases()
            assert "error" in json.loads(result)


class TestListTables:
    def test_no_database(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value="[]"
        ) as mock_q:
            list_tables()
            mock_q.assert_called_once_with("SHOW TABLES", max_rows=_MAX_ROWS_CAP)

    def test_with_database(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value="[]"
        ) as mock_q:
            list_tables(database="analytics")
            mock_q.assert_called_once_with(
                "SHOW TABLES IN analytics", max_rows=_MAX_ROWS_CAP
            )

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._query_to_json", side_effect=Exception("fail")
        ):
            result = list_tables()
            assert "error" in json.loads(result)


class TestDescribeTable:
    def test_success(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value="[]"
        ) as mock_q:
            describe_table("my_db.my_table")
            mock_q.assert_called_once_with(
                "DESCRIBE my_db.my_table", max_rows=_MAX_ROWS_CAP
            )

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._query_to_json", side_effect=Exception("fail")
        ):
            result = describe_table("bad_table")
            assert "error" in json.loads(result)
