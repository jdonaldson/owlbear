"""Tests for TrinoClient"""

import pytest
import polars as pl
from unittest.mock import Mock, patch, MagicMock

from owlbear import TrinoClient


@pytest.fixture
def mock_cursor():
    """Create a mock Trino cursor with description and data."""
    cursor = Mock()
    return cursor


@pytest.fixture
def mock_connection(mock_cursor):
    """Create a mock Trino connection."""
    conn = Mock()
    conn.cursor.return_value = mock_cursor
    return conn


@pytest.fixture
def trino_client():
    """Create a TrinoClient instance (no real connection needed)."""
    return TrinoClient(
        host="trino.example.com",
        port=443,
        user="test_user",
        catalog="hive",
        schema="default",
    )


class TestTrinoClientInit:
    def test_init_defaults(self):
        client = TrinoClient(host="localhost")
        assert client.host == "localhost"
        assert client.port == 443
        assert client.http_scheme == "https"
        assert client.user is None
        assert client.catalog is None
        assert client.schema is None

    def test_init_all_params(self):
        auth = Mock()
        client = TrinoClient(
            host="trino.example.com",
            port=8080,
            user="admin",
            catalog="hive",
            schema="analytics",
            auth=auth,
            http_scheme="http",
        )
        assert client.host == "trino.example.com"
        assert client.port == 8080
        assert client.user == "admin"
        assert client.catalog == "hive"
        assert client.schema == "analytics"
        assert client.auth is auth
        assert client.http_scheme == "http"

    def test_init_extra_kwargs(self):
        client = TrinoClient(host="localhost", source="owlbear-test")
        assert client.kwargs == {"source": "owlbear-test"}


class TestTrinoExecuteQuery:
    def test_basic_query(self, trino_client, mock_connection, mock_cursor):
        mock_cursor.description = [
            ("id", "integer", None, None, None, None, None),
            ("name", "varchar", None, None, None, None, None),
        ]
        mock_cursor.fetchall.return_value = [
            (1, "Alice"),
            (2, "Bob"),
        ]

        with patch.object(trino_client, "_get_connection", return_value=mock_connection):
            df = trino_client.execute_query("SELECT id, name FROM users")

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["id", "name"]
        assert len(df) == 2
        assert df.item(0, "id") == 1
        assert df.item(0, "name") == "Alice"
        assert df.item(1, "id") == 2
        assert df.item(1, "name") == "Bob"
        mock_connection.close.assert_called_once()

    def test_typed_columns(self, trino_client, mock_connection, mock_cursor):
        mock_cursor.description = [
            ("count", "bigint", None, None, None, None, None),
            ("ratio", "double", None, None, None, None, None),
            ("active", "boolean", None, None, None, None, None),
        ]
        mock_cursor.fetchall.return_value = [
            (100, 0.75, True),
            (200, 0.50, False),
        ]

        with patch.object(trino_client, "_get_connection", return_value=mock_connection):
            df = trino_client.execute_query("SELECT count, ratio, active FROM stats")

        assert df.dtypes[0] == pl.Int64
        assert df.dtypes[1] == pl.Float64
        assert df.dtypes[2] == pl.Boolean

    def test_with_nulls(self, trino_client, mock_connection, mock_cursor):
        mock_cursor.description = [
            ("id", "integer", None, None, None, None, None),
            ("value", "varchar", None, None, None, None, None),
        ]
        mock_cursor.fetchall.return_value = [
            (1, None),
            (2, "test"),
        ]

        with patch.object(trino_client, "_get_connection", return_value=mock_connection):
            df = trino_client.execute_query("SELECT id, value FROM t")

        assert len(df) == 2
        assert df.item(0, "value") is None
        assert df.item(1, "value") == "test"

    def test_empty_results(self, trino_client, mock_connection, mock_cursor):
        mock_cursor.description = [
            ("id", "integer", None, None, None, None, None),
            ("name", "varchar", None, None, None, None, None),
        ]
        mock_cursor.fetchall.return_value = []

        with patch.object(trino_client, "_get_connection", return_value=mock_connection):
            df = trino_client.execute_query("SELECT id, name FROM empty_table")

        assert isinstance(df, pl.DataFrame)
        assert df.columns == ["id", "name"]
        assert len(df) == 0

    def test_no_description(self, trino_client, mock_connection, mock_cursor):
        """DDL statements return no description."""
        mock_cursor.description = None
        mock_cursor.fetchall.return_value = []

        with patch.object(trino_client, "_get_connection", return_value=mock_connection):
            df = trino_client.execute_query("CREATE TABLE foo (id INT)")

        assert isinstance(df, pl.DataFrame)
        assert len(df) == 0

    def test_max_rows(self, trino_client, mock_connection, mock_cursor):
        mock_cursor.description = [
            ("id", "integer", None, None, None, None, None),
        ]
        mock_cursor.fetchmany.return_value = [(1,), (2,)]

        with patch.object(trino_client, "_get_connection", return_value=mock_connection):
            df = trino_client.execute_query("SELECT id FROM t", max_rows=2)

        mock_cursor.fetchmany.assert_called_once_with(2)
        assert len(df) == 2

    def test_connection_closed_on_error(self, trino_client, mock_connection, mock_cursor):
        mock_cursor.execute.side_effect = Exception("Connection failed")

        with patch.object(trino_client, "_get_connection", return_value=mock_connection):
            with pytest.raises(Exception, match="Connection failed"):
                trino_client.execute_query("SELECT 1")

        mock_connection.close.assert_called_once()


class TestTrinoGetConnection:
    def test_get_connection_params(self):
        auth = Mock()
        client = TrinoClient(
            host="trino.example.com",
            port=8080,
            user="admin",
            catalog="hive",
            schema="analytics",
            auth=auth,
            http_scheme="http",
        )

        with patch("trino.dbapi.connect") as mock_connect:
            client._get_connection()
            mock_connect.assert_called_once_with(
                host="trino.example.com",
                port=8080,
                http_scheme="http",
                user="admin",
                catalog="hive",
                schema="analytics",
                auth=auth,
            )

    def test_get_connection_minimal(self):
        client = TrinoClient(host="localhost")

        with patch("trino.dbapi.connect") as mock_connect:
            client._get_connection()
            mock_connect.assert_called_once_with(
                host="localhost",
                port=443,
                http_scheme="https",
            )
