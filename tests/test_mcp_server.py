"""Tests for the owlbear MCP server."""

import json
import pytest
import polars as pl
from unittest.mock import patch, MagicMock

from owlbear.mcp_server import (
    _get_client,
    _get_columns,
    _query_to_json,
    _is_scalar_stat_type,
    _MAX_ROWS_CAP,
    execute_query,
    list_databases,
    list_tables,
    describe_table,
    get_schema_context,
    profile_table,
    generate_snippet,
    explore_table,
    build_pipeline,
    get_config,
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
        cols = [{"column_name": "id", "data_type": "bigint"}]
        with patch("owlbear.mcp_server._get_columns", return_value=cols):
            result = json.loads(describe_table("my_db.my_table"))
        assert result == cols

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._get_columns", side_effect=Exception("fail")
        ):
            result = describe_table("bad_table")
            assert "error" in json.loads(result)


# ---------------------------------------------------------------------------
# _get_columns helper
# ---------------------------------------------------------------------------


class TestGetColumns:
    def test_information_schema_path(self):
        info_resp = json.dumps([
            {"column_name": "id", "data_type": "bigint"},
            {"column_name": "name", "data_type": "varchar"},
        ])
        with patch("owlbear.mcp_server._query_to_json", return_value=info_resp):
            result = _get_columns("mydb.mytable")
        assert len(result) == 2
        assert result[0]["column_name"] == "id"

    def test_falls_back_to_describe(self):
        describe_resp = json.dumps([
            {"col_name": "x", "data_type": "int"},
            {"col_name": "# Partition Information", "data_type": ""},
        ])
        with patch(
            "owlbear.mcp_server._query_to_json",
            side_effect=[RuntimeError("info_schema failed"), describe_resp],
        ):
            result = _get_columns("mydb.mytable")
        assert len(result) == 1
        assert result[0]["col_name"] == "x"

    def test_unqualified_table_name(self):
        info_resp = json.dumps([{"column_name": "a", "data_type": "int"}])
        with patch("owlbear.mcp_server._query_to_json", return_value=info_resp) as mock_q:
            _get_columns("mytable")
        # Should not include table_schema in WHERE
        call_sql = mock_q.call_args[0][0]
        assert "table_schema" not in call_sql
        assert "table_name = 'mytable'" in call_sql


# ---------------------------------------------------------------------------
# _is_scalar_stat_type helper
# ---------------------------------------------------------------------------


class TestIsScalarStatType:
    @pytest.mark.parametrize("dtype", [
        "boolean", "tinyint", "smallint", "int", "integer", "bigint",
        "float", "double", "real",
        "varchar", "varchar(255)", "char", "string",
        "date", "timestamp", "timestamp with time zone",
        "decimal", "decimal(10,2)",
    ])
    def test_scalar_types(self, dtype: str):
        assert _is_scalar_stat_type(dtype) is True

    @pytest.mark.parametrize("dtype", [
        "array<string>", "map<string,int>", "row(x int, y int)",
        "struct<a:int>",
    ])
    def test_complex_types(self, dtype: str):
        assert _is_scalar_stat_type(dtype) is False

    def test_case_insensitive(self):
        assert _is_scalar_stat_type("VARCHAR") is True
        assert _is_scalar_stat_type("BIGINT") is True


# ---------------------------------------------------------------------------
# profile_table tool
# ---------------------------------------------------------------------------


class TestProfileTable:
    def test_success(self):
        columns = [
            {"column_name": "id", "data_type": "bigint"},
            {"column_name": "name", "data_type": "varchar"},
            {"column_name": "tags", "data_type": "array<string>"},
        ]
        count_resp = json.dumps([{"cnt": 42}])
        stats_resp = json.dumps([{
            "id__null_count": 0,
            "id__distinct_count": 42,
            "id__min": "1",
            "id__max": "42",
            "name__null_count": 2,
            "name__distinct_count": 30,
            "name__min": "alice",
            "name__max": "zed",
            "tags__null_count": 5,
        }])
        sample_resp = json.dumps([{"id": 1, "name": "alice", "tags": ["a"]}])

        with patch("owlbear.mcp_server._get_columns", return_value=columns), \
             patch(
                "owlbear.mcp_server._query_to_json",
                side_effect=[count_resp, stats_resp, sample_resp],
             ):
            result = json.loads(profile_table("db.events", sample_size=10))

        assert result["table"] == "db.events"
        assert result["row_count"] == 42
        assert len(result["columns"]) == 3
        # scalar columns have distinct/min/max
        id_col = result["columns"][0]
        assert id_col["column"] == "id"
        assert "distinct_count" in id_col
        assert "min" in id_col
        # complex column has only null_count
        tags_col = result["columns"][2]
        assert tags_col["column"] == "tags"
        assert "distinct_count" not in tags_col
        assert len(result["sample_rows"]) == 1

    def test_error(self):
        with patch(
            "owlbear.mcp_server._get_columns",
            side_effect=RuntimeError("timeout"),
        ):
            result = json.loads(profile_table("bad_table"))
            assert "error" in result


# ---------------------------------------------------------------------------
# get_schema_context tool
# ---------------------------------------------------------------------------


class TestGetSchemaContext:
    def test_multiple_tables(self):
        t1_cols = [{"column_name": "a", "data_type": "int"}]
        t2_cols = [{"column_name": "b", "data_type": "varchar"}]

        with patch(
            "owlbear.mcp_server._get_columns",
            side_effect=[t1_cols, t2_cols],
        ):
            result = json.loads(get_schema_context("db.t1, db.t2"))

        assert "db.t1" in result
        assert "db.t2" in result
        assert result["db.t1"] == [{"column_name": "a", "data_type": "int"}]
        assert result["db.t2"] == [{"column_name": "b", "data_type": "varchar"}]

    def test_partial_failure(self):
        t1_cols = [{"column_name": "a", "data_type": "int"}]

        with patch(
            "owlbear.mcp_server._get_columns",
            side_effect=[t1_cols, RuntimeError("no such table")],
        ):
            result = json.loads(get_schema_context("db.t1, db.missing"))

        assert isinstance(result["db.t1"], list)
        assert "error" in result["db.missing"]

    def test_empty_input(self):
        result = json.loads(get_schema_context(""))
        assert result == {}


# ---------------------------------------------------------------------------
# generate_snippet tool
# ---------------------------------------------------------------------------


class TestGenerateSnippet:
    _mock_columns = [
        {"column_name": "user_id", "data_type": "bigint"},
        {"column_name": "name", "data_type": "varchar"},
        {"column_name": "score", "data_type": "double"},
    ]

    @pytest.mark.parametrize("operation", ["load", "filter", "aggregate", "join"])
    def test_operations(self, operation: str):
        with patch(
            "owlbear.mcp_server._get_columns", return_value=self._mock_columns
        ):
            result = json.loads(generate_snippet("db.users", operation))
        assert result["table"] == "db.users"
        assert result["operation"] == operation
        assert "AthenaClient" in result["snippet"]
        assert "db.users" in result["snippet"]

    def test_unknown_operation(self):
        result = json.loads(generate_snippet("db.users", "pivot"))
        assert "error" in result
        assert "pivot" in result["error"]

    def test_error_during_describe(self):
        with patch(
            "owlbear.mcp_server._get_columns",
            side_effect=RuntimeError("access denied"),
        ):
            result = json.loads(generate_snippet("db.users", "load"))
        assert "error" in result


# ---------------------------------------------------------------------------
# MCP prompts
# ---------------------------------------------------------------------------


class TestExploreTablePrompt:
    def test_returns_message_list(self):
        result = explore_table("db.events")
        assert isinstance(result, list)
        assert len(result) == 1
        msg = result[0]
        assert msg["role"] == "user"
        assert "db.events" in msg["content"]
        assert "DESCRIBE" in msg["content"]

    def test_table_name_in_content(self):
        result = explore_table("analytics.clicks")
        assert "analytics.clicks" in result[0]["content"]


class TestBuildPipelinePrompt:
    def test_returns_message_list(self):
        result = build_pipeline("db.orders", "compute daily revenue")
        assert isinstance(result, list)
        assert len(result) == 1
        msg = result[0]
        assert msg["role"] == "user"
        assert "db.orders" in msg["content"]
        assert "compute daily revenue" in msg["content"]

    def test_includes_boilerplate(self):
        result = build_pipeline("db.t", "test")
        content = result[0]["content"]
        assert "AthenaClient" in content
        assert "import polars" in content


# ---------------------------------------------------------------------------
# MCP resource: owlbear://config
# ---------------------------------------------------------------------------


class TestGetConfigResource:
    def test_athena_config(self):
        env = {
            "OWLBEAR_DATABASE": "mydb",
            "AWS_REGION": "us-west-2",
            "OWLBEAR_S3_OUTPUT_LOCATION": "s3://bucket/out/",
        }
        with patch.dict("os.environ", env, clear=False):
            result = json.loads(get_config())
        assert result["backend"] == "athena"
        assert result["database"] == "mydb"
        assert result["region"] == "us-west-2"
        assert result["s3_output_location"] == "s3://bucket/out/"

    def test_trino_config(self):
        env = {
            "OWLBEAR_BACKEND": "trino",
            "OWLBEAR_TRINO_HOST": "trino.example.com",
            "OWLBEAR_TRINO_PORT": "8080",
            "OWLBEAR_TRINO_USER": "analyst",
            "OWLBEAR_TRINO_CATALOG": "hive",
            "OWLBEAR_DATABASE": "default",
        }
        with patch.dict("os.environ", env, clear=False):
            result = json.loads(get_config())
        assert result["backend"] == "trino"
        assert result["host"] == "trino.example.com"
        assert result["port"] == 8080
        assert result["user"] == "analyst"
        assert result["catalog"] == "hive"
        assert result["database"] == "default"
