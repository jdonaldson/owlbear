"""Tests for the owlbear MCP server."""

import json
import pytest
import polars as pl
from unittest.mock import patch, MagicMock

from owlbear.mcp_server import (
    _get_client,
    _get_columns,
    _paginate,
    _query_to_json,
    _snippet_header,
    _snippet_query,
    _is_scalar_stat_type,
    _MAX_ROWS_CAP,
    _CACHE_THRESHOLD,
    _coerce_value,
    execute_query,
    explain_query,
    list_databases,
    list_tables,
    search_tables,
    describe_table,
    get_schema_context,
    profile_table,
    generate_snippet,
    show_partitions,
    df_list,
    df_drop,
    df_head,
    df_describe,
    df_schema,
    df_filter,
    df_select,
    df_group_by,
    df_sort,
    df_value_counts,
    explore_table,
    build_pipeline,
    get_config,
)
import owlbear.mcp_server as mcp_mod


@pytest.fixture(autouse=True)
def _reset_client():
    """Reset the lazy singleton and DataFrame cache before each test."""
    mcp_mod._client = None
    mcp_mod._dataframes.clear()
    mcp_mod._df_counter = 0
    yield
    mcp_mod._client = None
    mcp_mod._dataframes.clear()
    mcp_mod._df_counter = 0


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
    def test_small_result_no_cache(self):
        """Results below threshold return a plain list, no df_id."""
        small_df = pl.DataFrame({"a": [1, 2]})
        with patch("owlbear.mcp_server._query_to_df", return_value=small_df):
            result = json.loads(execute_query("SELECT 1", max_rows=10))
        assert isinstance(result, list)
        assert result == [{"a": 1}, {"a": 2}]
        assert len(mcp_mod._dataframes) == 0

    def test_large_result_cached(self):
        """Results at or above threshold are cached and include df_id."""
        large_df = pl.DataFrame({"x": list(range(_CACHE_THRESHOLD))})
        with patch("owlbear.mcp_server._query_to_df", return_value=large_df):
            result = json.loads(execute_query("SELECT *", max_rows=500))
        assert "df_id" in result
        assert result["df_id"] == "df_1"
        assert len(result["rows"]) == _CACHE_THRESHOLD
        assert "df_1" in mcp_mod._dataframes

    def test_default_max_rows(self):
        small_df = pl.DataFrame({"a": [1]})
        with patch("owlbear.mcp_server._query_to_df", return_value=small_df):
            execute_query("SELECT 1")

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._query_to_df", side_effect=RuntimeError("boom")
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
            result = json.loads(list_databases())
            mock_q.assert_called_once_with("SHOW DATABASES", max_rows=_MAX_ROWS_CAP)
            assert result["total"] == 1
            assert result["rows"] == [{"database": "db1"}]

    def test_pagination(self):
        rows = json.dumps([{"database": f"db{i}"} for i in range(5)])
        with patch("owlbear.mcp_server._query_to_json", return_value=rows):
            result = json.loads(list_databases(limit=2, offset=1))
        assert result["total"] == 5
        assert result["offset"] == 1
        assert result["limit"] == 2
        assert len(result["rows"]) == 2
        assert result["rows"][0]["database"] == "db1"
        assert result["rows"][1]["database"] == "db2"

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
            result = json.loads(list_tables())
            mock_q.assert_called_once_with("SHOW TABLES", max_rows=_MAX_ROWS_CAP)
            assert result == {"total": 0, "offset": 0, "limit": 0, "rows": []}

    def test_with_database(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value="[]"
        ) as mock_q:
            list_tables(database="analytics")
            mock_q.assert_called_once_with(
                "SHOW TABLES IN analytics", max_rows=_MAX_ROWS_CAP
            )

    def test_pagination(self):
        rows = json.dumps([{"tab_name": f"t{i}"} for i in range(10)])
        with patch("owlbear.mcp_server._query_to_json", return_value=rows):
            result = json.loads(list_tables(limit=3, offset=2))
        assert result["total"] == 10
        assert result["offset"] == 2
        assert result["limit"] == 3
        assert len(result["rows"]) == 3
        assert result["rows"][0]["tab_name"] == "t2"

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

    def test_stats_sampled(self):
        columns = [
            {"column_name": "id", "data_type": "bigint"},
        ]
        count_resp = json.dumps([{"cnt": 1000}])
        stats_resp = json.dumps([{
            "id__null_count": 0,
            "id__distinct_count": 100,
            "id__min": "1",
            "id__max": "1000",
        }])
        sample_resp = json.dumps([{"id": 1}])

        calls: list[str] = []

        def capture_query(sql: str, max_rows: int) -> str:
            calls.append(sql)
            if "COUNT(*)" in sql:
                return count_resp
            if "LIMIT" in sql:
                return sample_resp
            return stats_resp

        with patch("owlbear.mcp_server._get_columns", return_value=columns), \
             patch("owlbear.mcp_server._query_to_json", side_effect=capture_query):
            result = json.loads(profile_table("db.events", stats_sample_pct=10))

        assert result["stats_sampled"] is True
        assert result["stats_sample_pct"] == 10
        # The stats query should use TABLESAMPLE
        stats_sql = [c for c in calls if "null_count" in c][0]
        assert "TABLESAMPLE BERNOULLI(10)" in stats_sql
        # Row count should NOT use TABLESAMPLE
        count_sql = [c for c in calls if "COUNT(*)" in c][0]
        assert "TABLESAMPLE" not in count_sql

    def test_stats_not_sampled_by_default(self):
        columns = [{"column_name": "id", "data_type": "bigint"}]
        count_resp = json.dumps([{"cnt": 5}])
        stats_resp = json.dumps([{
            "id__null_count": 0, "id__distinct_count": 5,
            "id__min": "1", "id__max": "5",
        }])
        sample_resp = json.dumps([{"id": 1}])

        with patch("owlbear.mcp_server._get_columns", return_value=columns), \
             patch(
                "owlbear.mcp_server._query_to_json",
                side_effect=[count_resp, stats_resp, sample_resp],
             ):
            result = json.loads(profile_table("db.events"))

        assert result["stats_sampled"] is False
        assert result["stats_sample_pct"] == 0

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
        assert result["backend"] == "athena"
        assert "AthenaClient" in result["snippet"]
        assert "db.users" in result["snippet"]

    @pytest.mark.parametrize("operation", ["load", "filter", "aggregate", "join"])
    def test_trino_backend(self, operation: str):
        env = {"OWLBEAR_BACKEND": "trino"}
        with patch.dict("os.environ", env, clear=False), \
             patch("owlbear.mcp_server._get_columns", return_value=self._mock_columns):
            result = json.loads(generate_snippet("db.users", operation))
        assert result["backend"] == "trino"
        assert "TrinoClient" in result["snippet"]
        assert "client.results(" not in result["snippet"]
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
# explain_query tool
# ---------------------------------------------------------------------------


class TestExplainQuery:
    def test_success(self):
        plan = '[{"Query Plan": "TableScan ..."}]'
        with patch(
            "owlbear.mcp_server._query_to_json", return_value=plan
        ) as mock_q:
            result = explain_query("SELECT * FROM orders")
            mock_q.assert_called_once_with(
                "EXPLAIN SELECT * FROM orders", max_rows=_MAX_ROWS_CAP
            )
            assert result == plan

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._query_to_json",
            side_effect=RuntimeError("syntax error"),
        ):
            result = explain_query("BAD SQL")
            parsed = json.loads(result)
            assert "error" in parsed
            assert "syntax error" in parsed["error"]


# ---------------------------------------------------------------------------
# search_tables tool
# ---------------------------------------------------------------------------


class TestSearchTables:
    def test_without_database(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value="[]"
        ) as mock_q:
            result = json.loads(search_tables("%order%"))
            mock_q.assert_called_once_with(
                "SHOW TABLES LIKE '%order%'", max_rows=_MAX_ROWS_CAP
            )
            assert result == {"total": 0, "offset": 0, "limit": 0, "rows": []}

    def test_with_database(self):
        with patch(
            "owlbear.mcp_server._query_to_json", return_value="[]"
        ) as mock_q:
            search_tables("%click%", database="analytics")
            mock_q.assert_called_once_with(
                "SHOW TABLES IN analytics LIKE '%click%'", max_rows=_MAX_ROWS_CAP
            )

    def test_pagination(self):
        rows = json.dumps([{"tab_name": f"t{i}"} for i in range(5)])
        with patch("owlbear.mcp_server._query_to_json", return_value=rows):
            result = json.loads(search_tables("%t%", limit=2, offset=1))
        assert result["total"] == 5
        assert result["offset"] == 1
        assert result["limit"] == 2
        assert len(result["rows"]) == 2
        assert result["rows"][0]["tab_name"] == "t1"

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._query_to_json", side_effect=Exception("fail")
        ):
            result = search_tables("%x%")
            assert "error" in json.loads(result)


# ---------------------------------------------------------------------------
# show_partitions tool
# ---------------------------------------------------------------------------


class TestShowPartitions:
    def test_athena_backend(self):
        from owlbear.athena import AthenaClient

        mock_athena = MagicMock(spec=AthenaClient)
        rows = json.dumps([{"year": "2024", "month": "01"}])
        with patch("owlbear.mcp_server._get_client", return_value=mock_athena), \
             patch("owlbear.mcp_server._query_to_json", return_value=rows) as mock_q:
            result = json.loads(show_partitions("db.events"))
        mock_q.assert_called_once_with(
            "SHOW PARTITIONS db.events", max_rows=_MAX_ROWS_CAP
        )
        assert result["total"] == 1
        assert result["rows"] == [{"year": "2024", "month": "01"}]

    def test_trino_qualified_table(self):
        from owlbear.trino import TrinoClient

        mock_trino = MagicMock(spec=TrinoClient)
        rows = json.dumps([{"year": "2024"}])
        with patch("owlbear.mcp_server._get_client", return_value=mock_trino), \
             patch("owlbear.mcp_server._query_to_json", return_value=rows) as mock_q:
            result = json.loads(show_partitions("mydb.events"))
        mock_q.assert_called_once_with(
            'SELECT * FROM "mydb"."events$partitions"', max_rows=_MAX_ROWS_CAP
        )
        assert result["total"] == 1

    def test_trino_unqualified_table(self):
        from owlbear.trino import TrinoClient

        mock_trino = MagicMock(spec=TrinoClient)
        rows = json.dumps([])
        with patch("owlbear.mcp_server._get_client", return_value=mock_trino), \
             patch("owlbear.mcp_server._query_to_json", return_value=rows) as mock_q:
            result = json.loads(show_partitions("events"))
        mock_q.assert_called_once_with(
            'SELECT * FROM "events$partitions"', max_rows=_MAX_ROWS_CAP
        )
        assert result["total"] == 0

    def test_pagination(self):
        from owlbear.athena import AthenaClient

        mock_athena = MagicMock(spec=AthenaClient)
        rows = json.dumps([{"p": f"v{i}"} for i in range(6)])
        with patch("owlbear.mcp_server._get_client", return_value=mock_athena), \
             patch("owlbear.mcp_server._query_to_json", return_value=rows):
            result = json.loads(show_partitions("t", limit=2, offset=3))
        assert result["total"] == 6
        assert result["offset"] == 3
        assert result["limit"] == 2
        assert len(result["rows"]) == 2

    def test_error_returns_json(self):
        with patch(
            "owlbear.mcp_server._get_client",
            side_effect=RuntimeError("no backend"),
        ):
            result = show_partitions("bad_table")
            assert "error" in json.loads(result)


# ---------------------------------------------------------------------------
# DataFrame cache tools
# ---------------------------------------------------------------------------


def _seed_cache() -> pl.DataFrame:
    """Seed the cache with a sample DataFrame at df_1 and return it."""
    df = pl.DataFrame({
        "id": [1, 2, 3, 4, 5],
        "name": ["alice", "bob", "carol", "dave", None],
        "score": [10.0, 20.0, 30.0, 40.0, 50.0],
    })
    mcp_mod._dataframes["df_1"] = df
    mcp_mod._df_counter = 1
    return df


class TestDfList:
    def test_empty(self):
        result = json.loads(df_list())
        assert result == []

    def test_with_entries(self):
        _seed_cache()
        result = json.loads(df_list())
        assert len(result) == 1
        assert result[0]["df_id"] == "df_1"
        assert result[0]["rows"] == 5
        assert result[0]["cols"] == 3
        assert "id" in result[0]["columns"]


class TestDfDrop:
    def test_drop_existing(self):
        _seed_cache()
        result = json.loads(df_drop("df_1"))
        assert result == {"dropped": "df_1"}
        assert "df_1" not in mcp_mod._dataframes

    def test_drop_unknown(self):
        result = json.loads(df_drop("df_999"))
        assert "error" in result


class TestDfHead:
    def test_default_n(self):
        _seed_cache()
        result = json.loads(df_head("df_1"))
        assert len(result) == 5  # only 5 rows total

    def test_custom_n(self):
        _seed_cache()
        result = json.loads(df_head("df_1", n=2))
        assert len(result) == 2
        assert result[0]["id"] == 1

    def test_unknown_df(self):
        result = json.loads(df_head("df_999"))
        assert "error" in result


class TestDfDescribe:
    def test_success(self):
        _seed_cache()
        result = json.loads(df_describe("df_1"))
        assert isinstance(result, list)
        # describe() returns stats rows (count, null_count, mean, etc.)
        stats = {row["statistic"] for row in result}
        assert "count" in stats

    def test_unknown_df(self):
        result = json.loads(df_describe("df_999"))
        assert "error" in result


class TestDfSchema:
    def test_success(self):
        _seed_cache()
        result = json.loads(df_schema("df_1"))
        assert len(result) == 3
        names = [c["column"] for c in result]
        assert names == ["id", "name", "score"]
        # Check dtypes are strings
        assert all(isinstance(c["dtype"], str) for c in result)

    def test_unknown_df(self):
        result = json.loads(df_schema("df_999"))
        assert "error" in result


class TestDfFilter:
    def test_equality(self):
        _seed_cache()
        result = json.loads(df_filter("df_1", "id", "=", "2"))
        assert "df_id" in result
        assert len(result["rows"]) == 1
        assert result["rows"][0]["id"] == 2

    def test_greater_than(self):
        _seed_cache()
        result = json.loads(df_filter("df_1", "score", ">", "25.0"))
        assert len(result["rows"]) == 3

    def test_is_null(self):
        _seed_cache()
        result = json.loads(df_filter("df_1", "name", "is_null"))
        assert len(result["rows"]) == 1
        assert result["rows"][0]["name"] is None

    def test_is_not_null(self):
        _seed_cache()
        result = json.loads(df_filter("df_1", "name", "is_not_null"))
        assert len(result["rows"]) == 4

    def test_contains(self):
        _seed_cache()
        result = json.loads(df_filter("df_1", "name", "contains", "ob"))
        assert len(result["rows"]) == 1
        assert result["rows"][0]["name"] == "bob"

    def test_unknown_op(self):
        _seed_cache()
        result = json.loads(df_filter("df_1", "id", "LIKE", "1"))
        assert "error" in result

    def test_unknown_df(self):
        result = json.loads(df_filter("df_999", "id", "=", "1"))
        assert "error" in result

    def test_caches_result(self):
        _seed_cache()
        result = json.loads(df_filter("df_1", "id", "=", "1"))
        new_id = result["df_id"]
        assert new_id in mcp_mod._dataframes
        assert mcp_mod._dataframes[new_id].height == 1


class TestDfSelect:
    def test_select_columns(self):
        _seed_cache()
        result = json.loads(df_select("df_1", ["id", "name"]))
        assert "df_id" in result
        assert list(result["rows"][0].keys()) == ["id", "name"]

    def test_unknown_df(self):
        result = json.loads(df_select("df_999", ["id"]))
        assert "error" in result


class TestDfGroupBy:
    def test_sum(self):
        df = pl.DataFrame({"cat": ["a", "a", "b"], "val": [1, 2, 3]})
        mcp_mod._dataframes["df_1"] = df
        mcp_mod._df_counter = 1
        result = json.loads(df_group_by("df_1", ["cat"], "val", "sum"))
        assert "df_id" in result
        rows = sorted(result["rows"], key=lambda r: r["cat"])
        assert rows[0]["cat"] == "a"
        assert rows[0]["val"] == 3
        assert rows[1]["cat"] == "b"
        assert rows[1]["val"] == 3

    def test_count(self):
        df = pl.DataFrame({"cat": ["a", "a", "b"], "val": [1, 2, 3]})
        mcp_mod._dataframes["df_1"] = df
        mcp_mod._df_counter = 1
        result = json.loads(df_group_by("df_1", ["cat"], "val", "count"))
        rows = sorted(result["rows"], key=lambda r: r["cat"])
        assert rows[0]["val"] == 2  # "a" appears twice

    def test_unknown_agg_func(self):
        _seed_cache()
        result = json.loads(df_group_by("df_1", ["name"], "score", "variance"))
        assert "error" in result

    def test_unknown_df(self):
        result = json.loads(df_group_by("df_999", ["x"], "y", "sum"))
        assert "error" in result


class TestDfSort:
    def test_ascending(self):
        _seed_cache()
        result = json.loads(df_sort("df_1", ["score"]))
        assert "df_id" in result
        scores = [r["score"] for r in result["rows"]]
        assert scores == sorted(scores)

    def test_descending(self):
        _seed_cache()
        result = json.loads(df_sort("df_1", ["score"], descending=True))
        scores = [r["score"] for r in result["rows"]]
        assert scores == sorted(scores, reverse=True)

    def test_unknown_df(self):
        result = json.loads(df_sort("df_999", ["x"]))
        assert "error" in result


class TestDfValueCounts:
    def test_success(self):
        df = pl.DataFrame({"color": ["red", "red", "blue", "red", "blue"]})
        mcp_mod._dataframes["df_1"] = df
        mcp_mod._df_counter = 1
        result = json.loads(df_value_counts("df_1", "color"))
        assert "df_id" in result
        rows = result["rows"]
        assert len(rows) == 2
        # sorted by count desc
        assert rows[0]["color"] == "red"
        assert rows[0]["count"] == 3

    def test_unknown_df(self):
        result = json.loads(df_value_counts("df_999", "x"))
        assert "error" in result


class TestCoerceValue:
    def test_integer(self):
        assert _coerce_value(pl.Int64, "42") == 42

    def test_float(self):
        assert _coerce_value(pl.Float64, "3.14") == 3.14

    def test_boolean_true(self):
        assert _coerce_value(pl.Boolean, "true") is True

    def test_boolean_false(self):
        assert _coerce_value(pl.Boolean, "no") is False

    def test_string_passthrough(self):
        assert _coerce_value(pl.Utf8, "hello") == "hello"


class TestAutoIncrementingIds:
    def test_sequential_ids(self):
        """Multiple cache operations produce sequential IDs."""
        df1 = pl.DataFrame({"a": [1]})
        df2 = pl.DataFrame({"b": [2]})
        id1 = mcp_mod._cache_df(df1)
        id2 = mcp_mod._cache_df(df2)
        assert id1 == "df_1"
        assert id2 == "df_2"

    def test_transformation_chains(self):
        """Transformations produce new IDs, original remains."""
        _seed_cache()  # df_1
        result1 = json.loads(df_filter("df_1", "id", ">", "2"))
        result2 = json.loads(df_sort(result1["df_id"], ["id"]))
        assert result1["df_id"] == "df_2"
        assert result2["df_id"] == "df_3"
        # Original still exists
        assert "df_1" in mcp_mod._dataframes


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
