"""Owlbear MCP server — expose data-lake queries to AI assistants."""

from __future__ import annotations

import json
import os
import re
from typing import Union

import polars as pl
from mcp.server.fastmcp import FastMCP

from .athena import AthenaClient
from .trino import TrinoClient

mcp = FastMCP("owlbear")

_MAX_ROWS_CAP = 10_000
_CACHE_THRESHOLD = 50

# ---------------------------------------------------------------------------
# Client factory — lazy singleton from env vars
# ---------------------------------------------------------------------------

_client: Union[AthenaClient, TrinoClient, None] = None


def _get_client() -> Union[AthenaClient, TrinoClient]:
    global _client
    if _client is not None:
        return _client

    backend = os.environ.get("OWLBEAR_BACKEND", "athena").lower()

    if backend == "athena":
        _client = AthenaClient(
            database=os.environ.get("OWLBEAR_DATABASE", "default"),
            output_location=os.environ["OWLBEAR_S3_OUTPUT_LOCATION"],
            region=os.environ.get("AWS_REGION", "us-east-1"),
        )
    elif backend == "trino":
        _client = TrinoClient(
            host=os.environ["OWLBEAR_TRINO_HOST"],
            port=int(os.environ.get("OWLBEAR_TRINO_PORT", "443")),
            user=os.environ.get("OWLBEAR_TRINO_USER"),
            catalog=os.environ.get("OWLBEAR_TRINO_CATALOG"),
            schema=os.environ.get("OWLBEAR_DATABASE"),
        )
    else:
        raise ValueError(f"Unknown OWLBEAR_BACKEND: {backend!r} (expected 'athena' or 'trino')")

    return _client


# ---------------------------------------------------------------------------
# DataFrame cache
# ---------------------------------------------------------------------------

_dataframes: dict[str, pl.DataFrame] = {}
_df_counter: int = 0


def _next_df_id() -> str:
    """Return the next auto-incrementing DataFrame ID."""
    global _df_counter
    _df_counter += 1
    return f"df_{_df_counter}"


def _get_df(df_id: str) -> pl.DataFrame:
    """Retrieve a cached DataFrame by ID, raising KeyError if missing."""
    if df_id not in _dataframes:
        raise KeyError(f"Unknown DataFrame: {df_id}")
    return _dataframes[df_id]


def _cache_df(df: pl.DataFrame) -> str:
    """Cache *df* and return its assigned ID."""
    df_id = _next_df_id()
    _dataframes[df_id] = df
    return df_id


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _query_to_df(sql: str, max_rows: int) -> pl.DataFrame:
    """Run *sql* via the active backend and return a Polars DataFrame."""
    max_rows = min(max_rows, _MAX_ROWS_CAP)
    client = _get_client()

    if isinstance(client, AthenaClient):
        execution_id = client.query(sql)
        return client.results(execution_id, max_rows=max_rows)
    else:
        return client.query(sql, max_rows=max_rows)


def _query_to_json(sql: str, max_rows: int) -> str:
    """Run *sql* via the active backend and return JSON-serialised rows."""
    return json.dumps(_query_to_df(sql, max_rows).to_dicts(), default=str)


_SCALAR_STAT_PATTERN = re.compile(
    r"^("
    r"boolean|tinyint|smallint|int(eger)?|bigint"
    r"|float|double|real"
    r"|varchar|char|string"
    r"|date|timestamp"
    r"|decimal"
    r")",
    re.IGNORECASE,
)


def _is_scalar_stat_type(data_type: str) -> bool:
    """Return True if *data_type* supports MIN/MAX/COUNT(DISTINCT)."""
    return _SCALAR_STAT_PATTERN.match(data_type.strip()) is not None


def _get_columns(table: str) -> list[dict[str, str]]:
    """Get column names and types for *table*.

    Tries ``information_schema.columns`` first (reliable), falls back to
    ``DESCRIBE`` (which can fail on partitioned tables with ragged metadata).
    """
    # Parse schema/table from potentially qualified name
    parts = table.split(".")
    if len(parts) == 2:
        schema, tbl = parts
    else:
        schema, tbl = None, parts[-1]

    try:
        where = f"table_name = '{tbl}'"
        if schema:
            where += f" AND table_schema = '{schema}'"
        sql = (
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            f"WHERE {where} "
            "ORDER BY ordinal_position"
        )
        raw: list[dict[str, str]] = json.loads(_query_to_json(sql, max_rows=_MAX_ROWS_CAP))
        if raw:
            return raw
    except Exception:
        pass

    # Fallback to DESCRIBE
    raw = json.loads(_query_to_json(f"DESCRIBE {table}", max_rows=_MAX_ROWS_CAP))
    return [row for row in raw if not row.get("col_name", "").startswith("#")]


def _paginate(rows: list[dict[str, str]], limit: int, offset: int) -> str:
    """Apply offset/limit to *rows* and return a JSON envelope."""
    total = len(rows)
    sliced = rows[offset:] if limit == 0 else rows[offset : offset + limit]
    return json.dumps(
        {"total": total, "offset": offset, "limit": limit, "rows": sliced},
        default=str,
    )


# ---------------------------------------------------------------------------
# Snippet helpers — backend-aware code generation
# ---------------------------------------------------------------------------


def _snippet_header(backend: str) -> str:
    """Return the import + client-init block for *backend*."""
    if backend == "trino":
        return (
            "from owlbear import TrinoClient\n"
            "import polars as pl\n\n"
            "client = TrinoClient(\n"
            '    host="your_host",\n'
            "    port=443,\n"
            '    catalog="your_catalog",\n'
            '    schema="your_schema",\n'
            ")\n\n"
        )
    return (
        "from owlbear import AthenaClient\n"
        "import polars as pl\n\n"
        "client = AthenaClient(\n"
        '    database="your_database",\n'
        '    output_location="s3://your-bucket/results/",\n'
        ")\n\n"
    )


def _snippet_query(backend: str, sql: str) -> str:
    """Return the query + result lines for *backend*."""
    if backend == "trino":
        return f'df = client.query("{sql}")\n'
    return (
        f'eid = client.query("{sql}")\n'
        "df = client.results(eid)\n"
    )


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


@mcp.tool()
def execute_query(sql: str, max_rows: int = 500) -> str:
    """Execute a SQL query against the data lake and return results as JSON.

    Args:
        sql: The SQL query to execute.
        max_rows: Maximum rows to return (default 500, capped at 10 000).
    """
    try:
        df = _query_to_df(sql, max_rows)
        rows = df.to_dicts()
        if len(rows) >= _CACHE_THRESHOLD:
            df_id = _cache_df(df)
            return json.dumps({"df_id": df_id, "rows": rows}, default=str)
        return json.dumps(rows, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_databases(limit: int = 0, offset: int = 0) -> str:
    """List all databases available in the data lake.

    Args:
        limit: Maximum number of results to return (0 = no limit).
        offset: Number of results to skip before returning.
    """
    try:
        rows: list[dict[str, str]] = json.loads(
            _query_to_json("SHOW DATABASES", max_rows=_MAX_ROWS_CAP)
        )
        return _paginate(rows, limit, offset)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_tables(database: str | None = None, limit: int = 0, offset: int = 0) -> str:
    """List tables in a database (defaults to the configured database).

    Args:
        database: Optional database name. Uses the configured default if omitted.
        limit: Maximum number of results to return (0 = no limit).
        offset: Number of results to skip before returning.
    """
    try:
        sql = f"SHOW TABLES IN {database}" if database else "SHOW TABLES"
        rows: list[dict[str, str]] = json.loads(
            _query_to_json(sql, max_rows=_MAX_ROWS_CAP)
        )
        return _paginate(rows, limit, offset)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def describe_table(table: str) -> str:
    """Describe the columns and types of a table.

    Args:
        table: Fully-qualified or short table name (e.g. ``my_db.my_table``).
    """
    try:
        columns = _get_columns(table)
        return json.dumps(columns, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_schema_context(tables: str) -> str:
    """Batch DESCRIBE for multiple tables. Returns {table: [columns...]}.

    Args:
        tables: Comma-separated table names (e.g. ``db.t1, db.t2``).
    """
    table_list = [t.strip() for t in tables.split(",") if t.strip()]
    result: dict[str, list[dict[str, str]] | str] = {}
    for table in table_list:
        try:
            result[table] = _get_columns(table)
        except Exception as e:
            result[table] = f"error: {e}"
    return json.dumps(result, default=str)


@mcp.tool()
def profile_table(table: str, sample_size: int = 100, stats_sample_pct: int = 0) -> str:
    """Profile a table: schema, row count, column stats, and sample rows.

    Args:
        table: Fully-qualified or short table name.
        sample_size: Number of sample rows to return (default 100).
        stats_sample_pct: Percentage for TABLESAMPLE BERNOULLI on stats query
            (0 = full scan, 1-100 = sampled). Row count always uses full COUNT(*).
    """
    try:
        # 1. Schema
        columns = _get_columns(table)

        # 2. Row count (always exact)
        count_raw = json.loads(_query_to_json(f"SELECT COUNT(*) AS cnt FROM {table}", max_rows=1))
        row_count = count_raw[0]["cnt"] if count_raw else None

        # 3. Column stats
        stat_parts: list[str] = []
        for col in columns:
            name = col.get("col_name", col.get("column_name", ""))
            dtype = col.get("data_type", col.get("type", ""))
            quoted = f'"{name}"'
            stat_parts.append(f"COUNT(CASE WHEN {quoted} IS NULL THEN 1 END) AS \"{name}__null_count\"")
            if _is_scalar_stat_type(dtype):
                stat_parts.append(
                    f"COUNT(DISTINCT {quoted}) AS \"{name}__distinct_count\""
                )
                stat_parts.append(
                    f"CAST(MIN({quoted}) AS VARCHAR) AS \"{name}__min\""
                )
                stat_parts.append(
                    f"CAST(MAX({quoted}) AS VARCHAR) AS \"{name}__max\""
                )

        stats_sampled = 0 < stats_sample_pct <= 100
        if stat_parts:
            source = table
            if stats_sampled:
                source = f"{table} TABLESAMPLE BERNOULLI({stats_sample_pct})"
            stats_sql = f"SELECT {', '.join(stat_parts)} FROM {source}"
            stats_raw = json.loads(_query_to_json(stats_sql, max_rows=1))
            stats_row = stats_raw[0] if stats_raw else {}
        else:
            stats_row = {}

        # Reshape into per-column stats
        column_stats: list[dict[str, object]] = []
        for col in columns:
            name = col.get("col_name", col.get("column_name", ""))
            dtype = col.get("data_type", col.get("type", ""))
            entry: dict[str, object] = {
                "column": name,
                "type": dtype,
                "null_count": stats_row.get(f"{name}__null_count"),
            }
            if _is_scalar_stat_type(dtype):
                entry["distinct_count"] = stats_row.get(f"{name}__distinct_count")
                entry["min"] = stats_row.get(f"{name}__min")
                entry["max"] = stats_row.get(f"{name}__max")
            column_stats.append(entry)

        # 4. Sample rows
        sample_raw = json.loads(
            _query_to_json(f"SELECT * FROM {table} LIMIT {sample_size}", max_rows=sample_size)
        )

        return json.dumps(
            {
                "table": table,
                "row_count": row_count,
                "columns": column_stats,
                "sample_rows": sample_raw,
                "stats_sampled": stats_sampled,
                "stats_sample_pct": stats_sample_pct if stats_sampled else 0,
            },
            default=str,
        )
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def generate_snippet(table: str, operation: str) -> str:
    """Generate a Python code snippet using owlbear + Polars for a table.

    Args:
        table: Fully-qualified or short table name.
        operation: One of ``load``, ``filter``, ``aggregate``, ``join``.
    """
    valid_ops = {"load", "filter", "aggregate", "join"}
    if operation not in valid_ops:
        return json.dumps({"error": f"Unknown operation {operation!r}. Choose from: {', '.join(sorted(valid_ops))}"})

    try:
        columns = _get_columns(table)
        backend = os.environ.get("OWLBEAR_BACKEND", "athena").lower()

        col_names = [c.get("col_name", c.get("column_name", "")) for c in columns]
        col_types = [c.get("data_type", c.get("type", "")) for c in columns]

        # Pick representative columns
        numeric_col = next(
            (n for n, t in zip(col_names, col_types) if _is_scalar_stat_type(t) and re.match(r"(int|bigint|float|double|real|decimal|smallint|tinyint)", t, re.I)),
            col_names[0] if col_names else "col",
        )
        string_col = next(
            (n for n, t in zip(col_names, col_types) if re.match(r"(varchar|char|string)", t, re.I)),
            col_names[0] if col_names else "col",
        )

        header = _snippet_header(backend)

        if operation == "load":
            snippet = (
                header
                + _snippet_query(backend, f"SELECT * FROM {table} LIMIT 1000")
                + "print(df.head())\n"
            )
        elif operation == "filter":
            snippet = (
                header
                + _snippet_query(backend, f'SELECT * FROM {table} WHERE \\"{string_col}\\\\" IS NOT NULL LIMIT 1000')
                + f'filtered = df.filter(pl.col("{string_col}").is_not_null())\n'
                + "print(filtered.head())\n"
            )
        elif operation == "aggregate":
            snippet = (
                header
                + _snippet_query(backend, f"SELECT * FROM {table} LIMIT 10000")
                + f'result = df.group_by("{string_col}").agg(pl.col("{numeric_col}").mean())\n'
                + "print(result.head())\n"
            )
        else:  # join
            if backend == "trino":
                snippet = (
                    header
                    + f'df1 = client.query("SELECT * FROM {table} LIMIT 1000")\n\n'
                    + "# Load a second table to join with\n"
                    + 'df2 = client.query("SELECT * FROM other_table LIMIT 1000")\n\n'
                    + f'joined = df1.join(df2, on="{col_names[0] if col_names else "id"}", how="inner")\n'
                    + "print(joined.head())\n"
                )
            else:
                snippet = (
                    header
                    + f'eid1 = client.query("SELECT * FROM {table} LIMIT 1000")\n'
                    + "df1 = client.results(eid1)\n\n"
                    + "# Load a second table to join with\n"
                    + 'eid2 = client.query("SELECT * FROM other_table LIMIT 1000")\n'
                    + "df2 = client.results(eid2)\n\n"
                    + f'joined = df1.join(df2, on="{col_names[0] if col_names else "id"}", how="inner")\n'
                    + "print(joined.head())\n"
                )

        return json.dumps(
            {
                "table": table,
                "operation": operation,
                "backend": backend,
                "columns": col_names,
                "snippet": snippet,
            },
            default=str,
        )
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def explain_query(sql: str) -> str:
    """Run EXPLAIN on a SQL query and return the query plan.

    Args:
        sql: The SQL query to explain.
    """
    try:
        return _query_to_json(f"EXPLAIN {sql}", max_rows=_MAX_ROWS_CAP)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def search_tables(
    pattern: str,
    database: str | None = None,
    limit: int = 0,
    offset: int = 0,
) -> str:
    """Search for tables matching a SQL LIKE pattern (use ``%`` as wildcard).

    Args:
        pattern: SQL LIKE pattern (e.g. ``%order%`` to find all tables containing "order").
        database: Optional database name. Uses the configured default if omitted.
        limit: Maximum number of results to return (0 = no limit).
        offset: Number of results to skip before returning.
    """
    try:
        if database:
            sql = f"SHOW TABLES IN {database} LIKE '{pattern}'"
        else:
            sql = f"SHOW TABLES LIKE '{pattern}'"
        rows: list[dict[str, str]] = json.loads(
            _query_to_json(sql, max_rows=_MAX_ROWS_CAP)
        )
        return _paginate(rows, limit, offset)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def show_partitions(table: str, limit: int = 0, offset: int = 0) -> str:
    """Show partition keys and values for a partitioned table.

    Args:
        table: Fully-qualified or short table name (e.g. ``my_db.my_table``).
        limit: Maximum number of results to return (0 = no limit).
        offset: Number of results to skip before returning.
    """
    try:
        client = _get_client()
        if isinstance(client, TrinoClient):
            parts = table.split(".")
            if len(parts) == 2:
                schema, tbl = parts
                sql = f'SELECT * FROM "{schema}"."{tbl}$partitions"'
            else:
                sql = f'SELECT * FROM "{table}$partitions"'
        else:
            sql = f"SHOW PARTITIONS {table}"
        rows: list[dict[str, str]] = json.loads(
            _query_to_json(sql, max_rows=_MAX_ROWS_CAP)
        )
        return _paginate(rows, limit, offset)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# DataFrame cache tools — housekeeping & exploration
# ---------------------------------------------------------------------------


@mcp.tool()
def df_list() -> str:
    """List cached DataFrames with shape info (rows, cols, column names)."""
    entries = []
    for df_id, df in _dataframes.items():
        entries.append({
            "df_id": df_id,
            "rows": df.height,
            "cols": df.width,
            "columns": df.columns,
        })
    return json.dumps(entries, default=str)


@mcp.tool()
def df_drop(df_id: str) -> str:
    """Drop a cached DataFrame to free memory.

    Args:
        df_id: The DataFrame ID to drop (e.g. ``df_1``).
    """
    if df_id not in _dataframes:
        return json.dumps({"error": f"Unknown DataFrame: {df_id}"})
    del _dataframes[df_id]
    return json.dumps({"dropped": df_id})


@mcp.tool()
def df_head(df_id: str, n: int = 10) -> str:
    """Return the first N rows of a cached DataFrame as JSON.

    Args:
        df_id: The DataFrame ID (e.g. ``df_1``).
        n: Number of rows to return (default 10).
    """
    try:
        df = _get_df(df_id)
        return json.dumps(df.head(n).to_dicts(), default=str)
    except KeyError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def df_describe(df_id: str) -> str:
    """Return summary statistics (``df.describe()``) for a cached DataFrame.

    Args:
        df_id: The DataFrame ID (e.g. ``df_1``).
    """
    try:
        df = _get_df(df_id)
        return json.dumps(df.describe().to_dicts(), default=str)
    except KeyError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def df_schema(df_id: str) -> str:
    """Return column names and dtypes for a cached DataFrame.

    Args:
        df_id: The DataFrame ID (e.g. ``df_1``).
    """
    try:
        df = _get_df(df_id)
        schema = [{"column": name, "dtype": str(dtype)} for name, dtype in df.schema.items()]
        return json.dumps(schema, default=str)
    except KeyError as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# DataFrame cache tools — transformations (return new cached frames)
# ---------------------------------------------------------------------------


_FILTER_OPS = {"=", "!=", ">", "<", ">=", "<=", "is_null", "is_not_null", "contains"}


def _coerce_value(dtype: pl.DataType, value: str) -> object:
    """Best-effort cast of a string *value* to match *dtype*."""
    if dtype.is_integer():
        return int(value)
    if dtype.is_float():
        return float(value)
    if dtype == pl.Boolean:
        return value.lower() in ("true", "1", "yes")
    return value


@mcp.tool()
def df_filter(df_id: str, column: str, op: str, value: str = "") -> str:
    """Filter rows of a cached DataFrame and cache the result.

    Args:
        df_id: Source DataFrame ID.
        column: Column name to filter on.
        op: Operator — one of ``=``, ``!=``, ``>``, ``<``, ``>=``, ``<=``,
            ``is_null``, ``is_not_null``, ``contains``.
        value: Comparison value (ignored for is_null / is_not_null).
    """
    if op not in _FILTER_OPS:
        return json.dumps({"error": f"Unknown op {op!r}. Choose from: {', '.join(sorted(_FILTER_OPS))}"})
    try:
        df = _get_df(df_id)
        col = pl.col(column)
        if op == "is_null":
            expr = col.is_null()
        elif op == "is_not_null":
            expr = col.is_not_null()
        elif op == "contains":
            expr = col.cast(pl.Utf8).str.contains(value)
        else:
            coerced = _coerce_value(df.schema[column], value)
            ops = {
                "=": col.__eq__,
                "!=": col.__ne__,
                ">": col.__gt__,
                "<": col.__lt__,
                ">=": col.__ge__,
                "<=": col.__le__,
            }
            expr = ops[op](coerced)
        result = df.filter(expr)
        new_id = _cache_df(result)
        return json.dumps({"df_id": new_id, "rows": result.to_dicts()}, default=str)
    except KeyError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def df_select(df_id: str, columns: list[str]) -> str:
    """Select / reorder columns of a cached DataFrame and cache the result.

    Args:
        df_id: Source DataFrame ID.
        columns: List of column names to keep.
    """
    try:
        df = _get_df(df_id)
        result = df.select(columns)
        new_id = _cache_df(result)
        return json.dumps({"df_id": new_id, "rows": result.to_dicts()}, default=str)
    except KeyError as e:
        return json.dumps({"error": str(e)})


_AGG_FUNCS = {"sum", "mean", "count", "min", "max", "median", "first", "last"}


@mcp.tool()
def df_group_by(df_id: str, by: list[str], agg_column: str, agg_func: str) -> str:
    """Group a cached DataFrame and aggregate, caching the result.

    Args:
        df_id: Source DataFrame ID.
        by: Column(s) to group by.
        agg_column: Column to aggregate.
        agg_func: Aggregation function — one of ``sum``, ``mean``, ``count``,
            ``min``, ``max``, ``median``, ``first``, ``last``.
    """
    if agg_func not in _AGG_FUNCS:
        return json.dumps({"error": f"Unknown agg_func {agg_func!r}. Choose from: {', '.join(sorted(_AGG_FUNCS))}"})
    try:
        df = _get_df(df_id)
        agg_expr = getattr(pl.col(agg_column), agg_func)()
        result = df.group_by(by).agg(agg_expr).sort(by)
        new_id = _cache_df(result)
        return json.dumps({"df_id": new_id, "rows": result.to_dicts()}, default=str)
    except KeyError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def df_sort(df_id: str, by: list[str], descending: bool = False) -> str:
    """Sort a cached DataFrame and cache the result.

    Args:
        df_id: Source DataFrame ID.
        by: Column(s) to sort by.
        descending: Sort descending (default False).
    """
    try:
        df = _get_df(df_id)
        result = df.sort(by, descending=descending)
        new_id = _cache_df(result)
        return json.dumps({"df_id": new_id, "rows": result.to_dicts()}, default=str)
    except KeyError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def df_value_counts(df_id: str, column: str) -> str:
    """Frequency distribution of a column, cached as a new DataFrame.

    Args:
        df_id: Source DataFrame ID.
        column: Column to count values for.
    """
    try:
        df = _get_df(df_id)
        result = df.get_column(column).value_counts(sort=True)
        new_id = _cache_df(result)
        return json.dumps({"df_id": new_id, "rows": result.to_dicts()}, default=str)
    except KeyError as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# MCP prompts
# ---------------------------------------------------------------------------


@mcp.prompt()
def explore_table(table: str) -> list[dict[str, str]]:
    """Guided exploration of a data-lake table.

    Args:
        table: The table to explore.
    """
    return [
        {
            "role": "user",
            "content": (
                f"I'd like to explore the table **{table}**. Please:\n\n"
                f"1. Run `DESCRIBE {table}` to show the schema.\n"
                f"2. Sample a few rows with `SELECT * FROM {table} LIMIT 5`.\n"
                "3. Suggest 3 interesting analytical queries based on the columns.\n"
                "4. Summarize what this table appears to contain and how it could be used."
            ),
        }
    ]


@mcp.prompt()
def build_pipeline(table: str, goal: str) -> list[dict[str, str]]:
    """Generate a data pipeline using owlbear and Polars.

    Args:
        table: The source table.
        goal: What the pipeline should accomplish.
    """
    return [
        {
            "role": "user",
            "content": (
                f"Help me build a data pipeline for **{table}** to accomplish: {goal}\n\n"
                "Start from this boilerplate:\n\n"
                "```python\n"
                "from owlbear import AthenaClient\n"
                "import polars as pl\n\n"
                "client = AthenaClient(\n"
                '    database="your_database",\n'
                '    output_location="s3://your-bucket/results/",\n'
                ")\n\n"
                f'eid = client.query("SELECT * FROM {table} LIMIT 1000")\n'
                "df = client.results(eid)\n"
                "```\n\n"
                "Adapt this code to achieve the goal. Use Polars for transformations."
            ),
        }
    ]


# ---------------------------------------------------------------------------
# MCP resource
# ---------------------------------------------------------------------------


@mcp.resource("owlbear://config")
def get_config() -> str:
    """Expose the current owlbear backend configuration."""
    backend = os.environ.get("OWLBEAR_BACKEND", "athena").lower()
    config: dict[str, object] = {"backend": backend}

    if backend == "athena":
        config["database"] = os.environ.get("OWLBEAR_DATABASE", "default")
        config["region"] = os.environ.get("AWS_REGION", "us-east-1")
        config["s3_output_location"] = os.environ.get("OWLBEAR_S3_OUTPUT_LOCATION", "")
    elif backend == "trino":
        config["host"] = os.environ.get("OWLBEAR_TRINO_HOST", "")
        config["port"] = int(os.environ.get("OWLBEAR_TRINO_PORT", "443"))
        config["user"] = os.environ.get("OWLBEAR_TRINO_USER", "")
        config["catalog"] = os.environ.get("OWLBEAR_TRINO_CATALOG", "")
        config["database"] = os.environ.get("OWLBEAR_DATABASE", "")

    return json.dumps(config)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
