"""Owlbear MCP server — expose data-lake queries to AI assistants."""

from __future__ import annotations

import json
import os
import re
from typing import Union

from mcp.server.fastmcp import FastMCP

from .athena import AthenaClient
from .trino import TrinoClient

mcp = FastMCP("owlbear")

_MAX_ROWS_CAP = 10_000

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
# Shared helpers
# ---------------------------------------------------------------------------


def _query_to_json(sql: str, max_rows: int) -> str:
    """Run *sql* via the active backend and return JSON-serialised rows."""
    max_rows = min(max_rows, _MAX_ROWS_CAP)
    client = _get_client()

    if isinstance(client, AthenaClient):
        execution_id = client.query(sql)
        df = client.results(execution_id, max_rows=max_rows)
    else:
        df = client.query(sql, max_rows=max_rows)

    return json.dumps(df.to_dicts(), default=str)


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
        return _query_to_json(sql, max_rows)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_databases() -> str:
    """List all databases available in the data lake."""
    try:
        return _query_to_json("SHOW DATABASES", max_rows=_MAX_ROWS_CAP)
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_tables(database: str | None = None) -> str:
    """List tables in a database (defaults to the configured database).

    Args:
        database: Optional database name. Uses the configured default if omitted.
    """
    try:
        sql = f"SHOW TABLES IN {database}" if database else "SHOW TABLES"
        return _query_to_json(sql, max_rows=_MAX_ROWS_CAP)
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
def profile_table(table: str, sample_size: int = 100) -> str:
    """Profile a table: schema, row count, column stats, and sample rows.

    Args:
        table: Fully-qualified or short table name.
        sample_size: Number of sample rows to return (default 100).
    """
    try:
        # 1. Schema
        columns = _get_columns(table)

        # 2. Row count
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

        if stat_parts:
            stats_sql = f"SELECT {', '.join(stat_parts)} FROM {table}"
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

        header = (
            "from owlbear import AthenaClient\n"
            "import polars as pl\n\n"
            "client = AthenaClient(\n"
            '    database="your_database",\n'
            '    output_location="s3://your-bucket/results/",\n'
            ")\n\n"
        )

        if operation == "load":
            snippet = (
                header
                + f'eid = client.query("SELECT * FROM {table} LIMIT 1000")\n'
                + "df = client.results(eid)\n"
                + "print(df.head())\n"
            )
        elif operation == "filter":
            snippet = (
                header
                + f'eid = client.query("SELECT * FROM {table} WHERE \\"{string_col}\\\\" IS NOT NULL LIMIT 1000")\n'
                + "df = client.results(eid)\n"
                + f'filtered = df.filter(pl.col("{string_col}").is_not_null())\n'
                + "print(filtered.head())\n"
            )
        elif operation == "aggregate":
            snippet = (
                header
                + f'eid = client.query("SELECT * FROM {table} LIMIT 10000")\n'
                + "df = client.results(eid)\n"
                + f'result = df.group_by("{string_col}").agg(pl.col("{numeric_col}").mean())\n'
                + "print(result.head())\n"
            )
        else:  # join
            snippet = (
                header
                + f'eid1 = client.query("SELECT * FROM {table} LIMIT 1000")\n'
                + "df1 = client.results(eid1)\n\n"
                + '# Load a second table to join with\n'
                + 'eid2 = client.query("SELECT * FROM other_table LIMIT 1000")\n'
                + "df2 = client.results(eid2)\n\n"
                + f'joined = df1.join(df2, on="{col_names[0] if col_names else "id"}", how="inner")\n'
                + "print(joined.head())\n"
            )

        return json.dumps(
            {
                "table": table,
                "operation": operation,
                "columns": col_names,
                "snippet": snippet,
            },
            default=str,
        )
    except Exception as e:
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
