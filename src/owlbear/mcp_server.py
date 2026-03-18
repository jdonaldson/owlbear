"""Owlbear MCP server — expose data-lake queries to AI assistants."""

from __future__ import annotations

import json
import os
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
# Shared helper
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
        return _query_to_json(f"DESCRIBE {table}", max_rows=_MAX_ROWS_CAP)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
