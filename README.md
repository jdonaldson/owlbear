# owlbear

<img src="owlbear_framed.png" width="150" align="right" alt="Owlbear" />

**Feathers and claws for your data lake.**

Owlbear is a Python client that bridges **Athena** and **Trino** to **Polars** DataFrames via PyArrow. A wise chimera — part **Owl** ([Athena](https://aws.amazon.com/athena/), goddess of wisdom), part **Bear** ([Polars](https://pola.rs/), the bear constellation). Query your data lake with SQL, get back fast, typed DataFrames — no serialization or ODBC overhead.

## Features

- **Two backends**: `AthenaClient` (AWS Athena via boto3) and `TrinoClient` (direct Trino connection)
- Shared Presto-family type conversion — both backends produce identically typed Polars DataFrames
- Parameterized queries for safe value binding
- Athena result reuse for repeated queries
- Pagination support for large result sets (Athena) and row limits (both)
- Query cancellation and execution monitoring (Athena)
- Built-in retry logic with exponential backoff (Athena)
- **MCP server** for AI assistant integration (schema discovery + query execution)

## Installation

```bash
# With Athena backend
pip install "owlbear[athena]"

# With Trino backend
pip install "owlbear[trino]"

# Both backends
pip install "owlbear[all]"

# MCP server (includes both backends)
pip install "owlbear[mcp]"
```

### For Development

```bash
git clone https://github.com/jdonaldson/owlbear.git
cd owlbear
pip install -e ".[dev]"
```

## Prerequisites

- Python 3.10+
- **Athena**: AWS credentials configured (via AWS CLI, environment variables, or IAM roles) and an S3 bucket for query results
- **Trino**: A running Trino cluster with network access

## Quick Start

### Athena

```python
from owlbear import AthenaClient

client = AthenaClient(
    database="my_database",
    output_location="s3://my-bucket/athena-results/",
    region="us-east-1"
)

execution_id = client.query("SELECT * FROM orders LIMIT 5")
df = client.results(execution_id)
print(df)
```

```
shape: (5, 4)
┌─────────────┬────────────┬──────────────┬────────────┐
│ customer_id ┆ order_date ┆ order_amount ┆ status     │
│ ---         ┆ ---        ┆ ---          ┆ ---        │
│ i64         ┆ date       ┆ f64          ┆ str        │
╞═════════════╪════════════╪══════════════╪════════════╡
│ 1001        ┆ 2024-03-15 ┆ 249.99       ┆ shipped    │
│ 1002        ┆ 2024-03-15 ┆ 89.50        ┆ delivered  │
│ 1003        ┆ 2024-03-16 ┆ 1024.00      ┆ processing │
│ 1001        ┆ 2024-03-17 ┆ 54.25        ┆ shipped    │
│ 1004        ┆ 2024-03-17 ┆ 399.99       ┆ delivered  │
└─────────────┴────────────┴──────────────┴────────────┘
```

### Trino

```python
from owlbear import TrinoClient

client = TrinoClient(
    host="trino.example.com",
    port=443,
    user="analyst",
    catalog="hive",
    schema="default",
)

df = client.query("SELECT * FROM orders LIMIT 5")
print(df)
```

```
shape: (5, 4)
┌─────────────┬────────────┬──────────────┬────────────┐
│ customer_id ┆ order_date ┆ order_amount ┆ status     │
│ ---         ┆ ---        ┆ ---          ┆ ---        │
│ i64         ┆ date       ┆ f64          ┆ str        │
╞═════════════╪════════════╪══════════════╪════════════╡
│ 1001        ┆ 2024-03-15 ┆ 249.99       ┆ shipped    │
│ 1002        ┆ 2024-03-15 ┆ 89.50        ┆ delivered  │
│ 1003        ┆ 2024-03-16 ┆ 1024.00      ┆ processing │
│ 1001        ┆ 2024-03-17 ┆ 54.25        ┆ shipped    │
│ 1004        ┆ 2024-03-17 ┆ 399.99       ┆ delivered  │
└─────────────┴────────────┴──────────────┴────────────┘
```

## Usage Examples

### Parameterized Queries

```python
# Athena — parameters are passed as strings
execution_id = client.query(
    "SELECT * FROM orders WHERE customer_id = ?",
    parameters=["1001"],
)
df = client.results(execution_id)

# Trino — parameters are passed as native Python values
df = trino_client.query(
    "SELECT * FROM orders WHERE customer_id = ?",
    parameters=[1001],
)
```

### Result Reuse (Athena)

```python
# Re-use cached results for up to 60 minutes
execution_id = client.query(
    "SELECT COUNT(*) FROM orders",
    result_reuse_max_age=60,
)
```

### Asynchronous Query Execution

```python
# Start query without waiting
execution_id = client.query(
    "SELECT * FROM large_table",
    wait_for_completion=False
)

# Check query status
query_info = client.get_query_info(execution_id)
print(f"Query status: {query_info['Status']['State']}")

# Wait for completion and get results when ready
client._wait_for_completion(execution_id)
df = client.results(execution_id)
```

### Using Work Groups

```python
execution_id = client.query(
    query="SELECT COUNT(*) FROM my_table",
    work_group="my-workgroup"
)
df = client.results(execution_id)
```

### Using with Existing boto3 Session

```python
import boto3
from owlbear import AthenaClient

session = boto3.Session(profile_name='my-profile')
client = AthenaClient.from_session(
    session=session,
    database="my_db",
    output_location="s3://my-bucket/results/"
)
```

### Query Management

```python
# List available work groups
work_groups = client.list_work_groups()

# Cancel a running query
client.cancel_query(execution_id)

# Get detailed query information
query_info = client.get_query_info(execution_id)
print(f"Execution time: {query_info['Statistics']['TotalExecutionTimeInMillis']}ms")
print(f"Data processed: {query_info['Statistics']['DataProcessedInBytes']} bytes")
```

## MCP Server

Owlbear includes an [MCP](https://modelcontextprotocol.io/) server so AI assistants can query your data lake directly.

### Install

```bash
pip install "owlbear[mcp]"
```

### Tools

| Tool | Description |
|---|---|
| `execute_query(sql, max_rows=500)` | Run arbitrary SQL, return JSON rows. Results with 50+ rows are auto-cached and include a `df_id` for further operations. Set `OWLBEAR_MAX_ROWS` to cap results (0 = no cap, default) |
| `explain_query(sql)` | Run `EXPLAIN` on a query and return the query plan |
| `list_databases(limit=0, offset=0)` | List all available databases (paginated) |
| `list_tables(database?, limit=0, offset=0)` | List tables in a database (paginated, defaults to configured database) |
| `search_tables(pattern, database?, limit=0, offset=0)` | Search for tables matching a SQL `LIKE` pattern (`%` wildcard) |
| `describe_table(table)` | Show columns and types for a table |
| `get_schema_context(tables)` | Batch describe multiple comma-separated tables at once |
| `profile_table(table, sample_size=100, stats_sample_pct=0)` | One-call profiling: schema, row count, column stats, sample rows. Use `stats_sample_pct` (1-100) to sample stats on large tables |
| `generate_snippet(table, operation)` | Generate backend-aware owlbear + Polars Python code (`load`, `filter`, `aggregate`, `join`) |
| `show_partitions(table, limit=0, offset=0)` | Show partition keys and values for a partitioned table |

#### DataFrame Cache Tools

Results with 50+ rows from `execute_query` are automatically cached in server memory. These tools let you explore and transform cached DataFrames without re-querying the data lake.

| Tool | Description |
|---|---|
| `df_list()` | List cached DataFrames with shape info (rows, cols, column names) |
| `df_drop(df_id)` | Drop a cached DataFrame to free memory |
| `df_head(df_id, n=10)` | First N rows of a cached DataFrame as JSON |
| `df_describe(df_id)` | Summary statistics (`df.describe()`) |
| `df_schema(df_id)` | Column names and Polars dtypes |
| `df_filter(df_id, column, op, value)` | Filter rows (`=`, `!=`, `>`, `<`, `>=`, `<=`, `is_null`, `is_not_null`, `contains`). Returns new cached frame |
| `df_select(df_id, columns)` | Select/reorder columns. Returns new cached frame |
| `df_group_by(df_id, by, agg_column, agg_func)` | Group + aggregate (`sum`, `mean`, `count`, `min`, `max`, `median`, `first`, `last`). Returns new cached frame |
| `df_sort(df_id, by, descending=False)` | Sort rows. Returns new cached frame |
| `df_value_counts(df_id, column)` | Frequency distribution. Returns new cached frame |

### Prompts

| Prompt | Description |
|---|---|
| `explore_table(table)` | Guided exploration: describe schema, sample rows, suggest queries, summarize |
| `build_pipeline(table, goal)` | Generate a data pipeline with owlbear boilerplate for a stated goal |

### Resource

| URI | Description |
|---|---|
| `owlbear://config` | Current backend configuration (type, database, region/host) |

### Example: Profile a Table

Ask your AI assistant:

> Profile the `orders` table

The assistant calls `profile_table("orders")` and gets back:

```json
{
  "table": "orders",
  "row_count": 156398,
  "columns": [
    {"column": "order_id", "type": "bigint", "null_count": 0, "distinct_count": 156398, "min": "1", "max": "156398"},
    {"column": "customer_id", "type": "bigint", "null_count": 0, "distinct_count": 8423, "min": "1001", "max": "9999"},
    {"column": "order_date", "type": "date", "null_count": 0, "distinct_count": 731, "min": "2023-01-01", "max": "2024-12-31"},
    {"column": "amount", "type": "double", "null_count": 12, "distinct_count": 45210, "min": "0.99", "max": "9999.99"},
    {"column": "status", "type": "varchar", "null_count": 0, "distinct_count": 4, "min": "cancelled", "max": "shipped"},
    {"column": "items", "type": "array<varchar>", "null_count": 85}
  ],
  "sample_rows": [{"order_id": 1, "customer_id": 1001, "order_date": "2023-01-01", "amount": 249.99, "status": "shipped", "items": ["widget-a", "widget-b"]}],
  "stats_sampled": false,
  "stats_sample_pct": 0
}
```

Scalar columns (numbers, strings, dates) get `distinct_count`, `min`, and `max`. Complex columns (arrays, maps, structs) get only `null_count`.

For large tables, use `stats_sample_pct` to avoid full table scans on the stats query:

> Profile the orders table with 10% sampling

The assistant calls `profile_table("orders", stats_sample_pct=10)` — the stats query uses `TABLESAMPLE BERNOULLI(10)` while row count remains exact via `COUNT(*)`.

### Example: Batch Schema Lookup

> What columns do the orders and customers tables have?

The assistant calls `get_schema_context("mydb.orders, mydb.customers")` and gets schemas for both tables in one response — with per-table error isolation if one fails.

### Example: Generate Starter Code

> Generate an aggregation snippet for the orders table

The assistant calls `generate_snippet("orders", "aggregate")` and returns backend-aware Python code using real column names. With `OWLBEAR_BACKEND=athena`:

```python
from owlbear import AthenaClient
import polars as pl

client = AthenaClient(
    database="your_database",
    output_location="s3://your-bucket/results/",
)

eid = client.query("SELECT * FROM orders LIMIT 10000")
df = client.results(eid)
result = df.group_by("status").agg(pl.col("amount").mean())
print(result.head())
```

With `OWLBEAR_BACKEND=trino`, the snippet uses `TrinoClient` and the simpler `client.query()` → DataFrame API (no `execution_id` / `client.results()`).

### Example: Search for Tables

> Find all tables with "order" in the name

The assistant calls `search_tables("%order%")` and gets back a paginated list of matching tables — useful for discovering tables in large data lakes without knowing exact names.

### Example: Show Partitions

> What partitions does the events table have?

The assistant calls `show_partitions("analytics.events")` and gets back partition key/value pairs. On Athena this runs `SHOW PARTITIONS`, on Trino it queries the `$partitions` metadata table. Knowing the partition structure helps write cost-efficient queries that avoid full table scans.

### Example: Iterative DataFrame Exploration

> Show me all orders

The assistant calls `execute_query("SELECT * FROM orders")`. Since there are 500+ rows, the result is auto-cached:

```json
{"df_id": "df_1", "rows": [...]}
```

> What does the data look like?

The assistant calls `df_describe("df_1")` to get summary statistics — no re-query needed.

> Filter to just shipped orders and show the top 10 by amount

The assistant chains two operations:

1. `df_filter("df_1", "status", "=", "shipped")` → returns `{"df_id": "df_2", "rows": [...]}`
2. `df_sort("df_2", ["amount"], descending=True)` → returns `{"df_id": "df_3", "rows": [...]}`
3. `df_head("df_3", n=10)` → first 10 rows

> What's the average order amount by status?

`df_group_by("df_1", ["status"], "amount", "mean")` → returns `{"df_id": "df_4", "rows": [...]}`

Each transformation creates a new cached frame. Use `df_list()` to see all cached frames and `df_drop(df_id)` to free memory.

### Environment Variables

| Variable | Backend | Description | Default |
|---|---|---|---|
| `OWLBEAR_BACKEND` | both | `athena` or `trino` | `athena` |
| `OWLBEAR_DATABASE` | both | Default database/schema | `default` |
| `OWLBEAR_S3_OUTPUT_LOCATION` | athena | S3 path for query results | (required) |
| `AWS_REGION` | athena | AWS region | `us-east-1` |
| `AWS_PROFILE` | athena | AWS profile (via boto3) | — |
| `OWLBEAR_TRINO_HOST` | trino | Trino hostname | (required) |
| `OWLBEAR_TRINO_PORT` | trino | Trino port | `443` |
| `OWLBEAR_TRINO_USER` | trino | Trino user | — |
| `OWLBEAR_TRINO_CATALOG` | trino | Trino catalog | — |
| `OWLBEAR_MAX_ROWS` | both | Cap on `max_rows` for `execute_query` (0 = no cap) | `0` |

### Example `.mcp.json` (Athena)

```json
{
  "mcpServers": {
    "owlbear": {
      "command": "owlbear-mcp",
      "env": {
        "OWLBEAR_DATABASE": "my_database",
        "OWLBEAR_S3_OUTPUT_LOCATION": "s3://my-bucket/athena-results/",
        "AWS_REGION": "us-east-1",
        "AWS_PROFILE": "my-profile"
      }
    }
  }
}
```

### Example `.mcp.json` (Trino)

```json
{
  "mcpServers": {
    "owlbear": {
      "command": "owlbear-mcp",
      "env": {
        "OWLBEAR_BACKEND": "trino",
        "OWLBEAR_TRINO_HOST": "trino.example.com",
        "OWLBEAR_TRINO_PORT": "443",
        "OWLBEAR_TRINO_USER": "analyst",
        "OWLBEAR_TRINO_CATALOG": "hive",
        "OWLBEAR_DATABASE": "default"
      }
    }
  }
}
```

## Type Mapping

Owlbear automatically converts Presto/Trino/Athena SQL types to PyArrow (and then to Polars):

| SQL Type | PyArrow Type |
|---|---|
| `boolean` | `bool_()` |
| `tinyint` | `int8()` |
| `smallint` | `int16()` |
| `integer` | `int32()` |
| `bigint` | `int64()` |
| `real` / `float` | `float32()` |
| `double` | `float64()` |
| `decimal(p,s)` | `decimal128(p, s)` |
| `varchar` / `char` / `string` | `string()` |
| `varbinary` / `binary` | `binary()` |
| `date` | `date32()` |
| `timestamp` | `timestamp("us")` |
| `timestamp with time zone` | `timestamp("us", tz="UTC")` |
| `time` | `time64("us")` |
| `interval day to second` | `duration("us")` |
| `interval year to month` | `month_day_nano_interval()` |
| `array<T>` | `list_(T)` |
| `map<K,V>` | `map_(K, V)` |

Nested types like `array<array<integer>>` and `map<varchar,array<bigint>>` are fully supported.

## Configuration

### Environment Variables

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### IAM Permissions

Your AWS credentials need the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
                "athena:ListWorkGroups"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Resource": "arn:aws:s3:::your-athena-results-bucket/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetTable",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        }
    ]
}
```

## Testing

```bash
pytest tests/ -v
```

## Development

```bash
git clone https://github.com/jdonaldson/owlbear.git
cd owlbear
pip install -e ".[dev]"

black .        # format
ruff check .   # lint
mypy src/      # type check
```

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository on GitHub
2. Create a feature branch
3. Make your changes with tests
4. Ensure all tests pass and code is formatted
5. Submit a pull request
