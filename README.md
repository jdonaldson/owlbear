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

## Installation

```bash
# With Athena backend
pip install "owlbear[athena]"

# With Trino backend
pip install "owlbear[trino]"

# Both backends
pip install "owlbear[all]"
```

### For Development

```bash
git clone https://github.com/jdonaldson/owlbear.git
cd owlbear
pip install -e ".[dev]"
```

## Prerequisites

- Python 3.9+
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
