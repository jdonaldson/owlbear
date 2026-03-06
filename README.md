# owlbear

<img src="owlbear.png" width="200" align="right" />

**Feathers and claws for your data lake.**

Owlbear is a Python client that connects AWS Athena to Polars DataFrames. A wise chimera — part **Owl** ([Athena](https://aws.amazon.com/athena/), goddess of wisdom), part **Bear** ([Polars](https://pola.rs/), the bear constellation). Query your data lake with SQL, get back fast, typed DataFrames — no serialization or ODBC overhead.

## Features

- Execute Athena queries with automatic result retrieval as Polars DataFrames
- Support for query execution with work groups and custom configurations
- Pagination support for large result sets
- Comprehensive error handling and timeout management
- Query cancellation and execution monitoring
- Built-in retry logic with exponential backoff

## Installation

### From GitHub (Git)

```bash
pip install git+https://github.com/jdonaldson/owlbear.git
```

### For Development

```bash
git clone https://github.com/jdonaldson/owlbear.git
cd owlbear
pip install -e ".[dev]"
```

## Prerequisites

- Python 3.8+
- AWS credentials configured (via AWS CLI, environment variables, or IAM roles)
- Access to AWS Athena and an S3 bucket for query results

## Quick Start

```python
import polars as pl
from owlbear import OwlbearClient

# Initialize the client
client = OwlbearClient(
    database="my_database",
    output_location="s3://my-bucket/athena-results/",
    region="us-east-1"
)

# Execute a query and get results as a Polars DataFrame
query = "SELECT * FROM my_table LIMIT 100"
execution_id = client.execute_query(query)
df = client.get_results_polars(execution_id)

print(df.head())
```

## Usage Examples

### Basic Query Execution

```python
from owlbear import OwlbearClient

# Initialize client
client = OwlbearClient(
    database="analytics_db",
    output_location="s3://my-athena-results/queries/",
    region="us-west-2"
)

# Execute query with automatic waiting
query = """
SELECT
    customer_id,
    SUM(order_amount) as total_spent,
    COUNT(*) as order_count
FROM orders
WHERE order_date >= '2024-01-01'
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 50
"""

execution_id = client.execute_query(query, wait_for_completion=True)
results_df = client.get_results_polars(execution_id)

# Use Polars operations
top_customers = results_df.filter(pl.col("total_spent") > 1000)
print(f"Found {len(top_customers)} high-value customers")
```

### Asynchronous Query Execution

```python
# Start query without waiting
execution_id = client.execute_query(
    "SELECT * FROM large_table",
    wait_for_completion=False
)

# Check query status
query_info = client.get_query_info(execution_id)
print(f"Query status: {query_info['Status']['State']}")

# Wait for completion and get results when ready
client._wait_for_completion(execution_id)
df = client.get_results_polars(execution_id)
```

### Using Work Groups

```python
# Execute query with a specific work group
execution_id = client.execute_query(
    query="SELECT COUNT(*) FROM my_table",
    work_group="my-workgroup"
)
df = client.get_results_polars(execution_id)
```

### Handling Large Result Sets

```python
# Get results with pagination (limit to 5000 rows)
df = client.get_results_polars(execution_id, max_rows=5000)

# For larger datasets, consider using LIMIT in your SQL query
# or processing results in chunks
```

### Using with Existing boto3 Session

```python
import boto3
from owlbear import OwlbearClient

# Use existing session (useful for custom credential handling)
session = boto3.Session(profile_name='my-profile')
client = OwlbearClient.from_session(
    session=session,
    database="my_db",
    output_location="s3://my-bucket/results/"
)

# Or with custom config
from botocore.config import Config

config = Config(
    region_name='eu-west-1',
    retries={'max_attempts': 5}
)

client = OwlbearClient(
    database="my_db",
    output_location="s3://my-bucket/results/",
    config=config
)
```

### Query Management

```python
# List available work groups
work_groups = client.list_work_groups()
print(f"Available work groups: {work_groups}")

# Cancel a running query
client.cancel_query(execution_id)

# Get detailed query information
query_info = client.get_query_info(execution_id)
print(f"Query execution time: {query_info['Statistics']['TotalExecutionTimeInMillis']}ms")
print(f"Data processed: {query_info['Statistics']['DataProcessedInBytes']} bytes")
```

### Error Handling

```python
try:
    execution_id = client.execute_query("SELECT * FROM non_existent_table")
    df = client.get_results_polars(execution_id)
except Exception as e:
    if "Query failed" in str(e):
        print(f"Query execution failed: {e}")
    elif "timeout" in str(e).lower():
        print(f"Query timed out: {e}")
    else:
        print(f"Unexpected error: {e}")
```

## Advanced Usage

### Custom Query Context

```python
execution_id = client.execute_query(
    query="SELECT * FROM my_table",
    query_context={"Catalog": "my_catalog"},
    result_config={"EncryptionConfiguration": {"EncryptionOption": "SSE_S3"}}
)
```

### Working with Different Data Types

The library automatically handles various Athena data types using PyArrow for proper type inference:

```python
# Data types are automatically inferred and converted
df = client.get_results_polars(execution_id)

# Check the inferred types
print(df.dtypes)  # [Int32, Utf8, Float64, Boolean, Date32, etc.]

# No manual casting needed for basic types, but you can still cast if needed
df_modified = df.with_columns([
    pl.col("timestamp_col").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
])
```

## Configuration

### Environment Variables

You can configure AWS credentials using standard environment variables:

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

Run the test suite:

```bash
pytest tests/ -v
```

Run tests with coverage:

```bash
pytest tests/ --cov=src --cov-report=html
```

## Development

### Setup Development Environment

```bash
git clone https://github.com/jdonaldson/owlbear.git
cd owlbear
pip install -e ".[dev]"
```

### Code Quality

Format code:
```bash
black .
```

Lint code:
```bash
ruff check .
```

Type checking:
```bash
mypy src/
```

## License

MIT License - see LICENSE file for details.

## Contributing

1. Fork the repository on GitHub
2. Create a feature branch
3. Make your changes with tests
4. Ensure all tests pass and code is formatted
5. Submit a pull request

## Changelog

### v0.1.0 (2024-08-28)
- Initial release
- `OwlbearClient` for executing Athena SQL and returning typed Polars DataFrames via PyArrow
- Automatic Athena-to-PyArrow type mapping (integers, floats, decimals, timestamps, booleans, arrays, maps)
- Paginated result retrieval with configurable row limits
- Async query execution with exponential-backoff polling
- Work group support, query cancellation, and execution monitoring
