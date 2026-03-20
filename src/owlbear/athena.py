#!/usr/bin/env python3
"""AthenaClient — execute Athena SQL, get typed Polars DataFrames."""

import io

import boto3
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import time
from typing import Iterator, Optional, Any, cast
from botocore.config import Config

from .types import presto_type_to_pyarrow


class AthenaClient:
    def __init__(
        self,
        database: str,
        output_location: str,
        region: str = "us-east-1",
        session: Optional[boto3.Session] = None,
        config: Optional[Config] = None,
        **client_kwargs,
    ):
        # Store init params for lazy S3 client creation
        self._session = session
        self._region = region
        self._config = config
        self._client_kwargs = client_kwargs
        self._s3_client: Optional[Any] = None
        self._last_query_execution: Optional[dict[str, Any]] = None

        # Use provided session or create new one
        if session:
            self.client = session.client("athena", config=config, **client_kwargs)
        else:
            # Default config with retries
            default_config = Config(
                region_name=region,
                retries={"max_attempts": 3, "mode": "adaptive"},
                max_pool_connections=50,
            )
            final_config = config or default_config
            self.client = boto3.client("athena", config=final_config, **client_kwargs)

        self.database = database
        self.output_location = output_location

    @property
    def _s3(self):
        """Lazy S3 client, reusing the same session/credentials as the Athena client."""
        if self._s3_client is None:
            if self._session:
                self._s3_client = self._session.client(
                    "s3", config=self._config, **self._client_kwargs
                )
            else:
                default_config = Config(
                    region_name=self._region,
                    retries={"max_attempts": 3, "mode": "adaptive"},
                )
                final_config = self._config or default_config
                self._s3_client = boto3.client(
                    "s3", config=final_config, **self._client_kwargs
                )
        return self._s3_client

    @staticmethod
    def _parse_s3_uri(uri: str) -> tuple[str, str]:
        """Split ``s3://bucket/key/path`` into ``("bucket", "key/path")``."""
        if not uri.startswith("s3://"):
            raise ValueError(f"Not an S3 URI: {uri}")
        path = uri[5:]  # strip "s3://"
        bucket, _, key = path.partition("/")
        if not bucket:
            raise ValueError(f"Invalid S3 URI (no bucket): {uri}")
        return bucket, key

    def query(
        self,
        query: str,
        wait_for_completion: bool = True,
        work_group: Optional[str] = None,
        query_context: Optional[dict[str, str]] = None,
        result_config: Optional[dict[str, Any]] = None,
        parameters: Optional[list[str]] = None,
        result_reuse_max_age: Optional[int] = None,
    ) -> str:
        """Execute a query and return the execution ID.

        Args:
            query: SQL query string.
            wait_for_completion: Block until the query finishes.
            work_group: Athena workgroup name.
            query_context: Extra context key/values (merged with Database).
            result_config: Extra result-configuration key/values.
            parameters: Parameterized query execution parameters.
            result_reuse_max_age: If set, enable result reuse for this many
                minutes via ``ResultReuseConfiguration``.
        """
        # Build query execution context
        context = {"Database": self.database}
        if query_context:
            context.update(query_context)

        # Build result configuration
        config = {"OutputLocation": self.output_location}
        if result_config:
            config.update(result_config)

        # Prepare execution parameters
        params: dict[str, Any] = {
            "QueryString": query,
            "QueryExecutionContext": context,
            "ResultConfiguration": config,
        }

        if work_group:
            params["WorkGroup"] = work_group

        if parameters is not None:
            params["ExecutionParameters"] = parameters

        if result_reuse_max_age is not None:
            params["ResultReuseConfiguration"] = {
                "ResultReuseByAgeConfiguration": {
                    "Enabled": True,
                    "MaxAgeInMinutes": result_reuse_max_age,
                }
            }

        try:
            response = self.client.start_query_execution(**params)
            execution_id = response["QueryExecutionId"]

            if wait_for_completion:
                self._wait_for_completion(execution_id)

            return execution_id

        except Exception as e:
            raise Exception(f"Failed to execute query: {e}") from e

    def _wait_for_completion(self, execution_id: str, max_wait_time: int = 300) -> str:
        """Wait for query to complete with exponential backoff"""
        start_time = time.time()
        sleep_time = 1

        while time.time() - start_time < max_wait_time:
            try:
                response = self.client.get_query_execution(
                    QueryExecutionId=execution_id
                )
                status = response["QueryExecution"]["Status"]["State"]

                if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                    if status == "FAILED":
                        reason = response["QueryExecution"]["Status"].get(
                            "StateChangeReason", "Unknown error"
                        )
                        error_msg = (
                            response["QueryExecution"]["Status"]
                            .get("AthenaError", {})
                            .get("ErrorMessage", "")
                        )
                        raise Exception(f"Query failed: {reason}. Error: {error_msg}")
                    elif status == "CANCELLED":
                        raise Exception("Query was cancelled")
                    # Cache the full QueryExecution for later S3 path lookup
                    self._last_query_execution = response["QueryExecution"]
                    return status

                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 1.5, 10)  # Exponential backoff, max 10s

            except Exception as e:
                # Only handle specific AWS exceptions, let our own exceptions through
                if "Query failed:" in str(e) or "Query was cancelled" in str(e):
                    raise  # Re-raise our own exceptions
                elif "InvalidRequestException" in str(
                    type(e)
                ) or "InvalidRequestException" in str(e):
                    raise Exception(
                        f"Invalid query execution ID: {execution_id}"
                    ) from e
                else:
                    # For any other exception, continue the loop or re-raise
                    break

        raise TimeoutError(f"Query did not complete within {max_wait_time} seconds")

    # ------------------------------------------------------------------
    # S3 direct-read helpers
    # ------------------------------------------------------------------

    def _get_column_schema(self, execution_id: str) -> pa.Schema:
        """Fetch column metadata via a single get_query_results call."""
        response = self.client.get_query_results(
            QueryExecutionId=execution_id, MaxResults=1
        )
        column_info = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        fields = []
        for col in column_info:
            pa_type = presto_type_to_pyarrow(col["Type"])
            fields.append(pa.field(col["Name"], pa_type))
        return pa.schema(fields)

    def _read_s3_bytes(self, s3_uri: str) -> bytes:
        """Download raw bytes from S3."""
        bucket, key = self._parse_s3_uri(s3_uri)
        response = self._s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def _csv_to_dataframe(self, csv_bytes: bytes, schema: pa.Schema) -> pl.DataFrame:
        """Read CSV bytes into a typed Polars DataFrame using a PyArrow schema."""
        col_names = [f.name for f in schema]
        df_raw = pl.read_csv(csv_bytes, has_header=True, infer_schema=False)
        if df_raw.columns != col_names:
            df_raw.columns = col_names
        arrays = []
        for field in schema:
            col = df_raw.get_column(field.name)
            try:
                arrow_arr = pa.array(col.to_list(), type=field.type, from_pandas=True)
            except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
                arrow_arr = pa.array(col.to_list(), type=pa.string())
            arrays.append(arrow_arr)
        table = pa.table(arrays, schema=schema)
        return cast(pl.DataFrame, pl.from_arrow(table))

    # ------------------------------------------------------------------
    # Parquet S3 path (zero-copy, no schema lookup needed)
    # ------------------------------------------------------------------

    def _results_from_parquet(self, s3_uri: str, max_rows: int = 0) -> pl.DataFrame:
        """Read Parquet results directly from S3.

        Args:
            s3_uri: Full ``s3://bucket/key`` path to the Parquet file.
            max_rows: Limit rows returned (0 = all rows).
        """
        body = self._read_s3_bytes(s3_uri)
        table = pq.read_table(io.BytesIO(body))
        if max_rows > 0:
            table = table.slice(0, max_rows)
        return cast(pl.DataFrame, pl.from_arrow(table))

    def _results_iter_from_parquet(
        self, s3_uri: str, page_size: int = 1000
    ) -> Iterator[pl.DataFrame]:
        """Read Parquet results from S3 and yield in batches.

        Args:
            s3_uri: Full ``s3://bucket/key`` path to the Parquet file.
            page_size: Rows per batch.
        """
        body = self._read_s3_bytes(s3_uri)
        pf = pq.ParquetFile(io.BytesIO(body))
        for batch in pf.iter_batches(batch_size=page_size):
            df = cast(pl.DataFrame, pl.from_arrow(batch))
            if len(df) > 0:
                yield df

    # ------------------------------------------------------------------
    # CSV S3 path (needs schema lookup, but avoids paginated JSON API)
    # ------------------------------------------------------------------

    def _results_from_csv(
        self, execution_id: str, s3_uri: str, max_rows: int = 0
    ) -> pl.DataFrame:
        """Read CSV results directly from S3 with Athena type metadata.

        Args:
            execution_id: Query execution ID (for schema lookup).
            s3_uri: Full ``s3://bucket/key`` path to the CSV file.
            max_rows: Limit rows returned (0 = all rows).
        """
        schema = self._get_column_schema(execution_id)
        csv_bytes = self._read_s3_bytes(s3_uri)
        df = self._csv_to_dataframe(csv_bytes, schema)
        if max_rows > 0:
            df = df.head(max_rows)
        return df

    def _results_iter_from_csv(
        self, execution_id: str, s3_uri: str, page_size: int = 1000
    ) -> Iterator[pl.DataFrame]:
        """Read CSV results from S3 and yield in batches.

        Args:
            execution_id: Query execution ID (for schema lookup).
            s3_uri: Full ``s3://bucket/key`` path to the CSV file.
            page_size: Rows per batch.
        """
        schema = self._get_column_schema(execution_id)
        csv_bytes = self._read_s3_bytes(s3_uri)
        df = self._csv_to_dataframe(csv_bytes, schema)
        for offset in range(0, len(df), page_size):
            yield df.slice(offset, page_size)

    def results(self, execution_id: str, max_rows: int = 1000) -> pl.DataFrame:
        """Get query results as a Polars DataFrame.

        Tries S3 direct-read first (Parquet → CSV), then falls back to the
        paginated JSON API.
        """
        # --- S3 direct-read fast path ---
        try:
            qe = self._last_query_execution
            if qe is None or qe.get("QueryExecutionId") != execution_id:
                qe = self.get_query_info(execution_id)
            stmt_type = qe.get("StatementType", "")
            output_loc = qe.get("ResultConfiguration", {}).get("OutputLocation", "")
            if stmt_type == "DML" and output_loc:
                if output_loc.endswith(".parquet"):
                    return self._results_from_parquet(output_loc, max_rows=max_rows)
                else:
                    return self._results_from_csv(
                        execution_id, output_loc, max_rows=max_rows
                    )
        except Exception:
            pass  # Fall through to JSON API

        # --- Existing JSON API path ---
        try:
            # Get first batch to extract schema
            response = self.client.get_query_results(
                QueryExecutionId=execution_id, MaxResults=min(max_rows, 1000)
            )

            result_set = response["ResultSet"]
            column_info = result_set["ResultSetMetadata"]["ColumnInfo"]

            # Create PyArrow schema
            schema_fields = []
            for col in column_info:
                pa_type = presto_type_to_pyarrow(col["Type"])
                schema_fields.append(pa.field(col["Name"], pa_type))

            schema = pa.schema(schema_fields)

            # Initialize column data collectors
            columns_data = {col["Name"]: [] for col in column_info}

            # Process all batches
            next_token = None
            total_rows = 0

            while total_rows < max_rows:
                if next_token is None:
                    # First batch - skip header row
                    data_rows = result_set["Rows"][1:] if result_set["Rows"] else []
                else:
                    # Get next batch
                    params = {
                        "QueryExecutionId": execution_id,
                        "MaxResults": min(max_rows - total_rows, 1000),
                        "NextToken": next_token,
                    }
                    response = self.client.get_query_results(**params)
                    result_set = response["ResultSet"]
                    data_rows = result_set["Rows"]

                # Extract data from this batch
                for row in data_rows:
                    if total_rows >= max_rows:
                        break

                    for i, col_data in enumerate(row["Data"]):
                        col_name = column_info[i]["Name"]
                        col_type = column_info[i]["Type"]
                        value = self._extract_typed_value(col_data, col_type)
                        columns_data[col_name].append(value)

                    total_rows += 1

                # Check for more results
                next_token = response.get("NextToken")
                if not next_token or total_rows >= max_rows:
                    break

            # Create PyArrow table
            if total_rows == 0:
                # Empty result
                return cast(
                    pl.DataFrame,
                    pl.from_arrow(
                        pa.table(
                            {col["Name"]: [] for col in column_info}, schema=schema
                        )
                    ),
                )

            # Build PyArrow arrays with proper typing
            arrays = []
            for field in schema:
                col_name = field.name
                data = columns_data[col_name]

                try:
                    # Let PyArrow handle the conversion with the specified type
                    array = pa.array(data, type=field.type)
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    # Fallback to string type if conversion fails
                    array = pa.array(
                        [str(x) if x is not None else None for x in data],
                        type=pa.string(),
                    )

                arrays.append(array)

            # Create table and convert to Polars
            table = pa.table(arrays, names=[field.name for field in schema])
            return cast(pl.DataFrame, pl.from_arrow(table))

        except Exception as e:
            raise Exception(f"Failed to retrieve query results: {e}") from e

    def results_iter(
        self, execution_id: str, page_size: int = 1000
    ) -> Iterator[pl.DataFrame]:
        """Yield query results page-by-page as Polars DataFrames.

        Tries S3 direct-read first (Parquet → CSV), then falls back to the
        paginated JSON API.

        Args:
            execution_id: The query execution ID.
            page_size: Rows per page (capped at 1000, the Athena API limit).

        Yields:
            One ``pl.DataFrame`` per batch.
        """
        # --- S3 direct-read fast path ---
        try:
            qe = self._last_query_execution
            if qe is None or qe.get("QueryExecutionId") != execution_id:
                qe = self.get_query_info(execution_id)
            stmt_type = qe.get("StatementType", "")
            output_loc = qe.get("ResultConfiguration", {}).get("OutputLocation", "")
            if stmt_type == "DML" and output_loc:
                if output_loc.endswith(".parquet"):
                    yield from self._results_iter_from_parquet(
                        output_loc, page_size=page_size
                    )
                else:
                    yield from self._results_iter_from_csv(
                        execution_id, output_loc, page_size=page_size
                    )
                return
        except Exception:
            pass  # Fall through to JSON API

        # --- Existing JSON API path ---
        page_size = min(page_size, 1000)

        try:
            # First page – extract schema and skip header row
            response = self.client.get_query_results(
                QueryExecutionId=execution_id, MaxResults=page_size
            )

            result_set = response["ResultSet"]
            column_info = result_set["ResultSetMetadata"]["ColumnInfo"]

            # Build PyArrow schema (reused for every page)
            schema_fields = []
            for col in column_info:
                pa_type = presto_type_to_pyarrow(col["Type"])
                schema_fields.append(pa.field(col["Name"], pa_type))
            schema = pa.schema(schema_fields)

            # First batch skips the header row
            data_rows = result_set["Rows"][1:] if result_set["Rows"] else []

            while True:
                if data_rows:
                    columns_data = {col["Name"]: [] for col in column_info}
                    for row in data_rows:
                        for i, col_data in enumerate(row["Data"]):
                            col_name = column_info[i]["Name"]
                            col_type = column_info[i]["Type"]
                            value = self._extract_typed_value(col_data, col_type)
                            columns_data[col_name].append(value)

                    arrays = []
                    for field in schema:
                        data = columns_data[field.name]
                        try:
                            array = pa.array(data, type=field.type)
                        except (pa.ArrowInvalid, pa.ArrowTypeError):
                            array = pa.array(
                                [str(x) if x is not None else None for x in data],
                                type=pa.string(),
                            )
                        arrays.append(array)

                    table = pa.table(arrays, names=[field.name for field in schema])
                    yield cast(pl.DataFrame, pl.from_arrow(table))

                next_token = response.get("NextToken")
                if not next_token:
                    break

                response = self.client.get_query_results(
                    QueryExecutionId=execution_id,
                    MaxResults=page_size,
                    NextToken=next_token,
                )
                result_set = response["ResultSet"]
                data_rows = result_set["Rows"]

        except Exception as e:
            raise Exception(f"Failed to retrieve query results: {e}") from e

    def _extract_typed_value(self, col_data: dict[str, Any], athena_type: str) -> Any:
        """Extract and convert value based on Athena type"""
        if col_data.get("NullValue"):
            return None

        # Get the raw value
        if "VarCharValue" in col_data:
            raw_value = col_data["VarCharValue"]
        elif "BigIntValue" in col_data:
            return col_data["BigIntValue"]
        elif "DoubleValue" in col_data:
            return col_data["DoubleValue"]
        elif "BooleanValue" in col_data:
            return col_data["BooleanValue"]
        else:
            return None

        # Convert based on type
        athena_type = athena_type.lower()

        # For string-like types, preserve empty strings as-is
        is_string_type = (
            athena_type.startswith("varchar")
            or athena_type.startswith("char")
            or athena_type in ("string", "text")
        )

        if not raw_value and raw_value != "":
            return None

        if not is_string_type and raw_value == "":
            return None

        try:
            if athena_type in ["boolean", "bool"]:
                return raw_value.lower() in ("true", "1", "yes")
            elif athena_type in ["tinyint", "smallint", "int", "integer"]:
                return int(raw_value)
            elif athena_type in ["bigint", "long"]:
                return int(raw_value)
            elif athena_type in ["float", "real", "double", "double precision"]:
                return float(raw_value)
            elif athena_type.startswith("decimal") or athena_type.startswith("numeric"):
                return float(raw_value)  # PyArrow will handle decimal conversion
            else:
                return raw_value  # Keep as string for dates, timestamps, etc.
        except (ValueError, TypeError):
            return raw_value  # Fallback to string if conversion fails

    def get_query_info(self, execution_id: str) -> dict[str, Any]:
        """Get detailed information about a query execution"""
        try:
            response = self.client.get_query_execution(QueryExecutionId=execution_id)
            return response["QueryExecution"]
        except Exception as e:
            raise Exception(f"Failed to get query info: {e}") from e

    def cancel_query(self, execution_id: str) -> bool:
        """Cancel a running query"""
        try:
            self.client.stop_query_execution(QueryExecutionId=execution_id)
            return True
        except Exception as e:
            raise Exception(f"Failed to cancel query: {e}") from e

    def list_work_groups(self) -> list[str]:
        """List available Athena work groups"""
        try:
            response = self.client.list_work_groups()
            return [wg["Name"] for wg in response["WorkGroups"]]
        except Exception as e:
            raise Exception(f"Failed to list work groups: {e}") from e

    @classmethod
    def from_session(
        cls, session: boto3.Session, database: str, output_location: str, **kwargs
    ) -> "AthenaClient":
        """Create AthenaClient from existing boto3 session"""
        return cls(database, output_location, session=session, **kwargs)
