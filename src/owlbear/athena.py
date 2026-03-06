#!/usr/bin/env python3
"""AthenaClient — execute Athena SQL, get typed Polars DataFrames."""

import boto3
import polars as pl
import pyarrow as pa
import time
from typing import Optional, Dict, Any
from botocore.config import Config

from .types import presto_type_to_pyarrow


class AthenaClient:
    def __init__(
        self,
        database: str,
        output_location: str,
        region: str = 'us-east-1',
        session: Optional[boto3.Session] = None,
        config: Optional[Config] = None,
        **client_kwargs
    ):
        # Use provided session or create new one
        if session:
            self.client = session.client('athena', config=config, **client_kwargs)
        else:
            # Default config with retries
            default_config = Config(
                region_name=region,
                retries={'max_attempts': 3, 'mode': 'adaptive'},
                max_pool_connections=50
            )
            final_config = config or default_config
            self.client = boto3.client('athena', config=final_config, **client_kwargs)

        self.database = database
        self.output_location = output_location

    def execute_query(
        self,
        query: str,
        wait_for_completion: bool = True,
        work_group: Optional[str] = None,
        query_context: Optional[Dict[str, str]] = None,
        result_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """Execute a query and return the execution ID"""
        # Build query execution context
        context = {'Database': self.database}
        if query_context:
            context.update(query_context)

        # Build result configuration
        config = {'OutputLocation': self.output_location}
        if result_config:
            config.update(result_config)

        # Prepare execution parameters
        params = {
            'QueryString': query,
            'QueryExecutionContext': context,
            'ResultConfiguration': config
        }

        if work_group:
            params['WorkGroup'] = work_group

        try:
            response = self.client.start_query_execution(**params)
            execution_id = response['QueryExecutionId']

            if wait_for_completion:
                self._wait_for_completion(execution_id)

            return execution_id

        except Exception as e:
            raise Exception(f"Failed to execute query: {str(e)}")

    def _wait_for_completion(self, execution_id: str, max_wait_time: int = 300):
        """Wait for query to complete with exponential backoff"""
        start_time = time.time()
        sleep_time = 1

        while time.time() - start_time < max_wait_time:
            try:
                response = self.client.get_query_execution(QueryExecutionId=execution_id)
                status = response['QueryExecution']['Status']['State']

                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    if status == 'FAILED':
                        reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                        error_msg = response['QueryExecution']['Status'].get('AthenaError', {}).get('ErrorMessage', '')
                        raise Exception(f"Query failed: {reason}. Error: {error_msg}")
                    elif status == 'CANCELLED':
                        raise Exception("Query was cancelled")
                    return status

                time.sleep(sleep_time)
                sleep_time = min(sleep_time * 1.5, 10)  # Exponential backoff, max 10s

            except Exception as e:
                # Only handle specific AWS exceptions, let our own exceptions through
                if "Query failed:" in str(e) or "Query was cancelled" in str(e):
                    raise  # Re-raise our own exceptions
                elif "InvalidRequestException" in str(type(e)) or "InvalidRequestException" in str(e):
                    raise Exception(f"Invalid query execution ID: {execution_id}")
                else:
                    # For any other exception, continue the loop or re-raise
                    break

        raise TimeoutError(f"Query did not complete within {max_wait_time} seconds")

    def get_results_polars(self, execution_id: str, max_rows: int = 1000) -> pl.DataFrame:
        """Get query results as a Polars DataFrame using PyArrow for better type handling"""
        try:
            # Get first batch to extract schema
            response = self.client.get_query_results(
                QueryExecutionId=execution_id,
                MaxResults=min(max_rows, 1000)
            )

            result_set = response['ResultSet']
            column_info = result_set['ResultSetMetadata']['ColumnInfo']

            # Create PyArrow schema
            schema_fields = []
            for col in column_info:
                pa_type = presto_type_to_pyarrow(col['Type'])
                schema_fields.append(pa.field(col['Name'], pa_type))

            schema = pa.schema(schema_fields)

            # Initialize column data collectors
            columns_data = {col['Name']: [] for col in column_info}

            # Process all batches
            next_token = None
            total_rows = 0

            while total_rows < max_rows:
                if next_token is None:
                    # First batch - skip header row
                    data_rows = result_set['Rows'][1:] if result_set['Rows'] else []
                else:
                    # Get next batch
                    params = {
                        'QueryExecutionId': execution_id,
                        'MaxResults': min(max_rows - total_rows, 1000),
                        'NextToken': next_token
                    }
                    response = self.client.get_query_results(**params)
                    result_set = response['ResultSet']
                    data_rows = result_set['Rows']

                # Extract data from this batch
                for row in data_rows:
                    if total_rows >= max_rows:
                        break

                    for i, col_data in enumerate(row['Data']):
                        col_name = column_info[i]['Name']
                        col_type = column_info[i]['Type']
                        value = self._extract_typed_value(col_data, col_type)
                        columns_data[col_name].append(value)

                    total_rows += 1

                # Check for more results
                next_token = response.get('NextToken')
                if not next_token or total_rows >= max_rows:
                    break

            # Create PyArrow table
            if total_rows == 0:
                # Empty result
                return pl.from_arrow(pa.table({col['Name']: [] for col in column_info}, schema=schema))

            # Build PyArrow arrays with proper typing
            arrays = []
            for field in schema:
                col_name = field.name
                data = columns_data[col_name]

                try:
                    # Let PyArrow handle the conversion with the specified type
                    array = pa.array(data, type=field.type)
                except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
                    # Fallback to string type if conversion fails
                    array = pa.array([str(x) if x is not None else None for x in data], type=pa.string())

                arrays.append(array)

            # Create table and convert to Polars
            table = pa.table(arrays, names=[field.name for field in schema])
            return pl.from_arrow(table)

        except Exception as e:
            raise Exception(f"Failed to retrieve query results: {str(e)}")

    def _extract_typed_value(self, col_data: Dict[str, Any], athena_type: str) -> Any:
        """Extract and convert value based on Athena type"""
        if col_data.get('NullValue'):
            return None

        # Get the raw value
        if 'VarCharValue' in col_data:
            raw_value = col_data['VarCharValue']
        elif 'BigIntValue' in col_data:
            return col_data['BigIntValue']
        elif 'DoubleValue' in col_data:
            return col_data['DoubleValue']
        elif 'BooleanValue' in col_data:
            return col_data['BooleanValue']
        else:
            return None

        # Convert based on type
        athena_type = athena_type.lower()

        if not raw_value or raw_value == '':
            return None

        try:
            if athena_type in ['boolean', 'bool']:
                return raw_value.lower() in ('true', '1', 'yes')
            elif athena_type in ['tinyint', 'smallint', 'int', 'integer']:
                return int(raw_value)
            elif athena_type in ['bigint', 'long']:
                return int(raw_value)
            elif athena_type in ['float', 'real', 'double', 'double precision']:
                return float(raw_value)
            elif athena_type.startswith('decimal') or athena_type.startswith('numeric'):
                return float(raw_value)  # PyArrow will handle decimal conversion
            else:
                return raw_value  # Keep as string for dates, timestamps, etc.
        except (ValueError, TypeError):
            return raw_value  # Fallback to string if conversion fails

    def get_query_info(self, execution_id: str) -> Dict[str, Any]:
        """Get detailed information about a query execution"""
        try:
            response = self.client.get_query_execution(QueryExecutionId=execution_id)
            return response['QueryExecution']
        except Exception as e:
            raise Exception(f"Failed to get query info: {str(e)}")

    def cancel_query(self, execution_id: str) -> bool:
        """Cancel a running query"""
        try:
            self.client.stop_query_execution(QueryExecutionId=execution_id)
            return True
        except Exception as e:
            raise Exception(f"Failed to cancel query: {str(e)}")

    def list_work_groups(self) -> list:
        """List available Athena work groups"""
        try:
            response = self.client.list_work_groups()
            return [wg['Name'] for wg in response['WorkGroups']]
        except Exception as e:
            raise Exception(f"Failed to list work groups: {str(e)}")

    @classmethod
    def from_session(cls, session: boto3.Session, database: str, output_location: str, **kwargs):
        """Create AthenaClient from existing boto3 session"""
        return cls(database, output_location, session=session, **kwargs)
