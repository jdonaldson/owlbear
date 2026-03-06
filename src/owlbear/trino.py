#!/usr/bin/env python3
"""TrinoClient — execute Trino SQL, get typed Polars DataFrames."""

import polars as pl
import pyarrow as pa
from typing import Optional, Any

from .types import presto_type_to_pyarrow


class TrinoClient:
    def __init__(
        self,
        host: str,
        port: int = 443,
        user: Optional[str] = None,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        auth: Optional[Any] = None,
        http_scheme: str = "https",
        **kwargs,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.auth = auth
        self.http_scheme = http_scheme
        self.kwargs = kwargs

    def _get_connection(self):
        """Create a new Trino connection."""
        from trino.dbapi import connect

        params = {
            "host": self.host,
            "port": self.port,
            "http_scheme": self.http_scheme,
        }
        if self.user is not None:
            params["user"] = self.user
        if self.catalog is not None:
            params["catalog"] = self.catalog
        if self.schema is not None:
            params["schema"] = self.schema
        if self.auth is not None:
            params["auth"] = self.auth
        params.update(self.kwargs)

        return connect(**params)

    def query(self, query: str, max_rows: int = 0) -> pl.DataFrame:
        """Execute a query and return results as a Polars DataFrame.

        Args:
            query: SQL query string.
            max_rows: Maximum rows to return. 0 means no limit.

        Returns:
            A Polars DataFrame with typed columns.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(query)

            # Fetch rows
            if max_rows > 0:
                rows = cursor.fetchmany(max_rows)
            else:
                rows = cursor.fetchall()

            description = cursor.description
            if not description:
                return pl.DataFrame()

            col_names = [desc[0] for desc in description]
            col_types = [desc[1] for desc in description]

            if not rows:
                # Build empty DataFrame with correct schema
                schema_fields = []
                for name, type_str in zip(col_names, col_types):
                    pa_type = presto_type_to_pyarrow(type_str) if type_str else pa.string()
                    schema_fields.append(pa.field(name, pa_type))
                arrow_schema = pa.schema(schema_fields)
                return pl.from_arrow(
                    pa.table({name: [] for name in col_names}, schema=arrow_schema)
                )

            # Transpose rows into columns
            columns_data = list(zip(*rows))

            # Build PyArrow arrays
            arrays = []
            fields = []
            for i, (name, type_str) in enumerate(zip(col_names, col_types)):
                pa_type = presto_type_to_pyarrow(type_str) if type_str else pa.string()
                data = list(columns_data[i])
                try:
                    array = pa.array(data, type=pa_type)
                except (pa.ArrowInvalid, pa.ArrowTypeError):
                    array = pa.array(
                        [str(x) if x is not None else None for x in data],
                        type=pa.string(),
                    )
                    pa_type = pa.string()
                arrays.append(array)
                fields.append(pa.field(name, pa_type))

            table = pa.table(arrays, names=col_names)
            return pl.from_arrow(table)
        finally:
            conn.close()
