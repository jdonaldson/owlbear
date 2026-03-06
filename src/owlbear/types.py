"""Shared Presto-family type conversion for Athena and Trino backends."""

import pyarrow as pa


def presto_type_to_pyarrow(presto_type: str) -> pa.DataType:
    """Convert a Presto/Trino/Athena type name to a PyArrow data type."""
    presto_type = presto_type.lower()

    if presto_type in ["boolean", "bool"]:
        return pa.bool_()
    elif presto_type in ["tinyint", "smallint"]:
        return pa.int16()
    elif presto_type in ["int", "integer"]:
        return pa.int32()
    elif presto_type in ["bigint", "long"]:
        return pa.int64()
    elif presto_type in ["float", "real"]:
        return pa.float32()
    elif presto_type in ["double", "double precision"]:
        return pa.float64()
    elif presto_type == "date":
        return pa.date32()
    elif presto_type.startswith("timestamp"):
        return pa.timestamp("us")
    elif presto_type.startswith("decimal") or presto_type.startswith("numeric"):
        if "(" in presto_type:
            params = presto_type.split("(")[1].split(")")[0].split(",")
            precision = int(params[0])
            scale = int(params[1]) if len(params) > 1 else 0
            return pa.decimal128(precision, scale)
        return pa.decimal128(38, 18)
    elif presto_type.startswith("varchar") or presto_type.startswith("char"):
        return pa.string()
    elif presto_type in ["string", "text"]:
        return pa.string()
    elif presto_type.startswith("array"):
        element_type_str = presto_type[6:-1]  # Remove 'array<' and '>'
        element_type = presto_type_to_pyarrow(element_type_str)
        return pa.list_(element_type)
    elif presto_type.startswith("map"):
        return pa.map_(pa.string(), pa.string())
    else:
        return pa.string()
