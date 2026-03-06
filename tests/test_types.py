"""Tests for presto_type_to_pyarrow type conversion."""

import pytest
import pyarrow as pa

from owlbear.types import presto_type_to_pyarrow


class TestBasicTypes:
    def test_boolean(self):
        assert presto_type_to_pyarrow("boolean") == pa.bool_()
        assert presto_type_to_pyarrow("bool") == pa.bool_()
        assert presto_type_to_pyarrow("BOOLEAN") == pa.bool_()

    def test_small_integers(self):
        assert presto_type_to_pyarrow("tinyint") == pa.int16()
        assert presto_type_to_pyarrow("smallint") == pa.int16()

    def test_integer(self):
        assert presto_type_to_pyarrow("int") == pa.int32()
        assert presto_type_to_pyarrow("integer") == pa.int32()
        assert presto_type_to_pyarrow("INTEGER") == pa.int32()

    def test_bigint(self):
        assert presto_type_to_pyarrow("bigint") == pa.int64()
        assert presto_type_to_pyarrow("long") == pa.int64()

    def test_float(self):
        assert presto_type_to_pyarrow("float") == pa.float32()
        assert presto_type_to_pyarrow("real") == pa.float32()

    def test_double(self):
        assert presto_type_to_pyarrow("double") == pa.float64()
        assert presto_type_to_pyarrow("double precision") == pa.float64()

    def test_date(self):
        assert presto_type_to_pyarrow("date") == pa.date32()

    def test_timestamp(self):
        assert presto_type_to_pyarrow("timestamp") == pa.timestamp("us")
        assert presto_type_to_pyarrow("timestamp with time zone") == pa.timestamp("us")


class TestDecimalTypes:
    def test_decimal_with_params(self):
        assert presto_type_to_pyarrow("decimal(10,2)") == pa.decimal128(10, 2)

    def test_decimal_precision_only(self):
        assert presto_type_to_pyarrow("decimal(10)") == pa.decimal128(10, 0)

    def test_decimal_default(self):
        assert presto_type_to_pyarrow("decimal") == pa.decimal128(38, 18)

    def test_numeric(self):
        assert presto_type_to_pyarrow("numeric(5,3)") == pa.decimal128(5, 3)


class TestStringTypes:
    def test_varchar(self):
        assert presto_type_to_pyarrow("varchar") == pa.string()
        assert presto_type_to_pyarrow("varchar(255)") == pa.string()

    def test_char(self):
        assert presto_type_to_pyarrow("char") == pa.string()
        assert presto_type_to_pyarrow("char(10)") == pa.string()

    def test_string(self):
        assert presto_type_to_pyarrow("string") == pa.string()

    def test_text(self):
        assert presto_type_to_pyarrow("text") == pa.string()


class TestComplexTypes:
    def test_array_of_integers(self):
        result = presto_type_to_pyarrow("array<integer>")
        assert result == pa.list_(pa.int32())

    def test_array_of_strings(self):
        result = presto_type_to_pyarrow("array<varchar>")
        assert result == pa.list_(pa.string())

    def test_map(self):
        result = presto_type_to_pyarrow("map<varchar,varchar>")
        assert result == pa.map_(pa.string(), pa.string())


class TestUnknownTypes:
    def test_unknown_defaults_to_string(self):
        assert presto_type_to_pyarrow("json") == pa.string()
        assert presto_type_to_pyarrow("varbinary") == pa.string()
        assert presto_type_to_pyarrow("ipaddress") == pa.string()
