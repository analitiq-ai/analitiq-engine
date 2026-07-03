"""Tests for CsvFormatter.serialize_batch."""

import pytest

from src.destination.formatters.csv import CsvFormatter

RECORDS = [
    {"id": 1, "name": "Alice", "active": True},
    {"id": 2, "name": "Bob", "active": False},
]


@pytest.fixture()
def fmt():
    f = CsvFormatter()
    f.configure({})
    return f


# --- serialize_batch ---


def test_serialize_batch_empty_returns_empty_bytes(fmt):
    assert fmt.serialize_batch([]) == b""


def test_serialize_batch_includes_header_by_default(fmt):
    data = fmt.serialize_batch(RECORDS)
    lines = data.decode().splitlines()
    assert lines[0] == "id,name,active"
    assert len(lines) == 3


def test_serialize_batch_append_suppresses_header(fmt):
    data = fmt.serialize_batch(RECORDS, append=True)
    lines = data.decode().splitlines()
    assert lines[0] != "id,name,active"
    assert len(lines) == 2


def test_serialize_batch_include_header_false_suppresses_header(fmt):
    fmt.configure({"include_header": False})
    data = fmt.serialize_batch(RECORDS)
    lines = data.decode().splitlines()
    assert lines[0] != "id,name,active"
    assert len(lines) == 2


def test_serialize_batch_schema_controls_column_order(fmt):
    schema = {"properties": {"name": {}, "id": {}, "active": {}}}
    data = fmt.serialize_batch(RECORDS, schema=schema)
    lines = data.decode().splitlines()
    assert lines[0] == "name,id,active"


def test_serialize_batch_formats_bool_values(fmt):
    data = fmt.serialize_batch([{"flag": True}, {"flag": False}])
    lines = data.decode().splitlines()
    assert lines[1] == "true"
    assert lines[2] == "false"


def test_serialize_batch_formats_none_as_empty(fmt):
    data = fmt.serialize_batch([{"val": None}])
    # _format_value converts None to ""; csv.DictWriter quotes empty strings as ""
    assert data == b'val\r\n""\r\n'


def test_serialize_batch_formats_nested_value_as_json(fmt):
    # _format_value serializes list/dict to JSON; DictWriter quotes the embedded quotes
    data = fmt.serialize_batch([{"tags": ["a", "b"]}])
    assert b'"[""a"", ""b""]"' in data


def test_serialize_batch_include_header_false_with_append_suppresses(fmt):
    fmt.configure({"include_header": False})
    data = fmt.serialize_batch(RECORDS, append=True)
    lines = data.decode().splitlines()
    assert len(lines) == 2
    assert lines[0] != "id,name,active"


def test_serialize_batch_custom_delimiter(fmt):
    fmt.configure({"delimiter": "|"})
    data = fmt.serialize_batch([{"a": 1, "b": 2}])
    lines = data.decode().splitlines()
    assert "|" in lines[0]
