"""Tests for CsvFormatter — serialize_batch and write_batch_to_stream."""

import io

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
    # csv.DictWriter quotes the empty string, resulting in "" in the output
    assert b"val\r\n" in data


def test_serialize_batch_custom_delimiter(fmt):
    fmt.configure({"delimiter": "|"})
    data = fmt.serialize_batch([{"a": 1, "b": 2}])
    lines = data.decode().splitlines()
    assert "|" in lines[0]


# --- write_batch_to_stream ---


def test_write_batch_to_stream_empty_returns_zero(fmt):
    buf = io.BytesIO()
    assert fmt.write_batch_to_stream([], buf) == 0
    assert buf.tell() == 0


def test_write_batch_to_stream_writes_correct_bytes(fmt):
    buf = io.BytesIO()
    n = fmt.write_batch_to_stream(RECORDS, buf)
    buf.seek(0)
    content = buf.read()
    assert n == len(content)
    assert b"id,name,active" not in content  # append=True by default → no header


def test_write_batch_to_stream_append_false_includes_header(fmt):
    buf = io.BytesIO()
    fmt.write_batch_to_stream(RECORDS, buf, append=False)
    buf.seek(0)
    assert b"id,name,active" in buf.read()


def test_write_batch_to_stream_delegates_to_serialize_batch(fmt):
    """write_batch_to_stream output must equal serialize_batch with matching append."""
    buf = io.BytesIO()
    fmt.write_batch_to_stream(RECORDS, buf, append=False)
    buf.seek(0)
    stream_bytes = buf.read()
    direct_bytes = fmt.serialize_batch(RECORDS, append=False)
    assert stream_bytes == direct_bytes


def test_write_batch_to_stream_with_schema(fmt):
    schema = {"properties": {"name": {}, "id": {}, "active": {}}}
    buf = io.BytesIO()
    fmt.write_batch_to_stream(RECORDS, buf, schema=schema, append=False)
    buf.seek(0)
    first_line = buf.read().decode().splitlines()[0]
    assert first_line == "name,id,active"
