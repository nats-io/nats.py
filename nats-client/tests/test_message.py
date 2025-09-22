"""Tests for message module."""

import pytest
from nats.client.message import Headers


def test_headers_init():
    """Test Headers initialization."""
    # Single string value gets converted to a single-item list
    headers = Headers({"key1": "value1"})
    assert headers.get("key1") == "value1"
    assert headers.get_all("key1") == ["value1"]

    # List value stays as list
    headers = Headers({"key2": ["value2", "value3"]})
    assert headers.get("key2") == "value2"
    assert headers.get_all("key2") == ["value2", "value3"]

    # Mixed values
    headers = Headers({"key1": "value1", "key2": ["value2", "value3"]})
    assert headers.get("key1") == "value1"
    assert headers.get_all("key1") == ["value1"]
    assert headers.get("key2") == "value2"
    assert headers.get_all("key2") == ["value2", "value3"]

    # Invalid header values
    with pytest.raises(TypeError):
        Headers({"key1": 123})

    with pytest.raises(ValueError):
        Headers({"key1": ["value1", 123]})


def test_headers_get():
    """Test Headers.get() method."""
    # Single string value
    headers = Headers({"key1": "value1"})
    assert headers.get("key1") == "value1"

    # First value from list
    headers = Headers({"key2": ["value2", "value3"]})
    assert headers.get("key2") == "value2"

    # Empty list returns None
    headers = Headers({"key3": []})
    assert headers.get("key3") is None

    # Non-existent key returns None
    assert headers.get("nonexistent") is None


def test_headers_get_all():
    """Test Headers.get_all() method."""
    # Single string value becomes a list
    headers = Headers({"key1": "value1"})
    assert headers.get_all("key1") == ["value1"]

    # List remains as is
    headers = Headers({"key2": ["value2", "value3"]})
    assert headers.get_all("key2") == ["value2", "value3"]

    # Empty list stays empty
    headers = Headers({"key3": []})
    assert headers.get_all("key3") == []

    # Non-existent key returns empty list
    assert headers.get_all("nonexistent") == []


def test_headers_equality():
    """Test Headers equality comparison."""
    headers1 = Headers({"key1": "value1", "key2": ["value2", "value3"]})
    headers2 = Headers({"key1": "value1", "key2": ["value2", "value3"]})
    headers3 = Headers({"key1": "different", "key2": ["value2", "value3"]})

    assert headers1 == headers2
    assert headers1 != headers3
    assert headers1 != "not a headers object"
