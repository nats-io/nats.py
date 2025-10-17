"""Tests for message module."""

import pytest
from nats.client.message import Headers, Status


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
        Headers({"key1": 123})  # type: ignore[dict-item]

    with pytest.raises(ValueError):
        Headers({"key1": ["value1", 123]})  # type: ignore[list-item]


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


def test_status_creation():
    """Test creating Status objects."""
    # Test with code only
    status = Status(code="200")
    assert status.code == "200"
    assert status.description is None

    # Test with code and description
    status = Status(code="503", description="No Responders")
    assert status.code == "503"
    assert status.description == "No Responders"


def test_status_string_representation():
    """Test Status string conversion."""
    # With description
    status = Status(code="503", description="No Responders")
    assert str(status) == "503: No Responders"

    # Without description
    status = Status(code="200")
    assert str(status) == "200"

    # Empty description should be treated as None
    status = Status(code="400", description="")
    assert str(status) == "400"


def test_status_equality():
    """Test Status equality comparison."""
    status1 = Status(code="503", description="No Responders")
    status2 = Status(code="503", description="No Responders")
    status3 = Status(code="503", description="Service Unavailable")
    status4 = Status(code="400", description="No Responders")

    # Same code and description should be equal
    assert status1 == status2

    # Different description should not be equal
    assert status1 != status3

    # Different code should not be equal
    assert status1 != status4

    # Should not be equal to non-Status objects
    assert status1 != "503: No Responders"
    assert status1 != 503


def test_status_common_codes():
    """Test common status codes."""
    # Success
    success = Status(code="200", description="OK")
    assert str(success) == "200: OK"

    # Bad Request
    bad_request = Status(code="400", description="Bad Request")
    assert str(bad_request) == "400: Bad Request"

    # No Responders
    no_responders = Status(code="503", description="No Responders")
    assert str(no_responders) == "503: No Responders"

    # Server Error
    server_error = Status(code="500", description="Internal Server Error")
    assert str(server_error) == "500: Internal Server Error"
