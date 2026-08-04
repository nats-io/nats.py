"""Tests for message module."""

from collections.abc import MutableMapping

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
    assert headers.get("key2") == "value3"
    assert headers.get_all("key2") == ["value2", "value3"]

    # Mixed values
    headers = Headers({"key1": "value1", "key2": ["value2", "value3"]})
    assert headers.get("key1") == "value1"
    assert headers.get_all("key1") == ["value1"]
    assert headers.get("key2") == "value3"
    assert headers.get_all("key2") == ["value2", "value3"]

    # Empty / None init
    assert Headers().asdict() == {}
    assert Headers(None).asdict() == {}

    # Iterable of tuples
    headers = Headers([("a", "1"), ("b", ["2", "3"])])
    assert headers.get("a") == "1"
    assert headers.get_all("b") == ["2", "3"]

    # Invalid header values
    with pytest.raises(TypeError):
        Headers({"key1": 123})  # type: ignore[dict-item]

    with pytest.raises(ValueError, match="must be strings"):
        Headers({"key1": ["value1", 123]})  # type: ignore[list-item]

    # An empty value list would leave the mapping in an invalid state
    with pytest.raises(ValueError, match="must not be empty"):
        Headers({"key1": []})


def test_headers_get():
    """Test Headers.get() method."""
    # Single string value
    headers = Headers({"key1": "value1"})
    assert headers.get("key1") == "value1"

    # Last value from list (matches MutableMapping `[]` semantics)
    headers = Headers({"key2": ["value2", "value3"]})
    assert headers.get("key2") == "value3"

    # Non-existent key returns None
    assert headers.get("nonexistent") is None

    # Default value
    headers = Headers({"key1": "value1"})
    assert headers.get("nonexistent", "fallback") == "fallback"
    assert headers.get("key1", "fallback") == "value1"


def test_headers_get_all():
    """Test Headers.get_all() method."""
    # Single string value becomes a list
    headers = Headers({"key1": "value1"})
    assert headers.get_all("key1") == ["value1"]

    # List remains as is
    headers = Headers({"key2": ["value2", "value3"]})
    assert headers.get_all("key2") == ["value2", "value3"]

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

    # Case-insensitive equality
    assert Headers({"Trace-ID": "abc"}) == Headers({"trace-id": "abc"})

    # Value order within a key is significant
    assert Headers({"key": ["a", "b"]}) != Headers({"key": ["b", "a"]})


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


def test_headers_set():
    """Test Headers.set() method."""
    # Set a new header
    headers = Headers({})
    headers.set("key1", "value1")
    assert headers.get("key1") == "value1"
    assert headers.get_all("key1") == ["value1"]

    # Replace existing header
    headers.set("key1", "new_value")
    assert headers.get("key1") == "new_value"
    assert headers.get_all("key1") == ["new_value"]

    # Replace header with multiple values
    headers = Headers({"key2": ["value1", "value2", "value3"]})
    headers.set("key2", "single_value")
    assert headers.get("key2") == "single_value"
    assert headers.get_all("key2") == ["single_value"]

    # Case-insensitive: setting "Key1" replaces "key1"
    headers = Headers({"key1": "lowercase"})
    headers.set("Key1", "new_value")
    assert headers.get("key1") == "new_value"
    assert headers.get("Key1") == "new_value"
    assert len(headers) == 1


def test_headers_delete():
    """Test deleting headers via ``del`` (case-insensitive)."""
    # Delete existing header
    headers = Headers({"key1": "value1", "key2": "value2"})
    del headers["key1"]
    assert headers.get("key1") is None
    assert headers.get("key2") == "value2"

    # Deleting a non-existent header raises KeyError; pop with a default is silent
    with pytest.raises(KeyError):
        del headers["nonexistent"]
    assert headers.pop("nonexistent", None) is None
    assert headers.get("key2") == "value2"

    # Delete header with multiple values
    headers = Headers({"key3": ["value1", "value2", "value3"]})
    del headers["key3"]
    assert headers.get("key3") is None
    assert headers.get_all("key3") == []

    # Case-insensitive delete
    headers = Headers({"Content-Type": "application/json"})
    del headers["content-type"]
    assert "Content-Type" not in headers
    assert len(headers) == 0


def test_headers_delete_method():
    """Test Headers.delete() mirrors dict.pop (case-insensitive)."""
    # Returns the last value and removes the header
    headers = Headers({"key1": "value1", "key2": ["a", "b"]})
    assert headers.delete("key1") == "value1"
    assert "key1" not in headers
    assert headers.delete("key2") == "b"
    assert "key2" not in headers

    # Case-insensitive
    headers = Headers({"Content-Type": "application/json"})
    assert headers.delete("content-type") == "application/json"
    assert len(headers) == 0

    # Missing key raises KeyError unless a default is given
    with pytest.raises(KeyError):
        headers.delete("nonexistent")
    assert headers.delete("nonexistent", None) is None
    assert headers.delete("nonexistent", "fallback") == "fallback"


def test_headers_append():
    """Test Headers.append() method."""
    # Append to non-existent header (creates new)
    headers = Headers({})
    headers.append("key1", "value1")
    assert headers.get("key1") == "value1"
    assert headers.get_all("key1") == ["value1"]

    # Append to existing header
    headers.append("key1", "value2")
    assert headers.get("key1") == "value2"
    assert headers.get_all("key1") == ["value1", "value2"]

    # Append multiple times
    headers.append("key1", "value3")
    assert headers.get_all("key1") == ["value1", "value2", "value3"]

    # Case-insensitive append; original casing of key is preserved
    headers = Headers({"Content-Type": "application/json"})
    headers.append("content-type", "text/plain")
    assert headers.get_all("Content-Type") == ["application/json", "text/plain"]
    assert headers.get_all("CONTENT-TYPE") == ["application/json", "text/plain"]
    # The original-case key wins on iteration
    assert list(headers) == ["Content-Type"]


def test_headers_operations_integration():
    """Test combining set, del, and append operations."""
    headers = Headers({})

    # Build headers using operations
    headers.set("X-Custom", "value1")
    headers.append("X-Custom", "value2")
    headers.set("Authorization", "Bearer token")
    headers.append("Accept", "application/json")
    headers.append("Accept", "text/plain")

    assert headers.get_all("X-Custom") == ["value1", "value2"]
    assert headers.get("Authorization") == "Bearer token"
    assert headers.get_all("Accept") == ["application/json", "text/plain"]

    # Delete one header
    del headers["Authorization"]
    assert headers.get("Authorization") is None

    # Set replaces multi-value header
    headers.set("Accept", "application/xml")
    assert headers.get_all("Accept") == ["application/xml"]


def test_headers_is_mutable_mapping():
    """Headers implements the MutableMapping interface."""
    headers = Headers({"Content-Type": "application/json", "X-Trace": "abc"})
    assert isinstance(headers, MutableMapping)

    # __len__
    assert len(headers) == 2

    # __contains__ (case-insensitive)
    assert "Content-Type" in headers
    assert "content-type" in headers
    assert "CONTENT-TYPE" in headers
    assert "missing" not in headers
    assert 123 not in headers  # type: ignore[operator]

    # __iter__ preserves the original casing
    assert sorted(headers) == ["Content-Type", "X-Trace"]

    # __getitem__ is case-insensitive
    assert headers["Content-Type"] == "application/json"
    assert headers["content-type"] == "application/json"

    # KeyError on missing
    with pytest.raises(KeyError):
        _ = headers["missing"]

    # __setitem__
    headers["X-New"] = "1"
    assert headers["x-new"] == "1"
    assert "X-New" in headers

    # __delitem__ is case-insensitive
    del headers["x-trace"]
    assert "X-Trace" not in headers
    with pytest.raises(KeyError):
        del headers["x-trace"]

    # update from a dict
    headers.update({"Accept": "text/plain"})
    assert headers["accept"] == "text/plain"

    # dict() over a MutableMapping yields {original_case_key: last_value}
    headers = Headers({"Content-Type": ["a", "b"]})
    assert dict(headers) == {"Content-Type": "b"}


def test_headers_inherited_mixins_are_case_insensitive():
    """The inherited setdefault/pop/clear mixins fold case like the rest."""
    headers = Headers({"Content-Type": "application/json"})

    # setdefault returns the existing value for a differently-cased key and
    # does not insert a duplicate
    assert headers.setdefault("content-type", "ignored") == "application/json"
    assert len(headers) == 1

    # setdefault inserts when missing, preserving the casing it was given
    assert headers.setdefault("X-New", "v") == "v"
    assert list(headers) == ["Content-Type", "X-New"]

    # pop is case-insensitive, returns the last value, and removes the key
    headers = Headers({"Key": ["a", "b"]})
    assert headers.pop("key") == "b"
    assert "Key" not in headers
    with pytest.raises(KeyError):
        headers.pop("key")
    assert headers.pop("key", None) is None

    # clear empties the mapping
    headers = Headers({"a": "1", "b": "2"})
    headers.clear()
    assert len(headers) == 0
    assert list(headers) == []


def test_headers_case_insensitive_lookup_case_preserved_iteration():
    """Case-insensitive lookup but iteration preserves original casing."""
    headers = Headers({"Trace-ID": "abc"})
    assert headers["Trace-ID"] == "abc"
    assert headers["trace-id"] == "abc"
    assert headers["TRACE-ID"] == "abc"
    assert headers.get("trace-id") == "abc"
    assert headers.get_all("TRACE-ID") == ["abc"]

    # Iteration preserves the original casing the user wrote
    assert list(headers) == ["Trace-ID"]
    assert list(headers.keys()) == ["Trace-ID"]

    # Setting with a different case keeps lookups working but updates the
    # stored original casing
    headers.set("trace-id", "def")
    assert headers["Trace-ID"] == "def"
    assert list(headers) == ["trace-id"]


def test_headers_multi_items():
    """multi_items yields every value, items yields last value."""
    headers = Headers({"X-Multi": ["a", "b", "c"], "X-Single": "x"})
    items = sorted(headers.items())
    assert items == [("X-Multi", "c"), ("X-Single", "x")]

    multi = sorted(headers.multi_items())
    assert multi == [("X-Multi", "a"), ("X-Multi", "b"), ("X-Multi", "c"), ("X-Single", "x")]


def test_headers_asdict_preserves_case_and_lists():
    headers = Headers({"Content-Type": "application/json"})
    headers.append("content-type", "text/plain")
    assert headers.asdict() == {"Content-Type": ["application/json", "text/plain"]}


def test_headers_copy_preserves_multi_values():
    """Constructing Headers from another Headers keeps multi-valued entries."""
    original = Headers({"X-Multi": ["a", "b"], "X-Single": "x"})
    copy = Headers(original)
    assert copy.get_all("X-Multi") == ["a", "b"]
    assert copy.asdict() == {"X-Multi": ["a", "b"], "X-Single": ["x"]}
    assert copy == original
