"""Tests for JetStream API client functionality."""

from typing import NotRequired, Required, TypedDict

from nats.jetstream.api.client import check_response


class RequiredFieldsOnly(TypedDict):
    """TypedDict with only required fields."""

    name: str
    value: int


class MixedFields(TypedDict):
    """TypedDict with both required and optional fields."""

    required_field: Required[str]
    optional_field: NotRequired[int]


class AllOptionalFields(TypedDict, total=False):
    """TypedDict with all optional fields."""

    optional_name: str
    optional_value: int


def test_check_response_valid_exact_match():
    """Test check_response with data that exactly matches the TypedDict."""
    data = {"name": "test", "value": 42}
    is_valid, unknown, missing = check_response(data, RequiredFieldsOnly)

    assert is_valid is True
    assert unknown == set()
    assert missing == set()


def test_check_response_valid_with_extra_keys():
    """Test check_response with valid data but extra unknown keys."""
    data = {"name": "test", "value": 42, "extra_key": "unexpected"}
    is_valid, unknown, missing = check_response(data, RequiredFieldsOnly)

    assert is_valid is False
    assert unknown == {"extra_key"}
    assert missing == set()


def test_check_response_missing_required_keys():
    """Test check_response with missing required keys."""
    data = {"name": "test"}
    is_valid, unknown, missing = check_response(data, RequiredFieldsOnly)

    assert is_valid is False
    assert unknown == set()
    assert missing == {"value"}


def test_check_response_missing_all_required_keys():
    """Test check_response with all keys missing."""
    data = {}
    is_valid, unknown, missing = check_response(data, RequiredFieldsOnly)

    assert is_valid is False
    assert unknown == set()
    assert missing == {"name", "value"}


def test_check_response_mixed_required_optional_all_present():
    """Test check_response with mixed required/optional fields, all present."""
    data = {"required_field": "test", "optional_field": 42}
    is_valid, unknown, missing = check_response(data, MixedFields)

    assert is_valid is True
    assert unknown == set()
    assert missing == set()


def test_check_response_mixed_required_optional_only_required():
    """Test check_response with mixed fields, only required present."""
    data = {"required_field": "test"}
    is_valid, unknown, missing = check_response(data, MixedFields)

    assert is_valid is True
    assert unknown == set()
    assert missing == set()


def test_check_response_mixed_missing_required():
    """Test check_response with mixed fields, missing required field."""
    data = {"optional_field": 42}
    is_valid, unknown, missing = check_response(data, MixedFields)

    assert is_valid is False
    assert unknown == set()
    assert missing == {"required_field"}


def test_check_response_all_optional_empty():
    """Test check_response with all optional fields and empty data."""
    data = {}
    is_valid, unknown, missing = check_response(data, AllOptionalFields)

    assert is_valid is True
    assert unknown == set()
    assert missing == set()


def test_check_response_all_optional_partial():
    """Test check_response with all optional fields, partial data."""
    data = {"optional_name": "test"}
    is_valid, unknown, missing = check_response(data, AllOptionalFields)

    assert is_valid is True
    assert unknown == set()
    assert missing == set()


def test_check_response_all_optional_with_extra():
    """Test check_response with all optional fields and extra keys."""
    data = {"optional_name": "test", "unknown_key": "value"}
    is_valid, unknown, missing = check_response(data, AllOptionalFields)

    assert is_valid is False
    assert unknown == {"unknown_key"}
    assert missing == set()


def test_check_response_non_dict_data():
    """Test check_response with non-dict data."""
    is_valid, unknown, missing = check_response("not a dict", RequiredFieldsOnly)

    assert is_valid is False
    assert unknown == set()
    assert missing == set()


def test_check_response_none_data():
    """Test check_response with None."""
    is_valid, unknown, missing = check_response(None, RequiredFieldsOnly)

    assert is_valid is False
    assert unknown == set()
    assert missing == set()


def test_check_response_list_data():
    """Test check_response with list data."""
    is_valid, unknown, missing = check_response([], RequiredFieldsOnly)

    assert is_valid is False
    assert unknown == set()
    assert missing == set()
