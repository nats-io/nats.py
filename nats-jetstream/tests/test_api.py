"""Tests for JetStream API client functionality."""

import json
from typing import NotRequired, Required, TypedDict

import pytest
from nats.client.errors import StatusError
from nats.client.message import Message
from nats.jetstream.api.client import Client, check_response
from nats.jetstream.errors import MessageNotFoundError


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


class _RecordingClient:
    """Minimal fake NATS client that records request calls and replays a response."""

    def __init__(self, response: Message | None = None, error: Exception | None = None) -> None:
        self._response = response
        self._error = error
        self.calls: list[tuple[str, bytes]] = []

    async def request(self, subject: str, payload: bytes, *, timeout: float = 2.0) -> Message:
        self.calls.append((subject, payload))
        if self._error is not None:
            raise self._error
        assert self._response is not None
        return self._response


@pytest.mark.asyncio
async def test_stream_direct_get_by_sequence_uses_configured_prefix():
    """Custom prefix must be honored when fetching by sequence."""
    response = Message(subject="_INBOX.x", data=b"payload")
    fake = _RecordingClient(response=response)
    api = Client(fake, prefix="$JS.MY.API")

    result = await api.stream_direct_get("STREAM", seq=1)

    assert result is response
    assert len(fake.calls) == 1
    subject, payload = fake.calls[0]
    assert subject == "$JS.MY.API.DIRECT.GET.STREAM"
    assert not subject.startswith("$JS.API.DIRECT.GET")
    assert json.loads(payload.decode()) == {"seq": 1}


@pytest.mark.asyncio
async def test_stream_direct_get_last_by_subject_appends_to_subject():
    """Last-by-subject form must encode the subject in the API path with an empty body."""
    response = Message(subject="_INBOX.x", data=b"payload")
    fake = _RecordingClient(response=response)
    api = Client(fake, prefix="$JS.API")

    result = await api.stream_direct_get("STREAM", last_by_subj="FOO.BAR")

    assert result is response
    assert len(fake.calls) == 1
    subject, payload = fake.calls[0]
    assert subject == "$JS.API.DIRECT.GET.STREAM.FOO.BAR"
    assert payload == b""


@pytest.mark.asyncio
async def test_stream_direct_get_404_maps_to_message_not_found():
    """A 404 StatusError from the underlying client must surface as MessageNotFoundError."""
    fake = _RecordingClient(error=StatusError("404", "Message Not Found"))
    api = Client(fake, prefix="$JS.API")

    with pytest.raises(MessageNotFoundError):
        await api.stream_direct_get("STREAM", seq=99)


@pytest.mark.asyncio
async def test_stream_direct_get_requires_exactly_one_selector():
    """Caller must pass exactly one of seq / last_by_subj."""
    fake = _RecordingClient(response=Message(subject="_INBOX.x", data=b""))
    api = Client(fake, prefix="$JS.API")

    with pytest.raises(ValueError):
        await api.stream_direct_get("STREAM")

    with pytest.raises(ValueError):
        await api.stream_direct_get("STREAM", seq=1, last_by_subj="FOO")
