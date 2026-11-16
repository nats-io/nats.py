"""Parsing stream information without a running server."""

import pytest
from nats.jetstream import StreamAlternate
from nats.jetstream.stream import StreamInfo


def stream_response():
    return {
        "config": {"name": "TEST"},
        "created": "2026-01-01T00:00:00Z",
        "state": {"messages": 0, "bytes": 0, "first_seq": 0, "last_seq": 0, "consumer_count": 0},
    }


@pytest.mark.parametrize("strict", [False, True])
def test_stream_info_preserves_alternates(strict):
    response = stream_response()
    response["alternates"] = [
        {"name": "TEST", "cluster": "C1"},
        {"name": "MIRROR", "cluster": "C2", "domain": ""},
        {"name": "REMOTE", "cluster": "C3", "domain": "east"},
    ]

    info = StreamInfo.from_response(response, strict=strict)

    assert info.alternates == [
        StreamAlternate(name="TEST", cluster="C1"),
        StreamAlternate(name="MIRROR", cluster="C2", domain=""),
        StreamAlternate(name="REMOTE", cluster="C3", domain="east"),
    ]


@pytest.mark.parametrize("alternates", [None, []])
def test_stream_info_optional_alternates(alternates):
    response = stream_response()
    response["alternates"] = alternates
    assert StreamInfo.from_response(response, strict=True).alternates == alternates


def test_stream_info_without_alternates():
    assert StreamInfo.from_response(stream_response(), strict=True).alternates is None


@pytest.mark.parametrize("strict", [False, True])
def test_stream_info_unknown_alternate_fields(strict):
    response = stream_response()
    response["alternates"] = [{"name": "MIRROR", "cluster": "C1", "future_field": True}]
    if strict:
        with pytest.raises(ValueError, match="StreamAlternate.*future_field"):
            StreamInfo.from_response(response, strict=True)
    else:
        info = StreamInfo.from_response(response)
        assert info.alternates == [StreamAlternate(name="MIRROR", cluster="C1")]
