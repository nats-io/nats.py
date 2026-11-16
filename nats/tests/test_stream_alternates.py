"""Stream alternate locations returned by the JetStream API."""

from copy import deepcopy

import pytest

from nats.js import api


def stream_response():
    return {
        "config": {"name": "TEST"},
        "created": "2026-01-01T00:00:00Z",
        "state": {"messages": 0, "bytes": 0, "first_seq": 0, "last_seq": 0, "consumer_count": 0},
    }


def test_stream_info_preserves_alternates():
    response = stream_response()
    alternates = [
        {"name": "TEST", "cluster": "C1"},
        {"name": "MIRROR", "cluster": "C2", "domain": ""},
        {"name": "REMOTE", "cluster": "C3", "domain": "east"},
    ]
    response["alternates"] = deepcopy(alternates)

    info = api.StreamInfo.from_response(response)

    assert info.alternates is not None
    assert [(a.name, a.cluster, a.domain) for a in info.alternates] == [
        ("TEST", "C1", None),
        ("MIRROR", "C2", ""),
        ("REMOTE", "C3", "east"),
    ]
    assert all(isinstance(a, api.StreamAlternate) for a in info.alternates)
    assert info.as_dict()["alternates"] == alternates


@pytest.mark.parametrize("alternates", [None, []])
def test_stream_info_optional_alternates(alternates):
    response = stream_response()
    response["alternates"] = alternates
    assert api.StreamInfo.from_response(response).alternates == alternates


def test_stream_info_without_alternates():
    info = api.StreamInfo.from_response(stream_response())
    assert info.alternates is None
    assert "alternates" not in info.as_dict()
