"""Pull consumer request limits use the server's fields and duration units."""

import pytest

from nats.js.api import ConsumerConfig


def test_consumer_request_limits_serialization():
    config = ConsumerConfig(max_batch=10, max_expires=1.5, max_bytes=4096)

    data = config.as_dict()
    assert data["max_batch"] == 10
    assert data["max_expires"] == 1_500_000_000
    assert data["max_bytes"] == 4096

    restored = ConsumerConfig.from_response(data)
    assert restored.max_batch == 10
    assert restored.max_expires == 1.5
    assert restored.max_bytes == 4096


@pytest.mark.parametrize("seconds", [0, 0.25, 1.23456789, 5.0])
def test_consumer_max_expires_uses_seconds(seconds):
    config = ConsumerConfig(max_expires=seconds)
    data = config.as_dict()
    assert data["max_expires"] == int(seconds * 10**9)
    assert ConsumerConfig.from_response(data).max_expires == pytest.approx(seconds)


@pytest.mark.parametrize(
    "response",
    [{}, {"max_batch": None, "max_expires": None, "max_bytes": None}],
)
def test_consumer_request_limits_optional(response):
    config = ConsumerConfig.from_response(response)
    assert config.max_batch is None
    assert config.max_expires is None
    assert config.max_bytes is None
    assert not {"max_batch", "max_expires", "max_bytes"}.intersection(config.as_dict())


def test_consumer_request_limits_default_omitted():
    assert not {"max_batch", "max_expires", "max_bytes"}.intersection(ConsumerConfig().as_dict())


def test_consumer_request_limits_preserve_explicit_zero():
    config = ConsumerConfig.from_response({"max_batch": 0, "max_expires": 0, "max_bytes": 0})
    data = config.as_dict()
    assert data["max_batch"] == 0
    assert data["max_expires"] == 0
    assert data["max_bytes"] == 0
