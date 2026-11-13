"""Credentials must stay out of actual connection errors and reconnect logs."""

import logging
import traceback

import pytest
from nats.client import ClientStatus, _Server, connect
from nats.client.connection import establish_connection


@pytest.mark.parametrize("userinfo", ["private-token", "user:private-password"])
@pytest.mark.parametrize("address", ["127.0.0.1:1", "127.0.0.1:bad-port", ":4222"])
async def test_failed_reconnect_redacts_credentials(server, caplog, userinfo, address):
    client = await connect(server.client_url, reconnect_max_attempts=1, reconnect_time_wait=0)
    client._server_pool = [_Server(url=f"nats://{userinfo}@{address}")]
    try:
        with caplog.at_level(logging.DEBUG, logger="nats.client"):
            await client.force_reconnect()
        assert client.status == ClientStatus.CLOSED
        assert "private-token" not in caplog.text
        assert "private-password" not in caplog.text
        assert "!!!REDACTED!!!" in caplog.text
    finally:
        await client.close()


@pytest.mark.parametrize("url", ["nats://user:private-password@:4222", "nats://private-token@[broken"])
async def test_establish_error_redacts_credentials(url):
    with pytest.raises(ValueError) as error:
        await establish_connection(url, timeout=1, wants_tls=False)
    rendered = "".join(traceback.format_exception(error.value))
    assert "private-password" not in rendered
    assert "private-token" not in rendered


@pytest.mark.parametrize(
    "url", ["nats://private-token@localhost:bad-port", "invalid://user:private-password@localhost"]
)
async def test_connect_validation_redacts_credentials(url):
    with pytest.raises(ValueError) as error:
        await connect(url)
    rendered = "".join(traceback.format_exception(error.value))
    assert "private-password" not in rendered
    assert "private-token" not in rendered


async def test_websocket_uri_error_redacts_credentials():
    # websockets rejects a username-only URI and includes that URI in its error.
    url = "ws://private-token@localhost:4222"
    with pytest.raises(ConnectionError) as error:
        await connect(url)
    rendered = "".join(traceback.format_exception(error.value))
    assert "private-token" not in rendered
