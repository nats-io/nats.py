"""TLS policy options and diagnostics."""

import ssl
from unittest.mock import AsyncMock

import pytest
from nats.client import connect
from nats.client.connection import establish_connection
from nats.client.errors import ServerError, quote_server_text


@pytest.mark.parametrize("option", ["tls_required", "tls_handshake_first"])
async def test_conflicting_tls_policy_fails_before_network(monkeypatch, option):
    import nats.client as module

    dial = AsyncMock()
    monkeypatch.setattr(module, "establish_connection", dial)
    with pytest.raises(ValueError, match="tls_optional"):
        await connect(
            tls_optional=True,
            tls_required=option == "tls_required",
            tls_handshake_first=option == "tls_handshake_first",
        )
    dial.assert_not_called()


async def test_optional_context_allows_plaintext(server):
    async with await connect(server.client_url, tls=ssl.create_default_context(), tls_optional=True) as client:
        await client.flush(timeout=1)


async def test_context_cannot_silently_disable_verification(server):
    context = ssl.create_default_context()
    context.check_hostname = False
    with pytest.raises(ValueError, match="tls_skip_verify"):
        await connect(server.client_url, tls=context)


async def test_ignore_discovery_keeps_configured_pool(client):
    client._ignore_discovered_servers = True
    before = list(client._server_pool)
    info = client.server_info
    info.connect_urls = ["peer:4222"]
    client._learn_discovered_servers(info)
    assert client._server_pool == before


@pytest.mark.parametrize("name,expected", [("nats.example.", "nats.example"), ("nats.example..", "nats.example.")])
async def test_explicit_tls_name_strips_one_dot(monkeypatch, name, expected):
    import nats.client.connection as module

    connection = AsyncMock()
    connection.readline.return_value = b"INFO {}\r\n"
    dial = AsyncMock(return_value=connection)
    monkeypatch.setattr(module, "open_tcp_connection", dial)
    await establish_connection(
        "tls://host.:4222", timeout=1, wants_tls=True, tls_handshake_first=True, tls_hostname=name
    )
    assert dial.call_args.kwargs["server_hostname"] == expected


async def test_seed_tls_name_strips_dot_without_changing_dial_host(monkeypatch):
    import nats.client.connection as module

    connection = AsyncMock()
    connection.readline.return_value = b"INFO {}\r\n"
    dial = AsyncMock(return_value=connection)
    monkeypatch.setattr(module, "open_tcp_connection", dial)
    await establish_connection("tls://host.:4222", timeout=1, wants_tls=True, tls_handshake_first=True)
    assert dial.call_args.args[0] == "host."
    assert dial.call_args.kwargs["server_hostname"] == "host"


def test_server_text_is_bounded_and_terminal_safe():
    raw = "bad\r\n\x1b[31m" + "x" * 1000
    error = ServerError(raw)
    assert error.message == raw
    assert "\x1b" not in str(error)
    assert "\n" not in str(error)
    assert "\\x1b" in str(error)
    assert len(quote_server_text(raw)) < 300


@pytest.mark.parametrize("check_hostname,verify_mode", [(True, ssl.CERT_REQUIRED), (False, ssl.CERT_REQUIRED)])
async def test_skip_verify_does_not_mutate_supplied_context(monkeypatch, check_hostname, verify_mode):
    import nats.client as module

    context = ssl.create_default_context()
    context.check_hostname = check_hostname
    context.verify_mode = verify_mode
    dial = AsyncMock(side_effect=AssertionError("must fail before connecting"))
    monkeypatch.setattr(module, "establish_connection", dial)
    try:
        with pytest.raises(ValueError, match="already disable"):
            await connect(tls=context, tls_skip_verify=True)
    finally:
        assert context.check_hostname == check_hostname
        assert context.verify_mode == verify_mode
    dial.assert_not_called()


async def test_skip_verify_creates_private_context(server):
    async with await connect(server.client_url, tls_skip_verify=True, tls_optional=True) as client:
        assert client._tls is not None
        assert not client._tls.check_hostname
        assert client._tls.verify_mode == ssl.CERT_NONE


def test_local_tls_error_is_not_labeled_server_text():
    from nats.client.errors import SecureConnectionRequiredError, server_error_from_message

    assert str(SecureConnectionRequiredError()) == "secure connection required but server does not offer TLS"
    raw = "Secure Connection - TLS Required \n\x1b[31m"
    error = server_error_from_message(raw)
    assert isinstance(error, SecureConnectionRequiredError)
    assert error.message == raw
    assert "Untrusted server text:" in str(error)
    assert "\n" not in str(error)
    assert "\x1b" not in str(error)
