"""Verification failures remain fatal while explaining the diagnostic limit."""

import json
import ssl
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from nats.client import connect
from nats.client.connection import _verification_message
from nats.server import run


@pytest.mark.parametrize("transport", ["upgrade", "tls-first", "wss"])
@pytest.mark.parametrize("failure", ["hostname", "chain"])
async def test_verification_diagnostic_never_retries_without_verification(tmp_path, monkeypatch, transport, failure):
    import nats.client.connection as module

    certs = Path(__file__).parent / "certs"
    tls = (
        f"cert_file: {json.dumps(str(certs / 'server-cert.pem'))}, "
        f"key_file: {json.dumps(str(certs / 'server-key.pem'))}"
    )
    if transport == "wss":
        config = f"websocket {{ host: 127.0.0.1, port: -1, tls {{ {tls} }} }}"
    else:
        config = f"tls {{ {tls}, handshake_first: {'true' if transport == 'tls-first' else 'false'} }}"
    config_path = tmp_path / "server.conf"
    config_path.write_text(config)
    context = ssl.create_default_context(cafile=str(certs / "ca.pem") if failure == "hostname" else None)
    expected_name = "wrong.example" if failure == "hostname" else "localhost"
    async with await run(config_path=str(config_path), port=0) as server:
        dial = AsyncMock(wraps=module._connect_resolved)
        monkeypatch.setattr(module, "_connect_resolved", dial)
        url = server.websocket_url if transport == "wss" else server.client_url
        assert url is not None
        with pytest.raises(ConnectionError) as raised:
            await connect(
                url,
                tls=context,
                tls_hostname=expected_name,
                tls_handshake_first=transport == "tls-first",
                timeout=2,
                no_randomize=True,
            )
        message = str(raised.value)
        assert expected_name in message
        assert "Presented certificate names are unavailable" in message
        assert "tls_hostname" in message
        assert "client_advertise" in message
        assert context.check_hostname
        assert context.verify_mode == ssl.CERT_REQUIRED
        dial.assert_awaited_once()
        error = raised.value
        while error is not None and not isinstance(error, ssl.SSLCertVerificationError):
            error = error.__cause__
        assert isinstance(error, ssl.SSLCertVerificationError)


def test_verification_diagnostic_bounds_and_escapes_untrusted_text():
    error = ssl.SSLCertVerificationError(1, "failed")
    error.verify_message = "bad\n\x1b[31m" + "x" * 1000
    message = _verification_message("name\r\n" + "y" * 1000, error)
    assert "\n" not in message
    assert "\r" not in message
    assert "\x1b" not in message
    assert "\\x1b" in message
    assert len(message) < 1000


async def test_transport_failure_preserves_original_cause(monkeypatch):
    import nats.client.connection as module

    original = OSError("socket failure")
    monkeypatch.setattr(module.asyncio, "open_connection", AsyncMock(side_effect=original))
    with pytest.raises(ConnectionError) as raised:
        await module.open_tcp_connection("localhost", 4222)
    assert raised.value.__cause__ is original
