"""DNS has its own bound; resolved addresses share one network attempt."""

import asyncio
import socket
import urllib.request
from unittest.mock import AsyncMock, Mock
from urllib.parse import urlparse

import pytest
from nats.client import connect
from nats.client.connection import _connect_resolved, resolve_server
from nats.server import run


@pytest.mark.parametrize("websocket", [False, True])
async def test_dns_does_not_consume_connect_or_reconnect_budget(tmp_path, monkeypatch, websocket):
    import nats.client as module

    config = tmp_path / "server.conf"
    config.write_text("websocket { host: 127.0.0.1, port: -1, no_tls: true }")
    async with await run(config_path=str(config), host="127.0.0.1", port=0) as server:
        loop = asyncio.get_running_loop()
        getaddrinfo = loop.getaddrinfo
        lookups = []
        resolved_at = []
        deadlines = []

        async def slow_lookup(host, port, **kwargs):
            lookups.append(host)
            # Ensure a deadline made before DNS is observably earlier even on
            # clocks with coarse resolution, without constraining real I/O.
            await asyncio.sleep(0.05)
            addresses = await getaddrinfo(host, port, **kwargs)
            resolved_at.append(loop.time())
            return addresses

        establish = module.establish_connection

        async def check_deadline(url, *, timeout, deadline, **options):
            # Each initial/reconnect attempt must have its full network budget
            # left after DNS. Checking the deadline detects budget sharing
            # without requiring TCP/WebSocket/PONG to finish within 150 ms.
            assert len(resolved_at) == len(deadlines) + 1
            assert deadline >= resolved_at[-1] + timeout
            deadlines.append(deadline)
            return await establish(url, timeout=timeout, deadline=deadline, **options)

        monkeypatch.setattr(loop, "getaddrinfo", slow_lookup)
        monkeypatch.setattr(module, "establish_connection", check_deadline)
        url = server.websocket_url if websocket else server.client_url
        assert url is not None
        async with await connect(url, timeout=5, no_randomize=True) as client:
            await client.flush(timeout=5)
            await asyncio.wait_for(client.force_reconnect(), 10)
            await client.flush(timeout=5)
        assert len(lookups) == len(deadlines) == 2  # Neither dial repeated DNS.


async def test_dns_timeout_opens_no_socket(monkeypatch):
    import nats.client.connection as module

    async def stalled(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", stalled)
    opener = AsyncMock()
    monkeypatch.setattr(module, "open_tcp_connection", opener)
    with pytest.raises(TimeoutError, match="Name resolution timed out"):
        await connect("nats://example.invalid:4222", timeout=0.02)
    opener.assert_not_called()


async def test_cancelled_lookup_opens_no_socket(monkeypatch):
    import nats.client.connection as module

    entered = asyncio.Event()
    cancelled = asyncio.Event()

    async def stalled(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", stalled)
    opener = AsyncMock()
    monkeypatch.setattr(module, "open_tcp_connection", opener)
    task = asyncio.create_task(connect("nats://example.invalid:4222"))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()
    opener.assert_not_called()


async def test_resolved_addresses_fall_back_without_resolving_again(server, monkeypatch):
    port = urlparse(server.client_url).port
    addresses = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 1)),
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)),
    ]
    loop = asyncio.get_running_loop()
    original_connect = loop.sock_connect

    async def refuse_first(sock, address):
        if address[1] == 1:
            raise ConnectionRefusedError
        return await original_connect(sock, address)

    monkeypatch.setattr(loop, "sock_connect", refuse_first)
    resolver = AsyncMock(return_value=addresses)
    monkeypatch.setattr(loop, "getaddrinfo", resolver)
    async with await connect("nats://example.invalid:4222", timeout=5) as client:
        await client.flush(timeout=1)
    resolver.assert_awaited_once()


async def test_one_deadline_and_socket_cleanup_across_addresses(monkeypatch):
    import nats.client.connection as module

    sockets = [Mock(), Mock()]
    socket_factory = Mock(side_effect=sockets)
    monkeypatch.setattr(module.socket, "socket", socket_factory)
    calls = 0

    async def dial(sock, address):
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.sleep(0.03)
            raise ConnectionRefusedError
        await asyncio.Event().wait()

    monkeypatch.setattr(asyncio.get_running_loop(), "sock_connect", dial)
    addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)) for port in (1, 2)]
    with pytest.raises(TimeoutError):
        async with asyncio.timeout(0.06):
            await _connect_resolved(addresses)
    assert calls == 2
    for sock in sockets:
        sock.close.assert_called_once()


async def test_proxy_dns_remains_with_proxy_transport(monkeypatch):
    monkeypatch.setattr(urllib.request, "getproxies", lambda: {"http": "http://proxy.invalid:8080"})
    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: False)
    resolver = AsyncMock()
    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolver)
    assert await resolve_server("ws://only-resolvable-by-proxy.invalid", 1) is None
    resolver.assert_not_called()


@pytest.mark.parametrize("url,port", [("ws://localhost", 80), ("wss://localhost", 443), ("nats://localhost", 4222)])
async def test_resolution_uses_transport_default_port(monkeypatch, url, port):
    monkeypatch.setattr(urllib.request, "getproxies", lambda: {})
    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: False)
    resolver = AsyncMock(return_value=[])
    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolver)
    assert await resolve_server(url, 1) == []
    assert resolver.call_args.args == ("localhost", port)


@pytest.mark.parametrize(
    "scheme,proxy_scheme,proxied",
    [
        ("ws", "ws", True),
        ("wss", "wss", True),
        ("ws", "socks", True),
        ("wss", "socks", True),
        ("ws", "https", True),
        ("wss", "https", True),
        ("ws", "http", True),
        ("wss", "http", False),
        ("ws", "wss", False),
        ("wss", "ws", False),
    ],
)
@pytest.mark.parametrize("bypass", [False, True])
async def test_proxy_configuration_controls_local_resolution(monkeypatch, scheme, proxy_scheme, proxied, bypass):
    import builtins

    original_import = builtins.__import__

    def public_imports_only(name, *args, **kwargs):
        if name in ("websockets.proxy", "websockets.uri"):
            raise AssertionError("resolution must not import private WebSocket helpers")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", public_imports_only)
    monkeypatch.setattr(urllib.request, "getproxies", lambda: {proxy_scheme: "http://proxy.invalid:8080"})
    monkeypatch.setattr(urllib.request, "proxy_bypass", lambda host: bypass)
    resolver = AsyncMock(return_value=[])
    monkeypatch.setattr(asyncio.get_running_loop(), "getaddrinfo", resolver)
    result = await resolve_server(f"{scheme}://user:password@host.invalid:8443", 1)
    if proxied and not bypass:
        assert result is None
        resolver.assert_not_called()
    else:
        assert result == []
        resolver.assert_awaited_once_with("host.invalid", 8443, type=socket.SOCK_STREAM)
