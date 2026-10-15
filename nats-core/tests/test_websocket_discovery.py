"""Cluster discovery must retain WebSocket framing and TLS intent."""

import asyncio
import json
import socket
import ssl
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path
from urllib.parse import urlparse

import pytest
from nats.client import ServerInfo, _discovered_urls, connect
from nats.client.connection import WebSocketConnection
from nats.client.protocol.types import ServerInfo as ProtocolServerInfo
from nats.server import run


@asynccontextmanager
async def websocket_cluster(tmp_path, secure):
    certs = Path(__file__).parent / "certs"
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        route_port = reservation.getsockname()[1]
    tls_config = (
        "tls { cert_file: "
        + json.dumps(str(certs / "server-cert.pem"))
        + ", key_file: "
        + json.dumps(str(certs / "server-key.pem"))
        + " }"
        if secure
        else "no_tls: true"
    )
    async with AsyncExitStack() as stack:

        async def start_node(name, seed=False):
            config = tmp_path / f"{name}.conf"
            config.write_text(
                f"websocket {{ host: 127.0.0.1, port: -1, {tls_config} }}\n"
                f"cluster {{ name: discovery, host: 127.0.0.1, port: {route_port if seed else -1}, "
                f"routes: {json.dumps([] if seed else [f'nats://127.0.0.1:{route_port}'])} }}\n"
            )
            return await stack.enter_async_context(await run(config_path=str(config), port=0, server_name=name))

        seed = await start_node("seed", seed=True)
        yield seed, start_node


async def wait_for_peer(client, peer):
    port = urlparse(peer.websocket_url).port
    async with asyncio.timeout(5):
        while not any(urlparse(f"ws://{url}").port == port for url in client.server_info.connect_urls or []):
            await asyncio.sleep(0.01)


@pytest.mark.parametrize("transport", ["ws", "wss", "promoted-ws"])
@pytest.mark.parametrize("discovery", ["initial", "async"])
async def test_websocket_cluster_discovery_failover(tmp_path, transport, discovery):
    secure = transport != "ws"
    ctx = ssl.create_default_context(cafile=str(Path(__file__).parent / "certs" / "ca.pem")) if secure else None
    async with websocket_cluster(tmp_path, secure) as (seed, start_node):
        assert seed.websocket_url is not None
        url = seed.websocket_url.replace("wss://", "ws://", 1) if transport == "promoted-ws" else seed.websocket_url
        if discovery == "initial":
            peer = await start_node("peer")
            # Wait for cluster membership to propagate before the connection
            # under test, so its first INFO already contains the peer.
            async with await connect(url, tls=ctx, allow_reconnect=False) as probe:
                await wait_for_peer(probe, peer)

        async with await connect(
            url,
            tls=ctx,
            timeout=1,
            reconnect_time_wait=0.01,
            reconnect_time_wait_max=0.05,
            reconnect_max_attempts=3,
            no_randomize=True,
        ) as client:
            subscription = await client.subscribe("discovery.test")
            reconnected = asyncio.Event()
            client.add_reconnected_callback(reconnected.set)
            if discovery == "async":
                peer = await start_node("peer")
                await wait_for_peer(client, peer)

            await seed.shutdown()
            await asyncio.wait_for(reconnected.wait(), timeout=5)
            await client.publish("discovery.test", b"after failover")
            assert (await subscription.next(timeout=2)).data == b"after failover"
            info = client.server_info
            assert info is not None
            assert info.server_name == "peer"
            assert isinstance(client._connection, WebSocketConnection)
            assert client._connection._ws is not None
            assert bool(client._connection._ws.transport.get_extra_info("ssl_object")) == secure


def server_info(connect_urls: list[str], ws_connect_urls: list[str] | None = None) -> ServerInfo:
    info = ProtocolServerInfo(
        server_id="test",
        server_name="test",
        version="2.0.0",
        go="go1.20",
        host="localhost",
        port=4222,
        headers=True,
        max_payload=1048576,
        proto=1,
        connect_urls=connect_urls,
    )
    if ws_connect_urls is not None:
        info["ws_connect_urls"] = ws_connect_urls
    return ServerInfo.from_protocol(info)


@pytest.mark.parametrize("scheme", ["ws", "wss"])
def test_websocket_discovery_uses_client_scheme_and_strips_userinfo(scheme):
    info = server_info(
        connect_urls=[
            "127.0.0.1:8080",
            "[::1]:8081",
            "ws://user:password@localhost:8082",
            "nats://token@localhost:8083",
            "wss://localhost:8084",
        ]
    )
    assert _discovered_urls(info, scheme) == [
        f"{scheme}://127.0.0.1:8080",
        f"{scheme}://[::1]:8081",
        f"{scheme}://localhost:8082",
        f"{scheme}://localhost:8083",
        f"{scheme}://localhost:8084",
    ]


@pytest.mark.parametrize("ws_urls", [[], ["localhost:8080"]])
def test_websocket_discovery_prefers_explicit_websocket_addresses(ws_urls):
    info = server_info(connect_urls=["localhost:4222"], ws_connect_urls=ws_urls)
    assert _discovered_urls(info, "wss") == [f"wss://{url}" for url in ws_urls]
    assert _discovered_urls(info, None) == ["localhost:4222"]


def test_websocket_discovery_skips_malformed_entries():
    info = server_info(connect_urls=["ws://[broken", "localhost:bad", "ws:///missing-host", "localhost:8080"])
    assert _discovered_urls(info, "wss") == ["wss://localhost:8080"]
