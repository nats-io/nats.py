"""Real TLS cluster checks for discovery identity across multiple failovers."""

import asyncio
import json
import socket
import ssl
from contextlib import AsyncExitStack
from pathlib import Path
from urllib.parse import urlparse

import pytest
from nats.client import _Server, connect
from nats.server import run


@pytest.mark.parametrize("handshake_first", [False, True])
@pytest.mark.parametrize("slow_refusal", [False, True], ids=["normal", "slow-refusal"])
async def test_tcp_discovery_keeps_dns_identity_across_two_hops(tmp_path, monkeypatch, handshake_first, slow_refusal):
    # Keep the DNS identity under test without depending on an unused IPv6
    # localhost listener or the runner's external network interfaces.
    getaddrinfo = socket.getaddrinfo

    def resolve(host, port, *args, **kwargs):
        return getaddrinfo("127.0.0.1" if host == "localhost" else host, port, *args, **kwargs)

    monkeypatch.setattr(socket, "getaddrinfo", resolve)
    stopped_ports = set()
    delayed_ports = set()
    loop = asyncio.get_running_loop()
    sock_connect = loop.sock_connect

    async def connect_socket(sock, address):
        if slow_refusal and address[1] in stopped_ports:
            # Windows can spend the entire attempt budget on a stopped peer.
            # Exercise that path deterministically on every platform.
            delayed_ports.add(address[1])
            await asyncio.Event().wait()
        return await sock_connect(sock, address)

    monkeypatch.setattr(loop, "sock_connect", connect_socket)
    certs = Path(__file__).parent / "certs"
    context = ssl.create_default_context(cafile=str(certs / "ca.pem"))
    route_url = None
    async with AsyncExitStack() as stack:

        async def node(name, seed=False):
            path = tmp_path / f"{name}.conf"
            path.write_text(
                f"tls {{ cert_file: {json.dumps(str(certs / 'server-dns-cert.pem'))}, "
                f"key_file: {json.dumps(str(certs / 'server-key.pem'))}, handshake_first: {str(handshake_first).lower()} }}\n"
                f"cluster {{ name: names, host: 127.0.0.1, port: -1, "
                f"routes: {json.dumps([] if seed else [route_url])} }}\n"
                + (f"ports_file_dir: {json.dumps(str(tmp_path))}\n" if seed else "")
            )
            return await stack.enter_async_context(
                await run(config_path=str(path), host="127.0.0.1", port=0, server_name=name)
            )

        seed = await node("seed", True)
        # Read the port actually bound by the seed; never release a reserved
        # port and race another process to bind it again.
        async with asyncio.timeout(5):
            while route_url is None:
                for ports_file in tmp_path.glob("*.ports"):
                    try:
                        routes = json.loads(ports_file.read_text()).get("cluster", [])
                    except (OSError, ValueError):
                        continue
                    if routes:
                        route_url = routes[0]
                if route_url is None:
                    await asyncio.sleep(0.01)
        first = await node("first")
        second = await node("second")
        client = await stack.enter_async_context(
            await connect(
                seed.client_url.replace("127.0.0.1", "localhost"),
                tls=context,
                tls_handshake_first=handshake_first,
                no_randomize=True,
                reconnect_time_wait=0.01,
                # Leave room for the stopped seed's DNS/IP entries and the
                # stopped first peer before the live second peer is attempted.
                reconnect_timeout=0.5,
                timeout=1,
            )
        )

        async def wait_for(port):
            async with asyncio.timeout(3):
                while not any(urlparse(entry.url).port == port for entry in client._server_pool):
                    await asyncio.sleep(0.01)

        first_port, second_port = urlparse(first.client_url).port, urlparse(second.client_url).port
        await wait_for(first_port)
        await wait_for(second_port)
        assert all(urlparse(entry.url).hostname in ("localhost", "127.0.0.1") for entry in client._server_pool)
        client._server_pool[:] = [entry for entry in client._server_pool if urlparse(entry.url).port != second_port]
        changed = asyncio.Event()
        client.add_reconnected_callback(changed.set)
        stopped_ports.add(urlparse(seed.client_url).port)
        await seed.shutdown()
        await asyncio.wait_for(changed.wait(), 3)
        assert client._last_server is not None
        assert client._last_server.tls_hostname == "localhost"
        assert client.server_info is not None and client.server_info.server_name == "first"
        await wait_for(second_port)
        changed.clear()
        stopped_ports.add(first_port)
        await first.shutdown()
        await asyncio.wait_for(changed.wait(), 3)
        assert client._last_server.tls_hostname == "localhost"
        assert client.server_info is not None and client.server_info.server_name == "second"
        sub = await client.subscribe("verified")
        await client.publish("verified", b"ok")
        assert (await sub.next(timeout=1)).data == b"ok"
        if slow_refusal:
            assert delayed_ports, "the reconnect must exercise a stopped peer's full attempt timeout"


async def test_new_addresses_cannot_extend_a_limited_reconnect_cycle(client, monkeypatch):
    from nats.client import _add_discovered_servers

    client._reconnect_max_attempts = 2
    client._reconnect_time_wait = 0
    client._reconnect_time_wait_max = 0
    client._server_pool[:] = [_Server("nats://first:4222")]
    attempts = []

    async def failed(server):
        attempts.append(server.url)
        _add_discovered_servers(
            client._server_pool, [f"nats://new{len(attempts)}:4222"], history=client._reconnect_history
        )
        raise ConnectionError("peer refused")

    monkeypatch.setattr(client, "_reconnect_to_server", failed)
    await asyncio.wait_for(client.force_reconnect(), 1)
    assert len(attempts) <= 3
    assert not client._server_pool


@pytest.mark.parametrize("url", ["nats://[broken", "[broken", "nats://host:not-a-port"])
def test_malformed_manual_pool_entry_keeps_stable_identity(url):
    from nats.client import _server_address

    assert _server_address(url) == url
