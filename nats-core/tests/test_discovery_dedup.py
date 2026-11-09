"""Discovery must not duplicate seed URLs or repeated INFO advertisements."""

from urllib.parse import urlparse

import nats.client as client_module
import pytest
from nats.client import _Server, connect
from nats.client.protocol.types import ServerInfo


@pytest.mark.parametrize("phase", ["initial", "info", "reconnect"])
async def test_discovery_deduplicates_seed_and_new_addresses(server, monkeypatch, phase):
    establish = client_module.establish_connection
    hostport = urlparse(server.client_url).netloc
    peer = "127.0.0.1:1"
    advertised = []
    server_info: ServerInfo | None = None

    async def establish_with_discovery(*args, **kwargs):
        nonlocal server_info
        connection, info, tls = await establish(*args, **kwargs)
        info["connect_urls"] = list(advertised)
        server_info = info
        return connection, info, tls

    monkeypatch.setattr(client_module, "establish_connection", establish_with_discovery)
    if phase == "initial":
        advertised[:] = [hostport, peer, peer]
    client = await connect([server.client_url], no_randomize=True, reconnect_time_wait=0)
    try:
        assert server_info is not None
        if phase == "reconnect":
            advertised[:] = [hostport, peer, peer]
            await client.force_reconnect()
        elif phase == "info":
            server_info["connect_urls"] = [hostport, peer, peer]
            await client._handle_info(server_info)

        assert [s.url for s in client._server_pool] == [server.client_url, "nats://" + peer]
        server_info["connect_urls"] = [hostport, peer, peer]
        for _ in range(3):
            await client._handle_info(server_info)
        assert [s.url for s in client._server_pool] == [server.client_url, "nats://" + peer]
    finally:
        await client.close()


@pytest.mark.parametrize(
    "seed, advertised",
    [
        ("tls://user:secret@host:4222", "host:4222"),
        ("nats://[::1]:4222", "[::1]:4222"),
        ("nats://user:secret@[::1]:4222", "[::1]:4222"),
    ],
)
def test_discovery_preserves_seed_scheme_and_credentials(seed, advertised):
    pool = [_Server(url=seed)]
    client_module._add_discovered_servers(pool, [advertised, advertised, "other:4222", "other:4222"])
    assert [s.url for s in pool] == [seed, "other:4222"]
