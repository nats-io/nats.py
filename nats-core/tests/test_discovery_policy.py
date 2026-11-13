"""Discovery keeps transport, identity, credentials and retry provenance."""

from dataclasses import replace

import pytest
from nats.client import _add_discovered_servers, _discovered_urls, _Server


@pytest.mark.parametrize("scheme,secure", [(None, False), (None, True), ("ws", False), ("wss", True)])
def test_discovery_accepts_only_authorities(client_info, scheme, secure):
    client_info.connect_urls = ["user:pass@peer:4222", "tls://other:4222", "bad/path:4222", None, "[::1]:4222"]
    expected_scheme = scheme or ("tls" if secure else "nats")
    assert _discovered_urls(client_info, scheme, secure) == [
        f"{expected_scheme}://peer:4222",
        f"{expected_scheme}://[::1]:4222",
    ]


@pytest.fixture
def client_info():
    from nats.client import ServerInfo

    return ServerInfo.from_protocol(
        dict(
            server_name="x",
            max_payload=1048576,
            proto=1,
            server_id="x",
            version="2.14.0",
            go="go1",
            host="localhost",
            port=4222,
            headers=True,
        )
    )


@pytest.mark.parametrize("transport", ["tls", "wss"])
def test_ip_name_and_credentials_survive_multiple_hops(transport):
    pool = []
    credentials = ("token", None, None)
    _add_discovered_servers(
        pool, [f"{transport}://10.0.0.2:4222"], credentials, tls_hostname="seed.example", seed_hostname="seed.example"
    )
    first = pool[0]
    _add_discovered_servers(
        pool,
        [f"{transport}://10.0.0.3:4222"],
        first.credentials,
        tls_hostname=first.tls_hostname,
        seed_hostname=first.seed_hostname,
    )
    assert pool[1].tls_hostname == "seed.example"
    assert pool[1].credentials == credentials


def test_later_tls_report_strengthens_only_tls():
    pool = [_Server("nats://configured:4222")]
    first = ("first", None, None)
    _add_discovered_servers(pool, ["nats://10.0.0.2:4222"], first, seed_hostname="original.example", verified=False)
    _add_discovered_servers(
        pool, ["tls://10.0.0.2:4222", "tls://configured:4222"], ("second", None, None), tls_hostname="secure.example"
    )
    assert pool[0].url == "nats://configured:4222"
    assert pool[1].url == "tls://10.0.0.2:4222"
    assert pool[1].tls_hostname == "secure.example"
    assert pool[1].credentials == first
    _add_discovered_servers(pool, ["nats://10.0.0.2:4222"], ("third", None, None), verified=False)
    assert pool[1].url.startswith("tls://")


def test_plaintext_dns_is_not_a_verified_identity():
    pool = []
    _add_discovered_servers(
        pool,
        ["nats://unverified.example:4222", "nats://10.0.0.2:4222"],
        seed_hostname="configured.example",
        verified=False,
    )
    assert pool[0].missing_tls_name
    assert pool[1].tls_hostname == "configured.example"


async def test_info_replaces_peers_but_keeps_seeds_current_and_retry_history(client):
    seed = client._server_pool[0]
    current = _Server("nats://current:4222", is_implicit=True, reconnects=1)
    removed = _Server("nats://old:4222", is_implicit=True, reconnects=4)
    client._server_pool.extend([current, removed])
    client._last_server = current
    info = replace(client.server_info, connect_urls=["new:4222"])
    client._learn_discovered_servers(info, replace=True)
    assert seed in client._server_pool and current in client._server_pool
    assert removed not in client._server_pool
    client._learn_discovered_servers(replace(info, connect_urls=["old:4222"]), replace=True)
    revived = next(s for s in client._server_pool if "old:" in s.url)
    assert revived.reconnects == 4
    before = list(client._server_pool)
    client._learn_discovered_servers(replace(info, connect_urls=[]), replace=True)
    assert client._server_pool == before


def test_new_addresses_inherit_highest_current_failure_count():
    pool = [_Server("nats://seed:4222", reconnects=5)]
    _add_discovered_servers(pool, ["nats://new:4222"])
    assert pool[-1].reconnects == 5


@pytest.mark.parametrize("scheme", [None, "ws"])
@pytest.mark.parametrize("advertised", [["bad/path:4222"], ["tls://peer:4222", "[broken", None]])
async def test_invalid_discovery_update_retains_peers_and_retry_counts(client, scheme, advertised):
    old = _Server("nats://old:4222", is_implicit=True, reconnects=4)
    client._server_pool.append(old)
    client._websocket_scheme = scheme
    before = list(client._server_pool)
    history = dict(client._reconnect_history)
    info = replace(client.server_info, connect_urls=advertised, ws_connect_urls=advertised)
    client._learn_discovered_servers(info, replace=True)
    assert client._server_pool == before
    assert old.reconnects == 4
    assert client._reconnect_history == history
