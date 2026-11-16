"""Server advertisements cannot supply credentials for a new connection."""

import asyncio

import pytest

from nats.aio.client import Client
from tests import test_security_tls as tls_tests
from tests.test_security_tls import client_context, connect

peers = tls_tests.peers


def auth_fields(command):
    return {key: command[key] for key in ("user", "pass", "auth_token") if key in command}


async def discovered(nc, port):
    for _ in range(100):
        if any(s.port == port for s in nc.discovered_servers):
            return
        await asyncio.sleep(0.01)
    pytest.fail("discovery was not received")


@pytest.mark.parametrize("mode", ["plain", "tls", "wss"])
@pytest.mark.parametrize(
    "credentials,options,expected",
    [
        ("alice:client-secret", {}, {"user": "alice", "pass": "client-secret"}),
        ("client-token", {}, {"auth_token": "client-token"}),
        ("alice:", {}, {"user": "alice", "pass": ""}),
        (None, {}, {}),
        ("alice:client-secret", {"token": "option-token"}, {"auth_token": "option-token"}),
    ],
)
async def test_discovery_reuses_client_auth_and_strips_advertised_userinfo(peers, mode, credentials, options, expected):
    peer_options = dict(
        tls=mode != "plain", websocket=mode == "wss", info={"auth_required": True, "tls_required": mode == "tls"}
    )
    target = await peers(**peer_options)
    seed = await peers(**peer_options)
    seed.after_pong = {"connect_urls": [f"attacker:injected-secret@localhost:{target.port}"]}
    nc = Client()
    reconnected = asyncio.Event()

    async def reconnected_cb():
        reconnected.set()

    url = seed.url({"plain": "nats", "tls": "tls", "wss": "wss"}[mode])
    if credentials is not None:
        url = url.replace("://", f"://{credentials}@")
    try:
        await connect(
            nc,
            url,
            **options,
            **({"tls": client_context()} if mode != "plain" else {}),
            allow_reconnect=True,
            reconnect_time_wait=0,
            reconnected_cb=reconnected_cb,
        )
        await discovered(nc, target.port)
        assert nc.discovered_servers[0].username is None
        assert nc.discovered_servers[0].password is None
        entries = [s for s in nc._server_pool if s.discovered]
        assert "client-secret" not in repr(entries)
        assert "injected-secret" not in repr(entries)
        nc._server_pool = entries
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 3)
        assert auth_fields(seed.connects[0]) == expected
        assert auth_fields(target.connects[0]) == expected
        if mode == "wss":
            assert target.http_authorizations == [None]
    finally:
        await nc.close()


async def test_client_url_credentials_survive_multiple_discovery_hops(peers):
    third = await peers(info={"auth_required": True})
    second = await peers(info={"auth_required": True})
    first = await peers(info={"auth_required": True})
    first.after_pong = {"connect_urls": [f"bad:first@localhost:{second.port}"]}
    second.after_pong = {"connect_urls": [f"bad:second@localhost:{third.port}"]}
    nc = Client()
    reconnected = asyncio.Event()

    async def reconnected_cb():
        reconnected.set()

    try:
        await connect(
            nc,
            first.url().replace("://", "://client:secret@"),
            allow_reconnect=True,
            reconnect_time_wait=0,
            reconnected_cb=reconnected_cb,
        )
        for target in (second, third):
            await discovered(nc, target.port)
            nc._server_pool = [s for s in nc._server_pool if s.uri.port == target.port]
            reconnected.clear()
            await nc.force_reconnect()
            await asyncio.wait_for(reconnected.wait(), 3)
            assert auth_fields(target.connects[0]) == {"user": "client", "pass": "secret"}
    finally:
        await nc.close()


async def test_advertisement_cannot_replace_configured_seed_credentials(peers):
    target = await peers(info={"auth_required": True})
    seed = await peers(info={"auth_required": True})
    seed.after_pong = {"connect_urls": [f"attacker:secret@localhost:{target.port}"]}
    nc = Client()
    reconnected = asyncio.Event()

    async def reconnected_cb():
        reconnected.set()

    try:
        await connect(
            nc,
            [seed.url().replace("://", "://first:password@"), target.url().replace("://", "://second:password@")],
            allow_reconnect=True,
            reconnect_time_wait=0,
            reconnected_cb=reconnected_cb,
        )
        # Ensure the async INFO has been read before inspecting the pool.
        await nc.flush()
        await nc.flush()
        assert nc.discovered_servers == []
        assert len(nc.server_pool) == 2
        nc._server_pool = [s for s in nc._server_pool if s.uri.port == target.port]
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 3)
        assert auth_fields(target.connects[0]) == {"user": "second", "pass": "password"}
    finally:
        await nc.close()


async def test_discovery_accepts_only_addresses_and_strips_ipv6_userinfo(peers):
    seed = await peers()
    seed.after_pong = {
        "connect_urls": [
            "attacker:secret@[::1]:4223",
            "host:4224",
            "wss://evil:443",
            "host:4222/path",
            "host:4222?query",
            "host:4222#fragment",
            "host:bad",
            "host:65536",
            "host:0",
            "[broken",
            "host",
            "host:42\n23",
            None,
            123,
        ]
    }
    nc = Client()
    try:
        await connect(nc, seed.url())
        await discovered(nc, 4223)
        assert [(s.hostname, s.port, s.username) for s in nc.discovered_servers] == [
            ("::1", 4223, None),
            ("host", 4224, None),
        ]
    finally:
        await nc.close()


@pytest.mark.parametrize("mode", ["plain", "tls", "wss"])
async def test_pool_replacement_keeps_discovery_credentials_bound_to_active_server(peers, mode):
    peer_options = dict(
        tls=mode != "plain", websocket=mode == "wss", info={"auth_required": True, "tls_required": mode == "tls"}
    )
    old = await peers(**peer_options)
    new = await peers(**peer_options)
    target = await peers(**peer_options)
    scheme = {"plain": "nats", "tls": "tls", "wss": "wss"}[mode]
    old_url = old.url(scheme).replace("://", "://old:old-secret@")
    new_url = new.url(scheme).replace("://", "://new:new-secret@")
    nc = Client()
    reconnected = asyncio.Event()

    async def reconnected_cb():
        reconnected.set()

    try:
        await connect(
            nc,
            old_url,
            **({"tls": client_context()} if mode != "plain" else {}),
            allow_reconnect=True,
            reconnect_time_wait=0,
            max_reconnect_attempts=1,
            reconnected_cb=reconnected_cb,
        )
        nc.set_server_pool([new_url])
        old.after_pong = {"connect_urls": [f"localhost:{target.port}"]}
        await nc.flush()
        await discovered(nc, target.port)

        # The new configured seed is unavailable, so failover must use the
        # destination advertised by the still-connected old server.
        new.server.close()
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 3)
        assert auth_fields(target.connects[0]) == {"user": "old", "pass": "old-secret"}
        assert new.connects == []
        if mode == "wss":
            assert target.http_authorizations == [None]
    finally:
        await nc.close()


@pytest.mark.parametrize("mode", ["plain", "tls", "wss"])
@pytest.mark.parametrize("replacement_credentials", [None, "replacement:password", "replacement-token"])
async def test_pool_roundtrip_preserves_discovered_credentials(peers, mode, replacement_credentials):
    peer_options = dict(
        tls=mode != "plain", websocket=mode == "wss", info={"auth_required": True, "tls_required": mode == "tls"}
    )
    target = await peers(**peer_options)
    seed = await peers(**peer_options)
    scheme = {"plain": "nats", "tls": "tls", "wss": "wss"}[mode]
    seed.after_pong = {"connect_urls": [f"localhost:{target.port}"]}
    nc = Client()
    reconnected = asyncio.Event()

    async def reconnected_cb():
        reconnected.set()

    try:
        await connect(
            nc,
            seed.url(scheme).replace("://", "://client:secret@"),
            **({"tls": client_context()} if mode != "plain" else {}),
            allow_reconnect=True,
            reconnect_time_wait=0,
            max_reconnect_attempts=1,
            reconnected_cb=reconnected_cb,
        )
        await discovered(nc, target.port)
        urls = [s.geturl() for s in nc.servers]
        if replacement_credentials is not None:
            urls = [
                url.replace("://", f"://{replacement_credentials}@") if url == target.url(scheme) else url
                for url in urls
            ]
        nc.set_server_pool(urls)
        nc.set_server_pool([s.geturl() for s in nc.servers])

        # Stop accepting connections without closing the active connection.
        seed.server.close()
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 3)
        expected = {"user": "client", "pass": "secret"}
        if replacement_credentials == "replacement:password":
            expected = {"user": "replacement", "pass": "password"}
        elif replacement_credentials == "replacement-token":
            expected = {"auth_token": "replacement-token"}
        assert auth_fields(target.connects[0]) == expected
    finally:
        await nc.close()
