"""Only discovery received over the established transport may be trusted."""

import asyncio

import pytest

from nats.aio.client import Client
from nats.errors import NoServersError
from tests import test_security_tls as tls_tests
from tests.test_security_tls import client_context, connect

peers = tls_tests.peers


@pytest.mark.parametrize("reject_tls", [False, True])
async def test_plaintext_greeting_discovery_is_discarded(peers, reject_tls):
    attacker = await peers()
    seed = await peers(
        info={"tls_required": True, "connect_urls": [f"localhost:{attacker.port}"]},
        tls=True,
        reject_tls=reject_tls,
    )
    nc = Client()
    discoveries = []

    async def discovered_cb():
        discoveries.append(nc.discovered_servers)

    try:
        if reject_tls:
            with pytest.raises(NoServersError):
                await connect(
                    nc,
                    seed.url(),
                    tls=client_context(),
                    allow_reconnect=True,
                    max_reconnect_attempts=1,
                    reconnect_time_wait=0,
                    discovered_server_cb=discovered_cb,
                )
        else:
            await connect(nc, seed.url(), tls=client_context(), discovered_server_cb=discovered_cb)
        assert nc.discovered_servers == []
        assert "connect_urls" not in nc._server_info
        assert discoveries == []
        assert attacker.accepted == 0
    finally:
        await nc.close()


@pytest.mark.parametrize("timing", ["before", "after"])
async def test_only_fresh_tls_discovery_can_be_used_for_reconnect(peers, timing):
    attacker = await peers()
    target = await peers(tls=True, info={"tls_required": True})
    seed = await peers(
        tls=True,
        info={
            "tls_required": True,
            "connect_urls": [f"localhost:{attacker.port}"],
        },
    )
    setattr(seed, f"{timing}_pong", {"connect_urls": [f"localhost:{target.port}"]})
    nc = Client()
    reconnected = asyncio.Event()

    async def reconnected_cb():
        reconnected.set()

    try:
        await connect(
            nc,
            seed.url("tls"),
            tls=client_context(),
            allow_reconnect=True,
            max_reconnect_attempts=1,
            reconnect_time_wait=0,
            reconnected_cb=reconnected_cb,
        )
        for _ in range(100):
            if nc.discovered_servers:
                break
            await asyncio.sleep(0.01)
        assert [s.port for s in nc.discovered_servers] == [target.port]
        nc._server_pool = [s for s in nc._server_pool if s.discovered]
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 3)
        assert len(target.connects) == 1
        assert attacker.accepted == 0
    finally:
        await nc.close()


@pytest.mark.parametrize("mode", ["plain", "handshake-first", "wss"])
async def test_initial_discovery_allowed_when_no_upgrade_remains(peers, mode):
    target = await peers()
    seed = await peers(
        tls=mode != "plain",
        handshake_first=mode == "handshake-first",
        websocket=mode == "wss",
        info={"connect_urls": [f"localhost:{target.port}"]},
    )
    nc = Client()
    try:
        await connect(
            nc,
            seed.url("wss" if mode == "wss" else "nats"),
            **({"tls": client_context(), "tls_handshake_first": mode == "handshake-first"} if mode != "plain" else {}),
        )
        assert [s.port for s in nc.discovered_servers] == [target.port]
    finally:
        await nc.close()


async def test_reconnect_does_not_accept_plaintext_greeting_discovery(peers):
    attacker = await peers()
    seed = await peers(tls=True, info={"tls_required": True})
    nc = Client()
    reconnected = asyncio.Event()

    async def reconnected_cb():
        reconnected.set()

    try:
        await connect(
            nc,
            seed.url(),
            tls=client_context(),
            allow_reconnect=True,
            reconnect_time_wait=0,
            reconnected_cb=reconnected_cb,
        )
        seed.info["connect_urls"] = [f"localhost:{attacker.port}"]
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 3)
        assert nc.discovered_servers == []
        assert attacker.accepted == 0
    finally:
        await nc.close()
