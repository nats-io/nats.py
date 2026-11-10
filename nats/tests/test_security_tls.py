"""Wire-level regressions for TLS policy and discovery across connection attempts."""

import asyncio
import json
import ssl
from pathlib import Path
from urllib.parse import urlparse

import pytest
import pytest_asyncio

from nats.aio.client import Client
from nats.errors import NoServersError, SecureConnWantedError

CERTS = Path(__file__).parent / "certs"


def client_context():
    return ssl.create_default_context(cafile=str(CERTS / "ca.pem"))


def server_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(CERTS / "server-cert.pem"), str(CERTS / "server-key.pem"))
    return context


class Peer:
    def __init__(self, info=None, tls=False, handshake_first=False, websocket=False, reject_tls=False):
        self.info = dict(info or {})
        self.tls = tls
        self.handshake_first = handshake_first
        self.websocket = websocket
        self.reject_tls = reject_tls
        self.connects = []
        self.http_authorizations = []
        self.connections = []
        self.tasks = []
        self.accepted = 0
        self.after_pong = None
        self.before_pong = None
        self.runner = None

    async def start(self):
        if self.websocket:
            from aiohttp import web

            async def handle(request):
                self.http_authorizations.append(request.headers.get("Authorization"))
                ws = web.WebSocketResponse()
                await ws.prepare(request)
                self.accepted += 1
                self.connections.append(ws)
                await ws.send_bytes(self.info_line())
                async for message in ws:
                    if isinstance(message.data, bytes):
                        await self.command(message.data, ws.send_bytes)
                return ws

            app = web.Application()
            app.router.add_get("/", handle)
            self.runner = web.AppRunner(app)
            await self.runner.setup()
            site = web.TCPSite(self.runner, "127.0.0.1", 0, ssl_context=server_context() if self.tls else None)
            await site.start()
            self.server = site._server
        else:
            self.server = await asyncio.start_server(
                self.handle,
                "127.0.0.1",
                0,
                ssl=server_context() if self.handshake_first else None,
            )
        self.port = self.server.sockets[0].getsockname()[1]
        return self

    def info_line(self):
        return b"INFO " + json.dumps(self.info).encode() + b"\r\n"

    def url(self, scheme="nats"):
        return f"{scheme}://localhost:{self.port}"

    async def command(self, data, send):
        for line in data.splitlines():
            if line.startswith(b"CONNECT "):
                self.connects.append(json.loads(line[8:]))
            elif line == b"PING":
                if self.before_pong is not None:
                    await send(b"INFO " + json.dumps(self.before_pong).encode() + b"\r\n")
                await send(b"PONG\r\n")
                if self.after_pong is not None:
                    await send(b"INFO " + json.dumps(self.after_pong).encode() + b"\r\n")

    async def handle(self, reader, writer):
        self.tasks.append(asyncio.current_task())
        self.connections.append(writer)
        self.accepted += 1
        try:
            writer.write(self.info_line())
            await writer.drain()
            if self.reject_tls:
                return
            if self.tls and not self.handshake_first:
                transport = await asyncio.get_running_loop().start_tls(
                    writer.transport,
                    writer._protocol,
                    server_context(),
                    server_side=True,
                )
                writer._transport = transport

            async def send(data):
                writer.write(data)
                await writer.drain()

            while True:
                data = await reader.readline()
                if not data:
                    break
                await self.command(data, send)
        except (OSError, ValueError):
            # Expected when the client refuses the TLS handshake.
            pass
        finally:
            writer.close()

    async def stop(self):
        if self.runner:
            for ws in self.connections:
                await ws.close()
            await self.runner.cleanup()
        else:
            self.server.close()
            await self.server.wait_closed()
            for writer in self.connections:
                writer.close()
            for task in self.tasks:
                task.cancel()
            await asyncio.gather(*self.tasks, return_exceptions=True)


@pytest_asyncio.fixture
async def peers():
    servers = []

    async def create(**kwargs):
        peer = await Peer(**kwargs).start()
        servers.append(peer)
        return peer

    yield create
    for peer in reversed(servers):
        await peer.stop()


async def ignore_error(error):
    pass


async def connect(nc, servers, **kwargs):
    options = dict(allow_reconnect=False, connect_timeout=0.5, error_cb=ignore_error, dont_randomize=True)
    options.update(kwargs)
    await asyncio.wait_for(nc.connect(servers, **options), 4)


@pytest.mark.parametrize("setting", ["scheme", "upper-case-scheme", "context", "mixed-seeds"])
async def test_tls_intent_rejects_plaintext_without_connect(peers, setting):
    peer = await peers()
    nc = Client()
    servers = peer.url("tls" if setting == "scheme" else "nats")
    if setting == "upper-case-scheme":
        servers = peer.url("TLS")
    if setting == "mixed-seeds":
        servers = [peer.url(), peer.url("tls")]
    try:
        with pytest.raises(SecureConnWantedError):
            await connect(nc, servers, **({"tls": client_context()} if setting == "context" else {}))
        assert peer.connects == []
    finally:
        await nc.close()


@pytest.mark.parametrize("setting", ["scheme", "context", "handshake-first", "required"])
async def test_tls_negotiation_succeeds_and_verifies(peers, setting):
    peer = await peers(
        info={"tls_required" if setting == "required" else "tls_available": True},
        tls=True,
        handshake_first=setting == "handshake-first",
    )
    nc = Client()
    try:
        await connect(
            nc,
            peer.url("tls" if setting == "scheme" else "nats"),
            tls=client_context(),
            tls_handshake_first=setting == "handshake-first",
        )
        assert nc._transport_encrypted
        assert len(peer.connects) == 1
    finally:
        await nc.close()


async def test_server_required_tls_uses_default_verifying_context(peers, monkeypatch):
    peer = await peers(info={"tls_required": True}, tls=True)
    nc = Client()
    try:
        with pytest.raises(ssl.SSLCertVerificationError):
            await connect(nc, peer.url())
        assert peer.connects == []
        context = client_context()
        monkeypatch.setattr(ssl, "create_default_context", lambda: context)
        await connect(nc, peer.url())
        assert len(peer.connects) == 1
    finally:
        await nc.close()


async def test_failed_tls_handshakes_exhaust_retries_without_hanging(peers):
    peer = await peers(info={"tls_required": True}, reject_tls=True)
    nc = Client()
    try:
        with pytest.raises(NoServersError):
            await connect(
                nc,
                peer.url("tls"),
                tls=client_context(),
                allow_reconnect=True,
                max_reconnect_attempts=1,
                reconnect_time_wait=0,
            )
        assert peer.accepted == 2
        assert peer.connects == []
    finally:
        await asyncio.wait_for(nc.close(), 1)


@pytest.mark.parametrize("discovered", [False, True])
async def test_tls_reconnect_never_sends_connect_to_plaintext(peers, discovered):
    plain = await peers()
    secure = await peers(info={"tls_available": True}, tls=True)
    nc = Client()
    try:
        await connect(
            nc,
            [secure.url("tls")] + ([] if discovered else [plain.url()]),
            tls=client_context(),
            allow_reconnect=True,
            max_reconnect_attempts=1,
            reconnect_time_wait=0,
        )
        if discovered:
            secure.after_pong = {"connect_urls": [f"localhost:{plain.port}"]}
            await nc.flush()
            for _ in range(100):
                if nc.discovered_servers:
                    break
                await asyncio.sleep(0.01)
            assert nc.discovered_servers
        # Leave only the plaintext destination for the reconnect.
        nc._server_pool = [s for s in nc._server_pool if s.uri.port == plain.port]
        await nc.force_reconnect()
        for _ in range(200):
            if nc.is_closed:
                break
            await asyncio.sleep(0.01)
        assert nc.is_closed
        assert plain.accepted == 2
        assert plain.connects == []
    finally:
        await nc.close()


@pytest.mark.parametrize("scheme", ["ws", "wss"])
async def test_websocket_context_requires_tls(peers, scheme):
    peer = await peers(websocket=True, tls=True)
    nc = Client()
    try:
        await connect(nc, peer.url(scheme), tls=client_context())
        assert nc._transport_encrypted
        assert len(peer.connects) == 1
    finally:
        await nc.close()


async def test_secure_websocket_discovery_keeps_transport(peers):
    target = await peers(websocket=True, tls=True)
    seed = await peers(websocket=True, tls=True, info={"connect_urls": [f"localhost:{target.port}"]})
    nc = Client()
    reconnected = asyncio.Event()

    async def reconnected_cb():
        reconnected.set()

    try:
        await connect(
            nc,
            seed.url("wss"),
            tls=client_context(),
            allow_reconnect=True,
            reconnect_time_wait=0,
            reconnected_cb=reconnected_cb,
        )
        assert nc.discovered_servers[0].scheme == "wss"
        nc._server_pool = [s for s in nc._server_pool if s.discovered]
        await nc.force_reconnect()
        await asyncio.wait_for(reconnected.wait(), 3)
        assert len(target.connects) == 1
    finally:
        await nc.close()


@pytest.mark.parametrize("websocket", [False, True])
async def test_tls_hostname_verification_cannot_send_connect(peers, websocket):
    peer = await peers(info={"tls_required": True}, tls=True, websocket=websocket)
    nc = Client()
    try:
        # The default trust store rejects this test CA, before any NATS auth.
        with pytest.raises((OSError, NoServersError)):
            await connect(nc, peer.url("wss" if websocket else "tls"), max_reconnect_attempts=1, reconnect_time_wait=0)
        assert peer.connects == []
    finally:
        await asyncio.wait_for(nc.close(), 1)


async def test_websocket_context_does_not_send_plaintext_upgrade(peers):
    # A raw peer records whether the first bytes are an HTTP upgrade or TLS.
    first_bytes = asyncio.Future()

    async def handle(reader, writer):
        data = await reader.read(5)
        if not first_bytes.done():
            first_bytes.set_result(data)
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    nc = Client()
    try:
        port = server.sockets[0].getsockname()[1]
        with pytest.raises(NoServersError):
            await connect(
                nc, f"ws://localhost:{port}", tls=client_context(), max_reconnect_attempts=1, reconnect_time_wait=0
            )
        data = await asyncio.wait_for(first_bytes, 1)
        assert data.startswith(b"\x16\x03")  # TLS handshake record, never HTTP GET.
    finally:
        await nc.close()
        server.close()
        await server.wait_closed()


async def test_replacing_pool_cannot_remove_tls_requirement(peers):
    secure = await peers(tls=True, info={"tls_required": True})
    plain = await peers()
    nc = Client()
    try:
        await connect(
            nc,
            secure.url("tls"),
            tls=client_context(),
            allow_reconnect=True,
            max_reconnect_attempts=1,
            reconnect_time_wait=0,
        )
        nc.set_server_pool([plain.url()])
        await nc.force_reconnect()
        for _ in range(200):
            if nc.is_closed:
                break
            await asyncio.sleep(0.01)
        assert nc.is_closed
        assert plain.accepted == 2
        assert plain.connects == []
    finally:
        await nc.close()
