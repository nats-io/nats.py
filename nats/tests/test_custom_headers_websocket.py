import asyncio
import socket
import threading
import queue
import unittest

import pytest
import nats
from tests.utils import async_test

try:
    import aiohttp  # required by nats ws transport

    aiohttp_installed = True
except ModuleNotFoundError:
    aiohttp_installed = False


def start_header_catcher():
    """
    Minimal TCP listener that captures the incoming HTTP request lines
    (WebSocket handshake) and returns them via a Queue.
    """
    q = queue.Queue(maxsize=1)
    ln = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    ln.bind(("127.0.0.1", 0))
    ln.listen(1)
    host, port = ln.getsockname()

    def _accept_once():
        try:
            conn, _ = ln.accept()
            with conn:
                conn.settimeout(2.0)
                buf = b""
                while b"\r\n\r\n" not in buf:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                header_block = buf.split(b"\r\n\r\n", 1)[0]
                lines = header_block.decode("latin1", errors="replace").split("\r\n")
                q.put(lines)
        except Exception:
            q.put([])
        finally:
            try:
                ln.close()
            except OSError:
                pass

    threading.Thread(target=_accept_once, daemon=True).start()
    return f"{host}:{port}", q, (lambda: ln.close())


def has_header_value(headers, name, want):
    prefix = name.lower() + ":"
    for h in headers:
        if ":" not in h:
            continue
        if not h.lower().startswith(prefix):
            continue
        val = h.split(":", 1)[1].strip()
        for part in val.split(","):
            if part.strip().lower() == want.lower():
                return True
    return False


class TestHeaderCatcher(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()

    @async_test
    async def test_ws_headers_static_applied_on_handshake(self):
        if not aiohttp_installed:
            pytest.skip("aiohttp not installed")

        addr, got, close_ln = start_header_catcher()

        custom_headers = {
            "Authorization": ["Bearer Random Token"],
            "X-Multi": ["v1", "v2"],  # repeated header -> comma-joined
            "Accept": ["application/json", "text/plain; q=0.8"],
            "X-Feature-Flags": ["feature-a", "feature-b", "feature-c"],
            "Single-Header-Key": "Single-Header-Value",
        }

        try:
            # Connect to our catcher; it won't complete the upgrade.
            with self.assertRaises(Exception):
                await asyncio.wait_for(
                    nats.connect(
                        f"ws://{addr}",
                        ws_connection_headers=custom_headers,
                        allow_reconnect=False,
                    ),
                    timeout=1.0,
                )
        finally:
            headers = got.get(timeout=2.0)
            close_ln()

        self.assertTrue(has_header_value(headers, "Authorization", "Bearer Random Token"))
        self.assertTrue(has_header_value(headers, "X-Multi", "v1"))
        self.assertTrue(has_header_value(headers, "X-Multi", "v2"))
        self.assertTrue(has_header_value(headers, "Accept", "application/json"))
        self.assertTrue(has_header_value(headers, "Accept", "text/plain; q=0.8"))
        self.assertTrue(has_header_value(headers, "X-Feature-Flags", "feature-a"))
        self.assertTrue(has_header_value(headers, "X-Feature-Flags", "feature-b"))
        self.assertTrue(has_header_value(headers, "X-Feature-Flags", "feature-c"))
        self.assertTrue(has_header_value(headers, "Single-Header-Key", "Single-Header-Value"))
