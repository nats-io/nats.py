"""Tests for the inline protocol parser in Client._read_loop."""

import pytest
from nats.client import Client, ServerInfo
from nats.client.protocol.message import MAX_HEADER_SIZE, MAX_PAYLOAD_SIZE


class ScriptedConnection:
    """Connection that hands out a fixed sequence of chunks, then EOF."""

    def __init__(self, chunks: list[bytes]) -> None:
        self._chunks = list(chunks)
        self.written: list[bytes] = []
        self.closed = False

    async def read(self, n: int) -> bytes:
        return self._chunks.pop(0) if self._chunks else b""

    async def write(self, data: bytes) -> None:
        self.written.append(data)

    async def close(self) -> None:
        self.closed = True

    def is_connected(self) -> bool:
        return not self.closed

    async def readline(self) -> bytes:
        raise NotImplementedError

    async def readexactly(self, n: int) -> bytes:
        raise NotImplementedError


def _server_info() -> ServerInfo:
    return ServerInfo(
        server_id="test",
        server_name="test",
        version="0.0.0",
        go_version="go0",
        host="127.0.0.1",
        port=4222,
        headers=True,
        auth_required=False,
        tls_required=False,
        tls_available=False,
        tls_verify=False,
        max_payload=1024 * 1024,
        proto=1,
    )


async def run_read_loop(chunks: list[bytes]) -> tuple[list[tuple], bool]:
    """Feed chunks through _read_loop and return (dispatched events, disconnected)."""
    client = Client(ScriptedConnection(chunks), _server_info(), servers=["nats://127.0.0.1:4222"])
    events: list[tuple] = []
    disconnected = False

    async def handle_msg(subject, sid, reply, payload):
        events.append(("MSG", subject, sid, reply, payload))

    async def handle_hmsg(subject, sid, reply, headers, payload, status_code=None, status_description=None):
        events.append(("HMSG", subject, sid, reply, headers, payload, status_code, status_description))

    async def handle_ping():
        events.append(("PING",))

    async def handle_pong():
        events.append(("PONG",))

    async def handle_info(info):
        events.append(("INFO", info))

    async def handle_error(error):
        events.append(("ERR", error))

    async def force_disconnect():
        nonlocal disconnected
        disconnected = True

    client._handle_msg = handle_msg  # type: ignore[method-assign]
    client._handle_hmsg = handle_hmsg  # type: ignore[method-assign]
    client._handle_ping = handle_ping  # type: ignore[method-assign]
    client._handle_pong = handle_pong  # type: ignore[method-assign]
    client._handle_info = handle_info  # type: ignore[method-assign]
    client._handle_error = handle_error  # type: ignore[method-assign]
    client._force_disconnect = force_disconnect  # type: ignore[method-assign]

    await client._read_loop()
    return events, disconnected


MSG = b"MSG foo.bar 1 5\r\nhello\r\n"
MSG_REPLY = b"MSG foo.bar 2 _INBOX.x 3\r\nabc\r\n"
HMSG = b"HMSG foo.bar 3 24 29\r\nNATS/1.0\r\nA: 1\r\nB: 2\r\n\r\nhello\r\n"
HMSG_REPLY_STATUS = b"HMSG foo.bar 4 _INBOX.y 30 30\r\nNATS/1.0 503 No Responders\r\n\r\n\r\n"
INFO = b'INFO {"server_id":"abc","max_payload":1024}\r\n'
ERR = b"-ERR 'Authorization Violation'\r\n"

EXPECTED = [
    ("MSG", "foo.bar", "1", None, b"hello"),
    ("MSG", "foo.bar", "2", "_INBOX.x", b"abc"),
    ("HMSG", "foo.bar", "3", None, {"A": ["1"], "B": ["2"]}, b"hello", None, None),
    ("HMSG", "foo.bar", "4", "_INBOX.y", {}, b"", "503", "No Responders"),
    ("PING",),
    ("PONG",),
    ("INFO", {"server_id": "abc", "max_payload": 1024}),
    ("ERR", "Authorization Violation"),
]

STREAM = MSG + MSG_REPLY + HMSG + HMSG_REPLY_STATUS + b"PING\r\n" + b"PONG\r\n" + INFO + b"+OK\r\n" + ERR


async def test_read_loop_parses_every_message_type_in_one_chunk():
    events, disconnected = await run_read_loop([STREAM])

    assert events == EXPECTED
    assert disconnected


async def test_read_loop_reassembles_stream_split_at_every_byte():
    for split in range(1, len(STREAM)):
        events, _ = await run_read_loop([STREAM[:split], STREAM[split:]])
        assert events == EXPECTED, f"split at byte {split}"


async def test_read_loop_reassembles_stream_delivered_one_byte_at_a_time():
    events, _ = await run_read_loop([STREAM[i : i + 1] for i in range(len(STREAM))])

    assert events == EXPECTED


async def test_read_loop_handles_message_spanning_three_chunks():
    payload = b"x" * 200_000
    message = b"MSG big 1 %d\r\n" % len(payload) + payload + b"\r\n"

    events, _ = await run_read_loop([message[:10], message[10:100_000], message[100_000:]])

    assert events == [("MSG", "big", "1", None, payload)]


async def test_read_loop_handles_empty_payload():
    events, _ = await run_read_loop([b"MSG foo 1 0\r\n\r\n"])

    assert events == [("MSG", "foo", "1", None, b"")]


async def test_read_loop_disconnects_on_server_eof():
    events, disconnected = await run_read_loop([])

    assert events == []
    assert disconnected


@pytest.mark.parametrize(
    "chunk",
    [
        pytest.param(b"MSG foo 1 abc\r\n", id="msg-size-not-int"),
        pytest.param(b"MSG foo\r\n", id="msg-too-few-args"),
        pytest.param(b"MSG foo 1 %d\r\n" % (MAX_PAYLOAD_SIZE + 1), id="msg-payload-too-large"),
        pytest.param(b"MSG foo 1 5\r\nhelloXX", id="msg-missing-trailing-crlf"),
        pytest.param(b"HMSG foo 1 x 5\r\n", id="hmsg-size-not-int"),
        pytest.param(b"HMSG foo 1 5\r\n", id="hmsg-too-few-args"),
        pytest.param(b"HMSG foo 1 %d %d\r\n" % (MAX_HEADER_SIZE + 1, MAX_HEADER_SIZE + 1), id="hmsg-headers-too-large"),
        pytest.param(b"HMSG foo 1 10 %d\r\n" % (MAX_PAYLOAD_SIZE + 1), id="hmsg-total-too-large"),
        pytest.param(b"HMSG foo 1 10 5\r\n", id="hmsg-header-exceeds-total"),
        pytest.param(b"HMSG foo 1 12 12\r\nNATS/1.0\r\n\r\nXX", id="hmsg-missing-trailing-crlf"),
        pytest.param(b"MSX foo 1 5\r\nhello\r\n", id="unknown-op-m"),
        pytest.param(b"HELLO foo\r\n", id="unknown-op-h"),
        pytest.param(b"PUNG\r\n", id="unknown-op-p"),
        pytest.param(b"INFX {}\r\n", id="unknown-op-i"),
        pytest.param(b"+NO\r\n", id="unknown-op-plus"),
        pytest.param(b"-NOPE x\r\n", id="unknown-op-minus"),
        pytest.param(b"XYZ\r\n", id="unknown-op-other"),
        pytest.param(b"MSG " + b"a" * 5000, id="control-line-too-long"),
    ],
)
async def test_read_loop_disconnects_on_framing_error(chunk):
    events, disconnected = await run_read_loop([chunk, MSG])

    assert events == []
    assert disconnected


async def test_read_loop_drops_msg_with_invalid_utf8_subject_and_continues():
    events, _ = await run_read_loop([b"MSG \xff\xfe 1 2\r\nhi\r\n" + MSG])

    assert events == [("MSG", "foo.bar", "1", None, b"hello")]


async def test_read_loop_drops_hmsg_with_invalid_headers_and_continues():
    bad = b"HMSG foo 1 16 18\r\nNATS/1.0\r\nnope\r\nhi\r\n"

    events, _ = await run_read_loop([bad + MSG])

    assert events == [("MSG", "foo.bar", "1", None, b"hello")]


async def test_read_loop_drops_info_with_invalid_json_and_continues():
    events, _ = await run_read_loop([b"INFO {not json}\r\n" + MSG])

    assert events == [("MSG", "foo.bar", "1", None, b"hello")]


async def test_read_loop_does_not_confuse_payload_bytes_with_protocol():
    payload = b"PING\r\nMSG foo 1 5\r\nhello\r\n"
    message = b"MSG foo 1 %d\r\n" % len(payload) + payload + b"\r\n"

    events, _ = await run_read_loop([message])

    assert events == [("MSG", "foo", "1", None, payload)]
