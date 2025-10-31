"""Tests for NATS protocol message parsing and command encoding."""

import asyncio
import json

import pytest
from nats.client.protocol.command import (
    encode_connect,
    encode_hpub,
    encode_ping,
    encode_pong,
    encode_pub,
    encode_sub,
    encode_unsub,
)
from nats.client.protocol.message import (
    ParseError,
    parse_headers,
    parse_hmsg,
    parse_msg,
)
from nats.client.protocol.types import ConnectInfo


@pytest.mark.asyncio
async def test_parse_msg():
    """Test parsing MSG messages."""
    # Test valid MSG without reply
    reader = asyncio.StreamReader()
    reader.feed_data(b"hello\r\n")
    reader.feed_eof()

    msg = await parse_msg(reader, [b"foo.bar", b"1", b"5"])
    assert msg.subject == "foo.bar"
    assert msg.sid == "1"
    assert msg.reply_to is None
    assert msg.payload == b"hello"

    # Test valid MSG with reply
    reader = asyncio.StreamReader()
    reader.feed_data(b"hello\r\n")
    reader.feed_eof()

    msg = await parse_msg(reader, [b"foo.bar", b"1", b"reply.to", b"5"])
    assert msg.subject == "foo.bar"
    assert msg.sid == "1"
    assert msg.reply_to == "reply.to"
    assert msg.payload == b"hello"

    # Test invalid size
    reader = asyncio.StreamReader()
    with pytest.raises(ValueError):
        await parse_msg(reader, [b"foo.bar", b"1", b"invalid"])

    # Test not enough arguments
    reader = asyncio.StreamReader()
    with pytest.raises(ParseError, match="Invalid MSG: not enough arguments"):
        await parse_msg(reader, [b"foo.bar", b"1"])

    # Test payload too large
    reader = asyncio.StreamReader()
    with pytest.raises(ParseError, match="Payload too large"):
        await parse_msg(reader, [b"foo.bar", b"1", b"67108865"])


@pytest.mark.asyncio
async def test_parse_hmsg():
    """Test parsing HMSG messages."""
    # Test valid HMSG
    reader = asyncio.StreamReader()
    header_data = b"NATS/1.0\r\n\r\n"
    payload = b"hello"
    reader.feed_data(header_data + payload + b"\r\n")
    reader.feed_eof()

    header_size = len(header_data)
    total_size = header_size + len(payload)
    msg = await parse_hmsg(reader, [b"foo.bar", b"1", b"reply.to", str(header_size).encode(), str(total_size).encode()])
    assert msg.subject == "foo.bar"
    assert msg.sid == "1"
    assert msg.reply_to == "reply.to"
    assert msg.payload == b"hello"
    assert msg.headers == {}

    # Test invalid sizes
    reader = asyncio.StreamReader()
    with pytest.raises(ValueError):
        await parse_hmsg(reader, [b"foo.bar", b"1", b"reply.to", b"invalid", b"52"])

    # Test header size too large
    reader = asyncio.StreamReader()
    with pytest.raises(ParseError, match="Headers too large"):
        await parse_hmsg(reader, [b"foo.bar", b"1", b"reply.to", b"65537", b"65538"])

    # Test total size too large
    reader = asyncio.StreamReader()
    with pytest.raises(ParseError, match="Total message too large"):
        await parse_hmsg(reader, [b"foo.bar", b"1", b"reply.to", b"10", b"67108865"])

    # Test not enough arguments
    reader = asyncio.StreamReader()
    with pytest.raises(ParseError, match="Invalid HMSG: not enough arguments"):
        await parse_hmsg(reader, [b"foo.bar", b"1", b"10"])


def test_parse_headers():
    """Test parsing message headers."""
    # Test valid headers
    header_data = b"NATS/1.0\r\nfoo: bar\r\nmulti: val1\r\nmulti: val2\r\n\r\n"
    headers, status_code, status_description = parse_headers(header_data)
    assert headers == {
        "foo": ["bar"],
        "multi": ["val1", "val2"],
    }
    assert status_code is None
    assert status_description is None

    # Test headers with status
    header_data_with_status = b"NATS/1.0 503 No Responders\r\nfoo: bar\r\n\r\n"
    headers, status_code, status_description = parse_headers(header_data_with_status)
    assert headers == {"foo": ["bar"]}
    assert status_code == "503"
    assert status_description == "No Responders"

    # Test status only (no headers)
    status_data = b"NATS/1.0 503\r\n\r\n"
    headers, status_code, status_description = parse_headers(status_data)
    assert headers == {}
    assert status_code == "503"
    assert status_description is None

    # Test missing version
    with pytest.raises(ParseError, match="Invalid header format"):
        parse_headers(b"foo: bar\r\n\r\n")

    # Test invalid encoding
    with pytest.raises(ParseError, match="Invalid header encoding"):
        parse_headers(b"\xff\xff")


def test_encode_connect():
    """Test encoding CONNECT command."""
    info = ConnectInfo(
        verbose=False,
        pedantic=False,
        tls_required=False,
        lang="python",
        version="0.1.0",
        protocol=1,
        echo=True,
        no_responders=False,
        headers=True,
    )
    command = encode_connect(info)
    assert command.startswith(b"CONNECT {")
    assert command.endswith(b"}\r\n")

    # Verify JSON is valid
    json_str = command[8:-2].decode()  # Remove CONNECT and \r\n
    data = json.loads(json_str)
    assert data["lang"] == "python"
    assert data["version"] == "0.1.0"
    assert data["protocol"] == 1
    assert data["headers"] is True


def test_encode_pub():
    """Test encoding PUB command."""
    # Test without reply
    command = encode_pub("foo.bar", b"hello")
    assert command == [b"PUB foo.bar 5\r\n", b"hello", b"\r\n"]

    # Test with reply
    command = encode_pub("foo.bar", b"hello", reply_to="reply.to")
    assert command == [b"PUB foo.bar reply.to 5\r\n", b"hello", b"\r\n"]


def test_encode_hpub():
    """Test encoding HPUB command."""
    headers = {"foo": "bar", "multi": ["val1", "val2"]}
    payload = b"hello"

    # Test without reply
    command = encode_hpub("foo.bar", payload, headers=headers)
    assert len(command) == 4
    assert command[0].startswith(b"HPUB foo.bar")
    assert command[1].startswith(b"NATS/1.0\r\n")
    assert command[2] == payload
    assert command[3] == b"\r\n"

    # Test with reply
    command = encode_hpub("foo.bar", payload, reply_to="reply.to", headers=headers)
    assert len(command) == 4
    assert command[0].startswith(b"HPUB foo.bar reply.to")
    assert command[1].startswith(b"NATS/1.0\r\n")
    assert command[2] == payload
    assert command[3] == b"\r\n"


def test_encode_sub():
    """Test encoding SUB command."""
    # Test without queue group
    command = encode_sub("foo.bar", "1")
    assert command == b"SUB foo.bar 1\r\n"

    # Test with queue group
    command = encode_sub("foo.bar", "1", "queue")
    assert command == b"SUB foo.bar queue 1\r\n"


def test_encode_unsub():
    """Test encoding UNSUB command."""
    # Test without max messages
    command = encode_unsub("1")
    assert command == b"UNSUB 1\r\n"

    # Test with max messages
    command = encode_unsub("1", 5)
    assert command == b"UNSUB 1 5\r\n"


def test_encode_ping():
    """Test encoding PING command."""
    command = encode_ping()
    assert command == b"PING\r\n"


def test_encode_pong():
    """Test encoding PONG command."""
    command = encode_pong()
    assert command == b"PONG\r\n"


def test_parse_headers_unicode_error():
    """Test parsing headers with invalid UTF-8."""
    # Invalid UTF-8 in headers
    with pytest.raises(ParseError, match="Invalid header encoding"):
        parse_headers(b"NATS/1.0\r\n\xff\xfe\r\n\r\n")


def test_parse_headers_missing_colon():
    """Test parsing header line without colon."""
    with pytest.raises(ParseError, match="Invalid header line"):
        parse_headers(b"NATS/1.0\r\nInvalidHeaderLine\r\n\r\n")


@pytest.mark.asyncio
async def test_parse_info_missing_json():
    """Test parsing INFO message without JSON data."""
    from nats.client.protocol.message import parse_info

    with pytest.raises(ParseError, match="INFO message missing JSON data"):
        await parse_info([])


@pytest.mark.asyncio
async def test_parse_info_invalid_json():
    """Test parsing INFO message with invalid JSON."""
    from nats.client.protocol.message import parse_info

    with pytest.raises(ParseError, match="Invalid INFO JSON"):
        await parse_info([b"not-valid-json"])


@pytest.mark.asyncio
async def test_parse_err_missing_text():
    """Test parsing ERR message without error text."""
    from nats.client.protocol.message import parse_err

    with pytest.raises(ParseError, match="ERR message missing error text"):
        await parse_err([])


@pytest.mark.asyncio
async def test_parse_err_with_quotes():
    """Test parsing ERR message with quoted text."""
    from nats.client.protocol.message import parse_err

    err = await parse_err([b"'Permission", b"Denied'"])
    assert err.op == "ERR"
    assert err.error == "Permission Denied"


@pytest.mark.asyncio
async def test_parse_err_without_quotes():
    """Test parsing ERR message without quotes."""
    from nats.client.protocol.message import parse_err

    err = await parse_err([b"Unknown", b"Protocol", b"Error"])
    assert err.op == "ERR"
    assert err.error == "Unknown Protocol Error"


@pytest.mark.asyncio
async def test_parse_ping_message():
    """Test parsing PING message through parse() function."""
    from nats.client.protocol.message import parse

    reader = asyncio.StreamReader()
    reader.feed_data(b"PING\r\n")
    reader.feed_eof()

    msg = await parse(reader)
    assert msg is not None
    assert msg.op == "PING"


@pytest.mark.asyncio
async def test_parse_pong_message():
    """Test parsing PONG message through parse() function."""
    from nats.client.protocol.message import parse

    reader = asyncio.StreamReader()
    reader.feed_data(b"PONG\r\n")
    reader.feed_eof()

    msg = await parse(reader)
    assert msg is not None
    assert msg.op == "PONG"


@pytest.mark.asyncio
async def test_parse_unknown_operation():
    """Test parsing unknown operation raises ParseError."""
    from nats.client.protocol.message import parse

    reader = asyncio.StreamReader()
    reader.feed_data(b"UNKNOWN\r\n")
    reader.feed_eof()

    with pytest.raises(ParseError, match="Unknown operation"):
        await parse(reader)


@pytest.mark.asyncio
async def test_parse_control_line_too_long():
    """Test parsing control line that exceeds max length."""
    from nats.client.protocol.message import parse

    reader = asyncio.StreamReader()
    # Create a control line longer than MAX_CONTROL_LINE (4096)
    reader.feed_data(b"MSG " + b"x" * 4096 + b"\r\n")
    reader.feed_eof()

    with pytest.raises(ParseError, match="Control line too long"):
        await parse(reader)


@pytest.mark.asyncio
async def test_parse_err_message():
    """Test parsing ERR message through parse() function."""
    from nats.client.protocol.message import parse

    reader = asyncio.StreamReader()
    reader.feed_data(b"ERR 'Unknown Protocol'\r\n")
    reader.feed_eof()

    msg = await parse(reader)
    assert msg is not None
    assert msg.op == "ERR"
    assert msg.error == "Unknown Protocol"
