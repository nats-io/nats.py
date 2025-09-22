"""Tests for NATS protocol message parsing and command encoding."""

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
    Op,
    ParseError,
    parse_control_line,
    parse_headers,
    parse_hmsg_args,
    parse_msg_args,
)
from nats.client.protocol.types import ConnectInfo


def test_parse_control_line():
    """Test parsing control lines."""
    # Test valid MSG
    op, args = parse_control_line(b"MSG foo.bar 1 42")
    assert op == Op.MSG
    assert args == ["foo.bar", "1", "42"]

    # Test valid MSG with reply
    op, args = parse_control_line(b"MSG foo.bar 1 reply.to 42")
    assert op == Op.MSG
    assert args == ["foo.bar", "1", "reply.to", "42"]

    # Test valid HMSG
    op, args = parse_control_line(b"HMSG foo.bar 1 reply.to 10 52")
    assert op == Op.HMSG
    assert args == ["foo.bar", "1", "reply.to", "10", "52"]

    # Test valid PING
    op, args = parse_control_line(b"PING")
    assert op == Op.PING
    assert not args

    # Test valid PONG
    op, args = parse_control_line(b"PONG")
    assert op == Op.PONG
    assert not args

    # Test valid INFO
    op, args = parse_control_line(b'INFO {"server_id":"test"}')
    assert op == Op.INFO
    assert args == ['{"server_id":"test"}']

    # Test valid ERR
    op, args = parse_control_line(b"ERR 'Unknown subject'")
    assert op == Op.ERR
    assert args == ["'Unknown", "subject'"]

    # Test invalid operation
    with pytest.raises(ParseError, match="Unknown operation"):
        parse_control_line(b"INVALID foo bar")

    # Test empty line
    with pytest.raises(ParseError, match="Empty control line"):
        parse_control_line(b"")

    # Test line too long
    with pytest.raises(ParseError, match="Control line too long"):
        parse_control_line(b"MSG " + b"x" * 4096)


def test_parse_msg_args():
    """Test parsing MSG arguments."""
    # Test valid MSG without reply
    subject, sid, reply_to, size = parse_msg_args(["foo.bar", "1", "42"])
    assert subject == "foo.bar"
    assert sid == "1"
    assert reply_to is None
    assert size == 42

    # Test valid MSG with reply
    subject, sid, reply_to, size = parse_msg_args([
        "foo.bar", "1", "reply.to", "42"
    ])
    assert subject == "foo.bar"
    assert sid == "1"
    assert reply_to == "reply.to"
    assert size == 42

    # Test invalid size
    with pytest.raises(ParseError, match="Invalid payload size"):
        parse_msg_args(["foo.bar", "1", "invalid"])

    # Test not enough arguments
    with pytest.raises(ParseError, match="Invalid MSG: not enough arguments"):
        parse_msg_args(["foo.bar", "1"])

    # Test too many arguments
    with pytest.raises(ParseError, match="Invalid MSG: too many arguments"):
        parse_msg_args(["foo.bar", "1", "reply.to", "42", "extra"])


def test_parse_hmsg_args():
    """Test parsing HMSG arguments."""
    # Test valid HMSG
    subject, sid, reply_to, header_size, total_size = parse_hmsg_args([
        "foo.bar", "1", "reply.to", "10", "52"
    ])
    assert subject == "foo.bar"
    assert sid == "1"
    assert reply_to == "reply.to"
    assert header_size == 10
    assert total_size == 52

    # Test invalid sizes
    with pytest.raises(ParseError, match="Invalid size values"):
        parse_hmsg_args(["foo.bar", "1", "reply.to", "invalid", "52"])

    # Test header size too large
    with pytest.raises(ParseError, match="Header too large"):
        parse_hmsg_args(["foo.bar", "1", "reply.to", "65537", "65538"])

    # Test header size larger than total
    with pytest.raises(ParseError,
                       match="Header size .* larger than total size"):
        parse_hmsg_args(["foo.bar", "1", "reply.to", "52", "10"])

    # Test not enough arguments
    with pytest.raises(ParseError, match="Invalid HMSG: not enough arguments"):
        parse_hmsg_args(["foo.bar", "1", "reply.to", "10"])

    # Test too many arguments
    with pytest.raises(ParseError, match="Invalid HMSG: too many arguments"):
        parse_hmsg_args(["foo.bar", "1", "reply.to", "10", "52", "extra"])


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
    headers, status_code, status_description = parse_headers(
        header_data_with_status
    )
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
        auth_token=None,
        user=None,
        pass_=None,
        name=None,
        lang="python",
        version="0.1.0",
        protocol=1,
        echo=True,
        sig=None,
        jwt=None,
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
    command = encode_hpub(
        "foo.bar", payload, reply_to="reply.to", headers=headers
    )
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
