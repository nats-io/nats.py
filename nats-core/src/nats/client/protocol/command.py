"""NATS protocol command encoding."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from nats.client.protocol.types import ConnectInfo


def encode_connect(info: ConnectInfo) -> bytes:
    """Encode CONNECT command.

    Args:
        info: Connection information

    Returns:
        Encoded CONNECT command
    """
    connect_dict = dict(info)
    if "password" in connect_dict:
        connect_dict["pass"] = connect_dict.pop("password")

    return f"CONNECT {json.dumps(connect_dict)}\r\n".encode()


def encode_pub(
    subject: bytes,
    payload: bytes,
    *,
    reply: bytes | None = None,
) -> bytes:
    """Encode PUB command.

    Args:
        subject: Subject to publish to
        payload: Message payload
        reply: Optional reply subject

    Returns:
        Encoded PUB command with payload
    """
    if reply:
        command = b"PUB %b %b %d\r\n" % (subject, reply, len(payload))
    else:
        command = b"PUB %b %d\r\n" % (subject, len(payload))

    return command + payload + b"\r\n"


def encode_hpub(
    subject: bytes,
    payload: bytes,
    *,
    reply: bytes | None = None,
    headers: dict[str, str | list[str]],
) -> bytes:
    """Encode HPUB command.

    Args:
        subject: Subject to publish to
        payload: Message payload
        reply: Optional reply subject
        headers: Headers to include with the message

    Returns:
        Encoded HPUB command with headers and payload
    """
    header_lines = ["NATS/1.0"] + [
        f"{key}: {item}" for key, value in headers.items() for item in (value if isinstance(value, list) else [value])
    ]

    header_data = ("\r\n".join(header_lines) + "\r\n\r\n").encode()

    hdr_len = len(header_data)
    total_len = hdr_len + len(payload)

    if reply:
        command = b"HPUB %b %b %d %d\r\n" % (subject, reply, hdr_len, total_len)
    else:
        command = b"HPUB %b %d %d\r\n" % (subject, hdr_len, total_len)

    return command + header_data + payload + b"\r\n"


def encode_sub(subject: str, sid: str, queue: str | None = None) -> bytes:
    """Encode SUB command.

    Args:
        subject: Subject to subscribe to
        sid: Subscription ID
        queue: Optional queue group

    Returns:
        Encoded SUB command
    """
    if queue:
        return f"SUB {subject} {queue} {sid}\r\n".encode()
    return f"SUB {subject} {sid}\r\n".encode()


def encode_unsub(sid: str, max_msgs: int | None = None) -> bytes:
    """Encode UNSUB command.

    Args:
        sid: Subscription ID to unsubscribe
        max_msgs: Optional number of messages to receive before auto-unsubscribe

    Returns:
        Encoded UNSUB command
    """
    if max_msgs is not None:
        return f"UNSUB {sid} {max_msgs}\r\n".encode()
    return f"UNSUB {sid}\r\n".encode()


def encode_ping() -> bytes:
    """Encode PING command."""
    return b"PING\r\n"


def encode_pong() -> bytes:
    """Encode PONG command."""
    return b"PONG\r\n"
