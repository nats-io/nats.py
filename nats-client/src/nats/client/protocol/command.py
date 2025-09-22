"""NATS protocol command encoding.

This module provides functions to encode NATS protocol commands
that are sent from the client to the server. Each function handles
the proper formatting according to the NATS protocol specification.
"""

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
    return f"CONNECT {json.dumps(info)}\r\n".encode()


def encode_pub(
    subject: str,
    payload: bytes,
    *,
    reply_to: str | None = None,
) -> list[bytes]:
    """Encode PUB command.

    Args:
        subject: Subject to publish to
        payload: Message payload
        reply_to: Optional reply subject

    Returns:
        List of byte strings to write in sequence
    """
    # PUB format: PUB <subject> [reply-to] <#bytes>
    command = f"PUB {subject} {reply_to} {len(payload)}\r\n" if reply_to else f"PUB {subject} {len(payload)}\r\n"

    return [command.encode(), payload, b"\r\n"]


def encode_hpub(
    subject: str,
    payload: bytes,
    *,
    reply_to: str | None = None,
    headers: dict[str, str | list[str]],
) -> list[bytes]:
    """Encode HPUB command.

    Args:
        subject: Subject to publish to
        payload: Message payload
        reply_to: Optional reply subject
        headers: Headers to include with the message

    Returns:
        List of byte strings to write in sequence
    """
    # Format headers with version indicator
    header_lines = ["NATS/1.0"] + [
        f"{key}: {item}" for key, value in headers.items()
        for item in (value if isinstance(value, list) else [value])
    ]

    # IMPORTANT: Headers must end with \r\n\r\n (empty line after headers)
    header_data = ("\r\n".join(header_lines) + "\r\n\r\n").encode()

    # HPUB format: HPUB <subject> [reply-to] <#header bytes> <#total bytes>
    if reply_to:
        command = f"HPUB {subject} {reply_to} {len(header_data)} {len(header_data) + len(payload)}\r\n"
    else:
        command = f"HPUB {subject} {len(header_data)} {len(header_data) + len(payload)}\r\n"

    return [command.encode(), header_data, payload, b"\r\n"]


def encode_sub(
    subject: str, sid: str, queue_group: str | None = None
) -> bytes:
    """Encode SUB command.

    Args:
        subject: Subject to subscribe to
        sid: Subscription ID
        queue_group: Optional queue group

    Returns:
        Encoded SUB command
    """
    if queue_group:
        return f"SUB {subject} {queue_group} {sid}\r\n".encode()
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
