"""NATS protocol message parsing and type definitions.

This module implements the core protocol message parsing for the NATS protocol,
handling different message types including MSG, HMSG, PING, PONG, INFO, and ERR.
It provides both low-level parsing functions and the main `parse` function that
reads and interprets messages from a NATS server connection.
"""

from __future__ import annotations

import json
from enum import Enum
from typing import TYPE_CHECKING, Final, Literal

from nats.client.protocol.types import ServerInfo

if TYPE_CHECKING:
    import asyncio

# Protocol constants
CRLF: Final[bytes] = b"\r\n"
MAX_CONTROL_LINE: Final[int] = 4096  # Max length of control line
MAX_HEADER_SIZE: Final[int] = 64 * 1024  # Max header size (64KB)
MAX_PAYLOAD_SIZE: Final[int] = 64 * 1024 * 1024  # Max payload size (64MB)
MIN_MSG_ARGS: Final[int] = 3  # Minimum arguments for MSG command
MIN_HMSG_ARGS: Final[int] = 4  # Minimum arguments for HMSG command
MIN_STATUS_PARTS: Final[int
                        ] = 2  # Minimum parts for status line (NATS/1.0 CODE)
MIN_STATUS_PARTS_WITH_DESC: Final[int] = 3  # Parts for status with description


class Op(str, Enum):
    """NATS protocol operations."""

    MSG = "MSG"
    HMSG = "HMSG"
    PING = "PING"
    PONG = "PONG"
    INFO = "INFO"
    ERR = "ERR"


# Message type aliases
MsgResult = tuple[Literal["MSG"], str, str, str | None,
                  bytes]  # op, subject, sid, reply_to, payload
HMsgResult = tuple[
    Literal["HMSG"], str, str, str, dict[str, list[str]], bytes, str | None,
    str | None
]  # op, subject, sid, reply_to, headers, payload, status_code, status_description
InfoResult = tuple[Literal["INFO"], ServerInfo]  # op, info
ErrorResult = tuple[Literal["ERR"], str]  # op, error
PingResult = tuple[Literal["PING"]]  # op
PongResult = tuple[Literal["PONG"]]  # op

# Union of all possible message types
Message = MsgResult | HMsgResult | InfoResult | ErrorResult | PingResult | PongResult


class ParseError(Exception):
    """Parser error when handling NATS protocol messages."""


def parse_control_line(line: bytes) -> tuple[Op, list[str]]:
    """Parse a control line into operation and arguments.

    Args:
        line: Raw control line bytes

    Returns:
        Tuple of (operation, arguments)

    Raises:
        ParseError: If line is invalid or too long
    """
    if len(line) > MAX_CONTROL_LINE:
        msg = f"Control line too long: {len(line)} > {MAX_CONTROL_LINE}"
        raise ParseError(msg)

    try:
        parts = line.decode().split()
        if not parts:
            msg = "Empty control line"
            raise ParseError(msg)

        try:
            op = Op(parts[0])
        except ValueError as e:
            msg = f"Unknown operation: {parts[0]}"
            raise ParseError(msg) from e

        return op, parts[1:]

    except UnicodeDecodeError as e:
        msg = f"Invalid control line encoding: {e}"
        raise ParseError(msg) from e


def parse_msg_args(args: list[str]) -> tuple[str, str, str | None, int]:
    """Parse MSG arguments into components.

    Args:
        args: MSG command arguments

    Returns:
        Tuple of (subject, sid, reply_to, payload_size)

    Raises:
        ParseError: If arguments are invalid
    """
    match len(args):
        case 0 | 1 | 2:
            msg = "Invalid MSG: not enough arguments"
            raise ParseError(msg)
        case 3:
            subject, sid, size_str = args
            try:
                size = int(size_str)
            except ValueError as e:
                msg = f"Invalid payload size: {size_str}"
                raise ParseError(msg) from e
            return subject, sid, None, size
        case 4:
            subject, sid, reply_to, size_str = args
            try:
                size = int(size_str)
            except ValueError as e:
                msg = f"Invalid payload size: {size_str}"
                raise ParseError(msg) from e
            return subject, sid, reply_to, size
        case _:
            msg = "Invalid MSG: too many arguments"
            raise ParseError(msg)


def parse_hmsg_args(args: list[str]) -> tuple[str, str, str, int, int]:
    """Parse HMSG arguments into components.

    Args:
        args: HMSG command arguments

    Returns:
        Tuple of (subject, sid, reply_to, header_size, total_size)

    Raises:
        ParseError: If arguments are invalid
    """
    match len(args):
        case 0 | 1 | 2 | 3 | 4:
            msg = "Invalid HMSG: not enough arguments"
            raise ParseError(msg)
        case 5:
            subject, sid, reply_to, header_size_str, total_size_str = args
            try:
                header_size = int(header_size_str)
                total_size = int(total_size_str)
            except ValueError as e:
                msg = f"Invalid size values: {header_size_str}, {total_size_str}"
                raise ParseError(msg) from e

            if header_size > MAX_HEADER_SIZE:
                msg = f"Header too large: {header_size} > {MAX_HEADER_SIZE}"
                raise ParseError(msg)

            if header_size > total_size:
                msg = f"Header size {header_size} larger than total size {total_size}"
                raise ParseError(msg)

            return subject, sid, reply_to, header_size, total_size
        case _:
            msg = "Invalid HMSG: too many arguments"
            raise ParseError(msg)


def parse_headers(
    data: bytes
) -> tuple[dict[str, list[str]], str | None, str | None]:
    """Parse header data into multi-value dictionary and status information.

    Args:
        data: Raw header bytes

    Returns:
        Tuple of (headers dict, status_code, status_description)

    Raises:
        ParseError: If headers are invalid
    """
    try:
        lines = data.decode().split("\r\n")
    except UnicodeDecodeError as e:
        msg = f"Invalid header encoding: {e}"
        raise ParseError(msg) from e

    headers: dict[str, list[str]] = {}
    status_code: str | None = None
    status_description: str | None = None

    # First line should be NATS/1.0 (version)
    if not lines[0].startswith("NATS/"):
        msg = "Invalid header format: missing NATS version"
        raise ParseError(msg)

    # Parse NATS status line (e.g., "NATS/1.0 503" or "NATS/1.0 503 No Responders")
    status_line = lines[0]
    status_parts = status_line.split(" ", 2)  # Split into at most 3 parts
    if len(status_parts) >= MIN_STATUS_PARTS:
        status_code = status_parts[1]

        # If there's a description part, extract it
        if len(status_parts) >= MIN_STATUS_PARTS_WITH_DESC:
            status_description = status_parts[2]

    # Parse header key-value pairs
    for line in lines[1:]:
        if not line or ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if key in headers:
            # If header already exists, append to the list
            headers[key].append(value)
        else:
            # Initialize as a single-item list
            headers[key] = [value]

    return headers, status_code, status_description


def parse_info(json_data: str) -> ServerInfo:
    """Parse INFO JSON into ServerInfo.

    Args:
        json_data: INFO message JSON string

    Returns:
        Parsed ServerInfo object

    Raises:
        ParseError: If JSON is invalid
    """
    try:
        data = json.loads(json_data)
        return ServerInfo(data)
    except (json.JSONDecodeError, TypeError) as e:
        msg = f"Invalid INFO JSON: {e}"
        raise ParseError(msg) from e


def parse_err(text: str) -> str:
    """Parse ERR message.

    Args:
        text: Error message text

    Returns:
        Cleaned error message
    """
    # Remove quotes if present
    if text.startswith("'") and text.endswith("'"):
        text = text[1:-1]
    return text


async def parse(reader: asyncio.StreamReader) -> Message | None:
    """Parse a message from the protocol stream.

    Args:
        reader: AsyncIO stream reader

    Returns:
        Parsed protocol message or None if connection closed

    Raises:
        ParseError: If message format is invalid
    """
    try:
        # Read control line
        control_line = await reader.readline()
        if not control_line:
            return None

        control_line = control_line.rstrip()

        # Check control line length
        if len(control_line) > MAX_CONTROL_LINE:
            msg = f"Control line too long: {len(control_line)} bytes (max {MAX_CONTROL_LINE})"
            raise ParseError(msg)

        # Parse operation and arguments
        try:
            parts = control_line.split(b" ")
            op = parts[0]  # Keep as bytes
            args = parts[1:]  # Keep as bytes

        except Exception as e:
            msg = f"Invalid control line: {e}"
            raise ParseError(msg) from e

        # Handle different operations
        if op == b"MSG":
            # MSG format: MSG <subject> <sid> [reply-to] <#bytes>
            if len(args) < MIN_MSG_ARGS:
                msg = "Invalid MSG: not enough arguments"
                raise ParseError(msg)

            subject_bytes = args[0]
            sid_bytes = args[1]

            if len(args) == MIN_MSG_ARGS:
                # No reply subject
                reply_to_bytes = None
                size = int(args[2])
            else:
                # With reply subject
                reply_to_bytes = args[2]
                size = int(args[3])

            # Check payload size limit
            if size > MAX_PAYLOAD_SIZE:
                msg = f"Payload too large: {size} bytes (max {MAX_PAYLOAD_SIZE})"
                raise ParseError(msg)

            payload = await reader.readexactly(size)
            # Skip trailing CRLF
            await reader.readline()

            # Only convert to strings at the last moment
            subject = subject_bytes.decode()
            sid = sid_bytes.decode()
            reply_to = reply_to_bytes.decode(
            ) if reply_to_bytes is not None else None

            return (Op.MSG, subject, sid, reply_to, payload)

        if op == b"HMSG":
            # HMSG format: HMSG <subject> <sid> [reply-to] <#header bytes> <#total bytes>
            if len(args) < MIN_HMSG_ARGS:
                msg = "Invalid HMSG: not enough arguments"
                raise ParseError(msg)

            subject_bytes = args[0]
            sid_bytes = args[1]

            if len(args) == MIN_HMSG_ARGS:
                # No reply subject
                reply_to_bytes = None
                header_size = int(args[2])
                total_size = int(args[3])
            else:
                # With reply subject
                reply_to_bytes = args[2]
                header_size = int(args[3])
                total_size = int(args[4])

            # Check size limits
            if header_size > MAX_HEADER_SIZE:
                msg = f"Headers too large: {header_size} bytes (max {MAX_HEADER_SIZE})"
                raise ParseError(msg)

            if total_size > MAX_PAYLOAD_SIZE:
                msg = f"Total message too large: {total_size} bytes (max {MAX_PAYLOAD_SIZE})"
                raise ParseError(msg)

            # Read header bytes
            header_bytes = await reader.readexactly(header_size)

            # Use the parse_headers function to parse the headers
            headers, status_code, status_description = parse_headers(
                header_bytes
            )

            # Read payload (total size minus header size)
            payload_size = total_size - header_size
            payload = await reader.readexactly(payload_size)

            # Skip trailing CRLF
            await reader.readline()

            # Convert remaining bytes to strings only at the final step
            subject = subject_bytes.decode()
            sid = sid_bytes.decode()
            reply_to = reply_to_bytes.decode(
            ) if reply_to_bytes is not None else None

            return (
                Op.HMSG, subject, sid, reply_to, headers, payload, status_code,
                status_description
            )

        if op == b"PING":
            return (Op.PING, )

        if op == b"PONG":
            return (Op.PONG, )

        if op == b"INFO":
            if not args:
                msg = "INFO message missing JSON data"
                raise ParseError(msg)

            # Join the args and decode once for JSON parsing
            info_bytes = b" ".join(args)
            info_data = info_bytes.decode()

            try:
                data = json.loads(info_data)
                return (Op.INFO, ServerInfo(data))
            except json.JSONDecodeError as e:
                msg = f"Invalid INFO JSON: {e}"
                raise ParseError(msg) from e

        if op == b"ERR":
            if not args:
                msg = "ERR message missing error text"
                raise ParseError(msg)

            # Join the args and decode once
            error_bytes = b" ".join(args)
            error_text = error_bytes.decode()

            # Remove quotes if present
            if error_text.startswith("'") and error_text.endswith("'"):
                error_text = error_text[1:-1]

            return (Op.ERR, error_text)

        # Decode only for the error message
        msg = f"Unknown operation: {op.decode()}"
        raise ParseError(msg)

    except ValueError as e:
        msg = f"Invalid message format: {e}"
        raise ParseError(msg) from e
