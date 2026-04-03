"""NATS protocol message parsing and type definitions."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Final, Literal, NamedTuple, Protocol, cast, runtime_checkable

from nats.client.protocol.types import ServerInfo

if TYPE_CHECKING:
    pass


@runtime_checkable
class Reader(Protocol):
    """Protocol for reading data from a stream.

    This defines the minimal interface needed by the protocol parser.
    Both asyncio.StreamReader and Connection implement this protocol.
    """

    async def readline(self) -> bytes:
        """Read a line from the stream.

        Returns:
            Line read from the stream ending with newline
        """
        ...

    async def readexactly(self, n: int) -> bytes:
        """Read exactly n bytes from the stream.

        Args:
            n: Number of bytes to read

        Returns:
            Exactly n bytes

        Raises:
            asyncio.IncompleteReadError: If stream closed before n bytes were read
        """
        ...


CRLF: Final[bytes] = b"\r\n"
MAX_CONTROL_LINE: Final[int] = 4096
MAX_HEADER_SIZE: Final[int] = 64 * 1024
MAX_PAYLOAD_SIZE: Final[int] = 64 * 1024 * 1024
MIN_MSG_ARGS: Final[int] = 3
MIN_HMSG_ARGS: Final[int] = 4
MIN_STATUS_PARTS: Final[int] = 2
MIN_STATUS_PARTS_WITH_DESC: Final[int] = 3


class Msg(NamedTuple):
    """MSG protocol message."""

    op: Literal["MSG"]
    subject: str
    sid: str
    reply: str | None
    payload: bytes


class HMsg(NamedTuple):
    """HMSG protocol message."""

    op: Literal["HMSG"]
    subject: str
    sid: str
    reply: str | None
    headers: dict[str, list[str]]
    payload: bytes
    status_code: str | None
    status_description: str | None


class Info(NamedTuple):
    """INFO protocol message."""

    op: Literal["INFO"]
    info: ServerInfo


class Err(NamedTuple):
    """ERR protocol message."""

    op: Literal["ERR"]
    error: str


class Ping(NamedTuple):
    """PING protocol message."""

    op: Literal["PING"]


class Pong(NamedTuple):
    """PONG protocol message."""

    op: Literal["PONG"]


Message = Msg | HMsg | Info | Err | Ping | Pong


class ParseError(Exception):
    """Parser error when handling NATS protocol messages."""


def parse_headers(data: bytes) -> tuple[dict[str, list[str]], str | None, str | None]:
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

    if not lines[0].startswith("NATS/"):
        msg = "Invalid header format: missing NATS version"
        raise ParseError(msg)

    status_line = lines[0]
    status_parts = status_line.split(" ", 2)
    if len(status_parts) >= MIN_STATUS_PARTS:
        status_code = status_parts[1]

        if len(status_parts) >= MIN_STATUS_PARTS_WITH_DESC:
            status_description = status_parts[2]

    for line in lines[1:]:
        if not line:
            continue

        if ":" not in line:
            msg = f"Invalid header line (missing ':'): {line!r}"
            raise ParseError(msg)

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if key in headers:
            headers[key].append(value)
        else:
            headers[key] = [value]

    return headers, status_code, status_description


async def parse_msg(reader: Reader, args: list[bytes]) -> Msg:
    """Parse MSG message.

    Args:
        reader: Reader protocol implementation
        args: Message arguments

    Returns:
        Parsed MSG message

    Raises:
        ParseError: If message format is invalid
    """
    if len(args) < MIN_MSG_ARGS:
        msg = "Invalid MSG: not enough arguments"
        raise ParseError(msg)

    subject_bytes = args[0]
    sid_bytes = args[1]

    if len(args) == MIN_MSG_ARGS:
        reply_bytes = None
        size = int(args[2])
    else:
        reply_bytes = args[2]
        size = int(args[3])

    if size > MAX_PAYLOAD_SIZE:
        msg = f"Payload too large: {size} bytes (max {MAX_PAYLOAD_SIZE})"
        raise ParseError(msg)

    payload_with_crlf = await reader.readexactly(size + 2)
    payload = payload_with_crlf[:size]

    subject = subject_bytes.decode()
    sid = sid_bytes.decode()
    reply = reply_bytes.decode() if reply_bytes is not None else None

    return Msg("MSG", subject, sid, reply, payload)


async def parse_hmsg(reader: Reader, args: list[bytes]) -> HMsg:
    """Parse HMSG message.

    Args:
        reader: Reader protocol implementation
        args: Message arguments

    Returns:
        Parsed HMSG message

    Raises:
        ParseError: If message format is invalid
    """
    if len(args) < MIN_HMSG_ARGS:
        msg = "Invalid HMSG: not enough arguments"
        raise ParseError(msg)

    subject_bytes = args[0]
    sid_bytes = args[1]

    if len(args) == MIN_HMSG_ARGS:
        reply_bytes = None
        header_size = int(args[2])
        total_size = int(args[3])
    else:
        reply_bytes = args[2]
        header_size = int(args[3])
        total_size = int(args[4])

    if header_size > MAX_HEADER_SIZE:
        msg = f"Headers too large: {header_size} bytes (max {MAX_HEADER_SIZE})"
        raise ParseError(msg)

    if total_size > MAX_PAYLOAD_SIZE:
        msg = f"Total message too large: {total_size} bytes (max {MAX_PAYLOAD_SIZE})"
        raise ParseError(msg)

    header_bytes = await reader.readexactly(header_size)

    headers, status_code, status_description = parse_headers(header_bytes)

    payload_size = total_size - header_size
    payload_with_crlf = await reader.readexactly(payload_size + 2)
    payload = payload_with_crlf[:payload_size]

    subject = subject_bytes.decode()
    sid = sid_bytes.decode()
    reply = reply_bytes.decode() if reply_bytes is not None else None

    return HMsg("HMSG", subject, sid, reply, headers, payload, status_code, status_description)


async def parse_info(args: list[bytes]) -> Info:
    """Parse INFO message.

    Args:
        args: Message arguments

    Returns:
        Parsed INFO message

    Raises:
        ParseError: If message format is invalid
    """
    if not args:
        msg = "INFO message missing JSON data"
        raise ParseError(msg)

    info_bytes = b" ".join(args)
    info_data = info_bytes.decode()

    try:
        data = json.loads(info_data)
        return Info("INFO", cast(ServerInfo, data))
    except json.JSONDecodeError as e:
        msg = f"Invalid INFO JSON: {e}"
        raise ParseError(msg) from e


async def parse_err(args: list[bytes]) -> Err:
    """Parse ERR message.

    Args:
        args: Message arguments

    Returns:
        Parsed ERR message

    Raises:
        ParseError: If message format is invalid
    """
    if not args:
        msg = "ERR message missing error text"
        raise ParseError(msg)

    error_bytes = b" ".join(args)
    error_text = error_bytes.decode()

    if error_text.startswith("'") and error_text.endswith("'"):
        error_text = error_text[1:-1]

    return Err("ERR", error_text)


async def ping() -> Ping:
    """Create PING message.

    Returns:
        PING message
    """
    return Ping("PING")


async def pong() -> Pong:
    """Create PONG message.

    Returns:
        PONG message
    """
    return Pong("PONG")


async def parse(reader: Reader) -> Message | None:
    """Parse a message from the protocol stream.

    Args:
        reader: Reader protocol implementation

    Returns:
        Parsed protocol message or None if connection closed

    Raises:
        ParseError: If message format is invalid
    """
    control_line = await reader.readline()
    if not control_line:
        return None

    control_line = control_line.rstrip()

    if len(control_line) > MAX_CONTROL_LINE:
        msg = f"Control line too long: {len(control_line)} bytes (max {MAX_CONTROL_LINE})"
        raise ParseError(msg)

    parts = control_line.split(b" ", 5)
    op = parts[0]
    args = parts[1:]

    match op:
        case b"MSG":
            return await parse_msg(reader, args)
        case b"HMSG":
            return await parse_hmsg(reader, args)
        case b"PING":
            return await ping()
        case b"PONG":
            return await pong()
        case b"INFO":
            return await parse_info(args)
        case b"-ERR" | b"ERR":
            return await parse_err(args)
        case _:
            msg = f"Unknown operation: {op!r}"
            raise ParseError(msg)
