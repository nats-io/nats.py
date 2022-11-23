from __future__ import annotations

import abc
import asyncio
import ssl
from typing import Awaitable, Callable
from urllib.parse import ParseResult

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore[assignment]


class Transport(abc.ABC):
    @abc.abstractmethod
    async def connect_tls(
        self,
        uri: str | ParseResult,
        ssl_context: ssl.SSLContext,
        connect_timeout: int,
    ):
        """
        connect_tls is similar to connect except it tries to connect to a secure endpoint, using the provided ssl
        context. The uri can be provided as string in case the hostname differs from the uri hostname, in case it
        was provided as 'tls_hostname' on the options.
        """
        pass

    @abc.abstractmethod
    def write(self, payload: bytes) -> None:
        """
        Write bytes to underlying transport. Needs a call to drain() to be successfully written.
        """
        pass

    @abc.abstractmethod
    def writelines(self, payload: list[bytes]) -> None:
        """
        Writes a list of bytes, one by one, to the underlying transport. Needs a call to drain() to be successfully
        written.
        """
        pass

    @abc.abstractmethod
    async def read(self, buffer_size: int) -> bytes:
        """
        Reads a sequence of bytes from the underlying transport, up to buffer_size. The buffer_size is ignored in case
        the transport carries already frames entire messages (i.e. websocket).
        """
        pass

    @abc.abstractmethod
    async def readline(self) -> bytes:
        """
        Reads one whole frame of bytes (or message) from the underlying transport.
        """
        pass

    @abc.abstractmethod
    async def drain(self) -> None:
        """
        Flushes the bytes queued for transmission when calling write() and writelines().
        """
        pass

    @abc.abstractmethod
    async def wait_closed(self) -> None:
        """
        Waits until the connection is successfully closed.
        """
        pass

    @abc.abstractmethod
    def close(self) -> None:
        """
        Closes the underlying transport.
        """
        pass

    @abc.abstractmethod
    def at_eof(self) -> bool:
        """
        Returns if underlying transport is at eof.
        """
        pass

    @abc.abstractmethod
    def __bool__(self) -> bool:
        """
        Returns if the transport was initialized, either by calling connect of connect_tls.
        """
        pass


Connector = Callable[[ParseResult, int, int, 'ssl.SSLContext | None'], Awaitable[Transport]]


async def connect_tcp(
    uri: ParseResult,
    buffer_size: int,
    connect_timeout: int,
    ssl_context: ssl.SSLContext | None
) -> TcpTransport:
    r, w = await asyncio.wait_for(
        asyncio.open_connection(
            host=uri.hostname,
            port=uri.port,
            limit=buffer_size,
        ), connect_timeout
    )
    transport = TcpTransport(r, w)
    if ssl_context is not None:
        await transport.connect_tls(
            uri=uri,
            ssl_context=ssl_context,
            connect_timeout=connect_timeout,
        )
    return transport


class TcpTransport(Transport):

    def __init__(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        self._io_reader: asyncio.StreamReader = r
        self._io_writer: asyncio.StreamWriter = w

        # We keep a reference to the initial transport we used when
        # establishing the connection in case we later upgrade to TLS
        # after getting the first INFO message. This is in order to
        # prevent the GC closing the socket after we send CONNECT
        # and replace the transport.
        #
        # See https://github.com/nats-io/asyncio-nats/issues/43
        self._bare_io_reader: asyncio.StreamReader = r
        self._bare_io_writer: asyncio.StreamWriter = w

    async def connect_tls(
        self,
        uri: str | ParseResult,
        ssl_context: ssl.SSLContext,
        connect_timeout: int,
    ) -> None:
        # manually recreate the stream reader/writer with a tls upgraded transport
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        transport_future = asyncio.get_running_loop().start_tls(
            self._io_writer.transport,
            protocol,
            ssl_context,
            # hostname here will be passed directly as string
            server_hostname=uri if isinstance(uri, str) else uri.hostname
        )
        transport = await asyncio.wait_for(transport_future, connect_timeout)
        writer = asyncio.StreamWriter(
            transport, protocol, reader, asyncio.get_running_loop()
        )
        self._io_reader, self._io_writer = reader, writer

    def write(self, payload: bytes) -> None:
        self._io_writer.write(payload)

    def writelines(self, payload: list[bytes]) -> None:
        self._io_writer.writelines(payload)

    async def read(self, buffer_size: int) -> bytes:
        return await self._io_reader.read(buffer_size)

    async def readline(self) -> bytes:
        return await self._io_reader.readline()

    async def drain(self) -> None:
        await self._io_writer.drain()

    async def wait_closed(self) -> None:
        return await self._io_writer.wait_closed()

    def close(self) -> None:
        return self._io_writer.close()

    def at_eof(self) -> bool:
        return self._io_reader.at_eof()

    def __bool__(self) -> bool:
        return bool(self._io_writer) and bool(self._io_reader)


async def connect_ws(
    uri: ParseResult,
    buffer_size: int,
    connect_timeout: int,
    ssl_context: ssl.SSLContext | None
) -> WebSocketTransport:
    if not aiohttp:
        raise ImportError(
            "Could not import aiohttp transport, please install it with `pip install aiohttp`"
        )
    client = aiohttp.ClientSession()
    # for websocket library, the uri must contain the scheme already
    ws = await client.ws_connect(uri.geturl(), timeout=connect_timeout, ssl=ssl_context)
    return WebSocketTransport(ws, client)


class WebSocketTransport(Transport):

    def __init__(self, ws: aiohttp.ClientWebSocketResponse, client: aiohttp.ClientSession):
        self._ws = ws
        self._client = client
        self._pending: asyncio.Queue[bytes] = asyncio.Queue()
        self._close_task: asyncio.Future[bool] = asyncio.Future()

    async def connect_tls(
        self,
        uri: str | ParseResult,
        ssl_context: ssl.SSLContext,
        connect_timeout: int,
    ):
        self._ws = await self._client.ws_connect(
            uri if isinstance(uri, str) else uri.geturl(),
            ssl=ssl_context,
            timeout=connect_timeout
        )

    def write(self, payload: bytes) -> None:
        self._pending.put_nowait(payload)

    def writelines(self, payload: list[bytes]) -> None:
        for message in payload:
            self.write(message)

    async def read(self, buffer_size: int) -> bytes:
        return await self.readline()

    async def readline(self) -> bytes:
        data = await self._ws.receive()
        if data.type == aiohttp.WSMsgType.CLOSE:
            # if the connection terminated abruptly, return empty binary data to raise unexpected EOF
            return b''
        return data.data

    async def drain(self) -> None:
        # send all the messages pending
        while not self._pending.empty():
            message = self._pending.get_nowait()
            await self._ws.send_bytes(message)

    async def wait_closed(self) -> None:
        await self._close_task
        await self._client.close()

    def close(self) -> None:
        self._close_task = asyncio.create_task(self._ws.close())

    def at_eof(self) -> bool:
        return self._ws.closed

    def __bool__(self) -> bool:
        return bool(self._client)
