import abc
import asyncio
import sys
import ssl
from typing import Optional
from nats import errors
from urllib.parse import ParseResult
try:
    import aiohttp
except ImportError:
    aiohttp = None


class Transport(abc.ABC):

    @abc.abstractmethod
    async def connect(self, uri: ParseResult, buffer_size: int, connect_timeout: int):
        pass

    @abc.abstractmethod
    async def connect_tls(
        self, uri: ParseResult, ssl_context: ssl.SSLContext, buffer_size: int, connect_timeout: int,
    ):
        pass

    @abc.abstractmethod
    def write(self, payload):
        pass

    @abc.abstractmethod
    def writelines(self, payload):
        pass

    @abc.abstractmethod
    async def read(self, buffer_size: int):
        pass

    @abc.abstractmethod
    async def readline(self):
        pass

    @abc.abstractmethod
    async def drain(self):
        pass

    @abc.abstractmethod
    async def wait_closed(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def at_eof(self):
        pass

    @abc.abstractmethod
    def __bool__(self):
        pass


class TcpTransport(Transport):
    def __init__(self):
        self._bare_io_reader: Optional[asyncio.StreamReader] = None
        self._io_reader: Optional[asyncio.StreamReader] = None
        self._bare_io_writer: Optional[asyncio.StreamWriter] = None
        self._io_writer: Optional[asyncio.StreamWriter] = None

    async def connect(self, uri: ParseResult, buffer_size: int, connect_timeout: int):
        r, w = await asyncio.wait_for(asyncio.open_connection(
            host=uri.hostname,
            port=uri.port,
            limit=buffer_size,
        ), connect_timeout)
        # We keep a reference to the initial transport we used when
        # establishing the connection in case we later upgrade to TLS
        # after getting the first INFO message. This is in order to
        # prevent the GC closing the socket after we send CONNECT
        # and replace the transport.
        #
        # See https://github.com/nats-io/asyncio-nats/issues/43
        self._bare_io_reader = self._io_reader = r
        self._bare_io_writer = self._io_writer = w

    async def connect_tls(
        self, uri: ParseResult, ssl_context: ssl.SSLContext, buffer_size: int, connect_timeout: int,
    ):
        # loop.start_tls was introduced in python 3.7
        # the previous method is removed in 3.9
        if sys.version_info.minor >= 7:
            # manually recreate the stream reader/writer with a tls upgraded transport
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            transport_future = asyncio.get_running_loop().start_tls(
                self._io_writer.transport,
                protocol,
                ssl_context,
                server_hostname=uri.hostname
            )
            transport = await asyncio.wait_for(
                transport_future, connect_timeout
            )
            writer = asyncio.StreamWriter(
                transport, protocol, reader, asyncio.get_running_loop()
            )
            self._io_reader, self._io_writer = reader, writer
        else:
            transport = self._io_writer.transport
            sock = transport.get_extra_info('socket')
            if not sock:
                # This shouldn't happen
                raise errors.Error('nats: unable to get socket')

            connection_future = asyncio.open_connection(
                limit=buffer_size,
                sock=sock,
                ssl=ssl_context,
                server_hostname=uri.hostname,
            )
            self._io_reader, self._io_writer = await asyncio.wait_for(
                connection_future, connect_timeout
            )

    def write(self, payload):
        return self._io_writer.write(payload)

    def writelines(self, payload):
        return self._io_writer.writelines(payload)

    async def read(self, buffer_size: int):
        return await self._io_reader.read(buffer_size)

    async def readline(self):
        return await self._io_reader.readline()

    async def drain(self):
        return await self._io_writer.drain()

    async def wait_closed(self):
        return await self._io_writer.wait_closed()

    def close(self):
        return self._io_writer.close()

    def at_eof(self):
        return self._io_reader.at_eof()

    def __bool__(self):
        return bool(self._io_writer) and bool(self._io_reader)


class WebsocketTransport(Transport):
    def __init__(self):
        if not aiohttp:
            raise ImportError(
                "Could not import aiohttp transport, please install it with `pip install aiohttp`"
            )
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._client: Optional[aiohttp.ClientSession] = aiohttp.ClientSession()
        self._pending = asyncio.Queue()
        self._close_task = asyncio.Future()

    async def connect(self, uri: ParseResult, buffer_size: int, connect_timeout: int):
        # for websocket library, the uri must contain the scheme already
        self._ws = await self._client.ws_connect(uri.geturl(), timeout=connect_timeout)

    async def connect_tls(
        self, uri: ParseResult, ssl_context: ssl.SSLContext, buffer_size: int, connect_timeout: int,
    ):
        self._ws = await self._client.ws_connect(uri.geturl(), ssl_context=ssl_context, timeout=connect_timeout)

    def write(self, payload):
        self._pending.put_nowait(payload)

    def writelines(self, payload):
        for message in payload:
            self.write(message)

    async def read(self, buffer_size: int):
        return await self.readline()

    async def readline(self):
        data = await self._ws.receive()
        return data.data

    async def drain(self):
        # send all the messages pending
        while not self._pending.empty():
            message = self._pending.get_nowait()
            await self._ws.send_bytes(message)

    async def wait_closed(self):
        await self._close_task
        await self._client.close()
        self._ws = self._client = None

    def close(self):
        self._close_task = asyncio.create_task(self._ws.close())

    def at_eof(self):
        return self._ws._reader.at_eof()

    def __bool__(self):
        return bool(self._ws)
