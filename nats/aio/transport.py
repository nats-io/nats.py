import abc
import asyncio
import sys
import ssl
from typing import Optional
from nats import errors
try:
    import websockets
    import websockets.legacy.client
except ImportError:
    websockets = None


class Transport(abc.ABC):

    @abc.abstractmethod
    async def connect(self, hostname: str, port: int, buffer_size: int, connect_timeout: int):
        pass

    @abc.abstractmethod
    async def connect_tls(
        self, ssl_context: ssl.SSLContext, hostname: str, buffer_size: int, connect_timeout: int,
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

    async def connect(self, hostname: str, port: int, buffer_size: int, connect_timeout: int):
        r, w = await asyncio.wait_for(asyncio.open_connection(
            host=hostname,
            port=port,
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
        self, ssl_context: ssl.SSLContext, hostname: str, buffer_size: int, connect_timeout: int,
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
                server_hostname=hostname
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
                server_hostname=hostname,
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
        if not websockets:
            raise ImportError(
                "Could not import websockets transport, please install it with `pip install websockets===10.3`"
            )
        self._ws: Optional[websockets.legacy.client.WebSocketClientProtocol] = None

    async def connect(self, hostname: str, port: int, buffer_size: int, connect_timeout: int):
        self._ws = await websockets.connect(
            uri=hostname, port=port, max_size=buffer_size, open_timeout=connect_timeout
        )

    async def connect_tls(
        self, ssl_context: ssl.SSLContext, hostname: str, buffer_size: int, connect_timeout: int,
    ):
        raise NotImplementedError("TLS was not implemented for websocket transport")

    def write(self, payload):
        return self._ws.send(payload)

    def writelines(self, payload):
        for message in payload:
            self._ws.send(message)

    async def read(self, buffer_size: int):
        return await self._ws.read_frame(buffer_size)

    async def readline(self):
        return await self._ws.read_message()

    async def drain(self):
        return await self._ws.drain()

    async def wait_closed(self):
        return await self._ws.wait_closed()

    def close(self):
        return self._ws.close()

    def at_eof(self):
        return self._ws.eof_received()

    def __bool__(self):
        return bool(self._ws)
