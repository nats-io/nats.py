import asyncio
import json
from datetime import datetime
from urllib.parse import urlparse

__version__ = '0.1.0'
__lang__    = 'python3'

INFO_OP     = b'INFO'
CONNECT_OP  = b'CONNECT'
PING_OP     = b'PING'
PONG_OP     = b'PONG'
PUB_OP      = b'PUB'
SUB_OP      = b'SUB'
OK_OP       = b'+OK'
ERR_OP      = b'-ERR'
_CRLF_      = b'\r\n'
_SPC_       = b' '
_EMPTY_     = b''

PING_PROTO = PING_OP + _CRLF_
PONG_PROTO = PONG_OP + _CRLF_

DEFAULT_BUFFER_SIZE   = 32768
MAX_CONTROL_LINE_SIZE = 1024

class Client():
    """
    NATS client
    """

    def __init__(self):
        self.options = {}
        self._current_server = None
        self._server_info = {}
        self._loop = None
        self._server_pool = []
        self._io_reader = None
        self._io_writer = None
        self._err = None
        self._error_cb = None
        self._max_payload = 1048576

    @asyncio.coroutine
    def connect(self,
                servers=["nats://127.0.0.1:4222"],
                io_loop=asyncio.get_event_loop(),
                error_cb=None,
                name=None,
                pedantic=False,
                verbose=False,
                ):
        self._loop = io_loop
        self._error_cb = error_cb
        self._setup_server_pool(servers)
        self.options["verbose"] = verbose
        self.options["pedantic"] = pedantic
        self.options["name"] = name

        while True:
            try:
                yield from self._select_next_server()
                yield from self._process_connect_init()
                self._current_server.reconnects = 0
                break
            except ErrAuthorization as e:
                self._err = e
                self._current_server = datetime.now()
                self._current_server.reconnects += 1
            except ErrNoServers as e:
                self._err = e
                break

    @asyncio.coroutine
    def close(self):
        """
        Closes the socket to which we are connected.
        """
        self._io_writer.close()

    @asyncio.coroutine
    def publish(self, subject, payload):
        """
        Takes a subject string and a payload in bytes
        then publishes a PUB command.
        """
        payload_size = len(payload)
        if payload_size > self._max_payload:
            raise ErrMaxPayload

        payload_size_bytes = ("%d" % payload_size).encode()
        pub_cmd = PUB_OP + _SPC_ + subject.encode() + _SPC_ + payload_size_bytes + _CRLF_
        pub_cmd += payload + _CRLF_
        self._io_writer.write(pub_cmd)
        yield from self._io_writer.drain()

    def last_error(self):
        """
        Returns the last error which may have occured.
        """
        return self._err

    def _setup_server_pool(self, servers):
        for server in servers:
            uri = urlparse(server)
            self._server_pool.append(Srv(uri))

    @asyncio.coroutine
    def _select_next_server(self):
        """
        Looks up in the server pool for an available server
        and attempts to connect.
        """
        srv = None
        for s in self._server_pool:
            try:
                r, w = yield from asyncio.open_connection(s.uri.hostname, s.uri.port, limit=DEFAULT_BUFFER_SIZE)
                srv = s
                self._io_reader = r
                self._io_writer = w
                break
            except Exception as e:
                # TODO: Would be connect error
                s.last_attempt = datetime.now()

        if srv is None:
            raise ErrNoServers
        self._current_server = srv

    def _process_op_err(self, e):
        """
        Process errors which occured while reading or parsing
        the protocol. If allow_reconnect is enabled it will
        try to switch the server to which it is currently connected
        otherwise it will disconnect.
        """
        pass

    def _connect_command(self):
        '''
        Generates a JSON string with the params to be used
        when sending CONNECT to the server.

        ->> CONNECT {"lang": "python3"}

        '''
        options = {
            "verbose":  self.options["verbose"],
            "pedantic": self.options["pedantic"],
            "lang":     __lang__,
            "version":  __version__
        }
        if "auth_required" in self._server_info:
            if self._server_info["auth_required"] == True:
                options["user"] = self._current_server.uri.username
                options["pass"] = self._current_server.uri.password
        if self.options["name"] is not None:
            options["name"] = self.options["name"]

        connect_opts = json.dumps(options, sort_keys=True)
        return CONNECT_OP + _SPC_ + connect_opts.encode() + _CRLF_

    @asyncio.coroutine
    def _process_connect_init(self):
        """
        Process INFO received from the server and CONNECT to the server
        with authentication.
        """
        info_line = yield from self._io_reader.readline()
        _, info = info_line.split(INFO_OP+_SPC_, 1)
        self._server_info = json.loads(info.decode())
        self._max_payload = self._server_info["max_payload"]

        connect_cmd = self._connect_command()
        self._io_writer.write(connect_cmd)
        self._io_writer.write(PING_PROTO)
        yield from self._io_writer.drain()

        # TODO: Add readline timeout.
        next_op = yield from self._io_reader.readline()
        if next_op == OK_OP:
            print("just got ok, should get another line...")
            next_op = yield from self._io_reader.readline()

        if next_op == PONG_PROTO:
            print("TODO: Set connected status.")
        elif next_op == ERR_OP:
            print("TODO: Check error and raise")

class Srv(object):
    """
    Srv is a helper data structure to hold state of a server.
    """
    def __init__(self, uri):
        self.uri = uri
        self.reconnects = 0
        self.last_attempt = None

class ErrNoServers(Exception):
    pass

class ErrAuthorization(Exception):
    pass

class ErrMaxPayload(Exception):
    pass
