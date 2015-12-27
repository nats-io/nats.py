import asyncio
import json
import time
from datetime import datetime
from urllib.parse import urlparse
from nats.io.errors import *

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

DEFAULT_BUFFER_SIZE            = 32768
DEFAULT_RECONNECT_TIME_WAIT    = 2 # in seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 10

MAX_CONTROL_LINE_SIZE  = 1024

class Client():

    DISCONNECTED = 0
    CONNECTED    = 1
    CLOSED       = 2
    RECONNECTING = 3
    CONNECTING   = 4

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
        self._disconnected_cb = None
        self._closed_cb = None
        self._max_payload = 1048576
        self._ssid = 0
        self._status = Client.DISCONNECTED
        self.stats = {
            'in_msgs':    0,
            'out_msgs':   0,
            'in_bytes':   0,
            'out_bytes':  0,
            'reconnects': 0,
            'errors_received': 0
            }

    @asyncio.coroutine
    def connect(self,
                servers=["nats://127.0.0.1:4222"],
                io_loop=asyncio.get_event_loop(),
                error_cb=None,
                disconnected_cb=None,
                closed_cb=None,
                name=None,
                pedantic=False,
                verbose=False,
                allow_reconnect=True,
                reconnect_time_wait=DEFAULT_RECONNECT_TIME_WAIT,
                max_reconnect_attempts=DEFAULT_MAX_RECONNECT_ATTEMPTS,
                ):
        self._setup_server_pool(servers)
        self._loop = io_loop
        self._error_cb = error_cb
        self._disconnected_cb = disconnected_cb
        self._closed_cb = closed_cb

        self.options["verbose"] = verbose
        self.options["pedantic"] = pedantic
        self.options["name"] = name
        self.options["allow_reconnect"] = allow_reconnect
        self.options["reconnect_time_wait"] = reconnect_time_wait
        self.options["max_reconnect_attempts"] = max_reconnect_attempts

        while True:
            try:
                yield from self._select_next_server()
                yield from self._process_connect_init()
                self._current_server.reconnects = 0
                break
            except ErrNoServers as e:
                self._err = e
                raise e
            except NatsError as e:
                self._close(Client.DISCONNECTED, False)
                self._err = e
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1

    @asyncio.coroutine
    def close(self):
        """
        Closes the socket to which we are connected.
        """
        self._close(self, Client.CLOSED)

    def _close(self, status, do_cbs=True):
        if self.is_closed():
            print("closed already...")
            self._status = status
            return
        # TODO: Kick flusher
        # TODO: Remove anything in pending buffer
        # TODO: Stop ping interval
        # TODO: Cleanup subscriptions
        self._status = Client.CLOSED
        if do_cbs:
            if self._disconnected_cb is not None:
                self._disconnected_cb()
            if self._closed_cb is not None:
                self._closed_cb()

        if self._io_writer is not None:
            self._io_writer.close()

    @asyncio.coroutine
    def publish(self, subject, payload):
        """
        Takes a subject string and a payload in bytes
        then publishes a PUB command.
        """
        if self.is_closed():
            raise ErrConnectionClosed

        payload_size = len(payload)
        if payload_size > self._max_payload:
            raise ErrMaxPayload

        payload_size_bytes = ("%d" % payload_size).encode()
        pub_cmd = PUB_OP + _SPC_ + subject.encode() + _SPC_ + payload_size_bytes + _CRLF_
        pub_cmd += payload + _CRLF_
        self._io_writer.write(pub_cmd)
        yield from self._io_writer.drain()

    @asyncio.coroutine
    def subscribe(self, subject, queue="", cb=None):
        """
        Takes a subject string and optional queue string to send a SUB cmd,
        and a callback which to which nats.io.Msg will be dispatched.
        """
        if self.is_closed():
            raise ErrConnectionClosed

        self._ssid += 1
        ssid = "%d" % self._ssid
        sub_cmd = SUB_OP + _SPC_ + subject.encode() + _SPC_ + queue.encode() + _SPC_ +  ssid.encode() + _CRLF_
        self._io_writer.write(sub_cmd)
        yield from self._io_writer.drain()

    def last_error(self):
        """
        Returns the last error which may have occured.
        """
        return self._err

    def is_closed(self):
        return self._status == Client.CLOSED

    def is_reconnecting(self):
        return self._status == Client.RECONNECTING

    def is_connected(self):
        return self._status == Client.CONNECTED

    def is_connecting(self):
        return self._status == Client.CONNECTING

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
        now = time.monotonic()
        for s in self._server_pool:
            if s.reconnects > self.options["max_reconnect_attempts"]:
                continue
            if s.did_connect and now < s.last_attempt + self.options["reconnect_time_wait"]:
                yield from asyncio.sleep(self.options["reconnect_time_wait"])
            try:
                s.last_attempt = time.monotonic()
                r, w = yield from asyncio.open_connection(s.uri.hostname, s.uri.port, limit=DEFAULT_BUFFER_SIZE)
                srv = s
                self._io_reader = r
                self._io_writer = w
                s.did_connect = True
                break
            except Exception as e:
                self._err = e

        if srv is None:
            raise ErrNoServers
        self._current_server = srv

    @asyncio.coroutine
    def _process_err(self, err_msg):
        """
        Processes the raw error message sent by the server
        and close connection with current server.
        """
        if "'Stale Connection'" in err_msg:
            self._process_op_err(ErrStaleConnection)
            return

        if "'Authorization Violation'" in err_msg:
            self._err = ErrAuthorization
        else:
            self._err = NatsError("nats:"+err_msg)

        do_cbs = False
        if not self.is_connecting():
            do_cbs = True
        self._close(Client.CLOSED, do_cbs)

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
        self._status = Client.CONNECTING

        # TODO: Add readline timeout
        info_line = yield from self._io_reader.readline()
        _, info = info_line.split(INFO_OP+_SPC_, 1)
        self._server_info = json.loads(info.decode())
        self._max_payload = self._server_info["max_payload"]

        connect_cmd = self._connect_command()
        self._io_writer.write(connect_cmd)
        self._io_writer.write(PING_PROTO)
        yield from self._io_writer.drain()

        # TODO: Add readline timeout
        next_op = yield from self._io_reader.readline()
        if self.options["verbose"] and OK_OP in next_op:
            next_op = yield from self._io_reader.readline()

        if ERR_OP in next_op:
            err_line = next_op.decode()
            _, err_msg = err_line.split(" ", 1)
            # FIXME: Maybe handling could be more special here,
            # checking for ErrAuthorization for example.
            # yield from self._process_err(err_msg)
            raise NatsError("nats: "+err_msg.rstrip('\r\n'))

        if PONG_PROTO in next_op:
            self._status = Client.CONNECTED

class Srv(object):
    """
    Srv is a helper data structure to hold state of a server.
    """
    def __init__(self, uri):
        self.uri = uri
        self.reconnects = 0
        self.last_attempt = None
        self.did_connect = False
