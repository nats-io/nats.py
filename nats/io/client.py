# Copyright 2015 Apcera Inc. All rights reserved.

import asyncio
import json
import time
from datetime import datetime
from urllib.parse import urlparse
from nats.io.errors import *
from nats.protocol.parser import *

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

DEFAULT_PENDING_SIZE           = 1024 * 1024
DEFAULT_BUFFER_SIZE            = 32768
DEFAULT_RECONNECT_TIME_WAIT    = 2   # in seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 10
DEFAULT_PING_INTERVAL          = 120 # in seconds
DEFAULT_MAX_OUTSTANDING_PINGS  = 2
DEFAULT_MAX_PAYLOAD_SIZE       = 1048576

MAX_CONTROL_LINE_SIZE  = 1024

class Client():
    """
    Asyncio based client for NATS.
    """

    DISCONNECTED = 0
    CONNECTED    = 1
    CLOSED       = 2
    RECONNECTING = 3
    CONNECTING   = 4

    def __repr__(self):
        return "<nats client v{}>".format(__version__)

    def __init__(self):
        self._loop = None
        self._current_server = None
        self._server_info = {}
        self._server_pool = []
        self._reading_task = None
        self._ping_interval_task = None
        self._pings_outstanding = 0
        self._pongs_received = 0
        self._pongs = []
        self._io_reader = None
        self._io_writer = None
        self._err = None
        self._error_cb = None
        self._disconnected_cb = None
        self._closed_cb = None
        self._reconnected_cb = None
        self._max_payload = DEFAULT_MAX_PAYLOAD_SIZE
        self._ssid = 0
        self._subs = {}
        self._status = Client.DISCONNECTED
        self._ps = Parser(self)
        self._pending = []
        self.options = {}
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
                reconnected_cb=None,
                name=None,
                pedantic=False,
                verbose=False,
                allow_reconnect=True,
                reconnect_time_wait=DEFAULT_RECONNECT_TIME_WAIT,
                max_reconnect_attempts=DEFAULT_MAX_RECONNECT_ATTEMPTS,
                ping_interval=DEFAULT_PING_INTERVAL,
                max_outstanding_pings=DEFAULT_MAX_OUTSTANDING_PINGS,
                ):
        self._setup_server_pool(servers)
        self._loop = io_loop
        self._error_cb = error_cb
        self._disconnected_cb = disconnected_cb
        self._closed_cb = closed_cb
        self._reconnected_cb = reconnected_cb

        self.options["verbose"] = verbose
        self.options["pedantic"] = pedantic
        self.options["name"] = name
        self.options["allow_reconnect"] = allow_reconnect
        self.options["reconnect_time_wait"] = reconnect_time_wait
        self.options["max_reconnect_attempts"] = max_reconnect_attempts
        self.options["ping_interval"] = ping_interval
        self.options["max_outstanding_pings"] = max_outstanding_pings

        while True:
            try:
                yield from self._select_next_server()
                self._current_server.reconnects = 0
                self._status = Client.CONNECTING
                yield from self._process_connect_init()
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
        Closes the socket to which we are connected and
        sets the client to be in the CLOSED state.
        """
        self._close(self, Client.CLOSED)

    def _close(self, status, do_cbs=True):
        if self.is_closed:
            self._status = status
            return
        self._status = Client.CLOSED
        # TODO: Kick flusher
        # TODO: Remove anything in pending buffer
        if self._reading_task is not None:
            self._reading_task.cancel()

        if self._ping_interval_task is not None:
            self._ping_interval_task.cancel()

        # TODO: Cleanup subscriptions

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
        if self.is_closed:
            raise ErrConnectionClosed

        payload_size = len(payload)
        if payload_size > self._max_payload:
            raise ErrMaxPayload

        payload_size_bytes = ("%d" % payload_size).encode()
        pub_cmd = b''.join([PUB_OP, _SPC_, subject.encode(), _SPC_, payload_size_bytes, _CRLF_, payload, _CRLF_])
        yield from self._send_command(pub_cmd)

    @asyncio.coroutine
    def subscribe(self, subject, queue="", cb=None, future=None):
        """
        Takes a subject string and optional queue string to send a SUB cmd,
        and a callback which to which nats.io.Msg will be dispatched.
        """
        if self.is_closed:
            raise ErrConnectionClosed

        self._ssid += 1
        ssid = self._ssid
        sub = Subscription(subject=subject, queue=queue, cb=cb, future=future)
        self._subs[ssid] = sub
        yield from self._subscribe(sub, ssid)
        return ssid

    @asyncio.coroutine
    def _subscribe(self, sub, ssid):
        sub_cmd = b''.join([SUB_OP, _SPC_, sub.subject.encode(), _SPC_, sub.queue.encode(), _SPC_, ("%d" % ssid).encode(), _CRLF_])
        yield from self._send_command(sub_cmd)

    @asyncio.coroutine
    def flush(self, timeout=60):
        """
        Sends a pong to the server expecting a pong back ensuring
        what we have written so far has made it to the server and
        also enabling measuring of roundtrip time.
        In case a pong is not returned within the allowed timeout,
        then it will raise ErrTimeout.
        """
        if timeout <= 0:
            raise ErrBadTimeout

        if self.is_closed:
            raise ErrConnectionClosed

        try:
            future = asyncio.Future()
            yield from self._send_ping(future)
            yield from asyncio.wait_for(future, timeout, loop=self._loop)
        except asyncio.TimeoutError:
            raise ErrTimeout

    @property
    def last_error(self):
        """
        Returns the last error which may have occured.
        """
        return self._err

    @property
    def is_closed(self):
        return self._status == Client.CLOSED

    @property
    def is_reconnecting(self):
        return self._status == Client.RECONNECTING

    @property
    def is_connected(self):
        return self._status == Client.CONNECTED

    @property
    def is_connecting(self):
        return self._status == Client.CONNECTING

    @asyncio.coroutine
    def _send_command(self, cmd, priority=False):
        if priority:
            self._pending.insert(0, cmd)
        else:
            self._pending.append(cmd)

        if self.is_connected:
            yield from self._flush_pending()
        elif len(self._pending) > DEFAULT_PENDING_SIZE:
            yield from self._flush_pending()

    @asyncio.coroutine
    def _flush_pending(self):
        try:
            self._io_writer.write(b''.join(self._pending))
            yield from self._io_writer.drain()
            self._pending = []
        except OSError as e:
            self._process_op_err(e)

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
            if s.did_connect and now > s.last_attempt + self.options["reconnect_time_wait"]:
                yield from asyncio.sleep(self.options["reconnect_time_wait"])
            try:
                s.last_attempt = time.monotonic()
                r, w = yield from asyncio.open_connection(s.uri.hostname,
                                                          s.uri.port,
                                                          loop=self._loop,
                                                          limit=DEFAULT_BUFFER_SIZE,
                                                          )
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

    def _process_err(self, err_msg):
        """
        Processes the raw error message sent by the server
        and close connection with current server.
        """
        if STALE_CONNECTION in err_msg:
            self._process_op_err(ErrStaleConnection)
            return

        if AUTHORIZATION_VIOLATION in err_msg:
            self._err = ErrAuthorization
        else:
            self._err = NatsError("nats:"+err_msg)

        do_cbs = False
        if not self.is_connecting:
            do_cbs = True
        self._close(Client.CLOSED, do_cbs)

    def _process_op_err(self, e):
        """
        Process errors which occured while reading or parsing
        the protocol. If allow_reconnect is enabled it will
        try to switch the server to which it is currently connected
        otherwise it will disconnect.
        """
        if self.is_connecting or self.is_closed or self.is_reconnecting:
            return

        if self.options["allow_reconnect"] and self.is_connected:
            self._status = Client.RECONNECTING

            if self._reading_task is not None:
                self._reading_task.cancel()

            if self._ping_interval_task is not None:
                self._ping_interval_task.cancel()

            if self._io_writer is not None:
                self._io_writer.close()

            asyncio.Task(self._attempt_reconnect(), loop=self._loop)
        else:
            self._process_disconnect()
            self._err = e
            self._close(Client.CLOSED, True)

    @asyncio.coroutine
    def _attempt_reconnect(self):
        self._err = None
        if self._disconnected_cb is not None:
            self._disconnected_cb()

        if self.is_closed:
            return

        while True:
            try:
                yield from self._select_next_server()
                yield from self._process_connect_init()
                self.stats["reconnects"] += 1
                self._current_server.reconnects = 0
                # TODO: Resend susbcriptions (raw io_writer.writes)
                # TODO: flush_pending_size first (just io_write write)
                try:
                    # flush everything here
                    yield from self._io_writer.drain()
                except OSError as e:
                    self._err = e
                    self._status = Client.RECONNECTING
                    continue

                self._status = Client.CONNECTED

                yield from self.flush()
                if self._reconnected_cb is not None:
                    self._reconnected_cb()
                break
            except ErrNoServers as e:
                self._err = e
                yield from self.close()
                break
            except NatsError as e:
                self._err = e
                self._status = Client.RECONNECTING
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1

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

    def _process_ping(self):
        """
        Process PING sent by server.
        """
        asyncio.Task(self._send_command(PONG), loop=self._loop)

    def _process_pong(self):
        """
        Process PONG sent by server.
        """
        if len(self._pongs) > 0:
            future = self._pongs.pop(0)
            future.set_result(True)
            self._pongs_received += 1
            self._pings_outstanding -= 1

    def _process_msg(self, msg):
        """
        Process MSG sent by server.
        """
        sub = self._subs[msg.sid]
        sub.received += 1
        if sub.cb is not None:
            sub.cb(msg)
        elif sub.future is not None:
            sub.future.set_result(msg)
        self.stats['in_msgs']  += 1
        self.stats['in_bytes'] += len(msg.data)

    def _process_disconnect(self):
        """
        Process disconnection from the server and set client status
        to DISCONNECTED.
        """
        self._status = Client.DISCONNECTED

    @asyncio.coroutine
    def _process_connect_init(self):
        """
        Process INFO received from the server and CONNECT to the server
        with authentication.  It is also responsible of setting up the
        reading and ping interval tasks from the client.
        """
        self._status = Client.CONNECTING

        # FIXME: Add readline timeout
        info_line = yield from self._io_reader.readline()
        _, info = info_line.split(INFO_OP+_SPC_, 1)
        self._server_info = json.loads(info.decode())
        self._max_payload = self._server_info["max_payload"]

        # Refresh state of parser upon reconnect.
        if self.is_reconnecting:
            self._ps.reset()

        connect_cmd = self._connect_command()
        self._io_writer.write(connect_cmd)
        self._io_writer.write(PING_PROTO)
        yield from self._io_writer.drain()

        # FIXME: Add readline timeout
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

        self._reading_task = asyncio.Task(self._read_loop(), loop=self._loop)
        self._pongs = []
        self._pings_outstanding = 0
        self._ping_interval_task = asyncio.Task(self._ping_interval(), loop=self._loop)

    @asyncio.coroutine
    def _send_ping(self, future=None):
        if future is None:
            future = asyncio.Future()
        self._pongs.append(future)
        yield from self._send_command(PING_PROTO, priority=True)

    @asyncio.coroutine
    def _ping_interval(self):
        while True:
            yield from asyncio.sleep(self.options["ping_interval"],
                                     loop=self._loop)
            if not self.is_connected:
                continue
            try:
                self._pings_outstanding += 1
                if self._pings_outstanding > self.options["max_outstanding_pings"]:
                    self._process_op_err(ErrStaleConnection)
                    return
                yield from self._send_ping()
            except asyncio.CancelledError:
                break
            except asyncio.InvalidStateError:
                pass

    @asyncio.coroutine
    def _read_loop(self):
        """
        Coroutine which gathers bytes sent by the server
        and feeds them to the protocol parser.
        In case of error while reading, it will stop running
        and its task has to be rescheduled.
        """
        while True:
            try:
                should_bail = self.is_closed or self.is_reconnecting or self._io_reader.at_eof()
                if should_bail or self._io_reader is None:
                    break
                b = yield from self._io_reader.read(DEFAULT_BUFFER_SIZE)
                self._ps.parse(b)
            except asyncio.CancelledError:
                break
            except ErrProtocol:
                self._process_op_err(ErrProtocol)
                break
            except OSError as e:
                self._process_op_err(e)
                break

class Subscription():
    def __init__(self, **params):
        self.subject  = params["subject"]
        self.queue    = params["queue"]
        self.cb       = params["cb"]
        self.future   = params["future"]
        self.received = 0

class Srv():
    """
    Srv is a helper data structure to hold state of a server.
    """
    def __init__(self, uri):
        self.uri = uri
        self.reconnects = 0
        self.last_attempt = None
        self.did_connect = False

