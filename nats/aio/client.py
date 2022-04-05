# Copyright 2016-2022 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import asyncio
import base64
import ipaddress
import json
import logging
import ssl
import sys
import time
from dataclasses import dataclass
from email.parser import BytesParser
from random import shuffle
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)
from urllib.parse import ParseResult, urlparse

import nats.js
from nats import errors
from nats.nuid import NUID
from nats.protocol import command as prot_command
from nats.protocol.parser import (
    AUTHORIZATION_VIOLATION,
    PERMISSIONS_ERR,
    PONG,
    STALE_CONNECTION,
    Parser,
)

from .errors import ErrInvalidUserCredentials, ErrStaleConnection
from .msg import Msg
from .subscription import (
    DEFAULT_SUB_PENDING_BYTES_LIMIT,
    DEFAULT_SUB_PENDING_MSGS_LIMIT,
    Subscription,
)

__version__ = '2.1.0'
__lang__ = 'python3'
_logger = logging.getLogger(__name__)
PROTOCOL = 1

INFO_OP = b'INFO'
CONNECT_OP = b'CONNECT'
PING_OP = b'PING'
PONG_OP = b'PONG'
OK_OP = b'+OK'
ERR_OP = b'-ERR'
_CRLF_ = b'\r\n'
_SPC_ = b' '
EMPTY = ""

PING_PROTO = PING_OP + _CRLF_
PONG_PROTO = PONG_OP + _CRLF_
DEFAULT_INBOX_PREFIX = b'_INBOX'

DEFAULT_PENDING_SIZE = 2 * 1024 * 1024
DEFAULT_BUFFER_SIZE = 32768
DEFAULT_RECONNECT_TIME_WAIT = 2  # in seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 60
DEFAULT_PING_INTERVAL = 120  # in seconds
DEFAULT_MAX_OUTSTANDING_PINGS = 2
DEFAULT_MAX_PAYLOAD_SIZE = 1048576
DEFAULT_MAX_FLUSHER_QUEUE_SIZE = 1024
DEFAULT_FLUSH_TIMEOUT = 10  # in seconds
DEFAULT_CONNECT_TIMEOUT = 2  # in seconds
DEFAULT_DRAIN_TIMEOUT = 30  # in seconds
MAX_CONTROL_LINE_SIZE = 1024

NATS_HDR_LINE = bytearray(b'NATS/1.0\r\n')
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
NO_RESPONDERS_STATUS = "503"
CTRL_STATUS = "100"
STATUS_MSG_LEN = 3  # e.g. 20x, 40x, 50x
CTRL_LEN = len(_CRLF_)

Callback = Callable[[], Awaitable[None]]
ErrorCallback = Callable[[Exception], Awaitable[None]]
JWTCallback = Callable[[], Union[bytearray, bytes]]
Credentials = Union[str, Tuple[str, str]]
SignatureCallback = Callable[[str], bytes]


@dataclass
class Srv:
    """
    Srv is a helper data structure to hold state of a server.
    """
    uri: ParseResult
    reconnects: int = 0
    last_attempt: Optional[float] = None
    did_connect: bool = False
    discovered: bool = False
    tls_name: Optional[str] = None


async def _default_error_callback(ex: Exception) -> None:
    """
    Provides a default way to handle async errors if the user
    does not provide one.
    """
    _logger.error('nats: encountered error', exc_info=ex)


class Client:
    """
    Asyncio based client for NATS.
    """

    msg_class: Type[Msg] = Msg

    # FIXME: Use an enum instead.
    DISCONNECTED = 0
    CONNECTED = 1
    CLOSED = 2
    RECONNECTING = 3
    CONNECTING = 4
    DRAINING_SUBS = 5
    DRAINING_PUBS = 6

    def __repr__(self) -> str:
        return f"<nats client v{__version__}>"

    def __init__(self) -> None:
        self._current_server: Optional[Srv] = None
        self._server_info: Dict[str, Any] = {}
        self._server_pool: List[Srv] = []
        self._reading_task: Optional[asyncio.Task] = None
        self._ping_interval_task: Optional[asyncio.Task] = None
        self._pings_outstanding: int = 0
        self._pongs_received: int = 0
        self._pongs: List[asyncio.Future] = []
        self._bare_io_reader: Optional[asyncio.StreamReader] = None
        self._io_reader: Optional[asyncio.StreamReader] = None
        self._bare_io_writer: Optional[asyncio.StreamWriter] = None
        self._io_writer: Optional[asyncio.StreamWriter] = None
        self._err: Optional[Exception] = None

        # callbacks
        self._error_cb: ErrorCallback = _default_error_callback
        self._disconnected_cb: Optional[Callback] = None
        self._closed_cb: Optional[Callback] = None
        self._discovered_server_cb: Optional[Callback] = None
        self._reconnected_cb: Optional[Callback] = None

        self._reconnection_task: Union[asyncio.Task[None], None] = None
        self._reconnection_task_future: Optional[asyncio.Future] = None
        self._max_payload: int = DEFAULT_MAX_PAYLOAD_SIZE
        # This is the client id that the NATS server knows
        # about. Useful in debugging application errors
        # when logged with this identifier along
        # with nats server log.
        # This would make more sense if we log the server
        # connected to as well in case of cluster setup.
        self._client_id: Optional[str] = None
        self._sid: int = 0
        self._subs: Dict[int, Subscription] = {}
        self._status: int = Client.DISCONNECTED
        self._ps: Parser = Parser(self)

        # pending queue of commands that will be flushed to the server.
        self._pending: List[bytes] = []

        # current size of pending data in total.
        self._pending_data_size: int = 0

        # max pending size is the maximum size of the data that can be buffered.
        self._max_pending_size: int = 0

        self._flush_queue: Optional["asyncio.Queue[None]"] = None
        self._flusher_task: Optional[asyncio.Task] = None
        self._flush_timeout: float = 0
        self._hdr_parser: BytesParser = BytesParser()

        # New style request/response
        self._resp_map: Dict[str, asyncio.Future] = {}
        self._resp_sub_prefix: Optional[bytearray] = None
        self._nuid = NUID()
        self._inbox_prefix = bytearray(DEFAULT_INBOX_PREFIX)

        # NKEYS support
        #
        # user_jwt_cb is used to fetch and return the account
        # signed JWT for this user.
        self._user_jwt_cb: Optional[JWTCallback] = None

        # signature_cb is used to sign a nonce from the server while
        # authenticating with nkeys. The user should sign the nonce and
        # return the base64 encoded signature.
        self._signature_cb: Optional[SignatureCallback] = None

        # user credentials file can be a tuple or single file.
        self._user_credentials: Optional[Credentials] = None

        # file that contains the nkeys seed and its public key as a string.
        self._nkeys_seed: Optional[str] = None
        self._public_nkey: Optional[str] = None

        self.options: Dict[str, Any] = {}
        self.stats = {
            'in_msgs': 0,
            'out_msgs': 0,
            'in_bytes': 0,
            'out_bytes': 0,
            'reconnects': 0,
            'errors_received': 0,
        }

    async def connect(
        self,
        servers: Union[str, List[str]] = ["nats://localhost:4222"],
        error_cb: Optional[ErrorCallback] = None,
        disconnected_cb: Optional[Callback] = None,
        closed_cb: Optional[Callback] = None,
        discovered_server_cb: Optional[Callback] = None,
        reconnected_cb: Optional[Callback] = None,
        name: Optional[str] = None,
        pedantic: bool = False,
        verbose: bool = False,
        allow_reconnect: bool = True,
        connect_timeout: int = DEFAULT_CONNECT_TIMEOUT,
        reconnect_time_wait: int = DEFAULT_RECONNECT_TIME_WAIT,
        max_reconnect_attempts: int = DEFAULT_MAX_RECONNECT_ATTEMPTS,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        max_outstanding_pings: int = DEFAULT_MAX_OUTSTANDING_PINGS,
        dont_randomize: bool = False,
        flusher_queue_size: int = DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
        no_echo: bool = False,
        tls: Optional[ssl.SSLContext] = None,
        tls_hostname: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        drain_timeout: int = DEFAULT_DRAIN_TIMEOUT,
        signature_cb: Optional[SignatureCallback] = None,
        user_jwt_cb: Optional[JWTCallback] = None,
        user_credentials: Optional[Credentials] = None,
        nkeys_seed: Optional[str] = None,
        inbox_prefix: Union[str, bytes] = DEFAULT_INBOX_PREFIX,
        pending_size: int = DEFAULT_PENDING_SIZE,
        flush_timeout: Optional[float] = None,
    ) -> None:
        """
        Establishes a connection to NATS.

        :param servers: NATS Connection
        :param name: Label the connection with name (shown in NATS monitoring)
        :param error_cb: Callback to report errors.
        :param disconnected_cb: Callback to report disconnection from NATS.
        :param closed_cb: Callback to report when client stops reconnection to NATS.
        :param discovered_server_cb: Callback to report when a new server joins the cluster.
        :param pending_size: Max size of the pending buffer for publishing commands.
        :param flush_timeout: Max duration to wait for a forced flush to occur.

        Connecting setting all callbacks::

            import asyncio
            import nats

            async def main():
                async def disconnected_cb():
                    print('Got disconnected!')

                async def reconnected_cb():
                    print(f'Got reconnected to {nc.connected_url.netloc}')

                async def error_cb(e):
                    print(f'There was an error: {e}')

                async def closed_cb():
                    print('Connection is closed')

                # Connect to NATS with logging callbacks.
                nc = await nats.connect('demo.nats.io',
                                         error_cb=error_cb,
                                         reconnected_cb=reconnected_cb,
                                         disconnected_cb=disconnected_cb,
                                         closed_cb=closed_cb,
                                         )

                async def handler(msg):
                    print(f'Received a message on {msg.subject} {msg.reply}: {msg.data}')
                    await msg.respond(b'OK')

                sub = await nc.subscribe('help.please', cb=handler)

                resp = await nc.request('help.please', b'help')
                print('Response:', resp)

                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())

        Using a context manager::

            import asyncio
            import nats

            async def main():

                is_done = asyncio.Future()

                async def closed_cb():
                    print('Connection to NATS is closed.')
                    is_done.set_result(True)

                async with (await nats.connect('nats://demo.nats.io:4222', closed_cb=closed_cb)) as nc:
                    print(f'Connected to NATS at {nc.connected_url.netloc}...')

                    async def subscribe_handler(msg):
                        subject = msg.subject
                        reply = msg.reply
                        data = msg.data.decode()
                        print('Received a message on '{subject} {reply}': {data}'.format(
                            subject=subject, reply=reply, data=data))

                    await nc.subscribe('discover', cb=subscribe_handler)
                    await nc.flush()

                    for i in range(0, 10):
                        await nc.publish('discover', b'hello world')
                        await asyncio.sleep(0.1)

                await asyncio.wait_for(is_done, 60.0)

            if __name__ == '__main__':
                asyncio.run(main())

        """

        for cb in [error_cb, disconnected_cb, closed_cb, reconnected_cb,
                   discovered_server_cb]:
            if cb and not asyncio.iscoroutinefunction(cb):
                raise errors.InvalidCallbackTypeError

        self._setup_server_pool(servers)
        self._error_cb = error_cb or _default_error_callback
        self._closed_cb = closed_cb
        self._discovered_server_cb = discovered_server_cb
        self._reconnected_cb = reconnected_cb
        self._disconnected_cb = disconnected_cb

        # Custom inbox prefix
        if isinstance(inbox_prefix, str):
            inbox_prefix = inbox_prefix.encode()
        assert isinstance(inbox_prefix, bytes)
        self._inbox_prefix = bytearray(inbox_prefix)

        # NKEYS support
        self._signature_cb = signature_cb
        self._user_jwt_cb = user_jwt_cb
        self._user_credentials = user_credentials
        self._nkeys_seed = nkeys_seed

        # Customizable options
        self.options["verbose"] = verbose
        self.options["pedantic"] = pedantic
        self.options["name"] = name
        self.options["allow_reconnect"] = allow_reconnect
        self.options["dont_randomize"] = dont_randomize
        self.options["reconnect_time_wait"] = reconnect_time_wait
        self.options["max_reconnect_attempts"] = max_reconnect_attempts
        self.options["ping_interval"] = ping_interval
        self.options["max_outstanding_pings"] = max_outstanding_pings
        self.options["no_echo"] = no_echo
        self.options["user"] = user
        self.options["password"] = password
        self.options["token"] = token
        self.options["connect_timeout"] = connect_timeout
        self.options["drain_timeout"] = drain_timeout

        if tls:
            self.options['tls'] = tls
        if tls_hostname:
            self.options['tls_hostname'] = tls_hostname

        if self._user_credentials is not None or self._nkeys_seed is not None:
            self._setup_nkeys_connect()

        # Queue used to trigger flushes to the socket.
        self._flush_queue = asyncio.Queue(maxsize=flusher_queue_size)

        # Max size of buffer used for flushing commands to the server.
        self._max_pending_size = pending_size

        # Max duration for a force flush (happens when a buffer is full).
        self._flush_timeout = flush_timeout

        if self.options["dont_randomize"] is False:
            shuffle(self._server_pool)

        while True:
            try:
                await self._select_next_server()
                await self._process_connect_init()
                assert self._current_server, "the current server must be set by _select_next_server"
                self._current_server.reconnects = 0
                break
            except errors.NoServersError as e:
                if self.options["max_reconnect_attempts"] < 0:
                    # Never stop reconnecting
                    continue
                self._err = e
                raise e
            except (OSError, errors.Error, asyncio.TimeoutError) as e:
                self._err = e
                await self._error_cb(e)

                # Bail on first attempt if reconnecting is disallowed.
                if not self.options["allow_reconnect"]:
                    raise e

                await self._close(Client.DISCONNECTED, False)
                if self._current_server is not None:
                    self._current_server.last_attempt = time.monotonic()
                    self._current_server.reconnects += 1

    def _setup_nkeys_connect(self) -> None:
        if self._user_credentials is not None:
            self._setup_nkeys_jwt_connect()
        else:
            self._setup_nkeys_seed_connect()

    def _setup_nkeys_jwt_connect(self) -> None:
        assert self._user_credentials, "_user_credentials required"
        import os

        import nkeys

        creds = self._user_credentials
        if isinstance(creds, tuple):
            assert len(creds) == 2

            def user_cb() -> bytearray:
                contents = None
                with open(creds[0], 'rb') as f:
                    contents = bytearray(os.fstat(f.fileno()).st_size)
                    f.readinto(contents)  # type: ignore[attr-defined]
                return contents

            self._user_jwt_cb = user_cb

            def sig_cb(nonce: str) -> bytes:
                seed = None
                with open(creds[1], 'rb') as f:
                    seed = bytearray(os.fstat(f.fileno()).st_size)
                    f.readinto(seed)  # type: ignore[attr-defined]
                kp = nkeys.from_seed(seed)
                raw_signed = kp.sign(nonce.encode())
                sig = base64.b64encode(raw_signed)

                # Best effort attempt to clear from memory.
                kp.wipe()
                del kp
                del seed
                return sig

            self._signature_cb = sig_cb
        else:
            # Define the functions to be able to sign things using nkeys.
            def user_cb() -> bytearray:
                assert isinstance(creds, str)
                user_jwt = None
                with open(creds, 'rb') as f:
                    while True:
                        line = bytearray(f.readline())
                        if b'BEGIN NATS USER JWT' in line:
                            user_jwt = bytearray(f.readline())
                            break
                # Remove trailing line break but reusing same memory view.
                return user_jwt[:len(user_jwt) - 1]

            self._user_jwt_cb = user_cb

            def sig_cb(nonce: str) -> bytes:
                assert isinstance(creds, str)
                user_seed = None
                with open(creds, 'rb', buffering=0) as f:
                    for line in f:
                        # Detect line where the NKEY would start and end,
                        # then seek and read into a fixed bytearray that
                        # can be wiped.
                        if b'BEGIN USER NKEY SEED' in line:
                            nkey_start_pos = f.tell()
                            try:
                                next(f)
                            except StopIteration:
                                raise ErrInvalidUserCredentials
                            nkey_end_pos = f.tell()
                            nkey_size = nkey_end_pos - nkey_start_pos - 1
                            f.seek(nkey_start_pos)

                            # Only gather enough bytes for the user seed
                            # into the pre allocated bytearray.
                            user_seed = bytearray(nkey_size)
                            f.readinto(user_seed)  # type: ignore[attr-defined]
                kp = nkeys.from_seed(user_seed)
                raw_signed = kp.sign(nonce.encode())
                sig = base64.b64encode(raw_signed)

                # Delete all state related to the keys.
                kp.wipe()
                del user_seed
                del kp
                return sig

            self._signature_cb = sig_cb

    def _setup_nkeys_seed_connect(self) -> None:
        assert self._nkeys_seed, "Client.connect must be called first"
        import os

        import nkeys

        seed = None
        creds = self._nkeys_seed
        with open(creds, 'rb') as f:
            seed = bytearray(os.fstat(f.fileno()).st_size)
            f.readinto(seed)  # type: ignore[attr-defined]
        kp = nkeys.from_seed(seed)
        self._public_nkey = kp.public_key.decode()
        kp.wipe()
        del kp
        del seed

        def sig_cb(nonce: str) -> bytes:
            seed = None
            with open(creds, 'rb') as f:
                seed = bytearray(os.fstat(f.fileno()).st_size)
                f.readinto(seed)  # type: ignore[attr-defined]
            kp = nkeys.from_seed(seed)
            raw_signed = kp.sign(nonce.encode())
            sig = base64.b64encode(raw_signed)

            # Best effort attempt to clear from memory.
            kp.wipe()
            del kp
            del seed
            return sig

        self._signature_cb = sig_cb

    async def close(self) -> None:
        """
        Closes the socket to which we are connected and
        sets the client to be in the CLOSED state.
        No further reconnections occur once reaching this point.
        """
        await self._close(Client.CLOSED)

    async def _close(self, status: int, do_cbs: bool = True) -> None:
        if self.is_closed:
            self._status = status
            return
        self._status = Client.CLOSED

        # Kick the flusher once again so that Task breaks and avoid pending futures.
        await self._flush_pending()

        if self._reading_task is not None and not self._reading_task.cancelled(
        ):
            self._reading_task.cancel()

        if self._ping_interval_task is not None and not self._ping_interval_task.cancelled(
        ):
            self._ping_interval_task.cancel()

        if self._flusher_task is not None and not self._flusher_task.cancelled(
        ):
            self._flusher_task.cancel()

        if self._reconnection_task is not None and not self._reconnection_task.done(
        ):
            self._reconnection_task.cancel()

            # Wait for the reconection task to be done which should be soon.
            try:
                if self._reconnection_task_future is not None and not self._reconnection_task_future.cancelled(
                ):
                    await asyncio.wait_for(
                        self._reconnection_task_future,
                        self.options["reconnect_time_wait"],
                    )
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # Relinquish control to allow background tasks to wrap up.
        await asyncio.sleep(0)

        assert self._io_writer, "Client.connect must be called first"
        if self._current_server is not None:
            # In case there is any pending data at this point, flush before disconnecting.
            if self._pending_data_size > 0:
                self._io_writer.writelines(self._pending[:])
                self._pending = []
                self._pending_data_size = 0
                await self._io_writer.drain()

        # Cleanup subscriptions since not reconnecting so no need
        # to replay the subscriptions anymore.
        for sub in self._subs.values():
            # FIXME: Should we clear the pending queue here?
            if sub._wait_for_msgs_task and not sub._wait_for_msgs_task.done():
                sub._wait_for_msgs_task.cancel()
        self._subs.clear()

        if self._io_writer is not None:
            self._io_writer.close()
            try:
                await self._io_writer.wait_closed()
            except Exception as e:
                await self._error_cb(e)

        if do_cbs:
            if self._disconnected_cb is not None:
                await self._disconnected_cb()
            if self._closed_cb is not None:
                await self._closed_cb()

        # Set the client_id back to None
        self._client_id = None

    async def drain(self) -> None:
        """
        drain will put a connection into a drain state. All subscriptions will
        immediately be put into a drain state. Upon completion, the publishers
        will be drained and can not publish any additional messages. Upon draining
        of the publishers, the connection will be closed. Use the `closed_cb`
        option to know when the connection has moved from draining to closed.

        """
        if self.is_draining:
            return
        if self.is_closed:
            raise errors.ConnectionClosedError
        if self.is_connecting or self.is_reconnecting:
            raise errors.ConnectionReconnectingError

        drain_tasks = []
        for sub in self._subs.values():
            coro = sub._drain()
            task = asyncio.get_running_loop().create_task(coro)
            drain_tasks.append(task)

        drain_is_done = asyncio.gather(*drain_tasks)

        # Start draining the subscriptions.
        # Relinquish CPU to allow drain tasks to start in the background,
        # before setting state to draining.
        await asyncio.sleep(0)
        self._status = Client.DRAINING_SUBS

        try:
            await asyncio.wait_for(
                drain_is_done, self.options["drain_timeout"]
            )
        except asyncio.TimeoutError:
            drain_is_done.exception()
            drain_is_done.cancel()
            await self._error_cb(errors.DrainTimeoutError())
        except asyncio.CancelledError:
            pass
        finally:
            self._status = Client.DRAINING_PUBS
            await self.flush()
            await self._close(Client.CLOSED)

    async def publish(
        self,
        subject: str,
        payload: bytes = b'',
        reply: str = '',
        headers: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Publishes a NATS message.

        :param subject: Subject to which the message will be published.
        :param payload: Message data.
        :param reply: Inbox to which a responder can respond.
        :param headers: Optional message header.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect('demo.nats.io')

                # Publish as message with an inbox.
                inbox = nc.new_inbox()
                sub = await nc.subscribe('hello')

                # Simple publishing
                await nc.publish('hello', b'Hello World!')

                # Publish with a reply
                await nc.publish('hello', b'Hello World!', reply=inbox)

                # Publish with a reply
                await nc.publish('hello', b'With Headers', headers={'Foo':'Bar'})

                while True:
                    try:
                        msg = await sub.next_msg()
                    except:
                        break
                    print('----------------------')
                    print('Subject:', msg.subject)
                    print('Reply  :', msg.reply)
                    print('Data   :', msg.data)
                    print('Headers:', msg.header)

            if __name__ == '__main__':
                asyncio.run(main())

        """

        if self.is_closed:
            raise errors.ConnectionClosedError
        if self.is_draining_pubs:
            raise errors.ConnectionDrainingError

        payload_size = len(payload)
        if not self.is_connected:
            if self._max_pending_size <= 0 or payload_size + self._pending_data_size > self._max_pending_size:
                # Cannot publish during a reconnection when the buffering is disabled,
                # or if pending buffer is already full.
                raise errors.OutboundBufferLimitError

        if payload_size > self._max_payload:
            raise errors.MaxPayloadError
        await self._send_publish(
            subject, reply, payload, payload_size, headers
        )

    async def _send_publish(
        self,
        subject: str,
        reply: str,
        payload: bytes,
        payload_size: int,
        headers: Optional[Dict[str, Any]],
    ) -> None:
        """
        Sends PUB command to the NATS server.
        """
        if subject == "":
            # Avoid sending messages with empty replies.
            raise errors.BadSubjectError

        pub_cmd = None
        if headers is None:
            pub_cmd = prot_command.pub_cmd(subject, reply, payload)
        else:
            hdr = bytearray()
            hdr.extend(NATS_HDR_LINE)
            for k, v in headers.items():
                hdr.extend(k.encode())
                hdr.extend(b': ')
                hdr.extend(v.encode())
                hdr.extend(_CRLF_)
            hdr.extend(_CRLF_)
            pub_cmd = prot_command.hpub_cmd(subject, reply, hdr, payload)

        self.stats['out_msgs'] += 1
        self.stats['out_bytes'] += payload_size
        await self._send_command(pub_cmd)
        if self._flush_queue is not None and self._flush_queue.empty():
            await self._flush_pending()

    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb: Optional[Callable[[Msg], Awaitable[None]]] = None,
        future: Optional[asyncio.Future] = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ) -> Subscription:
        """
        subscribe registers interest in a given subject.

        If a callback is provided, messages will be processed asychronously.

        If a callback isn't provided, messages can be retrieved via an
        asynchronous iterator on the returned subscription object.
        """
        if not subject:
            raise errors.BadSubjectError

        if self.is_closed:
            raise errors.ConnectionClosedError

        if self.is_draining:
            raise errors.ConnectionDrainingError

        self._sid += 1
        sid = self._sid

        sub = Subscription(
            self,
            sid,
            subject,
            queue=queue,
            cb=cb,
            future=future,
            max_msgs=max_msgs,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

        sub._start(self._error_cb)
        self._subs[sid] = sub
        await self._send_subscribe(sub)
        return sub

    def _remove_sub(self, sid: int, max_msgs: int = 0) -> None:
        self._subs.pop(sid, None)

    async def _send_subscribe(self, sub: Subscription) -> None:
        sub_cmd = None
        if sub._queue is None:
            sub_cmd = prot_command.sub_cmd(sub._subject, EMPTY, sub._id)
        else:
            sub_cmd = prot_command.sub_cmd(sub._subject, sub._queue, sub._id)
        await self._send_command(sub_cmd)
        await self._flush_pending()

    async def _init_request_sub(self) -> None:
        self._resp_map = {}

        self._resp_sub_prefix = self._inbox_prefix[:]
        self._resp_sub_prefix.extend(b'.')
        self._resp_sub_prefix.extend(self._nuid.next())
        self._resp_sub_prefix.extend(b'.')
        resp_mux_subject = self._resp_sub_prefix[:]
        resp_mux_subject.extend(b'*')
        await self.subscribe(
            resp_mux_subject.decode(), cb=self._request_sub_callback
        )

    async def _request_sub_callback(self, msg: Msg) -> None:
        token = msg.subject[len(self._inbox_prefix) + 22 + 2:]
        try:
            fut = self._resp_map.get(token)
            if not fut:
                return
            fut.set_result(msg)
            self._resp_map.pop(token, None)
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            # Request may have timed out already so remove the entry
            self._resp_map.pop(token, None)

    async def request(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: float = 0.5,
        old_style: bool = False,
        headers: Dict[str, Any] = None,
    ) -> Msg:
        """
        Implements the request/response pattern via pub/sub
        using a single wildcard subscription that handles
        the responses.

        """
        if old_style:
            # FIXME: Support headers in old style requests.
            return await self._request_old_style(
                subject, payload, timeout=timeout
            )
        else:
            msg = await self._request_new_style(
                subject, payload, timeout=timeout, headers=headers
            )
        if msg.headers and msg.headers.get(nats.js.api.Header.STATUS
                                           ) == NO_RESPONDERS_STATUS:
            raise errors.NoRespondersError
        return msg

    async def _request_new_style(
        self,
        subject: str,
        payload: bytes,
        timeout: float = 1,
        headers: Dict[str, Any] = None,
    ) -> Msg:
        if self.is_draining_pubs:
            raise errors.ConnectionDrainingError

        if not self._resp_sub_prefix:
            await self._init_request_sub()
        assert self._resp_sub_prefix

        # Use a new NUID for the token inbox and then use the future.
        token = self._nuid.next()
        inbox = self._resp_sub_prefix[:]
        inbox.extend(token)
        future: asyncio.Future = asyncio.Future()
        self._resp_map[token.decode()] = future
        await self.publish(
            subject, payload, reply=inbox.decode(), headers=headers
        )

        # Wait for the response or give up on timeout.
        try:
            msg = await asyncio.wait_for(future, timeout)
            return msg
        except asyncio.TimeoutError:
            # Double check that the token is there already.
            self._resp_map.pop(token.decode())
            future.cancel()
            raise errors.TimeoutError

    def new_inbox(self) -> str:
        """
        new_inbox returns a unique inbox that can be used
        for NATS requests or subscriptions::

           # Create unique subscription to receive direct messages.
           inbox = nc.new_inbox()
           sub = await nc.subscribe(inbox)
           nc.publish('broadcast', b'', reply=inbox)
           msg = sub.next_msg()
        """
        next_inbox = self._inbox_prefix[:]
        next_inbox.extend(b'.')
        next_inbox.extend(self._nuid.next())
        return next_inbox.decode()

    async def _request_old_style(
        self, subject: str, payload: bytes, timeout: float = 1
    ) -> Msg:
        """
        Implements the request/response pattern via pub/sub
        using an ephemeral subscription which will be published
        with a limited interest of 1 reply returning the response
        or raising a Timeout error.
        """
        inbox = self.new_inbox()

        future: asyncio.Future[Msg] = asyncio.Future()
        sub = await self.subscribe(inbox, future=future, max_msgs=1)
        await sub.unsubscribe(limit=1)
        await self.publish(subject, payload, reply=inbox)

        try:
            msg = await asyncio.wait_for(future, timeout)
            if msg.headers:
                if msg.headers.get(nats.js.api.Header.STATUS
                                   ) == NO_RESPONDERS_STATUS:
                    raise errors.NoRespondersError
            return msg
        except asyncio.TimeoutError:
            await sub.unsubscribe()
            future.cancel()
            raise errors.TimeoutError

    async def _send_unsubscribe(self, sid: int, limit: int = 0) -> None:
        unsub_cmd = prot_command.unsub_cmd(sid, limit)
        await self._send_command(unsub_cmd)
        await self._flush_pending()

    async def flush(self, timeout: int = DEFAULT_FLUSH_TIMEOUT) -> None:
        """
        Sends a ping to the server expecting a pong back ensuring
        what we have written so far has made it to the server and
        also enabling measuring of roundtrip time.
        In case a pong is not returned within the allowed timeout,
        then it will raise nats.errors.TimeoutError
        """
        if timeout <= 0:
            raise errors.BadTimeoutError

        if self.is_closed:
            raise errors.ConnectionClosedError

        future: asyncio.Future = asyncio.Future()
        try:
            await self._send_ping(future)
            await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            future.cancel()
            raise errors.FlushTimeoutError

    @property
    def connected_url(self) -> Optional[ParseResult]:
        if self._current_server and self.is_connected:
            return self._current_server.uri
        return None

    @property
    def servers(self) -> List[str]:
        servers = []
        for srv in self._server_pool:
            servers.append(str(srv.uri))
        return servers

    @property
    def discovered_servers(self) -> List[str]:
        servers = []
        for srv in self._server_pool:
            if srv.discovered:
                servers.append(str(srv.uri))
        return servers

    @property
    def max_payload(self) -> int:
        """
        Returns the max payload which we received from the servers INFO
        """
        return self._max_payload

    @property
    def client_id(self) -> Optional[str]:
        """
        Returns the client id which we received from the servers INFO
        """
        return self._client_id

    @property
    def last_error(self) -> Optional[Exception]:
        """
        Returns the last error which may have occured.
        """
        return self._err

    @property
    def pending_data_size(self) -> int:
        return self._pending_data_size

    @property
    def is_closed(self) -> bool:
        return self._status == Client.CLOSED

    @property
    def is_reconnecting(self) -> bool:
        return self._status == Client.RECONNECTING

    @property
    def is_connected(self) -> bool:
        return (self._status == Client.CONNECTED) or self.is_draining

    @property
    def is_connecting(self) -> bool:
        return self._status == Client.CONNECTING

    @property
    def is_draining(self) -> bool:
        return (
            self._status == Client.DRAINING_SUBS
            or self._status == Client.DRAINING_PUBS
        )

    @property
    def is_draining_pubs(self) -> bool:
        return self._status == Client.DRAINING_PUBS

    async def _send_command(self, cmd: bytes, priority: bool = False) -> None:
        if priority:
            self._pending.insert(0, cmd)
        else:
            self._pending.append(cmd)
        self._pending_data_size += len(cmd)
        if self._max_pending_size > 0 and self._pending_data_size > self._max_pending_size:
            # Only flush force timeout on publish
            await self._flush_pending(force_flush=True)

    async def _flush_pending(
        self,
        force_flush: bool = False,
    ) -> None:
        assert self._flush_queue, "Client.connect must be called first"
        try:
            future = asyncio.Future()
            if not self.is_connected:
                future.set_result(None)
                return future

            # kick the flusher!
            await self._flush_queue.put(future)

            if force_flush:
                try:
                    await asyncio.wait_for(future, self._flush_timeout)
                except asyncio.TimeoutError:
                    # Report to the async callback that there was a timeout.
                    await self._error_cb(errors.FlushTimeoutError())

        except asyncio.CancelledError:
            pass

    def _setup_server_pool(self, connect_url: Union[str, List[str]]) -> None:
        if isinstance(connect_url, str):
            try:
                if "nats://" in connect_url or "tls://" in connect_url:
                    # Closer to how the Go client handles this.
                    # e.g. nats://localhost:4222
                    uri = urlparse(connect_url)
                elif ":" in connect_url:
                    # Expand the scheme for the user
                    # e.g. localhost:4222
                    uri = urlparse(f"nats://{connect_url}")
                else:
                    # Just use the endpoint with the default NATS port.
                    # e.g. demo.nats.io
                    uri = urlparse(f"nats://{connect_url}:4222")

                # In case only endpoint with scheme was set.
                # e.g. nats://demo.nats.io or localhost:
                if uri.port is None:
                    uri = urlparse(f"nats://{uri.hostname}:4222")
            except ValueError:
                raise errors.Error("nats: invalid connect url option")

            if uri.hostname is None or uri.hostname == "none":
                raise errors.Error("nats: invalid hostname in connect url")
            self._server_pool.append(Srv(uri))
        elif isinstance(connect_url, list):
            try:
                for server in connect_url:
                    uri = urlparse(server)
                    self._server_pool.append(Srv(uri))
            except ValueError:
                raise errors.Error("nats: invalid connect url option")
        else:
            raise errors.Error("nats: invalid connect url option")

    async def _select_next_server(self) -> None:
        """
        Looks up in the server pool for an available server
        and attempts to connect.
        """

        while True:
            if len(self._server_pool) == 0:
                self._current_server = None
                raise errors.NoServersError

            now = time.monotonic()
            s = self._server_pool.pop(0)
            if self.options["max_reconnect_attempts"] > 0:
                if s.reconnects > self.options["max_reconnect_attempts"]:
                    # Discard server since already tried to reconnect too many times
                    continue

            # Not yet exceeded max_reconnect_attempts so can still use
            # this server in the future.
            self._server_pool.append(s)
            if s.last_attempt is not None and now < s.last_attempt + self.options[
                    "reconnect_time_wait"]:
                # Backoff connecting to server if we attempted recently.
                await asyncio.sleep(self.options["reconnect_time_wait"])
            try:
                s.last_attempt = time.monotonic()
                connection_future = asyncio.open_connection(
                    s.uri.hostname, s.uri.port, limit=DEFAULT_BUFFER_SIZE
                )
                r, w = await asyncio.wait_for(
                    connection_future, self.options['connect_timeout']
                )
                self._current_server = s

                # We keep a reference to the initial transport we used when
                # establishing the connection in case we later upgrade to TLS
                # after getting the first INFO message. This is in order to
                # prevent the GC closing the socket after we send CONNECT
                # and replace the transport.
                #
                # See https://github.com/nats-io/asyncio-nats/issues/43
                self._bare_io_reader = self._io_reader = r
                self._bare_io_writer = self._io_writer = w
                break
            except Exception as e:
                s.last_attempt = time.monotonic()
                s.reconnects += 1

                self._err = e
                await self._error_cb(e)
                continue

    async def _process_err(self, err_msg: str) -> None:
        """
        Processes the raw error message sent by the server
        and close connection with current server.
        """
        assert self._error_cb, "Client.connect must be called first"
        if STALE_CONNECTION in err_msg:
            await self._process_op_err(errors.StaleConnectionError())
            return

        if AUTHORIZATION_VIOLATION in err_msg:
            self._err = errors.AuthorizationError()
        else:
            prot_err = err_msg.strip("'")
            m = f"nats: {prot_err}"
            err = errors.Error(m)
            self._err = err

            if PERMISSIONS_ERR in m:
                await self._error_cb(err)
                return

        do_cbs = False
        if not self.is_connecting:
            do_cbs = True

        # FIXME: Some errors such as 'Invalid Subscription'
        # do not cause the server to close the connection.
        # For now we handle similar as other clients and close.
        asyncio.create_task(self._close(Client.CLOSED, do_cbs))

    async def _process_op_err(self, e: Exception) -> None:
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
            self._ps.reset()

            if self._reconnection_task is not None and not self._reconnection_task.cancelled(
            ):
                # Cancel the previous task in case it may still be running.
                self._reconnection_task.cancel()

            self._reconnection_task = asyncio.get_running_loop().create_task(
                self._attempt_reconnect()
            )
        else:
            self._process_disconnect()
            self._err = e
            await self._close(Client.CLOSED, True)

    async def _attempt_reconnect(self) -> None:
        assert self._current_server, "Client.connect must be called first"
        if self._reading_task is not None and not self._reading_task.cancelled(
        ):
            self._reading_task.cancel()

        if self._ping_interval_task is not None and not self._ping_interval_task.cancelled(
        ):
            self._ping_interval_task.cancel()

        if self._flusher_task is not None and not self._flusher_task.cancelled(
        ):
            self._flusher_task.cancel()

        if self._io_writer is not None:
            self._io_writer.close()
            try:
                await self._io_writer.wait_closed()
            except Exception as e:
                await self._error_cb(e)

        self._err = None
        if self._disconnected_cb is not None:
            await self._disconnected_cb()

        if self.is_closed:
            return

        if "dont_randomize" not in self.options or not self.options[
                "dont_randomize"]:
            shuffle(self._server_pool)

        # Create a future that the client can use to control waiting
        # on the reconnection attempts.
        self._reconnection_task_future = asyncio.Future()
        while True:
            try:
                # Try to establish a TCP connection to a server in
                # the cluster then send CONNECT command to it.
                await self._select_next_server()
                assert self._io_writer, "_select_next_server must've set _io_writer"
                await self._process_connect_init()

                # Consider a reconnect to be done once CONNECT was
                # processed by the server successfully.
                self.stats["reconnects"] += 1

                # Reset reconnect attempts for this server
                # since have successfully connected.
                self._current_server.did_connect = True
                self._current_server.reconnects = 0

                # Replay all the subscriptions in case there were some.
                subs_to_remove = []
                for sid, sub in self._subs.items():
                    max_msgs = 0
                    if sub._max_msgs > 0:
                        # If we already hit the message limit, remove the subscription and don't
                        # resubscribe.
                        if sub._received >= sub._max_msgs:
                            subs_to_remove.append(sid)
                            continue
                        # auto unsubscribe the number of messages we have left
                        max_msgs = sub._max_msgs - sub._received

                    sub_cmd = prot_command.sub_cmd(
                        sub._subject, sub._queue, sid
                    )
                    self._io_writer.write(sub_cmd)

                    if max_msgs > 0:
                        unsub_cmd = prot_command.unsub_cmd(sid, max_msgs)
                        self._io_writer.write(unsub_cmd)

                for sid in subs_to_remove:
                    self._subs.pop(sid)

                await self._io_writer.drain()

                # Flush pending data before continuing in connected status.
                # FIXME: Could use future here and wait for an error result
                # to bail earlier in case there are errors in the connection.
                # await self._flush_pending(force_flush=True)
                await self._flush_pending()
                self._status = Client.CONNECTED
                await self.flush()
                if self._reconnected_cb is not None:
                    await self._reconnected_cb()
                self._reconnection_task_future = None
                break
            except errors.NoServersError as e:
                self._err = e
                await self.close()
                break
            except (OSError, errors.Error, TimeoutError) as e:
                self._err = e
                await self._error_cb(e)
                self._status = Client.RECONNECTING
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1
            except asyncio.CancelledError:
                break

        if self._reconnection_task_future is not None and not self._reconnection_task_future.cancelled(
        ):
            self._reconnection_task_future.set_result(True)

    def _connect_command(self) -> bytes:
        '''
        Generates a JSON string with the params to be used
        when sending CONNECT to the server.

          ->> CONNECT {"lang": "python3"}

        '''
        options = {
            "verbose": self.options["verbose"],
            "pedantic": self.options["pedantic"],
            "lang": __lang__,
            "version": __version__,
            "protocol": PROTOCOL
        }
        if "headers" in self._server_info:
            options["headers"] = self._server_info["headers"]
            options["no_responders"] = self._server_info["headers"]

        if "auth_required" in self._server_info:
            if self._server_info["auth_required"]:
                if "nonce" in self._server_info and self._signature_cb is not None:
                    sig = self._signature_cb(self._server_info["nonce"])
                    options["sig"] = sig.decode()

                    if self._user_jwt_cb is not None:
                        jwt = self._user_jwt_cb()
                        options["jwt"] = jwt.decode()
                    elif self._public_nkey is not None:
                        options["nkey"] = self._public_nkey
                # In case there is no password, then consider handle
                # sending a token instead.
                elif self.options["user"] is not None and self.options[
                        "password"] is not None:
                    options["user"] = self.options["user"]
                    options["pass"] = self.options["password"]
                elif self.options["token"] is not None:
                    options["auth_token"] = self.options["token"]
                elif self._current_server and self._current_server.uri.username is not None:
                    if self._current_server.uri.password is None:
                        options["auth_token"
                                ] = self._current_server.uri.username
                    else:
                        options["user"] = self._current_server.uri.username
                        options["pass"] = self._current_server.uri.password

        if self.options["name"] is not None:
            options["name"] = self.options["name"]
        if self.options["no_echo"] is not None:
            options["echo"] = not self.options["no_echo"]

        connect_opts = json.dumps(options, sort_keys=True)
        return b''.join([CONNECT_OP + _SPC_ + connect_opts.encode() + _CRLF_])

    async def _process_ping(self) -> None:
        """
        Process PING sent by server.
        """
        await self._send_command(PONG)
        await self._flush_pending()

    async def _process_pong(self) -> None:
        """
        Process PONG sent by server.
        """
        if len(self._pongs) > 0:
            future = self._pongs.pop(0)
            future.set_result(True)
            self._pongs_received += 1
            self._pings_outstanding = 0

    def _is_control_message(self, data, header: Dict[str,
                                                     str]) -> Optional[str]:
        if len(data) > 0:
            return None
        status = header.get(nats.js.api.Header.STATUS)
        if status == CTRL_STATUS:
            return header.get(nats.js.api.Header.DESCRIPTION)
        return None

    async def _process_msg(
        self,
        sid: int,
        subject: bytes,
        reply: bytes,
        data: bytes,
        headers: bytes,
    ) -> None:
        """
        Process MSG sent by server.
        """
        assert self._error_cb, "Client.connect must be called first"
        payload_size = len(data)
        self.stats['in_msgs'] += 1
        self.stats['in_bytes'] += payload_size

        sub = self._subs.get(sid)
        if not sub:
            # Skip in case no subscription present.
            return

        sub._received += 1
        if sub._max_msgs > 0 and sub._received >= sub._max_msgs:
            # Enough messages so can throwaway subscription now, the
            # pending messages will still be in the subscription
            # internal queue and the task will finish once the last
            # message is processed.
            self._subs.pop(sid, None)

        hdr: Optional[Dict[str, str]] = None
        if headers:
            hdr = {}

            # Check the rest of the headers in case there is any.
            raw_headers = headers[len(NATS_HDR_LINE):]
            try:
                parsed_hdr = self._hdr_parser.parsebytes(raw_headers)
                # Check if it is an inline status message like:
                #
                # NATS/1.0 404 No Messages
                #
                if len(parsed_hdr.items()) == 0:
                    line = headers[len(NATS_HDR_LINE) - 1:]
                    status = line[:STATUS_MSG_LEN]
                    desc = line[STATUS_MSG_LEN + 1:len(line) - CTRL_LEN -
                                CTRL_LEN]
                    hdr[nats.js.api.Header.STATUS] = status.decode()

                    # FIXME: Clean this up...
                    if len(desc) > 0:
                        # Heartbeat messages can have both headers and inline status,
                        # check that there are no pending headers to be parsed.
                        i = desc.find(_CRLF_)
                        if i > 0:
                            hdr[nats.js.api.Header.DESCRIPTION
                                ] = desc[:i].decode()
                            parsed_hdr = self._hdr_parser.parsebytes(
                                desc[i + CTRL_LEN:]
                            )
                            for k, v in parsed_hdr.items():
                                hdr[k] = v
                        else:
                            # Just inline status...
                            hdr[nats.js.api.Header.DESCRIPTION] = desc.decode()
                else:
                    for k, v in parsed_hdr.items():
                        hdr[k] = v
            except Exception as e:
                await self._error_cb(e)
                return

        msg = self._build_message(subject, reply, data, hdr)
        if not msg:
            return

        # Process flow control messages in case of using a JetStream context.
        ctrl_msg = None
        fcReply = None
        if sub._jsi:
            #########################################
            #                                       #
            # JetStream Control Messages Processing #
            #                                       #
            #########################################
            jsi = sub._jsi
            if hdr:
                ctrl_msg = self._is_control_message(data, hdr)

                # Check if the hearbeat has a "Consumer Stalled" header, if
                # so, the value is the FC reply to send a nil message to.
                # We will send it at the end of this function.
                if ctrl_msg and ctrl_msg.startswith("Idle"):
                    fcReply = hdr.get(nats.js.api.Header.CONSUMER_STALLED)

            # OrderedConsumer: checkOrderedMsgs
            if not ctrl_msg and jsi._ordered and msg.reply:
                did_reset = None
                tokens = Msg.Metadata._get_metadata_fields(msg.reply)
                # FIXME: Support JS Domains.
                sseq = int(tokens[5])
                dseq = int(tokens[6])
                if dseq != jsi._dseq:
                    # Pick up from where we last left.
                    did_reset = await jsi.reset_ordered_consumer(jsi._sseq + 1)
                else:
                    # Update our tracking
                    jsi._dseq = dseq + 1
                    jsi._sseq = sseq
                if did_reset:
                    return

        # Skip processing if this is a control message.
        if not ctrl_msg:
            # Check if it is an old style request.
            if sub._future:
                if sub._future.cancelled():
                    # Already gave up, nothing to do.
                    return
                sub._future.set_result(msg)
                return

            # Let subscription wait_for_msgs coroutine process the messages,
            # but in case sending to the subscription task would block,
            # then consider it to be an slow consumer and drop the message.
            try:
                sub._pending_size += payload_size
                # allow setting pending_bytes_limit to 0 to disable
                if sub._pending_bytes_limit > 0 and sub._pending_size >= sub._pending_bytes_limit:
                    # Subtract the bytes since the message will be thrown away
                    # so it would not be pending data.
                    sub._pending_size -= payload_size

                    await self._error_cb(
                        errors.SlowConsumerError(
                            subject=msg.subject,
                            reply=msg.reply,
                            sid=sid,
                            sub=sub
                        )
                    )
                    return
                sub._pending_queue.put_nowait(msg)
            except asyncio.QueueFull:
                await self._error_cb(
                    errors.SlowConsumerError(
                        subject=msg.subject, reply=msg.reply, sid=sid, sub=sub
                    )
                )

            # Store the ACK metadata from the message to
            # compare later on with the received heartbeat.
            if sub._jsi:
                sub._jsi.track_sequences(msg.reply)
        elif ctrl_msg.startswith("Flow") and msg.reply and sub._jsi:
            # This is a flow control message.
            # We will schedule the send of the FC reply once we have delivered the
            # DATA message that was received before this flow control message, which
            # has sequence `jsi.fciseq`. However, it is possible that this message
            # has already been delivered, in that case, we need to send the FC reply now.
            if sub.delivered >= sub._jsi._fciseq:
                fcReply = msg.reply
            else:
                # Schedule a reply after the previous message is delivered.
                sub._jsi.schedule_flow_control_response(msg.reply)

        # Handle flow control response.
        if fcReply:
            await self.publish(fcReply)

        if ctrl_msg and not msg.reply and ctrl_msg.startswith("Idle"):
            if sub._jsi:
                await sub._jsi.check_for_sequence_mismatch(msg)

    def _build_message(
        self,
        subject: bytes,
        reply: bytes,
        data: bytes,
        headers: Optional[Dict[str, str]],
    ):
        return self.msg_class(
            subject=subject.decode(),
            reply=reply.decode(),
            data=data,
            headers=headers,
            _client=self,
        )

    def _process_disconnect(self) -> None:
        """
        Process disconnection from the server and set client status
        to DISCONNECTED.
        """
        self._status = Client.DISCONNECTED

    def _process_info(
        self, info: Dict[str, Any], initial_connection: bool = False
    ) -> None:
        """
        Process INFO lines sent by the server to reconfigure client
        with latest updates from cluster to enable server discovery.
        """
        assert self._current_server, "Client.connect must be called first"
        if 'connect_urls' in info:
            if info['connect_urls']:
                connect_urls = []
                for connect_url in info['connect_urls']:
                    scheme = ''
                    if self._current_server.uri.scheme == 'tls':
                        scheme = 'tls'
                    else:
                        scheme = 'nats'

                    uri = urlparse(f"{scheme}://{connect_url}")
                    srv = Srv(uri)
                    srv.discovered = True

                    # Check whether we should reuse the original hostname.
                    if 'tls_required' in self._server_info and self._server_info['tls_required'] \
                            and self._host_is_ip(uri.hostname):
                        srv.tls_name = self._current_server.uri.hostname

                    # Filter for any similar server in the server pool already.
                    should_add = True
                    for s in self._server_pool:
                        if uri.netloc == s.uri.netloc:
                            should_add = False
                    if should_add:
                        connect_urls.append(srv)

                if self.options["dont_randomize"] is not True:
                    shuffle(connect_urls)
                for srv in connect_urls:
                    self._server_pool.append(srv)

                if not initial_connection and connect_urls and self._discovered_server_cb:
                    self._discovered_server_cb()

    def _host_is_ip(self, connect_url: Optional[str]) -> bool:
        try:
            ipaddress.ip_address(connect_url)
            return True
        except Exception:
            return False

    async def _process_connect_init(self) -> None:
        """
        Process INFO received from the server and CONNECT to the server
        with authentication.  It is also responsible of setting up the
        reading and ping interval tasks from the client.
        """
        assert self._io_reader, "must be called only from Client.connect"
        assert self._io_writer, "must be called only from Client.connect"
        assert self._current_server, "must be called only from Client.connect"
        self._status = Client.CONNECTING

        connection_completed = self._io_reader.readline()
        info_line = await asyncio.wait_for(
            connection_completed, self.options["connect_timeout"]
        )
        if INFO_OP not in info_line:
            raise errors.Error(
                "nats: empty response from server when expecting INFO message"
            )

        _, info = info_line.split(INFO_OP + _SPC_, 1)

        try:
            srv_info = json.loads(info.decode())
        except Exception:
            raise errors.Error("nats: info message, json parse error")

        self._server_info = srv_info
        self._process_info(srv_info, initial_connection=True)

        if 'max_payload' in self._server_info:
            self._max_payload = self._server_info["max_payload"]

        if 'client_id' in self._server_info:
            self._client_id = self._server_info["client_id"]

        if 'tls_required' in self._server_info and self._server_info[
                'tls_required']:
            ssl_context: Optional[ssl.SSLContext]
            if "tls" in self.options:
                ssl_context = self.options.get('tls')
            elif self._current_server.uri.scheme == 'tls':
                ssl_context = ssl.create_default_context()
            if ssl_context is None:
                raise errors.Error('nats: no ssl context provided')

            # Check whether to reuse the original hostname for an implicit route.
            hostname = None
            if "tls_hostname" in self.options:
                hostname = self.options["tls_hostname"]
            elif self._current_server.tls_name is not None:
                hostname = self._current_server.tls_name
            else:
                hostname = self._current_server.uri.hostname

            await self._io_writer.drain()  # just in case something is left

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
                    transport_future, self.options['connect_timeout']
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
                    limit=DEFAULT_BUFFER_SIZE,
                    sock=sock,
                    ssl=ssl_context,
                    server_hostname=hostname,
                )
                self._io_reader, self._io_writer = await asyncio.wait_for(
                    connection_future, self.options['connect_timeout']
                )

        # Refresh state of parser upon reconnect.
        if self.is_reconnecting:
            self._ps.reset()

        assert self._io_reader
        assert self._io_writer
        connect_cmd = self._connect_command()
        self._io_writer.write(connect_cmd)
        await self._io_writer.drain()
        if self.options["verbose"]:
            future = self._io_reader.readline()
            next_op = await asyncio.wait_for(
                future, self.options["connect_timeout"]
            )
            if OK_OP in next_op:
                # Do nothing
                pass
            elif ERR_OP in next_op:
                err_line = next_op.decode()
                _, err_msg = err_line.split(" ", 1)

                # FIXME: Maybe handling could be more special here,
                # checking for errors.AuthorizationError for example.
                # await self._process_err(err_msg)
                raise errors.Error("nats: " + err_msg.rstrip('\r\n'))

        self._io_writer.write(PING_PROTO)
        await self._io_writer.drain()

        future = self._io_reader.readline()
        next_op = await asyncio.wait_for(
            future, self.options["connect_timeout"]
        )

        if PONG_PROTO in next_op:
            self._status = Client.CONNECTED
        elif ERR_OP in next_op:
            err_line = next_op.decode()
            _, err_msg = err_line.split(" ", 1)

            # FIXME: Maybe handling could be more special here,
            # checking for ErrAuthorization for example.
            # await self._process_err(err_msg)
            raise errors.Error("nats: " + err_msg.rstrip('\r\n'))

        if PONG_PROTO in next_op:
            self._status = Client.CONNECTED

        self._reading_task = asyncio.get_running_loop().create_task(
            self._read_loop()
        )
        self._pongs = []
        self._pings_outstanding = 0
        self._ping_interval_task = asyncio.get_running_loop().create_task(
            self._ping_interval()
        )

        # Task for kicking the flusher queue
        self._flusher_task = asyncio.get_running_loop().create_task(
            self._flusher()
        )

    async def _send_ping(self, future: asyncio.Future = None) -> None:
        assert self._io_writer, "Client.connect must be called first"
        if future is None:
            future = asyncio.Future()
        self._pongs.append(future)
        self._io_writer.write(PING_PROTO)
        await self._flush_pending()

    async def _flusher(self) -> None:
        """
        Coroutine which continuously tries to consume pending commands
        and then flushes them to the socket.
        """
        assert self._error_cb, "Client.connect must be called first"
        assert self._io_writer, "Client.connect must be called first"
        assert self._flush_queue, "Client.connect must be called first"
        while True:
            if not self.is_connected or self.is_connecting:
                break

            future: asyncio.Future = await self._flush_queue.get()

            try:
                if self._pending_data_size > 0:
                    self._io_writer.writelines(self._pending[:])
                    self._pending = []
                    self._pending_data_size = 0
                    await self._io_writer.drain()
            except OSError as e:
                await self._error_cb(e)
                await self._process_op_err(e)
                break
            except (asyncio.CancelledError, RuntimeError, AttributeError):
                # RuntimeError in case the event loop is closed
                break
            finally:
                future.set_result(None)

    async def _ping_interval(self) -> None:
        while True:
            await asyncio.sleep(self.options["ping_interval"])
            if not self.is_connected:
                continue
            try:
                self._pings_outstanding += 1
                if self._pings_outstanding > self.options[
                        "max_outstanding_pings"]:
                    await self._process_op_err(ErrStaleConnection())
                    return
                await self._send_ping()
            except (asyncio.CancelledError, RuntimeError, AttributeError):
                break
            # except asyncio.InvalidStateError:
            #     pass

    async def _read_loop(self) -> None:
        """
        Coroutine which gathers bytes sent by the server
        and feeds them to the protocol parser.
        In case of error while reading, it will stop running
        and its task has to be rescheduled.
        """
        while True:
            try:
                should_bail = self.is_closed or self.is_reconnecting
                if should_bail or self._io_reader is None:
                    break
                if self.is_connected and self._io_reader.at_eof():
                    err = errors.UnexpectedEOF()
                    await self._error_cb(err)
                    await self._process_op_err(err)
                    break

                b = await self._io_reader.read(DEFAULT_BUFFER_SIZE)
                await self._ps.parse(b)
            except errors.ProtocolError:
                await self._process_op_err(errors.ProtocolError())
                break
            except OSError as e:
                await self._process_op_err(e)
                break
            except asyncio.CancelledError:
                break
            except Exception as ex:
                _logger.error('nats: encountered error', exc_info=ex)
                break
            # except asyncio.InvalidStateError:
            #     pass

    async def __aenter__(self) -> "Client":
        """For when NATS client is used in a context manager"""
        return self

    async def __aexit__(self, *exc_info) -> None:
        """Close connection to NATS when used in a context manager"""
        await self._close(Client.CLOSED, do_cbs=True)

    def jetstream(self, **opts) -> nats.js.JetStreamContext:
        """
        jetstream returns a context that can be used to produce and consume
        messages from NATS JetStream.

        :param prefix: Default JetStream API Prefix.
        :param domain: Optional domain used by the JetStream API.
        :param timeout: Timeout for all JS API actions.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='hello', subjects=['hello'])
                ack = await js.publish('hello', b'Hello JS!')
                print(f'Ack: stream={ack.stream}, sequence={ack.seq}')
                # Ack: stream=hello, sequence=1
                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())
        """
        return nats.js.JetStreamContext(self, **opts)

    def jsm(self, **opts) -> nats.js.JetStreamManager:
        """JetStream context for managing JetStream via JS API"""
        return nats.js.JetStreamManager(self, **opts)
