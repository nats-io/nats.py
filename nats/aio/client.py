# Copyright 2016-2019 The NATS Authors
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
import json
import time
import ssl
import ipaddress
import base64
from random import shuffle
from urllib.parse import urlparse

from nats.aio.errors import *
from nats.aio.utils import new_inbox
from nats.aio.nuid import NUID
from nats.protocol.parser import *

__version__ = '0.9.0'
__lang__ = 'python3'
PROTOCOL = 1

INFO_OP = b'INFO'
CONNECT_OP = b'CONNECT'
PING_OP = b'PING'
PONG_OP = b'PONG'
PUB_OP = b'PUB'
SUB_OP = b'SUB'
UNSUB_OP = b'UNSUB'
OK_OP = b'+OK'
ERR_OP = b'-ERR'
_CRLF_ = b'\r\n'
_SPC_ = b' '
_EMPTY_ = b''
EMPTY = ""

PING_PROTO = PING_OP + _CRLF_
PONG_PROTO = PONG_OP + _CRLF_
INBOX_PREFIX = bytearray(b'_INBOX.')
INBOX_PREFIX_LEN = len(INBOX_PREFIX) + 22 + 1

DEFAULT_PENDING_SIZE = 1024 * 1024
DEFAULT_BUFFER_SIZE = 32768
DEFAULT_RECONNECT_TIME_WAIT = 2  # in seconds
DEFAULT_MAX_RECONNECT_ATTEMPTS = 60
DEFAULT_PING_INTERVAL = 120  # in seconds
DEFAULT_MAX_OUTSTANDING_PINGS = 2
DEFAULT_MAX_PAYLOAD_SIZE = 1048576
DEFAULT_MAX_FLUSHER_QUEUE_SIZE = 1024
DEFAULT_CONNECT_TIMEOUT = 2  # in seconds
DEFAULT_DRAIN_TIMEOUT = 30  # in seconds
MAX_CONTROL_LINE_SIZE = 1024

# Default Pending Limits of Subscriptions
DEFAULT_SUB_PENDING_MSGS_LIMIT = 65536
DEFAULT_SUB_PENDING_BYTES_LIMIT = 65536 * 1024


class Subscription(object):
    def __init__(
            self,
            subject='',
            queue='',
            future=None,
            max_msgs=0,
            is_async=False,
            cb=None,
            coro=None
    ):
        self.subject = subject
        self.queue = queue
        self.future = future
        self.max_msgs = max_msgs
        self.received = 0
        self.is_async = is_async
        self.cb = cb
        self.coro = coro

        # Per subscription message processor
        self.pending_msgs_limit = None
        self.pending_bytes_limit = None
        self.pending_queue = None
        self.pending_size = 0
        self.wait_for_msgs_task = None


class Msg(object):
    __slots__ = ('subject', 'reply', 'data', 'sid')

    def __init__(self, subject='', reply='', data=b'', sid=0):
        self.subject = subject
        self.reply = reply
        self.data = data
        self.sid = sid

    def __repr__(self):
        return "<{}: subject='{}' reply='{}' data='{}...'>".format(
            self.__class__.__name__,
            self.subject,
            self.reply,
            self.data[:10].decode(),
        )


class Srv(object):
    """
    Srv is a helper data structure to hold state of a server.
    """

    def __init__(self, uri):
        self.uri = uri
        self.reconnects = 0
        self.last_attempt = None
        self.did_connect = False
        self.discovered = False
        self.tls_name = None


class Client(object):
    """
    Asyncio based client for NATS.
    """

    msg_class = Msg

    DISCONNECTED = 0
    CONNECTED = 1
    CLOSED = 2
    RECONNECTING = 3
    CONNECTING = 4
    DRAINING_SUBS = 5
    DRAINING_PUBS = 6

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
        self._bare_io_reader = None
        self._io_reader = None
        self._bare_io_writer = None
        self._io_writer = None
        self._err = None
        self._error_cb = None
        self._disconnected_cb = None
        self._closed_cb = None
        self._reconnected_cb = None
        self._reconnection_task = None
        self._reconnection_task_future = None
        self._max_payload = DEFAULT_MAX_PAYLOAD_SIZE
        self._ssid = 0
        self._subs = {}
        self._status = Client.DISCONNECTED
        self._ps = Parser(self)
        self._pending = []
        self._pending_data_size = 0
        self._flush_queue = None
        self._flusher_task = None

        # New style request/response
        self._resp_sub = None
        self._resp_map = None
        self._resp_sub_prefix = None
        self._nuid = NUID()

        # NKEYS support
        #
        # user_jwt_cb is used to fetch and return the account
        # signed JWT for this user.
        self._user_jwt_cb = None

        # signature_cb is used to sign a nonce from the server while
        # authenticating with nkeys. The user should sign the nonce and
        # return the base64 encoded signature.
        self._signature_cb = None

        # user credentials file can be a tuple or single file.
        self._user_credentials = None

        self.options = {}
        self.stats = {
            'in_msgs': 0,
            'out_msgs': 0,
            'in_bytes': 0,
            'out_bytes': 0,
            'reconnects': 0,
            'errors_received': 0,
        }

    @asyncio.coroutine
    def connect(
            self,
            servers=["nats://127.0.0.1:4222"],
            io_loop=None,
            loop=None,
            error_cb=None,
            disconnected_cb=None,
            closed_cb=None,
            reconnected_cb=None,
            name=None,
            pedantic=False,
            verbose=False,
            allow_reconnect=True,
            connect_timeout=DEFAULT_CONNECT_TIMEOUT,
            reconnect_time_wait=DEFAULT_RECONNECT_TIME_WAIT,
            max_reconnect_attempts=DEFAULT_MAX_RECONNECT_ATTEMPTS,
            ping_interval=DEFAULT_PING_INTERVAL,
            max_outstanding_pings=DEFAULT_MAX_OUTSTANDING_PINGS,
            dont_randomize=False,
            flusher_queue_size=DEFAULT_MAX_FLUSHER_QUEUE_SIZE,
            no_echo=False,
            tls=None,
            user=None,
            password=None,
            token=None,
            drain_timeout=DEFAULT_DRAIN_TIMEOUT,
            signature_cb=None,
            user_jwt_cb=None,
            user_credentials=None,
    ):
        self._setup_server_pool(servers)
        self._loop = io_loop or loop or asyncio.get_event_loop()
        self._error_cb = error_cb
        self._closed_cb = closed_cb
        self._reconnected_cb = reconnected_cb
        self._disconnected_cb = disconnected_cb

        # NKEYS support
        self._signature_cb = signature_cb
        self._user_jwt_cb = user_jwt_cb
        self._user_credentials = user_credentials

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

        if self._user_credentials is not None:
            self._setup_nkeys_connect()

        # Queue used to trigger flushes to the socket
        self._flush_queue = asyncio.Queue(
            maxsize=flusher_queue_size, loop=self._loop
        )

        if self.options["dont_randomize"] is False:
            shuffle(self._server_pool)

        while True:
            try:
                yield from self._select_next_server()
                yield from self._process_connect_init()
                self._current_server.reconnects = 0
                break
            except ErrNoServers as e:
                if self.options["max_reconnect_attempts"] < 0:
                    # Never stop reconnecting
                    continue
                self._err = e
                raise e
            except (OSError, NatsError, asyncio.TimeoutError) as e:
                self._err = e
                if self._error_cb is not None:
                    yield from self._error_cb(e)

                # Bail on first attempt if reconnecting is disallowed.
                if not self.options["allow_reconnect"]:
                    raise e

                yield from self._close(Client.DISCONNECTED, False)
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1

    def _setup_nkeys_connect(self):
        # NOTE: Should use bytearray throughout as that
        # is more secure in handling the secrets.
        import nkeys
        import os

        creds = self._user_credentials
        if isinstance(creds, tuple) and len(creds) > 1:

            def user_cb():
                contents = None
                with open(creds[0], 'rb') as f:
                    contents = bytearray(os.fstat(f.fileno()).st_size)
                    f.readinto(contents)
                return contents

            self._user_jwt_cb = user_cb

            def sig_cb(nonce):
                seed = None
                with open(creds[1], 'rb') as f:
                    seed = bytearray(os.fstat(f.fileno()).st_size)
                    f.readinto(seed)
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
            def user_cb():
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

            def sig_cb(nonce):
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
                                raise ErrInvalidUserCredentials()
                            nkey_end_pos = f.tell()
                            nkey_size = nkey_end_pos - nkey_start_pos - 1
                            f.seek(nkey_start_pos)

                            # Only gather enough bytes for the user seed
                            # into the pre allocated bytearray.
                            user_seed = bytearray(nkey_size)
                            f.readinto(user_seed)
                kp = nkeys.from_seed(user_seed)
                raw_signed = kp.sign(nonce.encode())
                sig = base64.b64encode(raw_signed)

                # Delete all state related to the keys.
                kp.wipe()
                del user_seed
                del kp
                return sig

            self._signature_cb = sig_cb

    @asyncio.coroutine
    def close(self):
        """
        Closes the socket to which we are connected and
        sets the client to be in the CLOSED state.
        No further reconnections occur once reaching this point.
        """
        yield from self._close(Client.CLOSED)

    @asyncio.coroutine
    def _close(self, status, do_cbs=True):
        if self.is_closed:
            self._status = status
            return
        self._status = Client.CLOSED

        # Kick the flusher once again so it breaks
        # and avoid pending futures.
        yield from self._flush_pending()

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
                    yield from asyncio.wait_for(
                        self._reconnection_task_future,
                        self.options["reconnect_time_wait"],
                        loop=self._loop,
                    )
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

        # Relinquish control to allow background tasks to wrap up.
        yield from asyncio.sleep(0, loop=self._loop)

        if self._current_server is not None:
            # In case there is any pending data at this point, flush before disconnecting.
            if self._pending_data_size > 0:
                self._io_writer.writelines(self._pending[:])
                self._pending = []
                self._pending_data_size = 0
                yield from self._io_writer.drain()

        # Cleanup subscriptions since not reconnecting so no need
        # to replay the subscriptions anymore.
        for i, sub in self._subs.items():
            # FIXME: Should we clear the pending queue here?
            if sub.wait_for_msgs_task is not None and not sub.wait_for_msgs_task.done(
            ):
                sub.wait_for_msgs_task.cancel()
        self._subs.clear()

        if self._io_writer is not None:
            self._io_writer.close()

        if do_cbs:
            if self._disconnected_cb is not None:
                yield from self._disconnected_cb()
            if self._closed_cb is not None:
                yield from self._closed_cb()

    @asyncio.coroutine
    def drain(self, sid=None):
        """
        Drain will put a connection into a drain state. All subscriptions will
        immediately be put into a drain state. Upon completion, the publishers
        will be drained and can not publish any additional messages. Upon draining
        of the publishers, the connection will be closed. Use the `closed_cb'
        option to know when the connection has moved from draining to closed.

        If a sid is passed, just the subscription with that sid will be drained
        without closing the connection.
        """
        if self.is_draining:
            return
        if self.is_closed:
            raise ErrConnectionClosed
        if self.is_connecting or self.is_reconnecting:
            raise ErrConnectionReconnecting

        if sid is not None:
            return self._drain_sub(sid)

        # Start draining the subscriptions
        self._status = Client.DRAINING_SUBS

        drain_tasks = []
        for ssid, sub in self._subs.items():
            task = self._drain_sub(ssid)
            drain_tasks.append(task)

        drain_is_done = asyncio.gather(*drain_tasks)
        try:
            yield from asyncio.wait_for(
                drain_is_done, self.options["drain_timeout"]
            )
        except asyncio.TimeoutError:
            drain_is_done.exception()
            drain_is_done.cancel()
            if self._error_cb is not None:
                yield from self._error_cb(ErrDrainTimeout)
        except asyncio.CancelledError:
            pass
        finally:
            self._status = Client.DRAINING_PUBS
            yield from self.flush()
            yield from self._close(Client.CLOSED)

    def _drain_sub(self, sid):
        sub = self._subs.get(sid)
        if sub is None:
            raise ErrBadSubscription

        nc = self

        # Draining happens async under a task per sub which can be awaited.
        @asyncio.coroutine
        def drain_subscription():
            nonlocal sub
            nonlocal sid
            nonlocal nc

            try:
                # Announce server that no longer want to receive more
                # messages in this sub and just process the ones remaining.
                yield from nc._unsubscribe(sid)

                # Roundtrip to ensure that the server has sent all messages.
                yield from nc.flush()

                # Wait until no more messages are left,
                # then cancel the subscription task.
                while sub.pending_queue.qsize() > 0:
                    yield from asyncio.sleep(0.1, loop=nc._loop)

                # Subscription is done and won't be receiving further
                # messages so can throw it away now.
                nc._remove_sub(sid)
            except asyncio.CancelledError:
                # In case draining of a connection times out then
                # the sub per task will be canceled as well.
                pass

        return self._loop.create_task(drain_subscription())

    @asyncio.coroutine
    def publish(self, subject, payload):
        """
        Sends a PUB command to the server on the specified subject.

          ->> PUB hello 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 5

        """
        if self.is_closed:
            raise ErrConnectionClosed
        if self.is_draining_pubs:
            raise ErrConnectionDraining

        payload_size = len(payload)
        if payload_size > self._max_payload:
            raise ErrMaxPayload
        yield from self._publish(subject, _EMPTY_, payload, payload_size)

    @asyncio.coroutine
    def publish_request(self, subject, reply, payload):
        """
        Publishes a message tagging it with a reply subscription
        which can be used by those receiving the message to respond.

           ->> PUB hello   _INBOX.2007314fe0fcb2cdc2a2914c1 5
           ->> MSG_PAYLOAD: world
           <<- MSG hello 2 _INBOX.2007314fe0fcb2cdc2a2914c1 5

        """
        if self.is_closed:
            raise ErrConnectionClosed
        if self.is_draining_pubs:
            raise ErrConnectionDraining

        payload_size = len(payload)
        if payload_size > self._max_payload:
            raise ErrMaxPayload
        yield from self._publish(
            subject, reply.encode(), payload, payload_size
        )

    @asyncio.coroutine
    def _publish(self, subject, reply, payload, payload_size):
        """
        Sends PUB command to the NATS server.
        """
        if subject == "":
            # Avoid sending messages with empty replies.
            raise ErrBadSubject

        payload_size_bytes = ("%d" % payload_size).encode()
        pub_cmd = b''.join([
            PUB_OP, _SPC_,
            subject.encode(), _SPC_, reply, _SPC_, payload_size_bytes, _CRLF_,
            payload, _CRLF_
        ])
        self.stats['out_msgs'] += 1
        self.stats['out_bytes'] += payload_size
        yield from self._send_command(pub_cmd)
        if self._flush_queue.empty():
            yield from self._flush_pending()

    @asyncio.coroutine
    def subscribe(
            self,
            subject,
            queue="",
            cb=None,
            future=None,
            max_msgs=0,
            is_async=False,
            pending_msgs_limit=DEFAULT_SUB_PENDING_MSGS_LIMIT,
            pending_bytes_limit=DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ):
        """
        Takes a subject string and optional queue string to send a SUB cmd,
        and a callback which to which messages (Msg) will be dispatched to
        be processed sequentially by default.
        """
        if subject == "":
            raise ErrBadSubject

        if self.is_closed:
            raise ErrConnectionClosed

        if self.is_draining:
            raise ErrConnectionDraining

        sub = Subscription(
            subject=subject,
            queue=queue,
            max_msgs=max_msgs,
            is_async=is_async,
        )
        if cb is not None:
            if asyncio.iscoroutinefunction(cb):
                sub.coro = cb
            elif hasattr(cb, "func") and asyncio.iscoroutinefunction(cb.func):
                sub.coro = cb
            elif sub.is_async:
                raise NatsError(
                    "nats: must use coroutine for async subscriptions"
                )
            else:
                # NOTE: Consider to deprecate this eventually, it should always
                # be coroutines otherwise they could affect the single thread,
                # for now still allow to be flexible.
                sub.cb = cb

            sub.pending_msgs_limit = pending_msgs_limit
            sub.pending_bytes_limit = pending_bytes_limit
            sub.pending_queue = asyncio.Queue(
                maxsize=pending_msgs_limit,
                loop=self._loop,
            )

            # Close the delivery coroutine over the sub and error handler
            # instead of having subscription type hold over state of the conn.
            err_cb = self._error_cb

            @asyncio.coroutine
            def wait_for_msgs():
                nonlocal sub
                nonlocal err_cb

                while True:
                    try:
                        msg = yield from sub.pending_queue.get()
                        sub.pending_size -= len(msg.data)

                        try:
                            # Invoke depending of type of handler.
                            if sub.coro is not None:
                                if sub.is_async:
                                    # NOTE: Deprecate this usage in a next release,
                                    # the handler implementation ought to decide
                                    # the concurrency level at which the messages
                                    # should be processed.
                                    self._loop.create_task(sub.coro(msg))
                                else:
                                    yield from sub.coro(msg)
                            elif sub.cb is not None:
                                if sub.is_async:
                                    raise NatsError(
                                        "nats: must use coroutine for async subscriptions"
                                    )
                                else:
                                    # Schedule regular callbacks to be processed sequentially.
                                    self._loop.call_soon(sub.cb, msg)
                        except asyncio.CancelledError:
                            # In case the coroutine handler gets cancelled
                            # then stop task loop and return.
                            break
                        except Exception as e:
                            # All errors from calling a handler
                            # are async errors.
                            if err_cb is not None:
                                yield from err_cb(e)

                    except asyncio.CancelledError:
                        break

            # Start task for each subscription, it should be cancelled
            # on both unsubscribe and closing as well.
            sub.wait_for_msgs_task = self._loop.create_task(wait_for_msgs())

        elif future is not None:
            # Used to handle the single response from a request.
            sub.future = future
        else:
            raise NatsError("nats: invalid subscription type")

        self._ssid += 1
        ssid = self._ssid
        self._subs[ssid] = sub
        yield from self._subscribe(sub, ssid)
        return ssid

    @asyncio.coroutine
    def subscribe_async(self, subject, **kwargs):
        """
        Sets the subcription to use a task per message to be processed.

        ..deprecated:: 7.0
          Will be removed 9.0.
        """
        kwargs["is_async"] = True
        sid = yield from self.subscribe(subject, **kwargs)
        return sid

    @asyncio.coroutine
    def unsubscribe(self, ssid, max_msgs=0):
        """
        Takes a subscription sequence id and removes the subscription
        from the client, optionally after receiving more than max_msgs.
        """
        if self.is_closed:
            raise ErrConnectionClosed
        if self.is_draining:
            raise ErrConnectionDraining

        self._remove_sub(ssid, max_msgs)

        # We will send these for all subs when we reconnect anyway,
        # so that we can suppress here.
        if not self.is_reconnecting:
            yield from self.auto_unsubscribe(ssid, max_msgs)

    def _remove_sub(self, ssid, max_msgs=0):
        sub = None
        try:
            sub = self._subs[ssid]
        except KeyError:
            # Already unsubscribed.
            return

        # In case subscription has already received enough messages
        # then announce to the server that we are unsubscribing and
        # remove the callback locally too.
        if max_msgs == 0 or sub.received >= max_msgs:
            self._subs.pop(ssid, None)

        # Cancel task from subscription if present.
        if sub.wait_for_msgs_task is not None and not sub.wait_for_msgs_task.done(
        ):
            sub.wait_for_msgs_task.cancel()

    @asyncio.coroutine
    def _subscribe(self, sub, ssid):
        sub_cmd = b''.join([
            SUB_OP, _SPC_,
            sub.subject.encode(), _SPC_,
            sub.queue.encode(), _SPC_, ("%d" % ssid).encode(), _CRLF_
        ])
        yield from self._send_command(sub_cmd)
        yield from self._flush_pending()

    @asyncio.coroutine
    def request(self, subject, payload, timeout=0.5, expected=1, cb=None):
        """
        Implements the request/response pattern via pub/sub
        using a single wildcard subscription that handles
        the responses.

        """
        if self.is_draining_pubs:
            raise ErrConnectionDraining

        # If callback given then continue to use old style.
        if cb is not None:
            next_inbox = INBOX_PREFIX[:]
            next_inbox.extend(self._nuid.next())
            inbox = next_inbox.decode()

            sid = yield from self.subscribe(inbox, cb=cb)
            yield from self.auto_unsubscribe(sid, expected)
            yield from self.publish_request(subject, inbox, payload)
            return sid

        if self._resp_sub_prefix is None:
            self._resp_map = {}

            # Create a prefix and single wildcard subscription once.
            self._resp_sub_prefix = INBOX_PREFIX[:]
            self._resp_sub_prefix.extend(self._nuid.next())
            self._resp_sub_prefix.extend(b'.')
            resp_mux_subject = self._resp_sub_prefix[:]
            resp_mux_subject.extend(b'*')
            sub = Subscription(subject=resp_mux_subject.decode())

            # FIXME: Allow setting pending limits for responses mux subscription.
            sub.pending_msgs_limit = DEFAULT_SUB_PENDING_MSGS_LIMIT
            sub.pending_bytes_limit = DEFAULT_SUB_PENDING_BYTES_LIMIT
            sub.pending_queue = asyncio.Queue(
                maxsize=sub.pending_msgs_limit,
                loop=self._loop,
            )

            # Single task for handling the requests
            @asyncio.coroutine
            def wait_for_msgs():
                nonlocal sub
                while True:
                    try:
                        msg = yield from sub.pending_queue.get()
                        token = msg.subject[INBOX_PREFIX_LEN:]

                        try:
                            fut = self._resp_map[token]
                            fut.set_result(msg)
                            del self._resp_map[token]
                        except (asyncio.CancelledError,
                                asyncio.InvalidStateError):
                            # Request may have timed out already so remove entry.
                            del self._resp_map[token]
                            continue
                        except KeyError:
                            # Future already handled so drop any extra
                            # responses which may have made it.
                            continue

                    except asyncio.CancelledError:
                        break

            sub.wait_for_msgs_task = self._loop.create_task(wait_for_msgs())

            # Store the subscription in the subscriptions map,
            # then send the protocol commands to the server.
            self._ssid += 1
            ssid = self._ssid
            self._subs[ssid] = sub
            yield from self._subscribe(sub, ssid)

        # Use a new NUID for the token inbox and then use the future.
        token = self._nuid.next()
        inbox = self._resp_sub_prefix[:]
        inbox.extend(token)
        future = asyncio.Future(loop=self._loop)
        self._resp_map[token.decode()] = future
        yield from self.publish_request(subject, inbox.decode(), payload)

        # Wait for the response or give up on timeout.
        try:
            msg = yield from asyncio.wait_for(future, timeout, loop=self._loop)
            return msg
        except asyncio.TimeoutError:
            future.cancel()
            raise ErrTimeout

    @asyncio.coroutine
    def timed_request(self, subject, payload, timeout=0.5):
        """
        Implements the request/response pattern via pub/sub
        using an ephemeral subscription which will be published
        with a limited interest of 1 reply returning the response
        or raising a Timeout error.

          ->> SUB _INBOX.2007314fe0fcb2cdc2a2914c1 90
          ->> UNSUB 90 1
          ->> PUB hello _INBOX.2007314fe0fcb2cdc2a2914c1 5
          ->> MSG_PAYLOAD: world
          <<- MSG hello 2 _INBOX.2007314fe0fcb2cdc2a2914c1 5

        """
        next_inbox = INBOX_PREFIX[:]
        next_inbox.extend(self._nuid.next())
        inbox = next_inbox.decode()

        future = asyncio.Future(loop=self._loop)
        sid = yield from self.subscribe(inbox, future=future, max_msgs=1)
        yield from self.auto_unsubscribe(sid, 1)
        yield from self.publish_request(subject, inbox, payload)

        try:
            msg = yield from asyncio.wait_for(future, timeout, loop=self._loop)
            return msg
        except asyncio.TimeoutError:
            future.cancel()
            raise ErrTimeout

    @asyncio.coroutine
    def auto_unsubscribe(self, sid, limit=1):
        """
        Sends an UNSUB command to the server.  Unsubscribe is one of the basic building
        blocks in order to be able to define request/response semantics via pub/sub
        by announcing the server limited interest a priori.
        """
        if self.is_draining:
            raise ErrConnectionDraining
        yield from self._unsubscribe(sid, limit)

    @asyncio.coroutine
    def _unsubscribe(self, sid, limit=1):
        b_limit = b''
        if limit > 0:
            b_limit = ("%d" % limit).encode()
        b_sid = ("%d" % sid).encode()
        unsub_cmd = b''.join([UNSUB_OP, _SPC_, b_sid, _SPC_, b_limit, _CRLF_])
        yield from self._send_command(unsub_cmd)
        yield from self._flush_pending()

    @asyncio.coroutine
    def flush(self, timeout=60):
        """
        Sends a ping to the server expecting a pong back ensuring
        what we have written so far has made it to the server and
        also enabling measuring of roundtrip time.
        In case a pong is not returned within the allowed timeout,
        then it will raise ErrTimeout.
        """
        if timeout <= 0:
            raise ErrBadTimeout

        if self.is_closed:
            raise ErrConnectionClosed

        future = asyncio.Future(loop=self._loop)
        try:
            yield from self._send_ping(future)
            yield from asyncio.wait_for(future, timeout, loop=self._loop)
        except asyncio.TimeoutError:
            future.cancel()
            raise ErrTimeout

    @property
    def connected_url(self):
        if self.is_connected:
            return self._current_server.uri
        else:
            return None

    @property
    def servers(self):
        servers = []
        for srv in self._server_pool:
            servers.append(srv)
        return servers

    @property
    def discovered_servers(self):
        servers = []
        for srv in self._server_pool:
            if srv.discovered:
                servers.append(srv)
        return servers

    @property
    def max_payload(self):
        """
        Returns the max payload which we received from the servers INFO
        """
        return self._max_payload

    @property
    def last_error(self):
        """
        Returns the last error which may have occured.
        """
        return self._err

    @property
    def pending_data_size(self):
        return self._pending_data_size

    @property
    def is_closed(self):
        return self._status == Client.CLOSED

    @property
    def is_reconnecting(self):
        return self._status == Client.RECONNECTING

    @property
    def is_connected(self):
        return (self._status == Client.CONNECTED) or self.is_draining

    @property
    def is_connecting(self):
        return self._status == Client.CONNECTING

    @property
    def is_draining(self):
        return (
            self._status == Client.DRAINING_SUBS
            or self._status == Client.DRAINING_PUBS
        )

    @property
    def is_draining_pubs(self):
        return self._status == Client.DRAINING_PUBS

    @asyncio.coroutine
    def _send_command(self, cmd, priority=False):
        if priority:
            self._pending.insert(0, cmd)
        else:
            self._pending.append(cmd)
        self._pending_data_size += len(cmd)
        if self._pending_data_size > DEFAULT_PENDING_SIZE:
            yield from self._flush_pending()

    @asyncio.coroutine
    def _flush_pending(self):
        try:
            # kick the flusher!
            yield from self._flush_queue.put(None)

            if not self.is_connected:
                return

        except asyncio.CancelledError:
            pass

    def _setup_server_pool(self, connect_url):
        if type(connect_url) is str:
            try:
                if "nats://" in connect_url or "tls://" in connect_url:
                    # Closer to how the Go client handles this.
                    # e.g. nats://127.0.0.1:4222
                    uri = urlparse(connect_url)
                elif ":" in connect_url:
                    # Expand the scheme for the user
                    # e.g. 127.0.0.1:4222
                    uri = urlparse("nats://%s" % connect_url)
                else:
                    # Just use the endpoint with the default NATS port.
                    # e.g. demo.nats.io
                    uri = urlparse("nats://%s:4222" % connect_url)

                # In case only endpoint with scheme was set.
                # e.g. nats://demo.nats.io or localhost:
                if uri.port is None:
                    uri = urlparse("nats://%s:4222" % uri.hostname)
            except ValueError:
                raise NatsError("nats: invalid connect url option")

            if uri.hostname is None or uri.hostname == "none":
                raise NatsError("nats: invalid hostname in connect url")
            self._server_pool.append(Srv(uri))
        elif type(connect_url) is list:
            try:
                for server in connect_url:
                    uri = urlparse(server)
                    self._server_pool.append(Srv(uri))
            except ValueError:
                raise NatsError("nats: invalid connect url option")
        else:
            raise NatsError("nats: invalid connect url option")

    @asyncio.coroutine
    def _select_next_server(self):
        """
        Looks up in the server pool for an available server
        and attempts to connect.
        """

        while True:
            if len(self._server_pool) == 0:
                self._current_server = None
                raise ErrNoServers

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
                yield from asyncio.sleep(
                    self.options["reconnect_time_wait"], loop=self._loop
                )
            try:
                s.last_attempt = time.monotonic()
                r, w = yield from asyncio.open_connection(
                    s.uri.hostname,
                    s.uri.port,
                    loop=self._loop,
                    limit=DEFAULT_BUFFER_SIZE
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
                if self._error_cb is not None:
                    yield from self._error_cb(e)
                continue

    @asyncio.coroutine
    def _process_err(self, err_msg):
        """
        Processes the raw error message sent by the server
        and close connection with current server.
        """
        if STALE_CONNECTION in err_msg:
            yield from self._process_op_err(ErrStaleConnection)
            return

        if AUTHORIZATION_VIOLATION in err_msg:
            self._err = ErrAuthorization
        else:
            m = b'nats: ' + err_msg[0]
            self._err = NatsError(m.decode())

        do_cbs = False
        if not self.is_connecting:
            do_cbs = True

        # FIXME: Some errors such as 'Invalid Subscription'
        # do not cause the server to close the connection.
        # For now we handle similar as other clients and close.
        self._loop.create_task(self._close(Client.CLOSED, do_cbs))

    @asyncio.coroutine
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
            self._ps.reset()

            if self._reconnection_task is not None and not self._reconnection_task.cancelled(
            ):
                # Cancel the previous task in case it may still be running.
                self._reconnection_task.cancel()

            self._reconnection_task = self._loop.create_task(
                self._attempt_reconnect()
            )
        else:
            self._process_disconnect()
            self._err = e
            yield from self._close(Client.CLOSED, True)

    @asyncio.coroutine
    def _attempt_reconnect(self):
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

        self._err = None
        if self._disconnected_cb is not None:
            yield from self._disconnected_cb()

        if self.is_closed:
            return

        if "dont_randomize" not in self.options or not self.options[
                "dont_randomize"]:
            shuffle(self._server_pool)

        # Create a future that the client can use to control waiting
        # on the reconnection attempts.
        self._reconnection_task_future = asyncio.Future(loop=self._loop)
        while True:
            try:
                # Try to establish a TCP connection to a server in
                # the cluster then send CONNECT command to it.
                yield from self._select_next_server()
                yield from self._process_connect_init()

                # Consider a reconnect to be done once CONNECT was
                # processed by the server successfully.
                self.stats["reconnects"] += 1

                # Reset reconnect attempts for this server
                # since have successfully connected.
                self._current_server.did_connect = True
                self._current_server.reconnects = 0

                # Replay all the subscriptions in case there were some.
                for ssid, sub in self._subs.items():
                    sub_cmd = b''.join([
                        SUB_OP, _SPC_,
                        sub.subject.encode(), _SPC_,
                        sub.queue.encode(), _SPC_, ("%d" % ssid).encode(),
                        _CRLF_
                    ])
                    self._io_writer.write(sub_cmd)
                yield from self._io_writer.drain()

                # Flush pending data before continuing in connected status.
                # FIXME: Could use future here and wait for an error result
                # to bail earlier in case there are errors in the connection.
                yield from self._flush_pending()
                self._status = Client.CONNECTED
                yield from self.flush()
                if self._reconnected_cb is not None:
                    yield from self._reconnected_cb()
                self._reconnection_task_future = None
                break
            except ErrNoServers as e:
                self._err = e
                yield from self.close()
                break
            except (OSError, NatsError, ErrTimeout) as e:
                self._err = e
                if self._error_cb is not None:
                    yield from self._error_cb(e)
                self._status = Client.RECONNECTING
                self._current_server.last_attempt = time.monotonic()
                self._current_server.reconnects += 1
            except asyncio.CancelledError:
                break

        if self._reconnection_task_future is not None and not self._reconnection_task_future.cancelled(
        ):
            self._reconnection_task_future.set_result(True)

    def _connect_command(self):
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
        if "auth_required" in self._server_info:
            if self._server_info["auth_required"]:
                if "nonce" in self._server_info and self._signature_cb is not None:
                    sig = self._signature_cb(self._server_info["nonce"])
                    options["sig"] = sig.decode()

                    if self._user_jwt_cb is not None:
                        jwt = self._user_jwt_cb()
                        options["jwt"] = jwt.decode()
                # In case there is no password, then consider handle
                # sending a token instead.
                elif self.options["user"] is not None and self.options[
                        "password"] is not None:
                    options["user"] = self.options["user"]
                    options["pass"] = self.options["password"]
                elif self.options["token"] is not None:
                    options["auth_token"] = self.options["token"]
                elif self._current_server.uri.username is not None:
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

    @asyncio.coroutine
    def _process_ping(self):
        """
        Process PING sent by server.
        """
        yield from self._send_command(PONG)
        yield from self._flush_pending()

    @asyncio.coroutine
    def _process_pong(self):
        """
        Process PONG sent by server.
        """
        if len(self._pongs) > 0:
            future = self._pongs.pop(0)
            future.set_result(True)
            self._pongs_received += 1
            self._pings_outstanding -= 1

    @asyncio.coroutine
    def _process_msg(self, sid, subject, reply, data):
        """
        Process MSG sent by server.
        """
        payload_size = len(data)
        self.stats['in_msgs'] += 1
        self.stats['in_bytes'] += payload_size

        sub = self._subs.get(sid)
        if sub is None:
            # Skip in case no subscription present.
            return

        sub.received += 1
        if sub.max_msgs > 0 and sub.received >= sub.max_msgs:
            # Enough messages so can throwaway subscription now.
            self._subs.pop(sid, None)
        msg = self._build_message(subject, reply, data)

        # Check if it is an old style request.
        if sub.future is not None:
            if sub.future.cancelled():
                # Already gave up, nothing to do.
                return
            sub.future.set_result(msg)
            return

        # Let subscription wait_for_msgs coroutine process the messages,
        # but in case sending to the subscription task would block,
        # then consider it to be an slow consumer and drop the message.
        try:
            sub.pending_size += payload_size
            if sub.pending_size >= sub.pending_bytes_limit:
                # Substract again the bytes since throwing away
                # the message so would not be pending data.
                sub.pending_size -= payload_size

                if self._error_cb is not None:
                    yield from self._error_cb(
                        ErrSlowConsumer(subject=subject, sid=sid)
                    )
                return
            sub.pending_queue.put_nowait(msg)
        except asyncio.QueueFull:
            if self._error_cb is not None:
                yield from self._error_cb(
                    ErrSlowConsumer(subject=subject, sid=sid)
                )

    def _build_message(self, subject, reply, data):
        return self.msg_class(
            subject=subject.decode(), reply=reply.decode(), data=data
        )

    def _process_disconnect(self):
        """
        Process disconnection from the server and set client status
        to DISCONNECTED.
        """
        self._status = Client.DISCONNECTED

    def _process_info(self, info):
        """
        Process INFO lines sent by the server to reconfigure client
        with latest updates from cluster to enable server discovery.
        """
        if 'connect_urls' in info:
            if info['connect_urls']:
                connect_urls = []
                for connect_url in info['connect_urls']:
                    scheme = ''
                    if self._current_server.uri.scheme == 'tls':
                        scheme = 'tls'
                    else:
                        scheme = 'nats'

                    uri = urlparse("{}://{}".format(scheme, connect_url))
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

    def _host_is_ip(self, connect_url):
        try:
            ipaddress.ip_address(connect_url)
            return True
        except:
            return False

    @asyncio.coroutine
    def _process_connect_init(self):
        """
        Process INFO received from the server and CONNECT to the server
        with authentication.  It is also responsible of setting up the
        reading and ping interval tasks from the client.
        """
        self._status = Client.CONNECTING

        connection_completed = self._io_reader.readline()
        info_line = yield from asyncio.wait_for(
            connection_completed, self.options["connect_timeout"]
        )
        if INFO_OP not in info_line:
            raise NatsError(
                "nats: empty response from server when expecting INFO message"
            )

        _, info = info_line.split(INFO_OP + _SPC_, 1)

        try:
            srv_info = json.loads(info.decode())
        except:
            raise NatsError("nats: info message, json parse error")

        self._server_info = srv_info
        self._process_info(srv_info)

        if 'max_payload' in self._server_info:
            self._max_payload = self._server_info["max_payload"]

        if 'tls_required' in self._server_info and self._server_info[
                'tls_required']:
            ssl_context = None
            if "tls" in self.options:
                ssl_context = self.options.get('tls')
            elif self._current_server.uri.scheme == 'tls':
                ssl_context = ssl.create_default_context()
            else:
                raise NatsError('nats: no ssl context provided')

            transport = self._io_writer.transport
            sock = transport.get_extra_info('socket')
            if not sock:
                # This shouldn't happen
                raise NatsError('nats: unable to get socket')

            yield from self._io_writer.drain(
            )  # just in case something is left

            # Check whether to reuse the original hostname for an implicit route.
            hostname = None
            if self._current_server.tls_name is not None:
                hostname = self._current_server.tls_name
            else:
                hostname = self._current_server.uri.hostname

            self._io_reader, self._io_writer = yield from asyncio.open_connection(
                loop=self._loop,
                limit=DEFAULT_BUFFER_SIZE,
                sock=sock,
                ssl=ssl_context,
                server_hostname=hostname,
            )

        # Refresh state of parser upon reconnect.
        if self.is_reconnecting:
            self._ps.reset()

        connect_cmd = self._connect_command()
        self._io_writer.write(connect_cmd)
        yield from self._io_writer.drain()
        if self.options["verbose"]:
            future = self._io_reader.readline()
            next_op = yield from asyncio.wait_for(
                future, self.options["connect_timeout"]
            )
            if OK_OP in next_op:
                # Do nothing
                pass
            elif ERR_OP in next_op:
                err_line = next_op.decode()
                _, err_msg = err_line.split(" ", 1)

                # FIXME: Maybe handling could be more special here,
                # checking for ErrAuthorization for example.
                # yield from self._process_err(err_msg)
                raise NatsError("nats: " + err_msg.rstrip('\r\n'))

        self._io_writer.write(PING_PROTO)
        yield from self._io_writer.drain()

        future = self._io_reader.readline()
        next_op = yield from asyncio.wait_for(
            future, self.options["connect_timeout"]
        )

        if PONG_PROTO in next_op:
            self._status = Client.CONNECTED
        elif ERR_OP in next_op:
            err_line = next_op.decode()
            _, err_msg = err_line.split(" ", 1)

            # FIXME: Maybe handling could be more special here,
            # checking for ErrAuthorization for example.
            # yield from self._process_err(err_msg)
            raise NatsError("nats: " + err_msg.rstrip('\r\n'))

        if PONG_PROTO in next_op:
            self._status = Client.CONNECTED

        self._reading_task = self._loop.create_task(self._read_loop())
        self._pongs = []
        self._pings_outstanding = 0
        self._ping_interval_task = self._loop.create_task(
            self._ping_interval()
        )

        # Task for kicking the flusher queue
        self._flusher_task = self._loop.create_task(self._flusher())

    @asyncio.coroutine
    def _send_ping(self, future=None):
        if future is None:
            future = asyncio.Future(loop=self._loop)
        self._pongs.append(future)
        self._io_writer.write(PING_PROTO)
        yield from self._flush_pending()

    @asyncio.coroutine
    def _flusher(self):
        """
        Coroutine which continuously tries to consume pending commands
        and then flushes them to the socket.
        """
        while True:
            if not self.is_connected or self.is_connecting:
                break

            try:
                yield from self._flush_queue.get()

                if self._pending_data_size > 0:
                    self._io_writer.writelines(self._pending[:])
                    self._pending = []
                    self._pending_data_size = 0
                    yield from self._io_writer.drain()
            except OSError as e:
                if self._error_cb is not None:
                    yield from self._error_cb(e)
                yield from self._process_op_err(e)
                break
            except asyncio.CancelledError:
                break

    @asyncio.coroutine
    def _ping_interval(self):
        while True:
            yield from asyncio.sleep(
                self.options["ping_interval"], loop=self._loop
            )
            if not self.is_connected:
                continue
            try:
                self._pings_outstanding += 1
                if self._pings_outstanding > self.options[
                        "max_outstanding_pings"]:
                    yield from self._process_op_err(ErrStaleConnection)
                    return
                yield from self._send_ping()
            except asyncio.CancelledError:
                break
            # except asyncio.InvalidStateError:
            #     pass

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
                should_bail = self.is_closed or self.is_reconnecting
                if should_bail or self._io_reader is None:
                    break
                if self.is_connected and self._io_reader.at_eof():
                    if self._error_cb is not None:
                        yield from self._error_cb(ErrStaleConnection)
                    yield from self._process_op_err(ErrStaleConnection)
                    break

                b = yield from self._io_reader.read(DEFAULT_BUFFER_SIZE)
                yield from self._ps.parse(b)
            except ErrProtocol:
                yield from self._process_op_err(ErrProtocol)
                break
            except OSError as e:
                yield from self._process_op_err(e)
                break
            except asyncio.CancelledError:
                break
            # except asyncio.InvalidStateError:
            #     pass

    def __enter__(self):
        """For when NATS client is used in a context manager"""

        return self

    def __exit__(self, *exc_info):
        """Close connection to NATS when used in a context manager"""

        self._loop.create_task(self._close(Client.CLOSED, True))
