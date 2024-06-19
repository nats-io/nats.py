# Copyright 2021-2022 The NATS Authors
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

from __future__ import annotations

import asyncio
import json
import time
from email.parser import BytesParser
from secrets import token_hex
from typing import TYPE_CHECKING, Awaitable, Callable, Optional, List, Dict

import nats.errors
import nats.js.errors
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import api
from nats.js.errors import BadBucketError, BucketNotFoundError, InvalidBucketNameError, NotFoundError, FetchTimeoutError
from nats.js.kv import KeyValue
from nats.js.manager import JetStreamManager
from nats.js.object_store import (
    VALID_BUCKET_RE, OBJ_ALL_CHUNKS_PRE_TEMPLATE, OBJ_ALL_META_PRE_TEMPLATE,
    OBJ_STREAM_TEMPLATE, ObjectStore
)

if TYPE_CHECKING:
    from nats import NATS

NO_RESPONDERS_STATUS = "503"

NATS_HDR_LINE = bytearray(b'NATS/1.0')
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
_CRLF_ = b'\r\n'
_CRLF_LEN_ = len(_CRLF_)
KV_STREAM_TEMPLATE = "KV_{bucket}"
KV_PRE_TEMPLATE = "$KV.{bucket}."
Callback = Callable[['Msg'], Awaitable[None]]

# For JetStream the default pending limits are larger.
DEFAULT_JS_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_JS_SUB_PENDING_BYTES_LIMIT = 256 * 1024 * 1024

# Max history limit for key value.
KV_MAX_HISTORY = 64


class JetStreamContext(JetStreamManager):
    """
    Fully featured context for interacting with JetStream.

    :param conn: NATS Connection
    :param prefix: Default JetStream API Prefix.
    :param domain: Optional domain used by the JetStream API.
    :param timeout: Timeout for all JS API actions.
    :param publish_async_max_pending: Maximum outstanding async publishes that can be inflight at one time.

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

    def __init__(
        self,
        conn: NATS,
        prefix: str = api.DEFAULT_PREFIX,
        domain: Optional[str] = None,
        timeout: float = 5,
        publish_async_max_pending: int = 4000,
    ) -> None:
        self._prefix = prefix
        if domain is not None:
            self._prefix = f"$JS.{domain}.API"
        self._nc = conn
        self._timeout = timeout
        self._hdr_parser = BytesParser()

        self._async_reply_prefix: Optional[bytearray] = None
        self._publish_async_futures: Dict[str, asyncio.Future] = {}

        self._publish_async_completed_event = asyncio.Event()
        self._publish_async_completed_event.set()

        self._publish_async_pending_semaphore = asyncio.Semaphore(publish_async_max_pending)

    @property
    def _jsm(self) -> JetStreamManager:
        return JetStreamManager(
            conn=self._nc,
            prefix=self._prefix,
            timeout=self._timeout,
        )

    async def _init_async_reply(self) -> None:
        self._publish_async_futures = {}

        self._async_reply_prefix = self._nc._inbox_prefix[:]
        self._async_reply_prefix.extend(b'.')
        self._async_reply_prefix.extend(self._nc._nuid.next())
        self._async_reply_prefix.extend(b'.')

        async_reply_subject = self._async_reply_prefix[:]
        async_reply_subject.extend(b'*')

        await self._nc.subscribe(
            async_reply_subject.decode(), cb=self._handle_async_reply
        )

    async def _handle_async_reply(self, msg: Msg) -> None:
        token = msg.subject[len(self._nc._inbox_prefix) + 22 + 2:]
        future = self._publish_async_futures.get(token)

        if not future:
            return

        if future.done():
            return

        # Handle no responders
        if msg.headers and msg.headers.get(api.Header.STATUS) == NO_RESPONDERS_STATUS:
            future.set_exception(nats.js.errors.NoStreamResponseError)
            return

        # Handle response errors
        try:
            resp = json.loads(msg.data)
            if 'error' in resp:
                err = nats.js.errors.APIError.from_error(resp['error'])
                future.set_exception(err)
                return

            ack = api.PubAck.from_response(resp)
            future.set_result(ack)
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass

    async def publish(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[Dict] = None
    ) -> api.PubAck:
        """
        publish emits a new message to JetStream and waits for acknowledgement.
        """
        hdr = headers
        if timeout is None:
            timeout = self._timeout
        if stream is not None:
            hdr = hdr or {}
            hdr[api.Header.EXPECTED_STREAM] = stream

        try:
            msg = await self._nc.request(
                subject,
                payload,
                timeout=timeout,
                headers=hdr,
            )
        except nats.errors.NoRespondersError:
            raise nats.js.errors.NoStreamResponseError

        resp = json.loads(msg.data)
        if 'error' in resp:
            raise nats.js.errors.APIError.from_error(resp['error'])
        return api.PubAck.from_response(resp)

    async def publish_async(
        self,
        subject: str,
        payload: bytes = b'',
        wait_stall: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[Dict] = None,
    ) -> asyncio.Future[api.PubAck]:
        """
        emits a new message to JetStream and returns a future that can be awaited for acknowledgement.
        """

        if not self._async_reply_prefix:
            await self._init_async_reply()
        assert self._async_reply_prefix

        hdr = headers
        if stream is not None:
            hdr = hdr or {}
            hdr[api.Header.EXPECTED_STREAM] = stream

        try:
            await asyncio.wait_for(self._publish_async_pending_semaphore.acquire(), timeout=wait_stall)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            raise nats.js.errors.TooManyStalledMsgsError

        # Use a new NUID + couple of unique token bytes to identify the request,
        # then use the future to get the response.
        token = self._nc._nuid.next()
        token.extend(token_hex(2).encode())
        inbox = self._async_reply_prefix[:]
        inbox.extend(token)

        future: asyncio.Future = asyncio.Future()

        def handle_done(future):
            self._publish_async_futures.pop(token.decode(), None)
            if len(self._publish_async_futures) == 0:
                self._publish_async_completed_event.set()

            self._publish_async_pending_semaphore.release()

        future.add_done_callback(handle_done)

        self._publish_async_futures[token.decode()] = future

        if self._publish_async_completed_event.is_set():
            self._publish_async_completed_event.clear()

        await self._nc.publish(
            subject, payload, reply=inbox.decode(), headers=hdr
        )

        return future

    def publish_async_pending(self) -> int:
        """
        returns the number of pending async publishes.
        """
        return len(self._publish_async_futures)

    async def publish_async_completed(self) -> None:
        """
        waits for all pending async publishes to be completed.
        """
        await self._publish_async_completed_event.wait()

    async def subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        cb: Optional[Callback] = None,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[api.ConsumerConfig] = None,
        manual_ack: bool = False,
        ordered_consumer: bool = False,
        idle_heartbeat: Optional[float] = None,
        flow_control: bool = False,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        deliver_policy: Optional[api.DeliverPolicy] = None,
        headers_only: Optional[bool] = None,
        inactive_threshold: Optional[float] = None,
    ) -> PushSubscription:
        """Create consumer if needed and push-subscribe to it.

        1. Check if consumer exists.
        2. Creates consumer if needed.
        3. Calls `subscribe_bind`.

        :param subject: Subject from a stream from JetStream.
        :param queue: Deliver group name from a set a of queue subscribers.
        :param durable: Name of the durable consumer to which the the subscription should be bound.
        :param stream: Name of the stream to which the subscription should be bound. If not set,
          then the client will automatically look it up based on the subject.
        :param manual_ack: Disables auto acking for async subscriptions.
        :param ordered_consumer: Enable ordered consumer mode.
        :param idle_heartbeat: Enable Heartbeats for a consumer to detect failures.
        :param flow_control: Enable Flow Control for a consumer.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='hello', subjects=['hello'])
                await js.publish('hello', b'Hello JS!')

                async def cb(msg):
                  print('Received:', msg)

                # Ephemeral Async Subscribe
                await js.subscribe('hello', cb=cb)

                # Durable Async Subscribe
                # NOTE: Only one subscription can be bound to a durable name. It also auto acks by default.
                await js.subscribe('hello', cb=cb, durable='foo')

                # Durable Sync Subscribe
                # NOTE: Sync subscribers do not auto ack.
                await js.subscribe('hello', durable='bar')

                # Queue Async Subscribe
                # NOTE: Here 'workers' becomes deliver_group, durable name and queue name.
                await js.subscribe('hello', 'workers', cb=cb)

            if __name__ == '__main__':
                asyncio.run(main())

        """
        if stream is None:
            stream = await self._jsm.find_stream_name_by_subject(subject)

        deliver = None
        consumer = None

        # If using a queue, that will be the consumer/durable name.
        if queue:
            if durable and durable != queue:
                raise nats.js.errors.Error(
                    f"cannot create queue subscription '{queue}' to consumer '{durable}'"
                )
            else:
                durable = queue

        consumer_info = None
        # Ephemeral subscribe always has to be auto created.
        should_create = not durable
        if durable:
            try:
                # TODO: Detect configuration drift with any present durable consumer.
                consumer_info = await self._jsm.consumer_info(stream, durable)
                consumer = durable
            except nats.js.errors.NotFoundError:
                should_create = True

        if consumer_info is not None:
            config = consumer_info.config
            # At this point, we know the user wants push mode, and the JS consumer is
            # really push mode.
            deliver_group = consumer_info.config.deliver_group
            if not deliver_group:
                # Prevent an user from attempting to create a queue subscription on
                # a JS consumer that was not created with a deliver group.
                if queue:
                    # TODO: Currently, this would not happen in client
                    # since the queue name is used as durable name.
                    raise nats.js.errors.Error(
                        "cannot create a queue subscription for a consumer without a deliver group"
                    )
                elif consumer_info.push_bound:
                    # Need to reject a non queue subscription to a non queue consumer
                    # if the consumer is already bound.
                    raise nats.js.errors.Error(
                        "consumer is already bound to a subscription"
                    )
            else:
                if not queue:
                    raise nats.js.errors.Error(
                        f"cannot create a subscription for a consumer with a deliver group {deliver_group}"
                    )
                elif queue != deliver_group:
                    raise nats.js.errors.Error(
                        f"cannot create a queue subscription {queue} for a consumer "
                        f"with a deliver group {deliver_group}"
                    )
        elif should_create:
            # Auto-create consumer if none found.
            if config is None:
                config = api.ConsumerConfig()
            if not config.durable_name:
                config.durable_name = durable
            if not config.deliver_group:
                config.deliver_group = queue
            if not config.headers_only:
                config.headers_only = headers_only
            if deliver_policy:
                # NOTE: deliver_policy is defaulting to ALL so check is different for this one.
                config.deliver_policy = deliver_policy
            if inactive_threshold:
                config.inactive_threshold = inactive_threshold

            # Create inbox for push consumer.
            deliver = self._nc.new_inbox()
            config.deliver_subject = deliver

            # Auto created consumers use the filter subject.
            config.filter_subject = subject

            # Heartbeats / FlowControl
            config.flow_control = flow_control
            if idle_heartbeat:
                config.idle_heartbeat = idle_heartbeat
            else:
                idle_heartbeat = config.idle_heartbeat or 5

            # Enable ordered consumer mode where at most there is
            # one message being delivered at a time.
            if ordered_consumer:
                config.flow_control = True
                config.ack_policy = api.AckPolicy.NONE
                config.max_deliver = 1
                config.ack_wait = 22 * 3600  # 22 hours
                config.idle_heartbeat = idle_heartbeat
                config.num_replicas = 1
                config.mem_storage = True

            consumer_info = await self._jsm.add_consumer(stream, config=config)
            consumer = consumer_info.name

        if consumer is None:
            raise TypeError("cannot detect consumer")
        if config is None:
            raise TypeError("config is required for existing durable consumer")
        return await self.subscribe_bind(
            cb=cb,
            stream=stream,
            config=config,
            manual_ack=manual_ack,
            ordered_consumer=ordered_consumer,
            consumer=consumer,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )

    async def subscribe_bind(
        self,
        stream: str,
        config: api.ConsumerConfig,
        consumer: str,
        cb: Optional[Callback] = None,
        manual_ack: bool = False,
        ordered_consumer: bool = False,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
    ) -> PushSubscription:
        """Push-subscribe to an existing consumer.
        """
        # By default, async subscribers wrap the original callback and
        # auto ack the messages as they are delivered.
        #
        # In case ack policy is none then we also do not require to ack.
        #
        if cb and (not manual_ack) and (config.ack_policy
                                        is not api.AckPolicy.NONE):
            cb = self._auto_ack_callback(cb)
        if config.deliver_subject is None:
            raise TypeError("config.deliver_subject is required")
        sub = await self._nc.subscribe(
            subject=config.deliver_subject,
            queue=config.deliver_group or "",
            cb=cb,
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        psub = JetStreamContext.PushSubscription(self, sub, stream, consumer)

        # Keep state to support ordered consumers.
        sub._jsi = JetStreamContext._JSI(
            js=self,
            conn=self._nc,
            stream=stream,
            ordered=ordered_consumer,
            psub=psub,
            sub=sub,
            ccreq=config,
        )

        if config.idle_heartbeat:
            sub._jsi._hbtask = asyncio.create_task(sub._jsi.activity_check())

        if ordered_consumer:
            sub._jsi._fctask = asyncio.create_task(
                sub._jsi.check_flow_control_response()
            )

        return psub

    @staticmethod
    def _auto_ack_callback(callback: Callback) -> Callback:

        async def new_callback(msg: Msg) -> None:
            await callback(msg)
            try:
                await msg.ack()
            except nats.errors.MsgAlreadyAckdError:
                pass

        return new_callback

    async def pull_subscribe(
        self,
        subject: str,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[api.ConsumerConfig] = None,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        inbox_prefix: bytes = api.INBOX_PREFIX,
    ) -> JetStreamContext.PullSubscription:
        """Create consumer and pull subscription.

        1. Find stream name by subject if `stream` is not passed.
        2. Create consumer with the given `config` if not created.
        3. Call `pull_subscribe_bind`.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='mystream', subjects=['foo'])
                await js.publish('foo', b'Hello World!')

                sub = await js.pull_subscribe('foo', stream='mystream')

                msgs = await sub.fetch()
                msg = msgs[0]
                await msg.ack()

                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())

        """
        if stream is None:
            stream = await self._jsm.find_stream_name_by_subject(subject)

        should_create = True
        try:
            if durable:
                await self._jsm.consumer_info(stream, durable)
                should_create = False
        except nats.js.errors.NotFoundError:
            pass

        consumer_name = durable
        if should_create:
            # If not found then attempt to create with the defaults.
            if config is None:
                config = api.ConsumerConfig()

            # Auto created consumers use the filter subject.
            # config.name = durable
            config.filter_subject = subject
            if durable:
                config.name = durable
                config.durable_name = durable
            else:
                consumer_name = self._nc._nuid.next().decode()
                config.name = consumer_name

            await self._jsm.add_consumer(stream, config=config)

        return await self.pull_subscribe_bind(
            durable=durable,
            stream=stream,
            inbox_prefix=inbox_prefix,
            pending_bytes_limit=pending_bytes_limit,
            pending_msgs_limit=pending_msgs_limit,
            name=consumer_name,
        )

    async def pull_subscribe_bind(
        self,
        consumer: Optional[str] = None,
        stream: Optional[str] = None,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        name: Optional[str] = None,
        durable: Optional[str] = None,
    ) -> JetStreamContext.PullSubscription:
        """
        pull_subscribe returns a `PullSubscription` that can be delivered messages
        from a JetStream pull based consumer by calling `sub.fetch`.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='mystream', subjects=['foo'])
                await js.publish('foo', b'Hello World!')

                msgs = await sub.fetch()
                msg = msgs[0]
                await msg.ack()

                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())

        """
        if not stream:
            raise ValueError("nats: stream name is required")
        deliver = inbox_prefix + self._nc._nuid.next()
        sub = await self._nc.subscribe(
            deliver.decode(),
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit
        )
        consumer_name = None
        # In nats.py v2.7.0 changing the first arg to be 'consumer' instead of 'durable',
        # but continue to support for backwards compatibility.
        if durable:
            consumer_name = durable
        elif name:
            # This should not be common and 'consumer' arg preferred instead but support anyway.
            consumer_name = name
        else:
            consumer_name = consumer
        return JetStreamContext.PullSubscription(
            js=self,
            sub=sub,
            stream=stream,
            consumer=consumer_name,
            deliver=deliver,
        )

    @classmethod
    def is_status_msg(cls, msg: Optional[Msg]) -> Optional[str]:
        if msg is None or msg.headers is None:
            return None
        return msg.headers.get(api.Header.STATUS)

    @classmethod
    def _is_processable_msg(cls, status: Optional[str], msg: Msg) -> bool:
        if not status:
            return True
        # Skip most 4XX errors and do not raise exception.
        if JetStreamContext._is_temporary_error(status):
            return False
        raise nats.js.errors.APIError.from_msg(msg)

    @classmethod
    def _is_temporary_error(cls, status: Optional[str]) -> bool:
        if status == api.StatusCode.NO_MESSAGES or status == api.StatusCode.CONFLICT \
           or status == api.StatusCode.REQUEST_TIMEOUT:
            return True
        else:
            return False

    @classmethod
    def _is_heartbeat(cls, status: Optional[str]) -> bool:
        if status == api.StatusCode.CONTROL_MESSAGE:
            return True
        else:
            return False

    @classmethod
    def _time_until(cls, timeout: Optional[float],
                    start_time: float) -> Optional[float]:
        if timeout is None:
            return None
        return timeout - (time.monotonic() - start_time)

    class _JSI():

        def __init__(
            self,
            js: JetStreamContext,
            conn: NATS,
            stream: str,
            ordered: Optional[bool],
            psub: JetStreamContext.PushSubscription,
            sub: Subscription,
            ccreq: api.ConsumerConfig,
        ) -> None:
            self._conn = conn
            self._js = js
            self._stream = stream
            self._ordered = ordered
            self._psub = psub
            self._sub = sub
            self._ccreq = ccreq

            # Heartbeat
            self._hbtask = None
            self._hbi = None
            if ccreq and ccreq.idle_heartbeat:
                self._hbi = ccreq.idle_heartbeat

            # Ordered Consumer implementation.
            self._dseq = 1
            self._sseq = 0
            self._cmeta: Optional[str] = None
            self._fcr: Optional[str] = None
            self._fcd = 0
            self._fciseq = 0
            self._active: Optional[bool] = True
            self._fctask = None

        def track_sequences(self, reply: str) -> None:
            self._fciseq += 1
            self._cmeta = reply

        def schedule_flow_control_response(self, reply: str) -> None:
            self._active = True
            self._fcr = reply
            self._fcd = self._fciseq

        def get_js_delivered(self):
            if self._sub._cb:
                return self._sub.delivered
            return self._fciseq - self._sub._pending_queue.qsize()

        async def activity_check(self):
            # Can at most miss two heartbeats.
            hbc_threshold = 2
            while True:
                try:
                    if self._conn.is_closed:
                        break

                    # Wait for all idle heartbeats to be received,
                    # one of them would have toggled the state of the
                    # consumer back to being active.
                    await asyncio.sleep(self._hbi * hbc_threshold)
                    active = self._active
                    self._active = False
                    if not active:
                        if self._ordered:
                            await self.reset_ordered_consumer(self._sseq + 1)
                except asyncio.CancelledError:
                    break

        async def check_flow_control_response(self):
            while True:
                try:
                    if self._conn.is_closed:
                        break

                    if (self._fciseq - self._psub._pending_queue.qsize()) >= self._fcd:
                        fc_reply = self._fcr
                        try:
                            if fc_reply:
                                await self._conn.publish(fc_reply)
                        except Exception:
                            pass
                        self._fcr = None
                        self._fcd = 0
                    await asyncio.sleep(0.25)
                except asyncio.CancelledError:
                    break

        async def check_for_sequence_mismatch(self,
                                              msg: Msg) -> Optional[bool]:
            self._active = True
            if not self._cmeta:
                return None

            tokens = msg._get_metadata_fields(self._cmeta)
            dseq = int(tokens[6])  # consumer sequence
            ldseq = None
            if msg.headers:
                ldseq_str = msg.headers.get(api.Header.LAST_CONSUMER)
                if ldseq_str:
                    ldseq = int(ldseq_str)
            did_reset = None

            if ldseq != dseq:
                sseq = int(tokens[5])  # stream sequence

                if self._ordered:
                    did_reset = await self.reset_ordered_consumer(
                        self._sseq + 1
                    )
                else:
                    ecs = nats.js.errors.ConsumerSequenceMismatchError(
                        stream_resume_sequence=sseq,
                        consumer_sequence=dseq,
                        last_consumer_sequence=ldseq,
                    )
                    await self._conn._error_cb(ecs)
            return did_reset

        async def reset_ordered_consumer(self, sseq: Optional[int]) -> bool:
            # FIXME: Handle AUTO_UNSUB called previously to this.

            # Replace current subscription.
            osid = self._sub._id
            self._conn._remove_sub(osid)
            new_deliver = self._conn.new_inbox()

            # Place new one.
            self._conn._sid += 1
            nsid = self._conn._sid
            self._conn._subs[nsid] = self._sub
            self._sub._id = nsid
            self._psub._id = nsid

            # unsub
            await self._conn._send_unsubscribe(osid)

            # resub
            self._sub._subject = new_deliver
            await self._conn._send_subscribe(self._sub)

            # relinquish cpu to let proto commands make it to the server.
            await asyncio.sleep(0)

            # Reset some items in jsi.
            self._cmeta = None
            self._dseq = 1

            # Reset consumer request for starting policy.
            config = self._ccreq
            config.deliver_subject = new_deliver
            config.deliver_policy = api.DeliverPolicy.BY_START_SEQUENCE
            config.opt_start_seq = sseq
            self._ccreq = config

            # Handle the creation of new consumer in a background task
            # to avoid blocking the process_msg coroutine further
            # when making the request.
            asyncio.create_task(self.recreate_consumer())

            return True

        async def recreate_consumer(self) -> None:
            try:
                cinfo = await self._js._jsm.add_consumer(
                    self._stream,
                    config=self._ccreq,
                    timeout=self._js._timeout
                )
                self._psub._consumer = cinfo.name
            except Exception as err:
                await self._conn._error_cb(err)

    class PushSubscription(Subscription):
        """
        PushSubscription is a subscription that is delivered messages.
        """

        def __init__(
            self,
            js: JetStreamContext,
            sub: Subscription,
            stream: str,
            consumer: str,
        ) -> None:
            self._js = js
            self._stream = stream
            self._consumer = consumer

            self._sub = sub
            self._conn = sub._conn
            self._id = sub._id
            self._subject = sub._subject
            self._queue = sub._queue
            self._max_msgs = sub._max_msgs
            self._received = sub._received
            self._cb = sub._cb
            self._future = sub._future
            self._closed = sub._closed

            # Per subscription message processor.
            self._pending_msgs_limit = sub._pending_msgs_limit
            self._pending_bytes_limit = sub._pending_bytes_limit
            self._pending_queue = sub._pending_queue
            self._pending_size = sub._pending_size
            self._wait_for_msgs_task = sub._wait_for_msgs_task
            self._message_iterator = sub._message_iterator
            self._pending_next_msgs_calls = sub._pending_next_msgs_calls

        async def consumer_info(self) -> api.ConsumerInfo:
            """
            consumer_info gets the current info of the consumer from this subscription.
            """
            info = await self._js._jsm.consumer_info(
                self._stream,
                self._consumer,
            )
            return info

        @property
        def delivered(self) -> int:
            """
            Number of delivered messages to this subscription so far.
            """
            return self._sub._received

        @delivered.setter
        def delivered(self, value):
            self._sub._received = value

        @property
        def _pending_size(self):
            return self._sub._pending_size

        @_pending_size.setter
        def _pending_size(self, value):
            self._sub._pending_size = value

        async def next_msg(self, timeout: Optional[float] = 1.0) -> Msg:
            """
            :params timeout: Time in seconds to wait for next message before timing out.
            :raises nats.errors.TimeoutError:

            next_msg can be used to retrieve the next message from a stream of messages using
            await syntax, this only works when not passing a callback on `subscribe`::
            """
            msg = await self._sub.next_msg(timeout)

            # In case there is a flow control reply present need to handle here.
            if self._sub and self._sub._jsi:
                self._sub._jsi._active = True
                if self._sub._jsi.get_js_delivered() >= self._sub._jsi._fciseq:
                    fc_reply = self._sub._jsi._fcr
                    if fc_reply:
                        await self._conn.publish(fc_reply)
                        self._sub._jsi._fcr = None
            return msg

    class PullSubscription:
        """
        PullSubscription is a subscription that can fetch messages.
        """

        def __init__(
            self,
            js: JetStreamContext,
            sub: Subscription,
            stream: str,
            consumer: str,
            deliver: bytes,
        ) -> None:
            # JS/JSM context
            self._js = js
            self._nc = js._nc

            # NATS Subscription
            self._sub = sub
            self._stream = stream
            self._consumer = consumer
            prefix = self._js._prefix
            self._nms = f'{prefix}.CONSUMER.MSG.NEXT.{stream}.{consumer}'
            self._deliver = deliver.decode()

        @property
        def pending_msgs(self) -> int:
            """
            Number of delivered messages by the NATS Server that are being buffered
            in the pending queue.
            """
            return self._sub._pending_queue.qsize()

        @property
        def pending_bytes(self) -> int:
            """
            Size of data sent by the NATS Server that is being buffered
            in the pending queue.
            """
            return self._sub._pending_size

        @property
        def delivered(self) -> int:
            """
            Number of delivered messages to this subscription so far.
            """
            return self._sub._received

        async def unsubscribe(self) -> None:
            """
            unsubscribe destroys the inboxes of the pull subscription making it
            unable to continue to receive messages.
            """
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            await self._sub.unsubscribe()

        async def consumer_info(self) -> api.ConsumerInfo:
            """
            consumer_info gets the current info of the consumer from this subscription.
            """
            info = await self._js._jsm.consumer_info(
                self._stream, self._consumer
            )
            return info

        async def fetch(
            self,
            batch: int = 1,
            timeout: Optional[float] = 5,
            heartbeat: Optional[float] = None
        ) -> List[Msg]:
            """
            fetch makes a request to JetStream to be delivered a set of messages.

            :param batch: Number of messages to fetch from server.
            :param timeout: Max duration of the fetch request before it expires.
            :param heartbeat: Idle Heartbeat interval in seconds for the fetch request.

            ::

                import asyncio
                import nats

                async def main():
                    nc = await nats.connect()
                    js = nc.jetstream()

                    await js.add_stream(name='mystream', subjects=['foo'])
                    await js.publish('foo', b'Hello World!')

                    msgs = await sub.fetch(5)
                    for msg in msgs:
                      await msg.ack()

                    await nc.close()

                if __name__ == '__main__':
                    asyncio.run(main())
            """
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            # FIXME: Check connection is not closed, etc...
            if batch < 1:
                raise ValueError("nats: invalid batch size")
            if timeout is not None and timeout <= 0:
                raise ValueError("nats: invalid fetch timeout")

            expires = int(
                timeout * 1_000_000_000
            ) - 100_000 if timeout else None
            if batch == 1:
                msg = await self._fetch_one(expires, timeout, heartbeat)
                return [msg]
            msgs = await self._fetch_n(batch, expires, timeout, heartbeat)
            return msgs

        async def _fetch_one(
            self,
            expires: Optional[int],
            timeout: Optional[float],
            heartbeat: Optional[float] = None
        ) -> Msg:
            queue = self._sub._pending_queue

            # Check the next message in case there are any.
            while not queue.empty():
                try:
                    msg = queue.get_nowait()
                    self._sub._pending_size -= len(msg.data)
                    status = JetStreamContext.is_status_msg(msg)
                    if status:
                        # Discard status messages at this point since were meant
                        # for other fetch requests.
                        continue
                    return msg
                except Exception:
                    # Fallthrough to make request in case this failed.
                    pass

            # Make lingering request with expiration and wait for response.
            next_req = {}
            next_req['batch'] = 1
            if expires:
                next_req['expires'] = int(expires)
            if heartbeat:
                next_req['idle_heartbeat'] = int(
                    heartbeat * 1_000_000_000
                )  # to nanoseconds

            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
            )

            start_time = time.monotonic()
            got_any_response = False
            while True:
                try:
                    deadline = JetStreamContext._time_until(
                        timeout, start_time
                    )
                    # Wait for the response or raise timeout.
                    msg = await self._sub.next_msg(timeout=deadline)

                    # Should have received at least a processable message at this point,
                    status = JetStreamContext.is_status_msg(msg)
                    if status:
                        if JetStreamContext._is_heartbeat(status):
                            got_any_response = True
                            continue

                        # In case of a temporary error, treat it as a timeout to retry.
                        if JetStreamContext._is_temporary_error(status):
                            raise nats.errors.TimeoutError
                        else:
                            # Any other type of status message is an error.
                            raise nats.js.errors.APIError.from_msg(msg)
                    else:
                        return msg
                except asyncio.TimeoutError:
                    deadline = JetStreamContext._time_until(
                        timeout, start_time
                    )
                    if deadline is not None and deadline < 0:
                        # No response from the consumer could have been
                        # due to a reconnect while the fetch request,
                        # the JS API not responding on time, or maybe
                        # there were no messages yet.
                        if got_any_response:
                            raise FetchTimeoutError
                        raise

        async def _fetch_n(
            self,
            batch: int,
            expires: Optional[int],
            timeout: Optional[float],
            heartbeat: Optional[float] = None
        ) -> List[Msg]:
            msgs = []
            queue = self._sub._pending_queue
            start_time = time.monotonic()
            got_any_response = False
            needed = batch

            # Fetch as many as needed from the internal pending queue.
            msg: Optional[Msg]

            while not queue.empty():
                try:
                    msg = queue.get_nowait()
                    self._sub._pending_size -= len(msg.data)
                    status = JetStreamContext.is_status_msg(msg)
                    if status:
                        # Discard status messages at this point since were meant
                        # for other fetch requests.
                        continue
                    needed -= 1
                    msgs.append(msg)
                except Exception:
                    pass

            # First request: Use no_wait to synchronously get as many available
            # based on the batch size until server sends 'No Messages' status msg.
            next_req = {}
            next_req['batch'] = needed
            if expires:
                next_req['expires'] = expires
            if heartbeat:
                next_req['idle_heartbeat'] = int(
                    heartbeat * 1_000_000_000
                )  # to nanoseconds
            next_req['no_wait'] = True
            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
            )
            await asyncio.sleep(0)

            try:
                msg = await self._sub.next_msg(timeout)
            except asyncio.TimeoutError:
                # Return any message that was already available in the internal queue.
                if msgs:
                    return msgs
                raise

            got_any_response = False

            status = JetStreamContext.is_status_msg(msg)
            if JetStreamContext._is_heartbeat(status):
                # Mark that we got any response from the server so this is not
                # a possible i/o timeout error or due to a disconnection.
                got_any_response = True
                pass
            elif JetStreamContext._is_processable_msg(status, msg):
                # First processable message received, do not raise error from now.
                msgs.append(msg)
                needed -= 1

                try:
                    for i in range(0, needed):
                        deadline = JetStreamContext._time_until(
                            timeout, start_time
                        )
                        msg = await self._sub.next_msg(timeout=deadline)
                        status = JetStreamContext.is_status_msg(msg)
                        if status == api.StatusCode.NO_MESSAGES:
                            # No more messages after this so fallthrough
                            # after receiving the rest.
                            break
                        elif JetStreamContext._is_heartbeat(status):
                            # Skip heartbeats.
                            got_any_response = True
                            continue
                        elif JetStreamContext._is_processable_msg(status, msg):
                            needed -= 1
                            msgs.append(msg)
                except asyncio.TimeoutError:
                    # Ignore any timeout errors at this point since
                    # at least one message has already arrived.
                    pass

            # Stop if have some messages.
            if len(msgs) > 0:
                return msgs

            # Second request: lingering request that will block until new messages
            # are made available and delivered to the client.
            next_req = {}
            next_req['batch'] = needed
            if expires:
                next_req['expires'] = expires
            if heartbeat:
                next_req['idle_heartbeat'] = int(
                    heartbeat * 1_000_000_000
                )  # to nanoseconds

            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
            )
            await asyncio.sleep(0)

            # Get the immediate next message which could be a status message
            # or a processable message.
            msg = None

            while True:
                # Check if already got enough at this point.
                if needed == 0:
                    return msgs

                deadline = JetStreamContext._time_until(timeout, start_time)
                if len(msgs) == 0:
                    # Not a single processable message has been received so far,
                    # if this timed out then let the error be raised.
                    try:
                        msg = await self._sub.next_msg(timeout=deadline)
                    except asyncio.TimeoutError:
                        if got_any_response:
                            raise FetchTimeoutError
                        raise
                else:
                    try:
                        msg = await self._sub.next_msg(timeout=deadline)
                    except asyncio.TimeoutError:
                        # Ignore any timeout since already got at least a message.
                        break

                if msg:
                    status = JetStreamContext.is_status_msg(msg)
                    if JetStreamContext._is_heartbeat(status):
                        got_any_response = True
                        continue

                    if not status:
                        needed -= 1
                        msgs.append(msg)
                        break
                    elif status == api.StatusCode.NO_MESSAGES or status:
                        # If there is still time, try again to get the next message
                        # or timeout.  This could be due to concurrent uses of fetch
                        # with the same inbox.
                        break
                    elif len(msgs) == 0:
                        raise nats.js.errors.APIError.from_msg(msg)

            # Wait for the rest of the messages to be delivered to the internal pending queue.
            try:
                for _ in range(needed):
                    deadline = JetStreamContext._time_until(
                        timeout, start_time
                    )
                    if deadline is not None and deadline < 0:
                        return msgs

                    msg = await self._sub.next_msg(timeout=deadline)
                    status = JetStreamContext.is_status_msg(msg)
                    if JetStreamContext._is_heartbeat(status):
                        got_any_response = True
                        continue
                    if JetStreamContext._is_processable_msg(status, msg):
                        needed -= 1
                        msgs.append(msg)
            except asyncio.TimeoutError:
                # Ignore any timeout errors at this point since
                # at least one message has already arrived.
                pass

            if len(msgs) == 0 and got_any_response:
                raise FetchTimeoutError

            return msgs

    ######################
    #                    #
    # KeyValue Context   #
    #                    #
    ######################

    async def key_value(self, bucket: str) -> KeyValue:
        if VALID_BUCKET_RE.match(bucket) is None:
            raise InvalidBucketNameError

        stream = KV_STREAM_TEMPLATE.format(bucket=bucket)
        try:
            si = await self.stream_info(stream)
        except NotFoundError:
            raise BucketNotFoundError
        if si.config.max_msgs_per_subject < 1:
            raise BadBucketError

        return KeyValue(
            name=bucket,
            stream=stream,
            pre=KV_PRE_TEMPLATE.format(bucket=bucket),
            js=self,
            direct=bool(si.config.allow_direct)
        )

    async def create_key_value(
        self,
        config: Optional[api.KeyValueConfig] = None,
        **params,
    ) -> KeyValue:
        """
        create_key_value takes an api.KeyValueConfig and creates a KV in JetStream.
        """
        if config is None:
            config = api.KeyValueConfig(bucket=params["bucket"])
        config = config.evolve(**params)

        if VALID_BUCKET_RE.match(config.bucket) is None:
            raise InvalidBucketNameError

        duplicate_window: float = 2 * 60  # 2 minutes

        if config.ttl and config.ttl < duplicate_window:
            duplicate_window = config.ttl

        if config.history > 64:
            raise nats.js.errors.KeyHistoryTooLargeError

        stream = api.StreamConfig(
            name=KV_STREAM_TEMPLATE.format(bucket=config.bucket),
            description=config.description,
            subjects=[f"$KV.{config.bucket}.>"],
            allow_direct=config.direct,
            allow_rollup_hdrs=True,
            deny_delete=True,
            discard=api.DiscardPolicy.NEW,
            duplicate_window=duplicate_window,
            max_age=config.ttl,
            max_bytes=config.max_bytes,
            max_consumers=-1,
            max_msg_size=config.max_value_size,
            max_msgs=-1,
            max_msgs_per_subject=config.history,
            num_replicas=config.replicas,
            storage=config.storage,
            republish=config.republish,
        )
        si = await self.add_stream(stream)
        assert stream.name is not None

        return KeyValue(
            name=config.bucket,
            stream=stream.name,
            pre=KV_PRE_TEMPLATE.format(bucket=config.bucket),
            js=self,
            direct=bool(si.config.allow_direct),
        )

    async def delete_key_value(self, bucket: str) -> bool:
        """
        delete_key_value deletes a JetStream KeyValue store by destroying
        the associated stream.
        """
        if VALID_BUCKET_RE.match(bucket) is None:
            raise InvalidBucketNameError

        stream = KV_STREAM_TEMPLATE.format(bucket=bucket)
        return await self.delete_stream(stream)

    #######################
    #                     #
    # ObjectStore Context #
    #                     #
    #######################

    async def object_store(self, bucket: str) -> ObjectStore:
        if VALID_BUCKET_RE.match(bucket) is None:
            raise InvalidBucketNameError

        stream = OBJ_STREAM_TEMPLATE.format(bucket=bucket)
        try:
            await self.stream_info(stream)
        except NotFoundError:
            raise BucketNotFoundError

        return ObjectStore(
            name=bucket,
            stream=stream,
            js=self,
        )

    async def create_object_store(
        self,
        bucket: str = None,
        config: Optional[api.ObjectStoreConfig] = None,
        **params,
    ) -> ObjectStore:
        """
        create_object_store takes an api.ObjectStoreConfig and creates a OBJ in JetStream.
        """
        if config is None:
            config = api.ObjectStoreConfig(bucket=bucket)
        else:
            config.bucket = bucket
        config = config.evolve(**params)

        if VALID_BUCKET_RE.match(config.bucket) is None:
            raise InvalidBucketNameError

        name = config.bucket
        chunks = OBJ_ALL_CHUNKS_PRE_TEMPLATE.format(bucket=name)
        meta = OBJ_ALL_META_PRE_TEMPLATE.format(bucket=name)

        max_bytes = config.max_bytes
        if max_bytes == 0:
            max_bytes = -1

        stream = api.StreamConfig(
            name=OBJ_STREAM_TEMPLATE.format(bucket=config.bucket),
            description=config.description,
            subjects=[chunks, meta],
            max_age=config.ttl,
            max_bytes=max_bytes,
            max_consumers=0,
            storage=config.storage,
            num_replicas=config.replicas,
            placement=config.placement,
            discard=api.DiscardPolicy.NEW,
            allow_rollup_hdrs=True,
            allow_direct=True,
        )
        await self.add_stream(stream)

        assert stream.name is not None
        return ObjectStore(
            name=config.bucket,
            stream=stream.name,
            js=self,
        )

    async def delete_object_store(self, bucket: str) -> bool:
        """
        delete_object_store will delete the underlying stream for the named object.
        """
        if VALID_BUCKET_RE.match(bucket) is None:
            raise InvalidBucketNameError

        stream = OBJ_STREAM_TEMPLATE.format(bucket=bucket)
        return await self.delete_stream(stream)
