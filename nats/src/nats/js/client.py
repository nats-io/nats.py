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
from copy import deepcopy
from email.parser import BytesParser
from secrets import token_hex
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
)

import nats.errors
import nats.js.errors
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import api
from nats.js.errors import (
    BadBucketError,
    BucketNotFoundError,
    FetchTimeoutError,
    InvalidBucketNameError,
    NotFoundError,
)
from nats.js.kv import KeyValue
from nats.js.manager import JetStreamManager, _validate_consumer_name, _validate_stream_name
from nats.js.object_store import (
    OBJ_ALL_CHUNKS_PRE_TEMPLATE,
    OBJ_ALL_META_PRE_TEMPLATE,
    OBJ_STREAM_TEMPLATE,
    VALID_BUCKET_RE,
    ObjectStore,
)

if TYPE_CHECKING:
    from nats import NATS

NO_RESPONDERS_STATUS = "503"

NATS_HDR_LINE = bytearray(b"NATS/1.0")
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
_CRLF_ = b"\r\n"
_CRLF_LEN_ = len(_CRLF_)
KV_STREAM_TEMPLATE = "KV_{bucket}"
KV_PRE_TEMPLATE = "$KV.{bucket}."
Callback = Callable[["Msg"], Awaitable[None]]

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
        self._async_reply_prefix.extend(b".")
        self._async_reply_prefix.extend(self._nc._nuid.next())
        self._async_reply_prefix.extend(b".")

        async_reply_subject = self._async_reply_prefix[:]
        async_reply_subject.extend(b"*")

        await self._nc.subscribe(async_reply_subject.decode(), cb=self._handle_async_reply)

    async def _handle_async_reply(self, msg: Msg) -> None:
        token = msg.subject[len(self._nc._inbox_prefix) + 22 + 2 :]
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
            if "error" in resp:
                err = nats.js.errors.APIError.from_error(resp["error"])
                future.set_exception(err)
                return

            ack = api.PubAck.from_response(resp)
            future.set_result(ack)
        except (asyncio.CancelledError, asyncio.InvalidStateError):
            pass

    async def publish(
        self,
        subject: str,
        payload: bytes = b"",
        timeout: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[Dict[str, Any]] = None,
        msg_ttl: Optional[float] = None,
    ) -> api.PubAck:
        """
        publish emits a new message to JetStream and waits for acknowledgement.

        :param subject: Subject to publish to.
        :param payload: Message payload.
        :param timeout: Request timeout in seconds.
        :param stream: Expected stream name.
        :param headers: Message headers.
        :param msg_ttl: Per-message TTL in seconds (requires NATS Server 2.11+).
        """
        hdr = headers
        if timeout is None:
            timeout = self._timeout
        if stream is not None:
            hdr = hdr or {}
            hdr[api.Header.EXPECTED_STREAM] = stream
        if msg_ttl is not None:
            hdr = hdr or {}
            # TTL header accepts seconds as integer or duration string
            hdr[api.Header.MSG_TTL] = str(int(msg_ttl))

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
        if "error" in resp:
            raise nats.js.errors.APIError.from_error(resp["error"])
        return api.PubAck.from_response(resp)

    async def publish_async(
        self,
        subject: str,
        payload: bytes = b"",
        wait_stall: Optional[float] = None,
        stream: Optional[str] = None,
        headers: Optional[Dict] = None,
        msg_ttl: Optional[float] = None,
    ) -> asyncio.Future[api.PubAck]:
        """
        emits a new message to JetStream and returns a future that can be awaited for acknowledgement.

        :param subject: Subject to publish to.
        :param payload: Message payload.
        :param wait_stall: Maximum time to wait for semaphore in seconds.
        :param stream: Expected stream name.
        :param headers: Message headers.
        :param msg_ttl: Per-message TTL in seconds (requires NATS Server 2.11+).
        """

        if not self._async_reply_prefix:
            await self._init_async_reply()
        assert self._async_reply_prefix

        hdr = headers
        if stream is not None:
            hdr = hdr or {}
            hdr[api.Header.EXPECTED_STREAM] = stream
        if msg_ttl is not None:
            hdr = hdr or {}
            # TTL header accepts seconds as integer or duration string
            hdr[api.Header.MSG_TTL] = str(int(msg_ttl))

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

        await self._nc.publish(subject, payload, reply=inbox.decode(), headers=hdr)

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
                raise nats.js.errors.Error(f"cannot create queue subscription '{queue}' to consumer '{durable}'")
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
                    raise nats.js.errors.Error("consumer is already bound to a subscription")
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

            # Create inbox for push consumer, if deliver_subject is not assigned already.
            if config.deliver_subject is None:
                deliver = self._nc.new_inbox()
                config.deliver_subject = deliver

            # Auto created consumers use the filter subject, unless filter_subjects is set.
            if not config.filter_subjects:
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
        """Push-subscribe to an existing consumer."""
        # By default, async subscribers wrap the original callback and
        # auto ack the messages as they are delivered.
        #
        # In case ack policy is none then we also do not require to ack.
        # Sourcing consumers (flow_control) are acknowledged by the sourcing
        # server through flow control messages, not by an ack reply per
        # message, so they must not be wrapped either.
        #
        # Compared by value rather than identity so a policy that arrived as a
        # plain string from the server is matched too.
        #
        if cb and (not manual_ack) and config.ack_policy not in (api.AckPolicy.NONE, api.AckPolicy.FLOW_CONTROL):
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
            sub._jsi._fctask = asyncio.create_task(sub._jsi.check_flow_control_response())

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
        inbox_prefix: Optional[bytes] = None,
        priority_group: Optional[str] = None,
    ) -> JetStreamContext.PullSubscription:
        """Create or attach to a pull consumer.

        Discover the stream from `subject` unless `stream` is supplied. An empty
        subject requires an explicit stream. Existing durable consumers must
        match the subject and requested config. The supplied config is copied.

        Unsubscribe deletes a consumer created by this subscription, including
        a durable. Existing consumers are retained. To manage consumer lifetime
        separately, use `add_consumer` followed by `pull_subscribe_bind`.

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
        # Work on a copy: defaults and generated names belong to this subscription.
        requested = (
            deepcopy(config)
            if config is not None
            else api.ConsumerConfig(deliver_policy=None, ack_policy=None, replay_policy=None)
        )
        config = deepcopy(config) if config is not None else api.ConsumerConfig()
        if durable and config.durable_name and durable != config.durable_name:
            raise ValueError(f"nats: duplicate consumer names ({config.durable_name} and {durable})")
        durable = durable or config.durable_name
        requested.durable_name = durable
        for consumer_name in (durable, config.name):
            if consumer_name:
                _validate_consumer_name(consumer_name)
        if not subject and not stream:
            raise ValueError("nats: subject required")
        if config.deliver_subject:
            raise ValueError("nats: cannot pull subscribe to push based consumer")
        if stream:
            _validate_stream_name(stream)
        if config.priority_groups and priority_group is None:
            raise ValueError("nats: priority_group is required when consumer has priority_groups configured")
        if not stream:
            stream = await self._jsm.find_stream_name_by_subject(subject)

        info = None
        if durable:
            try:
                info = await self._jsm.consumer_info(stream, durable)
            except NotFoundError as err:
                # A missing stream is not permission to attempt consumer creation.
                if err.err_code != 10014:
                    raise

        if info is not None:
            self._check_pull_consumer(info.config, subject, requested)
        else:
            # Auto created consumers use the filter subject, unless filter_subjects is set.
            if not config.filter_subjects:
                config.filter_subject = subject

            config.durable_name = durable
            config.name = config.name or durable or self._nc._nuid.next().decode()

            # Auto created consumers use the priority group, unless priority_groups is set.
            if not config.priority_groups and priority_group:
                config.priority_groups = [priority_group]

        # Allocate the inbox before creating the consumer, so an invalid inbox or
        # closed connection cannot leave an unused server consumer behind.
        sub = await self._pull_subscribe_bind(
            consumer=info.name if info is not None else config.name,
            stream=stream,
            inbox_prefix=inbox_prefix,
            pending_bytes_limit=pending_bytes_limit,
            pending_msgs_limit=pending_msgs_limit,
            priority_group=priority_group,
        )
        if info is None:
            try:
                info = await self._jsm.add_consumer(stream, config=config)
            except BaseException:
                try:
                    await sub._sub.unsubscribe()
                except nats.errors.Error:
                    pass
                raise
            sub._consumer = info.name
            sub._nms = f"{self._prefix}.CONSUMER.MSG.NEXT.{stream}.{info.name}"
            sub._owns_consumer = True
        return sub

    @staticmethod
    def _check_pull_consumer(
        server: api.ConsumerConfig,
        subject: Optional[str] = None,
        requested: Optional[api.ConsumerConfig] = None,
    ) -> None:
        if subject is not None and server.filter_subject and server.filter_subject != subject:
            raise ValueError("nats: subject does not match consumer")
        if server.deliver_subject:
            raise ValueError("nats: cannot pull subscribe to push based consumer")
        if requested is None:
            return
        # Like nats.go, check requested options without comparing server defaults
        # for omitted scalar fields. ConsumerConfig's enum defaults are explicit
        # when a config object is supplied; pass None to leave an enum unchecked.
        fields = (
            ("durable_name", "durable"),
            ("description", "description"),
            ("deliver_policy", "deliver policy"),
            ("opt_start_seq", "optional start sequence"),
            ("opt_start_time", "optional start time"),
            ("ack_policy", "ack policy"),
            ("ack_wait", "ack wait"),
            ("max_deliver", "max deliver"),
            ("replay_policy", "replay policy"),
            ("rate_limit_bps", "rate limit"),
            ("sample_freq", "sample frequency"),
            ("max_waiting", "max waiting"),
            ("max_ack_pending", "max ack pending"),
            ("flow_control", "flow control"),
            ("idle_heartbeat", "heartbeat"),
            ("num_replicas", "replicas"),
            ("mem_storage", "memory storage"),
        )
        for field, label in fields:
            wanted, actual = getattr(requested, field), getattr(server, field)
            if isinstance(wanted, (int, float)) and wanted <= 0:
                continue
            if wanted and wanted != actual:
                wanted, actual = getattr(wanted, "value", wanted), getattr(actual, "value", actual)
                raise ValueError(
                    f"nats: configuration requests {label} to be {wanted}, but consumer's value is {actual}"
                )

    async def pull_subscribe_bind(
        self,
        consumer: Optional[str] = None,
        stream: Optional[str] = None,
        inbox_prefix: Optional[bytes] = None,
        pending_msgs_limit: int = DEFAULT_JS_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_JS_SUB_PENDING_BYTES_LIMIT,
        name: Optional[str] = None,
        durable: Optional[str] = None,
        priority_group: Optional[str] = None,
    ) -> JetStreamContext.PullSubscription:
        """
        Bind to an existing pull consumer without taking ownership of it.

        Both `stream` and `consumer` are required. `name` and `durable` remain
        supported aliases for `consumer`; supplied aliases must agree. Consumer
        lookup rejects missing or push consumers. If lookup times out or the
        management API is unavailable, binding proceeds using the supplied name.
        Unsubscribe only removes the local inbox and retains the consumer.

        ::

            import asyncio
            import nats

            async def main():
                nc = await nats.connect()
                js = nc.jetstream()

                await js.add_stream(name='mystream', subjects=['foo'])
                await js.publish('foo', b'Hello World!')

                await js.add_consumer('mystream', durable_name='worker', filter_subject='foo')
                sub = await js.pull_subscribe_bind('worker', stream='mystream')
                msgs = await sub.fetch()
                msg = msgs[0]
                await msg.ack()

                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())

        """
        _validate_stream_name(stream)
        consumer_name = durable or name or consumer
        _validate_consumer_name(consumer_name)
        for alias in (consumer, name, durable):
            if alias and alias != consumer_name:
                raise ValueError(f"nats: duplicate consumer names ({consumer_name} and {alias})")
        try:
            info = await self._jsm.consumer_info(stream, consumer_name)
        except (nats.errors.TimeoutError, nats.js.errors.ServiceUnavailableError):
            # Like nats.go Bind, allow attaching when the info API is unavailable.
            pass
        else:
            self._check_pull_consumer(info.config)
        return await self._pull_subscribe_bind(
            consumer_name, stream, inbox_prefix, pending_msgs_limit, pending_bytes_limit, priority_group
        )

    async def _pull_subscribe_bind(
        self,
        consumer: str,
        stream: str,
        inbox_prefix: Optional[bytes],
        pending_msgs_limit: int,
        pending_bytes_limit: int,
        priority_group: Optional[str],
    ) -> JetStreamContext.PullSubscription:
        if inbox_prefix is None:
            inbox_prefix = bytes(self._nc._inbox_prefix[:]) + b"."

        deliver = inbox_prefix + self._nc._nuid.next()
        sub = await self._nc.subscribe(
            deliver.decode(),
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
        )
        return JetStreamContext.PullSubscription(
            js=self,
            sub=sub,
            stream=stream,
            consumer=consumer,
            deliver=deliver,
            group=priority_group,
        )

    @classmethod
    def is_status_msg(cls, msg: Optional[Msg]) -> Optional[str]:
        if msg is None or msg.headers is None:
            return None
        return msg.headers.get(api.Header.STATUS)

    @classmethod
    def _is_pin_id_mismatch_error(cls, status: Optional[str]) -> bool:
        return status == api.StatusCode.PIN_ID_MISMATCH

    @classmethod
    def _is_heartbeat(cls, status: Optional[str]) -> bool:
        if status == api.StatusCode.CONTROL_MESSAGE:
            return True
        else:
            return False

    class _JSI:
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

        async def check_for_sequence_mismatch(self, msg: Msg) -> Optional[bool]:
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
                    did_reset = await self.reset_ordered_consumer(self._sseq + 1)
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
                cinfo = await self._js._jsm.add_consumer(self._stream, config=self._ccreq, timeout=self._js._timeout)
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

        async def unsubscribe(self, limit: int = 0):
            """
            Unsubscribes from a subscription, canceling any heartbeat and flow control tasks,
            and optionally limits the number of messages to process before unsubscribing.
            """
            await super().unsubscribe(limit)

            if self._sub._jsi._hbtask:
                self._sub._jsi._hbtask.cancel()

            if self._sub._jsi._fctask:
                self._sub._jsi._fctask.cancel()

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
            group: Optional[str] = None,
        ) -> None:
            # JS/JSM context
            self._js = js
            self._nc = js._nc

            # NATS Subscription
            self._sub = sub
            self._stream = stream
            self._consumer = consumer
            prefix = self._js._prefix
            self._nms = f"{prefix}.CONSUMER.MSG.NEXT.{stream}.{consumer}"
            self._deliver = deliver.decode()
            self._pin_id: Optional[str] = None
            self._group = group
            self._owns_consumer = False

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

        @property
        def pin_id(self) -> Optional[str]:
            """
            Pin id assigned by the server when the consumer uses
            ``PriorityPolicy.PINNED``, or ``None`` if not pinned.
            """
            return self._pin_id

        def _build_next_req(
            self,
            batch: int,
            expires: Optional[int] = None,
            heartbeat: Optional[float] = None,
            no_wait: bool = False,
            min_pending: Optional[int] = None,
            min_ack_pending: Optional[int] = None,
            priority: Optional[int] = None,
        ) -> Dict[str, Any]:
            next_req: Dict[str, Any] = {"batch": batch}
            if expires:
                next_req["expires"] = int(expires)
            if heartbeat:
                next_req["idle_heartbeat"] = int(heartbeat * 1_000_000_000)  # to nanoseconds
            if no_wait:
                next_req["no_wait"] = True
            if self._group:
                next_req["group"] = self._group
            if self._pin_id:
                next_req["id"] = self._pin_id
            if min_pending is not None:
                next_req["min_pending"] = min_pending
            if min_ack_pending is not None:
                next_req["min_ack_pending"] = min_ack_pending
            if priority is not None:
                next_req["priority"] = priority
            return next_req

        async def unsubscribe(self) -> None:
            """
            unsubscribe destroys the inboxes of the pull subscription making it
            unable to continue to receive messages.

            Consumers created by this subscription are deleted. Consumers that
            already existed, or were explicitly bound, are retained.
            """
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            await self._sub.unsubscribe()
            if self._owns_consumer:
                await self._js._jsm.delete_consumer(self._stream, self._consumer)

        async def consumer_info(self) -> api.ConsumerInfo:
            """
            consumer_info gets the current info of the consumer from this subscription.
            """
            info = await self._js._jsm.consumer_info(self._stream, self._consumer)
            return info

        async def fetch(
            self,
            batch: int = 1,
            timeout: Optional[float] = 5,
            heartbeat: Optional[float] = None,
            min_pending: Optional[int] = None,
            min_ack_pending: Optional[int] = None,
            priority: Optional[int] = None,
        ) -> List[Msg]:
            """
            fetch makes a request to JetStream to be delivered a set of messages.

            :param batch: Maximum number of messages to fetch, including buffered messages.
            :param timeout: Maximum duration in seconds; None waits indefinitely.
            :param heartbeat: Idle heartbeat interval in seconds. Missing two intervals
                raises ``NoHeartbeatError`` if no messages have arrived. Must be
                less than half the timeout; zero or None disables monitoring.
            :param min_pending: Only deliver when the consumer has at least this many
                pending messages. Requires ``PriorityPolicy.OVERFLOW``.
            :param min_ack_pending: Only deliver when the consumer has at least this
                many unacknowledged messages. Requires ``PriorityPolicy.OVERFLOW``.
            :param priority: Priority of this request from 0 (highest) to 9.
                Requires ``PriorityPolicy.PRIORITIZED``.
            :returns: A list of at most ``batch`` messages. Once messages have been
                collected, a timeout, missing heartbeat, disconnect, subscription
                closure, or server error returns the partial batch successfully.
                Cancellation still propagates even after collecting messages.
            :raises ValueError: If the subscription is missing or a fetch option
                is invalid, including a negative heartbeat or a positive heartbeat
                greater than or equal to half a finite timeout.
            :raises nats.js.errors.NoHeartbeatError: If no message or heartbeat
                arrives for two heartbeat intervals and the batch is empty.
            :raises nats.js.errors.FetchDisconnectedError: If the connection
                disconnects or is reconnecting while requesting messages and the
                batch is empty.
            :raises nats.js.errors.FetchTimeoutError: If the deadline expires
                after receiving heartbeats but no messages. Subclasses
                ``nats.errors.TimeoutError``.
            :raises nats.errors.TimeoutError: If the deadline expires without
                receiving messages or heartbeats.
            :raises nats.errors.ConnectionClosedError: If the connection is
                already closed, or closes during an empty fetch.
            :raises nats.errors.BadSubscriptionError: If the subscription is
                already closed, or is unsubscribed during an empty fetch.
            :raises nats.js.errors.ServiceUnavailableError: If the server returns
                a 503 response and the batch is empty.
            :raises nats.js.errors.APIError: If the server returns another terminal
                error and the batch is empty, including consumer deletion,
                leadership change, or an exceeded MaxWaiting limit.
            :raises asyncio.CancelledError: If the caller cancels the fetch.

            ::

                import asyncio
                import nats

                async def main():
                    nc = await nats.connect()
                    js = nc.jetstream()

                    await js.add_stream(name='mystream', subjects=['foo'])
                    await js.publish('foo', b'Hello World!')

                    sub = await js.pull_subscribe('foo')
                    msgs = await sub.fetch(5)
                    for msg in msgs:
                      await msg.ack()

                    await nc.close()

                if __name__ == '__main__':
                    asyncio.run(main())
            """
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            if self._nc.is_closed:
                raise nats.errors.ConnectionClosedError
            if self._sub._closed:
                raise nats.errors.BadSubscriptionError
            if batch < 1:
                raise ValueError("nats: invalid batch size")
            if timeout is not None and timeout <= 0:
                raise ValueError("nats: invalid fetch timeout")
            if min_pending is not None and min_pending <= 0:
                raise ValueError("nats: min_pending must be more than 0")
            if min_ack_pending is not None and min_ack_pending <= 0:
                raise ValueError("nats: min_ack_pending must be more than 0")
            if priority is not None and not (0 <= priority <= 9):
                raise ValueError("nats: priority must be 0-9")

            if heartbeat is not None and heartbeat < 0:
                raise ValueError("nats: invalid heartbeat")
            if heartbeat and timeout is not None and 2 * heartbeat >= timeout:
                raise ValueError("nats: idle heartbeat value too large")

            deadline = time.monotonic() + timeout if timeout is not None else None
            msgs: List[Msg] = []

            def status_of(msg: Msg) -> Optional[str]:
                # Match nats.go v1.54.0's checkMsg: a non-empty payload is an
                # application message, even when it carries a Status header.
                # For example, Status: 409 with data must be delivered, not
                # raised as a server error. Only empty payloads can be control
                # frames; is_status_msg() alone only checks the header.
                return JetStreamContext.is_status_msg(msg) if not msg.data else None

            def append(msg: Msg) -> None:
                pin_id = msg.headers.get(api.Header.PIN_ID) if msg.headers else None
                if pin_id:
                    self._pin_id = pin_id
                msgs.append(msg)

            # Discard stale control frames, but leave excess messages for the next fetch.
            queue = self._sub._pending_queue
            while len(msgs) < batch:
                try:
                    msg = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._sub._pending_size -= len(msg.data)
                queue.task_done()
                if not status_of(msg):
                    append(msg)
            if len(msgs) == batch:
                return msgs

            no_wait = batch - len(msgs) > 1
            got_heartbeat = False
            retried_pin = False
            heartbeat_deadline = None
            disconnected = asyncio.get_running_loop().create_future()
            self._nc._disconnect_waiters.add(disconnected)

            def remaining() -> Optional[float]:
                return deadline - time.monotonic() if deadline is not None else None

            def timed_out() -> nats.errors.TimeoutError:
                return FetchTimeoutError() if got_heartbeat else nats.errors.TimeoutError()

            async def send_request() -> None:
                nonlocal heartbeat_deadline
                if disconnected.done() or self._nc.is_reconnecting:
                    raise nats.js.errors.FetchDisconnectedError
                ttl = remaining()
                if ttl is not None and ttl <= 0:
                    raise timed_out()
                expires = int((ttl - min(ttl * 0.1, 5)) * 1_000_000_000) if ttl is not None else None
                request_heartbeat = heartbeat
                if expires is not None and heartbeat and 2 * heartbeat * 1_000_000_000 >= expires:
                    request_heartbeat = None
                # Keep Python's prompt buffered-message probe: newer servers can
                # wait until expires even with no_wait. A probe without expires
                # completes immediately and cannot request server heartbeats.
                if no_wait and msgs:
                    expires = None
                    request_heartbeat = None
                req = self._build_next_req(
                    batch - len(msgs),
                    expires,
                    request_heartbeat,
                    no_wait,
                    min_pending,
                    min_ack_pending,
                    priority,
                )
                await self._nc.publish(self._nms, json.dumps(req).encode(), self._deliver)
                if heartbeat:
                    heartbeat_deadline = time.monotonic() + 2 * heartbeat

            # Sync subscriptions already have an unsubscribe notification.
            unsubscribed = self._sub._message_iterator._unsubscribed_future

            async def next_message() -> Msg:
                wait = remaining()
                if wait is not None and wait <= 0:
                    raise timed_out()
                if heartbeat_deadline is not None:
                    hb_wait = heartbeat_deadline - time.monotonic()
                    if hb_wait <= 0:
                        raise nats.js.errors.NoHeartbeatError
                    wait = min(wait, hb_wait) if wait is not None else hb_wait
                if self._nc.is_closed:
                    raise nats.errors.ConnectionClosedError
                if disconnected.done():
                    raise nats.js.errors.FetchDisconnectedError
                if unsubscribed.done():
                    raise nats.errors.BadSubscriptionError
                # Deliveries often arrive in bursts. Only allocate a receive task
                # when the queue is empty and we need to wait for an event.
                try:
                    msg = queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                else:
                    self._sub._pending_size -= len(msg.data)
                    queue.task_done()
                    return msg
                task = asyncio.create_task(self._sub.next_msg(timeout=wait))
                try:
                    await asyncio.wait((task, disconnected, unsubscribed), return_when=asyncio.FIRST_COMPLETED)
                    if self._nc.is_closed:
                        raise nats.errors.ConnectionClosedError
                    if disconnected.done():
                        raise nats.js.errors.FetchDisconnectedError
                    if unsubscribed.done():
                        raise nats.errors.BadSubscriptionError
                    try:
                        return task.result()
                    except nats.errors.TimeoutError:
                        if heartbeat_deadline is not None and time.monotonic() >= heartbeat_deadline:
                            raise nats.js.errors.NoHeartbeatError
                        raise timed_out()
                finally:
                    if not task.done():
                        task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

            try:
                await send_request()
                while len(msgs) < batch:
                    msg = await next_message()
                    if heartbeat:
                        heartbeat_deadline = time.monotonic() + 2 * heartbeat
                    status = status_of(msg)
                    if not status:
                        append(msg)
                        continue
                    if JetStreamContext._is_heartbeat(status):
                        got_heartbeat = True
                        continue
                    if JetStreamContext._is_pin_id_mismatch_error(status):
                        self._pin_id = None
                        if not retried_pin and not msgs:
                            retried_pin = True
                            await send_request()
                            continue
                    if status in (api.StatusCode.NO_MESSAGES, api.StatusCode.REQUEST_TIMEOUT):
                        if msgs:
                            break
                        if no_wait:
                            no_wait = False
                            await send_request()
                            continue
                        if status == api.StatusCode.REQUEST_TIMEOUT:
                            # A waiting request may expire before the local deadline.
                            # Do not leave a fresh request behind after fetch returns.
                            continue
                    if status == api.StatusCode.SERVICE_UNAVAILABLE:
                        raise nats.js.errors.ServiceUnavailableError
                    raise nats.js.errors.APIError(
                        code=int(status), description=(msg.headers or {}).get(api.Header.DESCRIPTION, "")
                    )
            except nats.errors.Error:
                # Like Go Fetch, a partial batch is successful on terminal errors.
                # Cancellation remains visible to the caller, even after delivery.
                if not msgs:
                    raise
            finally:
                self._nc._disconnect_waiters.discard(disconnected)
                disconnected.cancel()
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
            direct=bool(si.config.allow_direct),
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

        subject_delete_marker_ttl = None
        if config.limit_marker_ttl is not None and config.limit_marker_ttl > 0:
            info = await self.account_info()
            if not info.api.level or info.api.level < 1:
                raise nats.js.errors.KeyValueLimitMarkerTTLNotSupportedError()
            subject_delete_marker_ttl = config.limit_marker_ttl

        stream = api.StreamConfig(
            name=KV_STREAM_TEMPLATE.format(bucket=config.bucket),
            description=config.description,
            subjects=[f"$KV.{config.bucket}.>"],
            allow_direct=config.direct,
            allow_rollup_hdrs=True,
            allow_msg_ttl=True,
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
            subject_delete_marker_ttl=subject_delete_marker_ttl,
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
