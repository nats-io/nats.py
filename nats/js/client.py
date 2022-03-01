# Copyright 2021 The NATS Authors
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
import json
import time
from typing import TYPE_CHECKING, Awaitable, Callable, List, Optional

import nats.errors
import nats.js.errors
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import api
from nats.js.errors import BadBucketError, BucketNotFoundError, NotFoundError
from nats.js.kv import KeyValue
from nats.js.manager import JetStreamManager

if TYPE_CHECKING:
    from nats import NATS

NATS_HDR_LINE = bytearray(b'NATS/1.0\r\n')
NATS_HDR_LINE_SIZE = len(NATS_HDR_LINE)
KV_STREAM_TEMPLATE = "KV_{bucket}"
KV_PRE_TEMPLATE = "$KV.{bucket}."
Callback = Callable[['Msg'], Awaitable[None]]


class JetStreamContext(JetStreamManager):
    """
    Fully featured context for interacting with JetStream.

    :param conn: NATS Connection
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

    def __init__(
        self,
        conn: "NATS",
        prefix: str = api.DEFAULT_PREFIX,
        domain: Optional[str] = None,
        timeout: float = 5,
    ) -> None:
        self._prefix = prefix
        if domain is not None:
            self._prefix = f"$JS.{domain}.API"
        self._nc = conn
        self._timeout = timeout

    @property
    def _jsm(self) -> JetStreamManager:
        return JetStreamManager(
            conn=self._nc,
            prefix=self._prefix,
            timeout=self._timeout,
        )

    async def publish(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: float = None,
        stream: str = None,
        headers: dict = None
    ) -> api.PubAck:
        """
        publish emits a new message to JetStream.
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
    ) -> Subscription:
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
                config.ack_policy = api.AckPolicy.EXPLICIT
                config.max_deliver = 1
                config.ack_wait = 22 * 3600  # 22 hours
                config.idle_heartbeat = idle_heartbeat

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
        )

    async def subscribe_bind(
        self,
        stream: str,
        config: api.ConsumerConfig,
        consumer: str,
        cb: Optional[Callback] = None,
        manual_ack: bool = False,
        ordered_consumer: bool = False,
    ) -> Subscription:
        """Push-subscribe to an existing consumer.
        """
        # By default, async subscribers wrap the original callback and
        # auto ack the messages as they are delivered.
        if cb and not manual_ack:
            cb = self._auto_ack_callback(cb)
        if config.deliver_subject is None:
            raise TypeError("config.deliver_subject is required")
        sub = await self._nc.subscribe(
            subject=config.deliver_subject,
            queue=config.deliver_group or "",
            cb=cb,
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
        durable: str,
        stream: Optional[str] = None,
        config: Optional[api.ConsumerConfig] = None,
    ) -> "JetStreamContext.PullSubscription":
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

                msgs = await sub.fetch()
                msg = msgs[0]
                await msg.ack()

                await nc.close()

            if __name__ == '__main__':
                asyncio.run(main())

        """
        if stream is None:
            stream = await self._jsm.find_stream_name_by_subject(subject)

        try:
            # TODO: Detect configuration drift with the consumer.
            await self._jsm.consumer_info(stream, durable)
        except nats.js.errors.NotFoundError:
            # If not found then attempt to create with the defaults.
            if config is None:
                config = api.ConsumerConfig()
            # Auto created consumers use the filter subject.
            config.filter_subject = subject
            config.durable_name = durable
            await self._jsm.add_consumer(stream, config=config)

        return await self.pull_subscribe_bind(durable=durable, stream=stream)

    async def pull_subscribe_bind(
        self,
        durable: str,
        stream: str,
        inbox_prefix: bytes = api.INBOX_PREFIX,
    ) -> "JetStreamContext.PullSubscription":
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
        deliver = inbox_prefix + self._nc._nuid.next()
        sub = await self._nc.subscribe(deliver.decode())
        return JetStreamContext.PullSubscription(
            js=self,
            sub=sub,
            stream=stream,
            consumer=durable,
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
        # FIXME: Skip any other 4XX errors?
        if status == api.StatusCode.NO_MESSAGES:
            return False
        if status == api.StatusCode.REQUEST_TIMEOUT:
            raise nats.errors.TimeoutError
        raise nats.js.errors.APIError.from_msg(msg)

    @classmethod
    def _time_until(cls, timeout: Optional[float],
                    start_time: float) -> Optional[float]:
        if timeout is None:
            return None
        return timeout - (time.monotonic() - start_time)

    class _JSI():

        def __init__(
            self,
            js: "JetStreamContext",
            conn: "NATS",
            stream: str,
            ordered: Optional[bool],
            psub: "JetStreamContext.PushSubscription",
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

            self._dseq = 1
            self._sseq = 0
            self._cmeta: Optional[str] = None
            self._fciseq = 0
            self._active: Optional[bool] = None

        def track_sequences(self, reply: str) -> None:
            self._fciseq += 1
            self._cmeta = reply

        def schedule_flow_control_response(self, reply: str) -> None:
            pass

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
                    if self._conn._error_cb:
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
            js: "JetStreamContext",
            sub: Subscription,
            stream: str,
            consumer: str,
        ) -> None:
            self._js = js
            self._stream = stream
            self._consumer = consumer

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

        async def consumer_info(self) -> api.ConsumerInfo:
            """
            consumer_info gets the current info of the consumer from this subscription.
            """
            info = await self._js._jsm.consumer_info(
                self._stream,
                self._consumer,
            )
            return info

    class PullSubscription:
        """
        PullSubscription is a subscription that can fetch messages.
        """

        def __init__(
            self,
            js: "JetStreamContext",
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

        async def unsubscribe(self) -> None:
            """
            unsubscribe destroys de inboxes of the pull subscription making it
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

        async def fetch(self,
                        batch: int = 1,
                        timeout: Optional[float] = 5) -> List[Msg]:
            """
            fetch makes a request to JetStream to be delivered a set of messages.

            :param batch: Number of messages to fetch from server.
            :param timeout: Max duration of the fetch request before it expires.

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
                msg = await self._fetch_one(expires, timeout)
                return [msg]
            msgs = await self._fetch_n(batch, expires, timeout)
            return msgs

        async def _fetch_one(
            self,
            expires: Optional[int],
            timeout: Optional[float],
        ) -> Msg:
            queue = self._sub._pending_queue

            # Check the next message in case there are any.
            while not queue.empty():
                try:
                    msg = queue.get_nowait()
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
            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
            )

            # Wait for the response or raise timeout.
            msg = await self._sub.next_msg(timeout)

            # Should have received at least a processable message at this point,
            # any other type of status message is an error.
            status = JetStreamContext.is_status_msg(msg)
            if JetStreamContext._is_processable_msg(status, msg):
                return msg
            raise nats.js.errors.APIError.from_msg(msg)

        async def _fetch_n(
            self,
            batch: int,
            expires: Optional[int],
            timeout: Optional[float],
        ) -> List[Msg]:
            msgs = []
            queue = self._sub._pending_queue
            start_time = time.monotonic()
            needed = batch

            # Fetch as many as needed from the internal pending queue.
            msg: Optional[Msg]
            while not queue.empty():
                try:
                    msg = queue.get_nowait()
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
            next_req['no_wait'] = True
            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
            )
            await asyncio.sleep(0)

            # Wait for first message or timeout.
            msg = await self._sub.next_msg(timeout)
            status = JetStreamContext.is_status_msg(msg)
            if JetStreamContext._is_processable_msg(status, msg):
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
                        elif JetStreamContext._is_processable_msg(status, msg):
                            needed -= 1
                            msgs.append(msg)
                except asyncio.TimeoutError:
                    # Ignore any timeout errors at this point since
                    # at least one message has already arrived.
                    pass

            # Stop if enough messages already.
            if needed == 0:
                return msgs

            # Second request: lingering request that will block until new messages
            # are made available and delivered to the client.
            next_req = {}
            next_req['batch'] = needed
            if expires:
                next_req['expires'] = expires
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
                    msg = await self._sub.next_msg(timeout=deadline)
                else:
                    try:
                        msg = await self._sub.next_msg(timeout=deadline)
                    except asyncio.TimeoutError:
                        # Ignore any timeout since already got at least a message.
                        break

                if msg:
                    status = JetStreamContext.is_status_msg(msg)
                    if not status:
                        needed -= 1
                        msgs.append(msg)
                        break
                    elif status == api.StatusCode.NO_MESSAGES:
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
                    if JetStreamContext._is_processable_msg(status, msg):
                        needed -= 1
                        msgs.append(msg)
            except asyncio.TimeoutError:
                # Ignore any timeout errors at this point since
                # at least one message has already arrived.
                pass

            return msgs

    async def get_last_msg(
        self,
        stream_name: str,
        subject: str,
    ) -> api.RawStreamMsg:
        """
        get_last_msg retrieves a message from a stream.
        """
        req_subject = f"{self._prefix}.STREAM.MSG.GET.{stream_name}"
        req = {'last_by_subj': subject}
        data = json.dumps(req)
        resp = await self._api_request(req_subject, data.encode())
        raw_msg = api.RawStreamMsg.from_response(resp['message'])
        if raw_msg.hdrs:
            hdrs = base64.b64decode(raw_msg.hdrs)
            raw_headers = hdrs[len(NATS_HDR_LINE):]
            parsed_headers = self._jsm._hdr_parser.parsebytes(raw_headers)
            headers = {}
            for k, v in parsed_headers.items():
                headers[k] = v
            raw_msg.headers = headers
        return raw_msg

    async def key_value(self, bucket: str) -> KeyValue:
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

        stream = api.StreamConfig(
            name=KV_STREAM_TEMPLATE.format(bucket=config.bucket),
            description=None,
            subjects=[f"$KV.{config.bucket}.>"],
            max_msgs_per_subject=config.history,
            max_bytes=config.max_bytes,
            max_age=config.ttl,
            max_msg_size=config.max_value_size,
            storage=config.storage,
            num_replicas=config.replicas,
            allow_rollup_hdrs=True,
            deny_delete=True,
        )
        await self.add_stream(stream)

        assert stream.name is not None
        return KeyValue(
            name=config.bucket,
            stream=stream.name,
            pre=KV_PRE_TEMPLATE.format(bucket=config.bucket),
            js=self,
        )

    async def delete_key_value(self, bucket: str) -> bool:
        """
        delete_key_value deletes a JetStream KeyValue store by destroying
        the associated stream.
        """
        stream = KV_STREAM_TEMPLATE.format(bucket=bucket)
        return await self.delete_stream(stream)
