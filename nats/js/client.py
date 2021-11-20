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
import json
import time
import nats.errors
import nats.js.errors
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js.manager import JetStreamManager
from nats.js.kv import KeyValueManager
from nats.js import api
from typing import Any, Dict, List, Optional, Callable
from dataclasses import asdict

LAST_CONSUMER_SEQ_HDR = "Nats-Last-Consumer"


class JetStream:
    """
    JetStream returns a context that can be used to produce and consume
    messages from NATS JetStream.

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
        conn,
        prefix=api.DefaultPrefix,
        domain=None,
        timeout=5,
    ):
        self._prefix = prefix
        if domain is not None:
            self._prefix = f"$JS.{domain}.API"
        self._nc = conn
        self._timeout = timeout
        self._jsm = JetStreamManager(
            conn, prefix=prefix, domain=domain, timeout=timeout
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
            if headers is None:
                hdr = {}
            hdr[nats.js.api.ExpectedStreamHdr] = stream

        try:
            msg = await self._nc.request(
                subject, payload, timeout=timeout, headers=hdr
            )
        except nats.errors.NoRespondersError:
            raise nats.js.errors.NoStreamResponseError

        resp = json.loads(msg.data)
        if 'error' in resp:
            raise nats.js.errors.APIError.from_error(resp['error'])

        return api.PubAck.loads(**resp)

    async def subscribe(
        self,
        subject: str,
        queue: Optional[str] = None,
        cb: Optional[Callable[['Msg'], None]] = None,
        durable: Optional[str] = None,
        stream: Optional[str] = None,
        config: Optional[api.ConsumerConfig] = None,
        manual_ack: Optional[bool] = False,
        ordered_consumer: Optional[bool] = False,
        idle_heartbeat: Optional[float] = None,
        flow_control: Optional[bool] = False,
    ):
        """
        subscribe returns a `Subscription` that is bound to a push based consumer.

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

        cinfo = None
        consumerFound = False
        shouldCreate = False

        # Ephemeral subscribe always has to be auto created.
        if not durable:
            shouldCreate = True
        else:
            try:
                # TODO: Detect configuration drift with any present durable consumer.
                cinfo = await self._jsm.consumer_info(stream, durable)
                config = cinfo.config
                consumerFound = True
                consumer = durable
            except nats.js.errors.NotFoundError:
                shouldCreate = True
                consumerFound = False

        if consumerFound:
            # At this point, we know the user wants push mode, and the JS consumer is
            # really push mode.
            if not config.deliver_group:
                # Prevent an user from attempting to create a queue subscription on
                # a JS consumer that was not created with a deliver group.
                if queue:
                    # TODO: Currently, this would not happen in client
                    # since the queue name is used as durable name.
                    raise nats.js.errors.Error(
                        "cannot create a queue subscription for a consumer without a deliver group"
                    )
                elif cinfo.push_bound:
                    # Need to reject a non queue subscription to a non queue consumer
                    # if the consumer is already bound.
                    raise nats.js.errors.Error(
                        "consumer is already bound to a subscription"
                    )
            else:
                if not queue:
                    raise nats.js.errors.Error(
                        f"cannot create a subscription for a consumer with a deliver group {config.deliver_group}"
                    )
                elif queue != config.deliver_group:
                    raise nats.js.errors.Error(
                        f"cannot create a queue subscription {queue} for a consumer with a deliver group {config.deliver_group}"
                    )
        elif shouldCreate:
            # Auto-create consumer if none found.
            if config is None:
                # Defaults
                config = api.ConsumerConfig(
                    ack_policy=api.AckPolicy.explicit,
                )
            elif isinstance(config, dict):
                config = api.ConsumerConfig.loads(**config)
            elif not isinstance(config, api.ConsumerConfig):
                raise ValueError("nats: invalid ConsumerConfig")

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
            if idle_heartbeat or config.idle_heartbeat:
                if config.idle_heartbeat:
                    idle_heartbeat = config.idle_heartbeat
                idle_heartbeat = int(idle_heartbeat * 1_000_000_000)
                config.idle_heartbeat = idle_heartbeat

            # Enable ordered consumer mode where at most there is
            # one message being delivered at a time.
            if ordered_consumer:
                if not idle_heartbeat:
                    idle_heartbeat = 5 * 1_000_000_000
                config.flow_control = True
                config.ack_policy = api.AckPolicy.explicit
                config.max_deliver = 1
                config.ack_wait = 22 * 3600 * 1_000_000_000  # 22 hours
                config.idle_heartbeat = idle_heartbeat

            cinfo = await self._jsm.add_consumer(stream, config=config)
            consumer = cinfo.name

        # By default, async subscribers wrap the original callback and
        # auto ack the messages as they are delivered.
        if cb and not manual_ack:
            ocb = cb

            async def new_cb(msg):
                await ocb(msg)
                await msg.ack()

            cb = new_cb

        sub = await self._nc.subscribe(
            config.deliver_subject, config.deliver_group, cb=cb
        )
        psub = JetStream.PushSubscription(self, sub, stream, consumer)

        # Keep state to support ordered consumers.
        jsi = JetStream._JSI()
        jsi._js = self
        jsi._conn = self._nc
        jsi._stream = stream
        jsi._ordered = ordered_consumer
        jsi._psub = psub
        jsi._sub = sub
        jsi._ccreq = config
        sub._jsi = jsi
        return psub

    async def pull_subscribe(
        self,
        subject: str,
        durable: str,
        stream: str = None,
        config: api.ConsumerConfig = None,
    ):
        """
        pull_subscribe returns a `PullSubscription` that can be delivered messages
        from a JetStream pull based consumer by calling `sub.fetch`.

        In case 'stream' is passed, there will not be a lookup of the stream
        based on the subject.

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
                # Defaults
                config = api.ConsumerConfig(
                    ack_policy=api.AckPolicy.explicit,
                )
            elif isinstance(config, dict):
                config = api.ConsumerConfig.loads(**config)
            elif not isinstance(config, api.ConsumerConfig):
                raise ValueError("nats: invalid ConsumerConfig")

            config.durable_name = durable
            await self._jsm.add_consumer(stream, config=config)

        # FIXME: Make this inbox prefix customizable.
        deliver = api.InboxPrefix[:]
        deliver.extend(self._nc._nuid.next())

        consumer = durable
        sub = await self._nc.subscribe(deliver.decode())
        return JetStream.PullSubscription(self, sub, stream, consumer, deliver)

    @classmethod
    def is_status_msg(cls, msg):
        if msg is not None and \
           msg.header is not None and \
           api.StatusHdr in msg.header:
            return True
        else:
            return False

    class _JSI():
        def __init__(self):
            self._stream = None
            self._ordered = None
            self._conn = None
            self._psub = None
            self._sub = None
            self._dseq = 1
            self._sseq = 0
            self._ccreq = None
            self._cmeta = None
            self._fciseq = 0
            self._active = None

            # flow control response
            self._fcr = None
            # flow control sequence
            self._fcd = None

            # background task that resets an ordered consumer
            self._reset_task = None

        def track_sequences(self, reply):
            self._fciseq += 1
            self._cmeta = reply

        def schedule_flow_control_response(self, reply):
            self._fcr = reply
            self._fcd = self._fciseq

        async def check_for_sequence_mismatch(self, msg):
            self._active = True
            if not self._cmeta:
                return

            tokens = msg._get_metadata_fields(self._cmeta)
            dseq = int(tokens[6])  # consumer sequence
            ldseq = int(msg.header.get(LAST_CONSUMER_SEQ_HDR))
            did_reset = None

            if ldseq != dseq:
                sseq = int(tokens[5])  # stream sequence

                if self._ordered:
                    did_reset = await self.reset_ordered_consumer(
                        self._sseq + 1
                    )
                else:
                    ecs = nats.js.errors.ConsumerSequenceMismatchError(
                        sseq,
                        dseq,
                        ldseq,
                    )
                    if self._conn._error_cb:
                        await self._conn._error_cb(ecs)
            return did_reset

        async def reset_ordered_consumer(self, sseq):
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
            self._fcr = None
            self._fcd = None

            # Reset consumer request for starting policy.
            config = self._ccreq
            config.deliver_subject = new_deliver
            config.deliver_policy = nats.js.api.DeliverPolicy.by_start_sequence
            config.opt_start_seq = sseq
            self._ccreq = config

            # Handle the creation of new consumer in a background task
            # to avoid blocking the process_msg coroutine further
            # when making the request.
            self._reset_task = asyncio.create_task(self.recreate_consumer())

            return True

        async def recreate_consumer(self):
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
        def __init__(self, js, sub, stream, consumer):
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

        async def consumer_info(self):
            """
            consumer_info gets the current info of the consumer from this subscription.
            """
            info = await self._js._jsm.consumer_info(
                self._stream, self._consumer
            )
            return info

    class PullSubscription:
        """
        PullSubscription is a subscription that can fetch messages.
        """
        def __init__(self, js, sub, stream, consumer, deliver):
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

        async def unsubscribe():
            """
            unsubscribe destroys de inboxes of the pull subscription making it
            unable to continue to receive messages.
            """
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            await self._sub.unsubscribe()
            self._sub = None

        async def fetch(self, batch: int = 1, timeout: int = 5):
            if self._sub is None:
                raise ValueError("nats: invalid subscription")

            # FIXME: Check connection is not closed, etc...

            if batch < 1:
                raise ValueError("nats: invalid batch size")
            if timeout <= 0:
                raise ValueError("nats: invalid fetch timeout")

            msgs = []
            expires = (timeout * 1_000_000_000) - 100_000
            if batch == 1:
                msg = await self._fetch_one(batch, expires, timeout)
                msgs.append(msg)
            else:
                msgs = await self._fetch_n(batch, expires, timeout)
            return msgs

        async def _fetch_one(self, batch, expires, timeout):
            queue = self._sub._pending_queue

            # Check the next message in case there are any.
            if not queue.empty():
                try:
                    msg = queue.get_nowait()
                    return msg
                except:
                    # Fallthrough to make request in case this fails.
                    pass

            # Make lingering request with expiration.
            next_req = {}
            next_req['batch'] = 1
            next_req['expires'] = expires

            # Make publish request and wait for response.
            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
            )

            # Wait for the response.
            msg = None
            try:
                fut = queue.get()
                msg = await asyncio.wait_for(fut, timeout=timeout)
            except asyncio.TimeoutError:
                raise nats.errors.TimeoutError

            # Should have received at least a message at this point,
            # if that is not the case then error already.
            if JetStream.is_status_msg(msg):
                if api.StatusHdr in msg.headers:
                    raise nats.js.errors.APIError.from_msg(msg)

            return msg

        async def _fetch_n(self, batch, expires, timeout):
            # TODO: Implement fetching more than one.
            raise NotImplementedError

    class _JS():
        def __init__(
            self,
            conn=None,
            prefix=None,
            stream=None,
            consumer=None,
            nms=None,
        ):
            self._prefix = prefix
            self._nc = conn
            self._stream = stream
            self._consumer = consumer
            self._nms = nms


class JetStreamContext(JetStream, JetStreamManager, KeyValueManager):
    """
    JetStreamContext includes the fully featured context for interacting
    with JetStream.
    """
    def __init__(self, conn, **opts):
        super().__init__(conn, **opts)
