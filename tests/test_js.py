import asyncio
import datetime
import random
import time
import unittest
import uuid

import pytest
import nats
import nats.js.api
from nats.aio.client import Client as NATS, __version__
from nats.aio.errors import *
from nats.errors import *
from nats.js.errors import *
from tests.utils import *


class PublishTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_publish(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()

        with pytest.raises(NoStreamResponseError):
            await js.publish("foo", b'bar')

        await js.add_stream(name="QUUX", subjects=["quux"])

        ack = await js.publish("quux", b'bar:1', stream="QUUX")
        assert ack.stream == "QUUX"
        assert ack.seq == 1

        ack = await js.publish("quux", b'bar:2')
        assert ack.stream == "QUUX"
        assert ack.seq == 2

        with pytest.raises(BadRequestError) as err:
            await js.publish("quux", b'bar', stream="BAR")
        assert err.value.err_code == 10060

        await nc.close()

    @async_test
    async def test_publish_verbose(self):
        nc = NATS()
        await nc.connect(verbose=False)
        js = nc.jetstream()

        with pytest.raises(NoStreamResponseError):
            await js.publish("foo", b'bar')

        await js.add_stream(name="QUUX", subjects=["quux"])

        ack = await js.publish("quux", b'bar:1', stream="QUUX")
        assert ack.stream == "QUUX"
        assert ack.seq == 1

        ack = await js.publish("quux", b'bar:2')
        assert ack.stream == "QUUX"
        assert ack.seq == 2

        with pytest.raises(BadRequestError) as err:
            await js.publish("quux", b'bar', stream="BAR")
        assert err.value.err_code == 10060

        await nc.close()


class PullSubscribeTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_auto_create_consumer(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()
        await js.add_stream(
            name="TEST2", subjects=["a1", "a2", "a3", "a4"]
        )

        for i in range(1, 10):
            await js.publish("a1", f'a1:{i}'.encode())

        # Should use a2 as the filter subject and receive a subset.
        sub = await js.pull_subscribe("a2", "auto")
        await js.publish("a2", b'one')

        for i in range(10, 20):
            await js.publish("a3", f'a3:{i}'.encode())

        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        assert msg.data == b'one'

        # Getting another message should timeout for the a2 subject.
        with pytest.raises(TimeoutError):
            await sub.fetch(1, timeout=1)

        # Customize consumer config.
        sub = await js.pull_subscribe(
            "a2", "auto2", config=nats.js.api.ConsumerConfig(max_waiting=10)
        )
        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        assert msg.data == b'one'

        info = await js.consumer_info("TEST2", "auto2")
        assert info.config.max_waiting == 10

        sub = await js.pull_subscribe("a3", "auto3")
        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        assert msg.data == b'a3:10'

        # Getting all messages from stream.
        sub = await js.pull_subscribe("", "all")
        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        assert msg.data == b'a1:1'

        for _ in range(2, 10):
            msgs = await sub.fetch(1)
            msg = msgs[0]
            await msg.ack()

        # subject a2
        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        assert msg.data == b'one'

        # subject a3
        msgs = await sub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        assert msg.data == b'a3:10'

        await nc.close()
        await asyncio.sleep(1)

    @async_test
    async def test_fetch_one(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="TEST1", subjects=["foo.1", "bar"])

        ack = await js.publish("foo.1", f'Hello from NATS!'.encode())
        assert ack.stream == "TEST1"
        assert ack.seq == 1

        # Bind to the consumer that is already present.
        sub = await js.pull_subscribe("foo.1", "dur")
        msgs = await sub.fetch(1)
        for msg in msgs:
            await msg.ack()

        msg = msgs[0]
        assert msg.metadata.sequence.stream == 1
        assert msg.metadata.sequence.consumer == 1
        assert datetime.datetime.now() > msg.metadata.timestamp
        assert msg.metadata.num_pending == 0
        assert msg.metadata.num_delivered == 1

        with pytest.raises(asyncio.TimeoutError):
            await sub.fetch(timeout=1)

        for i in range(0, 10):
            await js.publish(
                "foo.1", f"i:{i}".encode(), headers={'hello': 'world'}
            )

        # nak
        msgs = await sub.fetch()
        msg = msgs[0]

        info = await js.consumer_info("TEST1", "dur", timeout=1)
        self.assertEqual(msg.header, {'hello': 'world'})

        await msg.nak()

        info = await js.consumer_info("TEST1", "dur", timeout=1)
        self.assertEqual(info.stream_name, "TEST1")
        assert info.num_ack_pending == 1
        assert info.num_redelivered == 0

        # in_progress
        msgs = await sub.fetch()
        for msg in msgs:
            await msg.in_progress()

        # term
        msgs = await sub.fetch()
        for msg in msgs:
            await msg.term()

        info = await js.consumer_info("TEST1", "dur", timeout=1)
        assert info.num_ack_pending == 1
        assert info.num_redelivered == 1

        await nc.close()

    @async_test
    async def test_fetch_one_wait_forever(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="TEST111", subjects=["foo.111"])

        ack = await js.publish("foo.111", f'Hello from NATS!'.encode())
        assert ack.stream == "TEST111"
        assert ack.seq == 1

        # Bind to the consumer that is already present.
        sub = await js.pull_subscribe("foo.111", "dur")
        msgs = await sub.fetch(1, None)
        for msg in msgs:
            await msg.ack()

        msg = msgs[0]
        assert msg.metadata.sequence.stream == 1
        assert msg.metadata.sequence.consumer == 1
        assert datetime.datetime.now() > msg.metadata.timestamp
        assert msg.metadata.num_pending == 0
        assert msg.metadata.num_delivered == 1

        received = False

        async def f():
            nonlocal received
            await sub.fetch(1, None)
            received = True

        task = asyncio.create_task(f())
        self.assertFalse(received)
        await asyncio.sleep(1)
        self.assertFalse(received)
        await asyncio.sleep(1)
        await js.publish("foo.111", 'Hello from NATS!'.encode())
        await task
        assert received

    @async_test
    async def test_add_pull_consumer_via_jsm(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="events", subjects=["events.a"])
        await js.add_consumer(
            "events",
            durable_name="a",
            deliver_policy=nats.js.api.DeliverPolicy.ALL,
            max_deliver=20,
            max_waiting=512,
            # ack_wait=30,
            max_ack_pending=1024,
            filter_subject="events.a"
        )
        await js.publish("events.a", b'hello world')
        sub = await js.pull_subscribe_bind("a", stream="events")
        msgs = await sub.fetch(1)
        for msg in msgs:
            await msg.ack()
        info = await js.consumer_info("events", "a")
        self.assertEqual(0, info.num_pending)

    @async_long_test
    async def test_fetch_n(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()

        await js.add_stream(name="TESTN", subjects=["a", "b", "c"])

        for i in range(0, 10):
            await js.publish("a", f'i:{i}'.encode())

        sub = await js.pull_subscribe(
            "a",
            "durable-1",
            config=nats.js.api.ConsumerConfig(max_waiting=3),
        )
        info = await sub.consumer_info()
        assert info.config.max_waiting == 3

        # 10 messages
        # -5 fetched
        # -----------
        #  5 pending
        msgs = await sub.fetch(5)
        assert len(msgs) == 5

        i = 0
        for msg in msgs:
            assert msg.data == f'i:{i}'.encode()
            await msg.ack()
            i += 1
        info = await sub.consumer_info()
        assert info.num_pending == 5

        # 5 messages
        # -10 fetched
        # -----------
        #  5 pending
        msgs = await sub.fetch(10, timeout=0.5)
        assert len(msgs) == 5

        i = 5
        for msg in msgs:
            assert msg.data == f'i:{i}'.encode()
            await msg.ack()
            i += 1

        info = await sub.consumer_info()
        assert info.num_ack_pending == 0
        assert info.num_redelivered == 0
        assert info.delivered.stream_seq == 10
        assert info.delivered.consumer_seq == 10
        assert info.ack_floor.stream_seq == 10
        self.assertEqual(info.ack_floor.consumer_seq, 10)
        assert info.num_pending == 0

        # 1 message
        # -1 fetched
        # ----------
        # 0 pending
        # 1 ack pending
        await js.publish("a", b'i:11')
        msgs = await sub.fetch(2, timeout=0.5)

        # Leave this message unacked.
        msg = msgs[0]
        unacked_msg = msg

        assert msg.data == b'i:11'
        info = await sub.consumer_info()
        assert info.num_waiting < 2
        assert info.num_pending == 0
        assert info.num_ack_pending == 1

        inflight = []
        inflight.append(msg)

        # +1 message
        #  1 extra from before but request has expired so does not count.
        # +1 ack pending since previous message not acked.
        # +1 pending to be consumed.
        await js.publish("a", b'i:12')

        # Inspect the internal buffer which should be a 408 at this point.
        try:
            msg = await sub._sub.next_msg(timeout=0.5)
            self.assertEqual(msg.headers['Status'], '408')
        except (nats.errors.TimeoutError, TypeError):
            pass

        info = await sub.consumer_info()
        assert info.num_waiting == 0
        assert info.num_pending == 1
        assert info.num_ack_pending == 1

        # Start background task that gathers messages.
        fut = asyncio.create_task(sub.fetch(3, timeout=2))
        await asyncio.sleep(0.5)
        await js.publish("a", b'i:13')
        await js.publish("a", b'i:14')

        # It should have enough time to be able to get the 3 messages,
        # the no wait message will send the first message plus a 404
        # no more messages error.
        msgs = await fut
        assert len(msgs) == 3
        for msg in msgs:
            await msg.ack_sync()

        info = await sub.consumer_info()
        assert info.num_ack_pending == 1
        assert info.num_redelivered == 0
        assert info.num_waiting == 0
        assert info.num_pending == 0
        assert info.delivered.stream_seq == 14
        assert info.delivered.consumer_seq == 14

        # Message 10 is the last message that got acked.
        assert info.ack_floor.stream_seq == 10
        self.assertEqual(info.ack_floor.consumer_seq, 10)

        # Unacked last message so that ack floor is updated.
        await unacked_msg.ack_sync()

        info = await sub.consumer_info()
        assert info.num_pending == 0
        assert info.ack_floor.stream_seq == 14
        self.assertEqual(info.ack_floor.consumer_seq, 14)

        # No messages at this point.
        for _ in range(0, 5):
            with pytest.raises(TimeoutError):
                msg = await sub.fetch(1, timeout=0.5)

        # Max waiting is 3 so it should be stuck at 2 but consumer_info resets this.
        info = await sub.consumer_info()
        assert info.num_waiting <= 1

        # Following requests ought to cancel the previous ones.
        #
        # for i in range(0, 5):
        #     with pytest.raises(TimeoutError):
        #         msg = await sub.fetch(2, timeout=0.5)
        # info = await sub.consumer_info()
        # assert info.num_waiting== 1

        await nc.close()

    @async_test
    async def test_fetch_max_waiting_fetch_one(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="TEST3", subjects=["max"])

        sub = await js.pull_subscribe(
            "max",
            "example",
            config=nats.js.api.ConsumerConfig(max_waiting=3),
        )
        results = await asyncio.gather(
            sub.fetch(1, timeout=1),
            sub.fetch(1, timeout=1),
            sub.fetch(1, timeout=1),
            sub.fetch(1, timeout=1),
            sub.fetch(1, timeout=1),
            return_exceptions=True,
        )

        for e in results:
            if isinstance(e, asyncio.TimeoutError):
                continue
            else:
                assert isinstance(e, APIError)
                break

        info = await js.consumer_info("TEST3", "example")
        assert info.num_waiting == 3

        for i in range(0, 10):
            await js.publish("max", b'foo')

        async def pub():
            while True:
                await js.publish("max", b'foo')
                await asyncio.sleep(0)

        producer = asyncio.create_task(pub())

        async def cb():
            future = await asyncio.gather(
                sub.fetch(1, timeout=1),
                sub.fetch(512, timeout=1),
                sub.fetch(512, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                sub.fetch(1, timeout=1),
                return_exceptions=True,
            )
            for e in future:
                if isinstance(e, (asyncio.TimeoutError, APIError)):
                    continue

        tasks = []
        for _ in range(0, 100):
            task = asyncio.create_task(cb())
            tasks.append(task)
            await asyncio.sleep(0)

        for task in tasks:
            await task

        producer.cancel()

        await nc.close()

    @async_test
    async def test_fetch_max_waiting_fetch_n(self):
        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="TEST31", subjects=["max"])

        sub = await js.pull_subscribe(
            "max",
            "example",
            config=nats.js.api.ConsumerConfig(max_waiting=3),
        )
        results = await asyncio.gather(
            sub.fetch(2, timeout=1),
            sub.fetch(2, timeout=1),
            sub.fetch(2, timeout=1),
            sub.fetch(2, timeout=1),
            sub.fetch(2, timeout=1),
            return_exceptions=True,
        )

        for e in results:
            if isinstance(e, asyncio.TimeoutError):
                continue
            else:
                assert isinstance(e, APIError)
                break

        # Should get at least one Request Timeout error.
        info = await js.consumer_info("TEST31", "example")
        assert info.num_waiting == 3
        await nc.close()

    @async_long_test
    async def test_fetch_concurrent(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.add_stream(name="TESTN10", subjects=["a", "b", "c"])

        async def go_publish():
            i = 0
            while True:
                payload = f'{i}'.encode()
                await js.publish("a", payload)
                i += 1
                await asyncio.sleep(0.01)

        task = asyncio.create_task(go_publish())

        sub = await js.pull_subscribe(
            "a",
            "durable-1",
            config=nats.js.api.ConsumerConfig(max_waiting=3),
        )
        info = await sub.consumer_info()
        assert info.config.max_waiting == 3

        start_time = time.monotonic()
        errors = []
        m = {}
        while True:
            a = time.monotonic() - start_time
            if a > 2:  # seconds
                break
            try:
                results = await asyncio.gather(
                    sub.fetch(2, timeout=1),
                    sub.fetch(2, timeout=1),
                    sub.fetch(2, timeout=1),
                    sub.fetch(2, timeout=1),
                    sub.fetch(2, timeout=1),
                    sub.fetch(2, timeout=1),
                    # return_exceptions=True,
                )
                for batch in results:
                    for msg in batch:
                        m[int(msg.data.decode())] = msg
                        assert msg.header is None
            except Exception as e:
                errors.append(e)
        for e in errors:
            if isinstance(e, asyncio.TimeoutError):
                continue
            else:
                # Only 408 errors should ever bubble up.
                assert isinstance(e, APIError)
                assert e.code == 408
        task.cancel()

        await nc.close()


class JSMTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_stream_management(self):
        nc = NATS()
        await nc.connect()
        jsm = nc.jsm()

        acc = await jsm.account_info()
        assert isinstance(acc, nats.js.api.AccountInfo)

        # Create stream
        stream = await jsm.add_stream(
            name="hello", subjects=["hello", "world", "hello.>"]
        )
        assert isinstance(stream, nats.js.api.StreamInfo)
        assert isinstance(stream.config, nats.js.api.StreamConfig)
        assert stream.config.name == "hello"
        assert isinstance(stream.state, nats.js.api.StreamState)

        # Create without name
        with pytest.raises(ValueError):
            await jsm.add_stream(subjects=["hello", "world", "hello.>"])
        # Create with config, but without name
        with pytest.raises(ValueError):
            await jsm.add_stream(nats.js.api.StreamConfig())
        # Create with config, name is provided as kwargs
        stream_with_name = await jsm.add_stream(
            nats.js.api.StreamConfig(), name="hi"
        )
        assert stream_with_name.config.name == "hi"

        # Get info
        current = await jsm.stream_info("hello")
        stream.did_create = None
        assert stream == current

        assert isinstance(current, nats.js.api.StreamInfo)
        assert isinstance(current.config, nats.js.api.StreamConfig)
        assert current.config.name == "hello"
        assert isinstance(current.state, nats.js.api.StreamState)

        # Send messages
        producer = nc.jetstream()
        ack = await producer.publish('world', b'Hello world!')
        assert ack.stream == "hello"
        assert ack.seq == 1

        current = await jsm.stream_info("hello")
        assert current.state.messages == 1
        assert current.state.bytes == 47

        # Delete stream
        is_deleted = await jsm.delete_stream("hello")
        assert is_deleted

        # Not foundError since there is none
        with pytest.raises(NotFoundError):
            await jsm.stream_info("hello")

        await nc.close()

    @async_test
    async def test_consumer_management(self):
        nc = NATS()
        await nc.connect()
        jsm = nc.jsm()

        acc = await jsm.account_info()
        assert isinstance(acc, nats.js.api.AccountInfo)

        # Create stream.
        await jsm.add_stream(name="ctests", subjects=["a", "b", "c.>"])

        # Create durable consumer.
        cinfo = await jsm.add_consumer(
            "ctests",
            durable_name="dur",
            ack_policy="explicit",
        )

        # Fail with missing stream.
        with pytest.raises(NotFoundError) as err:
            await jsm.consumer_info("missing", "c")
        assert err.value.err_code == 10059

        # Get consumer, there should be no changes.
        current = await jsm.consumer_info("ctests", "dur")
        assert cinfo == current

        # Delete consumer.
        ok = await jsm.delete_consumer("ctests", "dur")
        assert ok

        # Consumer lookup should not be 404 now.
        with pytest.raises(NotFoundError) as err:
            await jsm.consumer_info("ctests", "dur")
        assert err.value.err_code == 10014

        # Create ephemeral consumer.
        cinfo = await jsm.add_consumer(
            "ctests",
            ack_policy="explicit",
            deliver_subject="asdf",
        )
        # Should not be empty.
        assert len(cinfo.name) > 0
        ok = await jsm.delete_consumer("ctests", cinfo.name)
        assert ok

        # Ephemeral no longer found after delete.
        with pytest.raises(NotFoundError):
            await jsm.delete_consumer("ctests", cinfo.name)

        await nc.close()


class SubscribeTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_queue_subscribe_deliver_group(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.add_stream(name="qsub", subjects=["quux"])

        a, b, c = ([], [], [])

        async def cb1(msg):
            a.append(msg)

        async def cb2(msg):
            b.append(msg)

        async def cb3(msg):
            c.append(msg)

        subs = []

        # First queue subscriber will create.
        sub1 = await js.subscribe("quux", "wg", cb=cb1)

        # Rest of queue subscribers will bind to the same.
        sub2 = await js.subscribe("quux", "wg", cb=cb2)
        sub3 = await js.subscribe("quux", "wg", cb=cb3)

        # All should be bound to the same subject.
        assert sub1.subject == sub2.subject
        assert sub1.subject == sub3.subject

        subs.append(sub1)
        subs.append(sub2)
        subs.append(sub3)

        for i in range(100):
            await js.publish("quux", f'Hello World {i}'.encode())

        delivered = [a, b, c]
        for _ in range(5):
            await asyncio.sleep(0.5)
            total = len(a) + len(b) + len(c)
            if total == 100:
                break

        # Check that there was a good balance among the group members.
        self.assertEqual(len(a) + len(b) + len(c), 100)
        for q in delivered:
            assert 10 <= len(q) <= 70

        # Now unsubscribe all.
        await sub1.unsubscribe()
        await sub2.drain()
        await sub3.unsubscribe()

        # Confirm that no more messages are received.
        for _ in range(200, 210):
            await js.publish("quux", f'Hello World {i}'.encode())

        with pytest.raises(BadSubscriptionError):
            await sub1.unsubscribe()

        with pytest.raises(BadSubscriptionError):
            await sub2.drain()

        await nc.close()

    @async_test
    async def test_subscribe_push_bound(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.add_stream(name="pbound", subjects=["pbound"])

        a, b = ([], [])

        async def cb1(msg):
            a.append(msg)

        async def cb2(msg):
            b.append(msg)

        for i in range(10):
            await js.publish("pbound", f'Hello World {i}'.encode())

        # First subscriber will create.
        await js.subscribe("pbound", cb=cb1, durable="singleton")
        await asyncio.sleep(0.5)

        info = await js.consumer_info("pbound", "singleton")
        assert info.push_bound

        # Rest of subscribers will not bind because it is already bound.
        with pytest.raises(nats.js.errors.Error) as err:
            await js.subscribe("pbound", cb=cb2, durable="singleton")
        self.assertEqual(
            err.value.description,
            "consumer is already bound to a subscription"
        )

        with pytest.raises(nats.js.errors.Error) as err:
            await js.subscribe(
                "pbound", queue="foo", cb=cb2, durable="singleton"
            )
        self.assertEqual(
            err.value.description,
            "cannot create queue subscription 'foo' to consumer 'singleton'"
        )

        # Wait the delivery of the messages.
        for i in range(5):
            if len(a) == 10:
                break
            await asyncio.sleep(0.2)
        self.assertEqual(len(a), 10)

        # Create a sync subscriber now.
        sub2 = await js.subscribe("pbound", durable="two")
        msg = await sub2.next_msg()
        assert msg.data == b'Hello World 0'
        assert msg.metadata.sequence.stream == 1
        assert msg.metadata.sequence.consumer == 1
        self.assertEqual(sub2.pending_msgs, 9)

        await nc.close()

    @async_test
    async def test_ephemeral_subscribe(self):
        nc = await nats.connect()
        js = nc.jetstream()

        subject = "ephemeral"
        await js.add_stream(name=subject, subjects=[subject])

        for i in range(10):
            await js.publish(subject, f'Hello World {i}'.encode())

        # First subscriber will create.
        sub1 = await js.subscribe(subject)
        sub2 = await js.subscribe(subject)

        recvd = 0
        async for msg in sub1.messages:
            recvd += 1
            await msg.ack_sync()

            if recvd == 10:
                break

        # Both should be able to process the messages at their own pace.
        self.assertEqual(sub1.pending_msgs, 0)
        self.assertEqual(sub2.pending_msgs, 10)

        info1 = await sub1.consumer_info()
        self.assertEqual(info1.stream_name, "ephemeral")
        self.assertEqual(info1.num_ack_pending, 0)
        assert len(info1.name) > 0

        info2 = await sub2.consumer_info()
        self.assertEqual(info2.stream_name, "ephemeral")
        self.assertEqual(info2.num_ack_pending, 10)
        assert len(info2.name) > 0
        assert info1.name != info2.name


class AckPolicyTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_double_acking_pull_subscribe(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.add_stream(name="TESTACKS", subjects=["test"])
        for i in range(0, 10):
            await js.publish("test", f'{i}'.encode())

        # Pull Subscriber
        psub = await js.pull_subscribe("test", "durable")
        msgs = await psub.fetch(1)
        msg = msgs[0]
        await msg.ack()
        with pytest.raises(MsgAlreadyAckdError):
            await msg.ack()

        info = await psub.consumer_info()
        assert info.num_pending == 9
        assert info.num_ack_pending == 0

        msgs = await psub.fetch(1)
        msg = msgs[0]
        await msg.nak()
        with pytest.raises(MsgAlreadyAckdError):
            await msg.ack()
        info = await psub.consumer_info()
        assert info.num_pending == 8
        assert info.num_ack_pending == 1

        msgs = await psub.fetch(1)
        msg = msgs[0]
        await msg.in_progress()
        await asyncio.sleep(0.5)
        await msg.in_progress()
        await msg.ack()

        info = await psub.consumer_info()
        assert info.num_pending == 8
        assert info.num_ack_pending == 0

        await nc.close()

    @async_test
    async def test_double_acking_subscribe(self):
        errors = []

        async def error_handler(e):
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)

        js = nc.jetstream()
        await js.add_stream(name="TESTACKS", subjects=["test"])
        for i in range(0, 10):
            await js.publish("test", f'{i}'.encode())

        async def ocb(msg):
            # Ack the first one only.
            if msg.metadata.sequence.stream == 1:
                await msg.ack()
                return

            await msg.nak()
            await msg.ack()

            # Backoff to avoid a lot of redeliveries from nak protocol,
            # could get thousands per sec otherwise.
            if msg.metadata.sequence.stream == 10:
                await asyncio.sleep(1)

        # Subscriber
        sub = await js.subscribe("test", cb=ocb)
        await asyncio.sleep(0.5)
        info = await sub.consumer_info()
        assert info.num_ack_pending == 9
        assert info.num_pending == 0
        await sub.unsubscribe()

        assert len(errors) > 0
        assert isinstance(errors[0], MsgAlreadyAckdError)

        await nc.close()

    @async_test
    async def test_nak_delay(self):
        nc = await nats.connect()
        stream = uuid.uuid4().hex
        subject = uuid.uuid4().hex

        js = nc.jetstream()
        await js.add_stream(name=stream, subjects=[subject])
        await js.publish(subject, b'message')

        psub = await js.pull_subscribe(subject, "durable")
        msgs = await psub.fetch(1)
        msg = msgs[0]
        await msg.nak()

        msgs = await psub.fetch(1)
        msg = msgs[0]
        await msg.nak(2)

        received = False

        async def f():
            nonlocal received
            await psub.fetch(1, None)
            received = True

        task = asyncio.create_task(f())
        self.assertFalse(received)
        await asyncio.sleep(1)
        self.assertFalse(received)
        await asyncio.sleep(0.5)
        self.assertFalse(received)
        await asyncio.sleep(0.6)
        assert task.done()
        assert received


class OrderedConsumerTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_flow_control(self):
        errors = []

        async def error_handler(e):
            print("Error:", e)
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)

        js = nc.jetstream()

        subject = "flow"
        await js.add_stream(name=subject, subjects=[subject])

        async def cb(msg):
            await msg.ack()

        with pytest.raises(nats.js.errors.APIError) as err:
            sub = await js.subscribe(subject, cb=cb, flow_control=True)
        self.assertEqual(
            err.value.description,
            "consumer with flow control also needs heartbeats"
        )

        sub = await js.subscribe(
            subject, cb=cb, flow_control=True, idle_heartbeat=0.5
        )

        tasks = []

        async def producer():
            mlen = 128 * 1024
            msg = b'A' * mlen

            # Send it in chunks
            i = 0
            chunksize = 256
            while i < mlen:
                chunk = None
                if mlen - i <= chunksize:
                    chunk = msg[i:]
                else:
                    chunk = msg[i:i + chunksize]
                i += chunksize
                task = asyncio.create_task(js.publish(subject, chunk))
                tasks.append(task)

        task = asyncio.create_task(producer())
        await asyncio.wait_for(task, timeout=1)
        await asyncio.gather(*tasks)

        for _ in range(0, 5):
            await sub.consumer_info()
            await asyncio.sleep(0.5)

        await nc.close()

    @async_test
    async def test_ordered_consumer(self):
        errors = []

        async def error_handler(e):
            print("Error:", e)
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        subject = "osub"
        await js.add_stream(name=subject, subjects=[subject], storage="memory")

        msgs = []

        async def cb(msg):
            msgs.append(msg)
            await msg.ack()

        sub = await js.subscribe(
            subject,
            cb=cb,
            ordered_consumer=True,
            idle_heartbeat=0.3,
        )

        tasks = []
        expected_size = 1048576

        async def producer():
            mlen = 1024 * 1024
            msg = b'A' * mlen

            # Send it in chunks
            i = 0
            chunksize = 1024
            while i < mlen:
                chunk = None
                if mlen - i <= chunksize:
                    chunk = msg[i:]
                else:
                    chunk = msg[i:i + chunksize]
                i += chunksize
                task = asyncio.create_task(
                    js.publish(subject, chunk, headers={'data': "true"})
                )
                await asyncio.sleep(0)
                tasks.append(task)

        task = asyncio.create_task(producer())
        await asyncio.wait_for(task, timeout=4)
        await asyncio.gather(*tasks)
        assert len(msgs) == 1024

        received_payload = bytearray(b'')
        for msg in msgs:
            received_payload.extend(msg.data)
        assert len(received_payload) == expected_size

        for _ in range(0, 5):
            info = await sub.consumer_info()
            if info.num_pending == 0:
                break
            await asyncio.sleep(0.5)

        info = await sub.consumer_info()
        assert info.num_pending == 0
        await nc.close()

    @async_long_test
    async def test_ordered_consumer_single_loss(self):
        errors = []

        async def error_handler(e):
            print("Error:", e)
            errors.append(e)

        # Consumer
        nc = await nats.connect(error_cb=error_handler)

        # Producer
        nc2 = await nats.connect(error_cb=error_handler)

        js = nc.jetstream()
        js2 = nc2.jetstream()

        # Consumer will lose some messages.
        orig_build_message = nc._build_message

        def _build_message(subject, reply, data, headers):
            r = random.randint(0, 100)
            if r <= 10 and headers and headers.get("data"):
                nc._build_message = orig_build_message
                return

            return nc.msg_class(
                subject=subject.decode(),
                reply=reply.decode(),
                data=data,
                headers=headers,
                _client=nc,
            )

        # Override to introduce arbitrary loss.
        nc._build_message = _build_message

        subject = "osub2"
        await js2.add_stream(
            name=subject, subjects=[subject], storage="memory"
        )

        # Consumer callback.
        future = asyncio.Future()
        msgs = []

        async def cb(msg):
            msgs.append(msg)
            if len(msgs) >= 1024:
                if not future.done():
                    future.set_result(True)
            await msg.ack()

        sub = await js.subscribe(
            subject,
            cb=cb,
            ordered_consumer=True,
            idle_heartbeat=0.3,
        )

        tasks = []
        expected_size = 1048576

        async def producer():
            mlen = 1024 * 1024
            msg = b'A' * mlen

            # Send it in chunks
            i = 0
            chunksize = 1024
            while i < mlen:
                chunk = None
                if mlen - i <= chunksize:
                    chunk = msg[i:]
                else:
                    chunk = msg[i:i + chunksize]
                i += chunksize
                task = asyncio.create_task(
                    nc2.publish(subject, chunk, headers={'data': "true"})
                )
                tasks.append(task)

        task = asyncio.create_task(producer())
        await asyncio.wait_for(task, timeout=2)
        await asyncio.gather(*tasks)

        # Async publishing complete
        await nc2.flush()
        await asyncio.sleep(1)

        # Wait until callback receives all the messages.
        await asyncio.wait_for(future, timeout=5)
        assert len(msgs) == 1024

        received_payload = bytearray(b'')
        for msg in msgs:
            received_payload.extend(msg.data)
        assert len(received_payload) == expected_size

        for _ in range(0, 5):
            info = await sub.consumer_info()
            if info.num_pending == 0:
                break
            await asyncio.sleep(1)

        info = await sub.consumer_info()
        assert info.num_pending == 0
        await nc.close()
        await nc2.close()


class KVTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_kv_simple(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()
        await js.add_stream(name="mystream")

        kv = await js.create_key_value(bucket="TEST", history=5, ttl=3600)
        status = await kv.status()
        self.assertEqual(status.bucket, "TEST")
        assert status.values == 0
        assert status.history == 5
        assert status.ttl == 3600

        seq = await kv.put("hello", b'world')
        assert seq == 1

        entry = await kv.get("hello")
        assert "hello" == entry.key
        assert b'world' == entry.value

        status = await kv.status()
        assert status.values == 1

        for i in range(0, 100):
            await kv.put(f"hello.{i}", b'Hello JS KV!')

        status = await kv.status()
        assert status.values == 101

        await kv.delete("hello.1")

        with pytest.raises(KeyDeletedError) as err:
            await kv.get("hello.1")
        assert err.value.entry.key == 'hello.1'
        assert err.value.entry.revision == 102
        self.assertEqual(err.value.entry.value, None)
        self.assertEqual(err.value.op, 'DEL')

        await kv.purge("hello.5")

        with pytest.raises(KeyDeletedError) as err:
            await kv.get("hello.5")
        assert err.value.entry.key == 'hello.5'
        assert err.value.entry.revision == 103
        self.assertEqual(err.value.entry.value, None)
        self.assertEqual(err.value.op, 'PURGE')

        status = await kv.status()
        assert status.values == 102

        await kv.purge("hello.*")

        with pytest.raises(NotFoundError):
            await kv.get("hello.5")

        status = await kv.status()
        assert status.values == 2

        entry = await kv.get("hello")
        assert "hello" == entry.key
        assert b'world' == entry.value
        assert 1 == entry.revision

        # Now get the the same KV via lookup.
        kv = await js.key_value("TEST")
        entry = await kv.get("hello")
        assert "hello" == entry.key
        assert b'world' == entry.value
        assert 1 == entry.revision

        status = await kv.status()
        assert status.values == 2

        for i in range(100, 200):
            await kv.put(f"hello.{i}", b'Hello JS KV!')

        status = await kv.status()
        assert status.values == 102

        with pytest.raises(NotFoundError):
            await kv.get("hello.5")

        entry = await kv.get("hello.102")
        assert "hello.102" == entry.key
        self.assertEqual(b'Hello JS KV!', entry.value)
        assert 107 == entry.revision

        await nc.close()

    @async_test
    async def test_not_kv(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        with pytest.raises(BucketNotFoundError):
            await js.key_value(bucket="TEST2")

        await js.add_stream(name="KV_TEST3")

        with pytest.raises(BadBucketError):
            await js.key_value(bucket="TEST3")
