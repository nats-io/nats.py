import asyncio
import base64
import datetime
from hashlib import sha256
import random
import string
import time
import unittest
from unittest import mock
import uuid
import json
import io
import tempfile

import pytest
import nats
import nats.js.api
from nats.aio.msg import Msg
from nats.aio.client import Client as NATS, __version__
from nats.aio.errors import *
from nats.errors import *
from nats.js.errors import *
from tests.utils import *

try:
    from fast_mail_parser import parse_email
except ImportError:
    parse_email = None


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
        await js.add_stream(name="TEST2", subjects=["a1", "a2", "a3", "a4"])

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
        assert msg.header == {'hello': 'world'}

        await msg.nak()

        info = await js.consumer_info("TEST1", "dur", timeout=1)
        assert info.stream_name == "TEST1"
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

        await asyncio.sleep(1)
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
        assert received is False
        await asyncio.sleep(1)
        assert received is False
        await asyncio.sleep(1)
        await js.publish("foo.111", b'Hello from NATS!')
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
        assert 0 == info.num_pending

    @async_long_test
    async def test_fetch_n(self):
        nc = NATS()
        await nc.connect()

        server_version = nc.connected_server_version
        if server_version.major == 2 and server_version.minor < 9:
            pytest.skip('needs to run at least on v2.9.0')

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
            await msg.ack_sync()
            i += 1

        info = await sub.consumer_info()
        assert info.num_ack_pending == 0
        assert info.num_redelivered == 0
        assert info.delivered.stream_seq == 10
        assert info.delivered.consumer_seq == 10
        assert info.ack_floor.stream_seq == 10
        assert info.ack_floor.consumer_seq == 10
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
            assert msg.headers['Status'] == '408'
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

        # It should receive the first one that is available already only.
        msgs = await fut
        assert len(msgs) == 1
        for msg in msgs:
            await msg.ack_sync()

        info = await sub.consumer_info()
        assert info.num_ack_pending == 1
        assert info.num_redelivered == 0
        assert info.num_waiting == 0
        assert info.num_pending == 2
        assert info.delivered.stream_seq == 12
        assert info.delivered.consumer_seq == 12

        # Message 10 is the last message that got acked.
        assert info.ack_floor.stream_seq == 10
        assert info.ack_floor.consumer_seq == 10

        # Unacked last message so that ack floor is updated.
        await unacked_msg.ack_sync()

        info = await sub.consumer_info()
        assert info.num_pending == 2
        assert info.ack_floor.stream_seq == 12
        assert info.ack_floor.consumer_seq == 12

        # No messages at this point.
        msgs = await sub.fetch(1, timeout=0.5)
        self.assertEqual(msgs[0].data, b'i:13')

        msgs = await sub.fetch(1, timeout=0.5)
        self.assertEqual(msgs[0].data, b'i:14')

        with pytest.raises(TimeoutError):
            await sub.fetch(1, timeout=0.5)

        with pytest.raises(nats.errors.Error):
            await sub.fetch(1, timeout=0.5)

        # Max waiting is 3 so it should be stuck at 2 but consumer_info resets this.
        info = await sub.consumer_info()
        assert info.num_waiting <= 1
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

        # info = await js.consumer_info("TEST3", "example")
        # assert info.num_waiting == 0

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
            elif isinstance(e, APIError):
                raise e

        # info = await js.consumer_info("TEST31", "example")
        # assert info.num_waiting == 0
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
                # There should be no API level errors on fetch,
                # only timeouts or messages.
                raise e
        task.cancel()

        await nc.close()

    @async_long_test
    async def test_fetch_headers(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()

        await js.add_stream(name="test-nats", subjects=["test.nats.1"])
        await js.publish("test.nats.1", b'first_msg', headers={'': ''})
        sub = await js.pull_subscribe("test.nats.1", "durable")
        msgs = await sub.fetch(1)
        assert msgs[0].header == None

        msg = await js.get_msg("test-nats", 1)
        assert msgs[0].header == None

        # NOTE: Headers with empty spaces are ignored.
        await js.publish(
            "test.nats.1",
            b'second_msg',
            headers={
                '  AAA AAA AAA  ': '               ',
                ' B B B ': '                       '
            }
        )
        msgs = await sub.fetch(1)
        assert msgs[0].header == None

        msg = await js.get_msg("test-nats", 2)
        assert msgs[0].header == None

        # NOTE: As soon as there is a message with empty spaces are ignored.
        await js.publish(
            "test.nats.1",
            b'third_msg',
            headers={
                '  AAA-AAA-AAA  ': '     a          ',
                '  AAA-BBB-AAA  ': '               ',
                ' B B B ': '        a               '
            }
        )
        msgs = await sub.fetch(1)
        assert msgs[0].header['AAA-AAA-AAA'] == 'a'
        assert msgs[0].header['AAA-BBB-AAA'] == ''

        msg = await js.get_msg("test-nats", 3)
        assert msg.header['AAA-AAA-AAA'] == 'a'
        assert msg.header['AAA-BBB-AAA'] == ''

        if not parse_email:
            await nc.close()
            return

        await js.publish(
            "test.nats.1",
            b'third_msg',
            headers={
                '  AAA AAA AAA  ': '     a          ',
                '  AAA-BBB-AAA  ': '     b          ',
                ' B B B ': '        a               '
            }
        )
        msgs = await sub.fetch(1)
        assert msgs[0].header == {'AAA-BBB-AAA': 'b'}

        msg = await js.get_msg("test-nats", 4)
        assert msg.header == None

        await nc.close()

    @async_test
    async def test_pull_subscribe_limits(self):
        nc = NATS()

        errors = []

        async def error_cb(err):
            errors.append(err)

        await nc.connect(error_cb=error_cb)

        js = nc.jetstream()
        await js.add_stream(name="TEST2", subjects=["a1", "a2", "a3", "a4"])

        for i in range(1, 10):
            await js.publish("a1", f'a1:{i}'.encode())

        # Shorter msgs limit, and disable bytes limit to not get slow consumers.
        sub = await js.pull_subscribe(
            "a3",
            "auto",
            pending_msgs_limit=50,
            pending_bytes_limit=-1,
        )
        for i in range(0, 100):
            await js.publish("a3", b'test')

        # Internal buffer will drop some of the messages due to reaching limit.
        msgs = await sub.fetch(100, timeout=1)
        i = 0
        for msg in msgs:
            i += 1
            await asyncio.sleep(0)
            await msg.ack()
        assert 50 <= len(msgs) <= 51
        assert sub.pending_msgs == 0
        assert sub.pending_bytes == 0

        # Infinite queue and pending bytes.
        sub = await js.pull_subscribe(
            "a3",
            "two",
            pending_msgs_limit=-1,
            pending_bytes_limit=-1,
        )
        msgs = await sub.fetch(100, timeout=1)
        for msg in msgs:
            await msg.ack()
        assert len(msgs) <= 100
        assert sub.pending_msgs == 0
        assert sub.pending_bytes == 0

        # Consumer has a single message pending but none in buffer.
        await js.publish("a3", b'last message')
        info = await sub.consumer_info()
        assert info.num_pending == 1
        assert sub.pending_msgs == 0

        # Remove interest
        await sub.unsubscribe()
        with pytest.raises(TimeoutError):
            await sub.fetch(1, timeout=1)

        # The pending message is still there, but not possible to consume.
        info = await sub.consumer_info()
        assert info.num_pending == 1

        await nc.close()

    @async_test
    async def test_fetch_cancelled_errors_raised(self):
        import tracemalloc
        tracemalloc.start()

        nc = NATS()
        await nc.connect()

        js = nc.jetstream()

        await js.add_stream(name="test", subjects=["test.a"])
        await js.add_consumer(
            "test",
            durable_name="a",
            deliver_policy=nats.js.api.DeliverPolicy.ALL,
            max_deliver=20,
            max_waiting=512,
            max_ack_pending=1024,
            filter_subject="test.a"
        )

        sub = await js.pull_subscribe("test.a", "test", stream="test")

        # FIXME: RuntimeWarning: coroutine 'Queue.get' was never awaited
        # is raised here due to the mock usage.
        with self.assertRaises(asyncio.CancelledError):
            with unittest.mock.patch(
                    "asyncio.wait_for",
                    unittest.mock.AsyncMock(side_effect=asyncio.CancelledError
                                            )):
                await sub.fetch(batch=1, timeout=0.1)

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
        current.cluster = None
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

        stream_config = current.config
        stream_config.subjects.append("extra")
        updated_stream = await jsm.update_stream(stream_config)
        assert updated_stream.config.subjects == [
            'hello', 'world', 'hello.>', 'extra'
        ]

        # Purge Stream
        is_purged = await jsm.purge_stream("hello")
        assert is_purged
        current = await jsm.stream_info("hello")
        assert current.state.messages == 0
        assert current.state.bytes == 0

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

    @async_test
    async def test_jsm_get_delete_msg(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()
        jsm = nc.jsm()

        # Create stream
        stream = await jsm.add_stream(name="foo", subjects=["foo.>"])

        await js.publish("foo.a.1", b'Hello', headers={'foo': 'bar'})
        await js.publish("foo.b.1", b'World')
        await js.publish("foo.c.1", b'!!!')

        # GetMsg
        msg = await jsm.get_msg("foo", 2)
        assert msg.subject == 'foo.b.1'
        assert msg.data == b'World'

        msg = await jsm.get_msg("foo", 3)
        assert msg.subject == 'foo.c.1'
        assert msg.data == b'!!!'

        msg = await jsm.get_msg("foo", 1)
        assert msg.subject == 'foo.a.1'
        assert msg.data == b'Hello'
        assert msg.headers["foo"] == "bar"
        assert msg.hdrs == 'TkFUUy8xLjANCmZvbzogYmFyDQoNCg=='

        with pytest.raises(BadRequestError):
            await jsm.get_msg("foo", 0)

        # DeleteMsg
        stream_info = await jsm.stream_info("foo")
        assert stream_info.state.messages == 3

        ok = await jsm.delete_msg("foo", 2)
        assert ok

        stream_info = await jsm.stream_info("foo")
        assert stream_info.state.messages == 2

        msg = await jsm.get_msg("foo", 1)
        assert msg.data == b"Hello"

        # Deleted message should be gone now.
        with pytest.raises(NotFoundError):
            await jsm.get_msg("foo", 2)

        msg = await jsm.get_msg("foo", 3)
        assert msg.data == b"!!!"

        await nc.close()

    @async_test
    async def test_jsm_stream_management(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()
        jsm = nc.jsm()

        await jsm.add_stream(name="foo")
        await jsm.add_stream(name="bar")
        await jsm.add_stream(name="quux")

        streams = await jsm.streams_info()

        expected = ["foo", "bar", "quux"]
        responses = []
        for stream in streams:
            responses.append(stream.config.name)

        for name in expected:
            assert name in responses

        await nc.close()

    @async_test
    async def test_jsm_consumer_management(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()
        jsm = nc.jsm()

        await jsm.add_stream(name="hello", subjects=["hello"])

        durables = ["a", "b", "c"]

        subs = []
        for durable in durables:
            sub = await js.pull_subscribe("hello", durable)
            subs.append(sub)

        consumers = await jsm.consumers_info("hello")
        assert len(consumers) == 3

        expected = ["a", "b", "c"]
        responses = []
        for consumer in consumers:
            responses.append(consumer.config.durable_name)

        for name in expected:
            assert name in responses

        await nc.close()

    @async_test
    async def test_number_of_consumer_replicas(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.add_stream(name="TESTREPLICAS", subjects=["test.replicas"])
        for i in range(0, 10):
            await js.publish("test.replicas", f'{i}'.encode())

        # Create consumer
        config = nats.js.api.ConsumerConfig(
            num_replicas=1, durable_name="mycons"
        )
        cons = await js.add_consumer(stream="TESTREPLICAS", config=config)
        if cons.config.num_replicas:
            assert cons.config.num_replicas == 1

        await nc.close()

    @async_test
    async def test_consumer_with_name(self):
        nc = NATS()
        await nc.connect()
        js = nc.jetstream()
        jsm = nc.jsm()

        tsub = await nc.subscribe("$JS.API.CONSUMER.>")

        # Create stream.
        await jsm.add_stream(name="ctests", subjects=["a", "b", "c.>"])
        await js.publish("a", b'hello world!')
        await js.publish("b", b'hello world!!')
        await js.publish("c.d", b'hello world!!!')
        await js.publish("c.d.e", b'hello world!!!!')

        # Create ephemeral pull consumer with a name.
        stream_name = "ctests"
        consumer_name = "ephemeral"
        cinfo = await jsm.add_consumer(
            stream_name,
            name=consumer_name,
            ack_policy="explicit",
        )
        assert cinfo.config.name == consumer_name

        msg = await tsub.next_msg()
        assert msg.subject == '$JS.API.CONSUMER.CREATE.ctests.ephemeral'

        sub = await js.pull_subscribe_bind(consumer_name, stream_name)
        msgs = await sub.fetch(1)
        assert msgs[0].data == b'hello world!'
        ok = await msgs[0].ack_sync()
        assert ok

        msg = await tsub.next_msg()
        assert msg.subject == '$JS.API.CONSUMER.MSG.NEXT.ctests.ephemeral'

        # Create durable pull consumer with a name.
        consumer_name = "durable"
        cinfo = await jsm.add_consumer(
            stream_name,
            name=consumer_name,
            durable_name=consumer_name,
            ack_policy="explicit",
        )
        assert cinfo.config.name == consumer_name
        msg = await tsub.next_msg()
        assert msg.subject == '$JS.API.CONSUMER.CREATE.ctests.durable'

        sub = await js.pull_subscribe_bind(consumer_name, stream_name)
        msgs = await sub.fetch(1)
        assert msgs[0].data == b'hello world!'
        ok = await msgs[0].ack_sync()
        assert ok
        msg = await tsub.next_msg()
        assert msg.subject == '$JS.API.CONSUMER.MSG.NEXT.ctests.durable'

        # Create durable pull consumer with a name and a filter_subject
        consumer_name = "durable2"
        cinfo = await jsm.add_consumer(
            stream_name,
            name=consumer_name,
            durable_name=consumer_name,
            filter_subject="b",
            ack_policy="explicit",
        )
        assert cinfo.config.name == consumer_name
        msg = await tsub.next_msg()
        assert msg.subject == '$JS.API.CONSUMER.CREATE.ctests.durable2.b'

        sub = await js.pull_subscribe_bind(consumer_name, stream_name)
        msgs = await sub.fetch(1)
        assert msgs[0].data == b'hello world!!'
        ok = await msgs[0].ack_sync()
        assert ok
        msg = await tsub.next_msg()
        assert msg.subject == '$JS.API.CONSUMER.MSG.NEXT.ctests.durable2'

        # Create durable pull consumer with a name and a filter_subject
        consumer_name = "durable3"
        cinfo = await jsm.add_consumer(
            stream_name,
            name=consumer_name,
            durable_name=consumer_name,
            filter_subject=">",
            ack_policy="explicit",
        )
        assert cinfo.config.name == consumer_name
        msg = await tsub.next_msg()
        assert msg.subject == '$JS.API.CONSUMER.CREATE.ctests.durable3'

        sub = await js.pull_subscribe_bind(consumer_name, stream_name)
        msgs = await sub.fetch(1)
        assert msgs[0].data == b'hello world!'
        ok = await msgs[0].ack_sync()
        assert ok
        msg = await tsub.next_msg()
        assert msg.subject == '$JS.API.CONSUMER.MSG.NEXT.ctests.durable3'

        # name and durable must match if both present.
        with pytest.raises(BadRequestError) as err:
            await jsm.add_consumer(
                stream_name,
                name="name1",
                durable_name="name2",
                ack_policy="explicit",
            )
        assert err.value.err_code == 10017
        assert err.value.description == 'consumer name in subject does not match durable name in request'

        # Create ephemeral pull consumer with a name and inactive threshold.
        stream_name = "ctests"
        consumer_name = "inactive"
        cinfo = await jsm.add_consumer(
            stream_name,
            name=consumer_name,
            ack_policy="explicit",
            inactive_threshold=2,  # seconds
        )
        assert cinfo.config.name == consumer_name

        sub = await js.pull_subscribe_bind(consumer_name, stream_name)
        msgs = await sub.fetch(1)
        assert msgs[0].data == b'hello world!'
        ok = await msgs[0].ack_sync()
        assert ok

        cinfo = await sub.consumer_info()
        assert cinfo.config.inactive_threshold == 2.0

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
        assert len(a) + len(b) + len(c) == 100
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
        assert err.value.description == "consumer is already bound to a subscription"

        with pytest.raises(nats.js.errors.Error) as err:
            await js.subscribe(
                "pbound", queue="foo", cb=cb2, durable="singleton"
            )
        exp = "cannot create queue subscription 'foo' to consumer 'singleton'"
        assert err.value.description == exp

        # Wait the delivery of the messages.
        for i in range(5):
            if len(a) == 10:
                break
            await asyncio.sleep(0.2)
        assert len(a) == 10

        # Create a sync subscriber now.
        sub2 = await js.subscribe("pbound", durable="two")
        msg = await sub2.next_msg()
        assert msg.data == b'Hello World 0'
        assert msg.metadata.sequence.stream == 1
        assert msg.metadata.sequence.consumer == 1
        assert sub2.pending_msgs == 9

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
        assert sub1.pending_msgs == 0
        assert sub2.pending_msgs == 10

        info1 = await sub1.consumer_info()
        assert info1.stream_name == "ephemeral"
        assert info1.num_ack_pending == 0
        assert len(info1.name) > 0

        info2 = await sub2.consumer_info()
        assert info2.stream_name == "ephemeral"
        assert info2.num_ack_pending == 10
        assert len(info2.name) > 0
        assert info1.name != info2.name

    @async_test
    async def test_subscribe_bind(self):
        nc = await nats.connect()
        js = nc.jetstream()

        stream_name = "hello-stream"
        subject_name = "hello-subject"
        consumer_name = "alice"
        await js.add_stream(name=stream_name, subjects=[subject_name])

        # Create the consumer and assign a deliver subject which
        # will then be picked up on bind.
        inbox = nc.new_inbox()
        config = nats.js.api.ConsumerConfig(deliver_subject=inbox)
        consumer_info = await js.add_consumer(
            stream=stream_name,
            config=config,
            durable_name=consumer_name,
        )
        assert consumer_info.stream_name == stream_name
        assert consumer_info.name == consumer_name
        assert consumer_info.config.durable_name == consumer_name

        # Subscribe using the deliver subject that was chosen before.
        sub = await js.subscribe_bind(
            stream=consumer_info.stream_name,
            consumer=consumer_info.name,
            config=consumer_info.config,
        )
        for i in range(10):
            await js.publish(subject_name, f'Hello World {i}'.encode())

        msgs = []
        for i in range(0, 10):
            msg = await sub.next_msg()
            msgs.append(msg)
            await msg.ack()
            await asyncio.sleep(0.2)
        assert len(msgs) == 10
        assert sub.pending_msgs == 0

        info = await sub.consumer_info()
        assert info.num_ack_pending == 0
        assert info.num_pending == 0

    @async_test
    async def test_subscribe_custom_limits(self):
        errors = []

        async def error_cb(err):
            errors.append(err)

        nc = await nats.connect(error_cb=error_cb)
        js = nc.jetstream()

        await js.add_stream(name="cqsub", subjects=["quux"])

        a, b, c = ([], [], [])

        async def cb1(msg):
            a.append(msg)

        async def cb2(msg):
            b.append(msg)

        async def cb3(msg):
            c.append(msg)
            await asyncio.sleep(2)

        subs = []

        sub1 = await js.subscribe("quux", "wg", cb=cb1, pending_bytes_limit=15)
        assert sub1._pending_bytes_limit == 15
        assert sub1._pending_msgs_limit == 512 * 1024

        sub2 = await js.subscribe("quux", "wg", cb=cb2, pending_msgs_limit=-1)
        assert sub2._pending_bytes_limit == 256 * 1024 * 1024
        assert sub2._pending_msgs_limit == -1

        sub3 = await js.subscribe("quux", "wg", cb=cb3, pending_msgs_limit=5)
        assert sub3._pending_bytes_limit == 256 * 1024 * 1024
        assert sub3._pending_msgs_limit == 5

        # All should be bound to the same subject.
        assert sub1.subject == sub2.subject
        assert sub1.subject == sub3.subject

        subs.append(sub1)
        subs.append(sub2)
        subs.append(sub3)

        for i in range(100):
            await js.publish("quux", f'Hello World {i}'.encode())

        # Only 3rd group should have ran into slow consumer errors.
        await asyncio.sleep(1)
        assert len(errors) > 0
        assert sub3.pending_msgs == 5
        for error in errors:
            assert error.sid == sub3._id

        await nc.close()


class AckPolicyTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_ack_v2_tokens(self):
        nc = await nats.connect()

        # At least 11 tokens case
        msg = Msg(nc)
        domain = "foo_domain"
        account_hash = "bar_account"
        stream_name = "stream"
        consumer_name = "consumer"
        num_delivered = 1
        stream_sequence = 2
        consumer_sequence = 2
        timestamp = 1662856107340506000
        num_pending = 20
        msg.reply = f"$JS.ACK.{domain}.{account_hash}.{stream_name}.{consumer_name}.{num_delivered}.{stream_sequence}.{consumer_sequence}.{timestamp}.{num_pending}"
        meta = msg.metadata
        assert meta.domain == domain
        assert meta.stream == stream_name
        assert meta.consumer == consumer_name
        assert meta.sequence.stream == stream_sequence
        assert meta.sequence.consumer == consumer_sequence
        assert meta.num_delivered == num_delivered
        assert meta.num_pending == num_pending
        exp = datetime.datetime(
            2022, 9, 11, 0, 28, 27, 340506, tzinfo=datetime.timezone.utc
        )
        assert meta.timestamp.astimezone(datetime.timezone.utc) == exp

        # Complete v2 tokens (last one discarded)
        msg = Msg(nc)
        msg.reply = f"$JS.ACK.{domain}.{account_hash}.{stream_name}.{consumer_name}.{num_delivered}.{stream_sequence}.{consumer_sequence}.{timestamp}.{num_pending}.123456"
        meta = msg.metadata
        assert meta.domain == domain
        assert meta.stream == stream_name
        assert meta.consumer == consumer_name
        assert meta.sequence.stream == stream_sequence
        assert meta.sequence.consumer == consumer_sequence
        assert meta.num_delivered == num_delivered
        assert meta.num_pending == num_pending
        assert meta.timestamp.astimezone(datetime.timezone.utc
                                         ) == datetime.datetime(
                                             2022,
                                             9,
                                             11,
                                             0,
                                             28,
                                             27,
                                             340506,
                                             tzinfo=datetime.timezone.utc
                                         )

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

        info = await psub.consumer_info()
        assert info.num_pending == 9
        assert info.num_ack_pending == 1

        msg = msgs[0]
        await msg.ack()
        await asyncio.sleep(0.5)
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
        await asyncio.sleep(1)

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
        assert received is False
        await asyncio.sleep(1)
        assert received is False
        await asyncio.sleep(0.5)
        assert received is False
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
        assert err.value.description == "consumer with flow control also needs heartbeats"

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

        def _build_message(sid, subject, reply, data, headers):
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
                _sid=sid,
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

        kv = await js.create_key_value(bucket="TEST", history=5, ttl=3600)
        status = await kv.status()
        assert status.bucket == "TEST"
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

        # Get after delete is again a not found error.
        with pytest.raises(KeyNotFoundError) as err:
            await kv.get("hello.1")

        assert err.value.entry.key == 'hello.1'
        assert err.value.entry.revision == 102
        assert err.value.entry.value == None
        assert err.value.op == 'DEL'

        await kv.purge("hello.5")

        with pytest.raises(KeyNotFoundError) as err:
            await kv.get("hello.5")

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

        # Now get the same KV via lookup.
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
        assert b'Hello JS KV!' == entry.value
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

    @async_test
    async def test_kv_basic(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        bucket = "TEST"
        kv = await js.create_key_value(
            bucket=bucket,
            history=5,
            ttl=3600,
            description="Basic KV",
            direct=False
        )
        status = await kv.status()

        si = await js.stream_info("KV_TEST")
        config = si.config
        assert config.description == "Basic KV"
        assert config.subjects == ['$KV.TEST.>']

        # Check server version for some of these.
        assert config.allow_rollup_hdrs == True
        assert config.deny_delete == True
        assert config.deny_purge == False
        assert config.discard == 'new'
        assert config.duplicate_window == 120.0
        assert config.max_age == 3600.0
        assert config.max_bytes == -1
        assert config.max_consumers == -1
        assert config.max_msg_size == -1
        assert config.max_msgs == -1
        assert config.max_msgs_per_subject == 5
        assert config.mirror == None
        assert config.no_ack == False
        assert config.num_replicas == 1
        assert config.placement == None
        assert config.retention == 'limits'
        assert config.sealed == False
        assert config.sources == None
        assert config.storage == 'file'
        assert config.template_owner == None

        version = nc.connected_server_version
        if version.major == 2 and version.minor < 9:
            assert config.allow_direct == None
        else:
            assert config.allow_direct == False

        # Nothing from start
        with pytest.raises(KeyNotFoundError):
            await kv.get(f"name")

        # Simple Put
        revision = await kv.put(f"name", b'alice')
        assert revision == 1

        # Simple Get
        result = await kv.get(f"name")
        assert result.revision == 1
        assert result.value == b'alice'

        # Delete
        ok = await kv.delete(f"name")
        assert ok

        # Deleting then getting again should be a not found error still,
        # although internall this is a KeyDeletedError.
        with pytest.raises(KeyNotFoundError):
            await kv.get(f"name")

        # Recreate with different name.
        revision = await kv.create("name", b'bob')
        assert revision == 3

        # Expect last revision to be 4
        with pytest.raises(BadRequestError):
            await kv.delete("name", last=4)

        # Correct revision should work.
        ok = await kv.delete("name", last=3)
        assert ok

        # Conditional Updates.
        revision = await kv.update("name", b"hoge", last=4)
        assert revision == 5

        # Should fail since revision number not the latest.
        with pytest.raises(BadRequestError):
            await kv.update("name", b"hoge", last=3)

        # Update with correct latest.
        revision = await kv.update("name", b"fuga", last=revision)
        assert revision == 6

        # Create a different key.
        revision = await kv.create("age", b'2038')
        assert revision == 7

        # Get current.
        entry = await kv.get("age")
        assert entry.value == b'2038'
        assert entry.revision == 7

        # Update the new key.
        revision = await kv.update("age", b'2039', last=revision)
        assert revision == 8

        # Get latest.
        entry = await kv.get("age")
        assert entry.value == b'2039'
        assert entry.revision == 8

        # Internally uses get msg API instead of get last msg.
        entry = await kv.get("age", revision=7)
        assert entry.value == b'2038'
        assert entry.revision == 7

        # Getting past keys with the wrong expected subject is an error.
        with pytest.raises(KeyNotFoundError) as err:
            entry = await kv.get("age", revision=6)
            assert entry.value == b'fuga'
            assert entry.revision == 6
        assert str(
            err.value
        ) == "nats: key not found: expected '$KV.TEST.age', but got '$KV.TEST.name'"

        with pytest.raises(KeyNotFoundError) as err:
            await kv.get("age", revision=5)

        with pytest.raises(KeyNotFoundError) as err:
            await kv.get("age", revision=4)

        entry = await kv.get("name", revision=3)
        assert entry.value == b'bob'

        with pytest.raises(KeyWrongLastSequenceError,
                           match="nats: wrong last sequence: 8"):
            await kv.create("age", b'1')

        # Now let's delete and recreate.
        await kv.delete("age", last=8)
        await kv.create("age", b'final')

        with pytest.raises(KeyWrongLastSequenceError,
                           match="nats: wrong last sequence: 10"):
            await kv.create("age", b'1')

        entry = await kv.get("age")
        assert entry.revision == 10

    @async_test
    async def test_kv_direct_get_msg(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)

        version = nc.connected_server_version
        if version.major == 2 and version.minor < 9:
            pytest.skip("KV Direct feature requires nats-server v2.9.0")

        js = nc.jetstream()

        bucket = "TEST"
        kv = await js.create_key_value(
            bucket=bucket,
            history=5,
            ttl=3600,
            description="Direct KV",
            direct=True
        )

        si = await js.stream_info("KV_TEST")
        config = si.config
        assert config.description == "Direct KV"
        assert config.subjects == ['$KV.TEST.>']
        await kv.create("A", b'1')
        await kv.create("B", b'2')
        await kv.create("C", b'3')
        await kv.create("D", b'4')
        await kv.create("E", b'5')
        await kv.create("F", b'6')

        await kv.put("C", b'33')
        await kv.put("D", b'44')
        await kv.put("C", b'333')

        # Check with low level msg APIs.

        msg = await js.get_msg("KV_TEST", seq=1, direct=True)
        assert msg.data == b'1'

        # last by subject
        msg = await js.get_msg("KV_TEST", subject="$KV.TEST.C", direct=True)
        assert msg.data == b'333'

        # next by subject
        msg = await js.get_msg(
            "KV_TEST", seq=4, next=True, subject="$KV.TEST.C", direct=True
        )
        assert msg.data == b'33'

    @async_test
    async def test_kv_direct(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        version = nc.connected_server_version
        if version.major == 2 and version.minor < 9:
            pytest.skip("KV Direct feature requires nats-server v2.9.0")

        bucket = "TEST"
        await js.create_key_value(
            bucket=bucket,
            history=5,
            ttl=3600,
            description="Explicit Direct KV",
            direct=True
        )
        kv = await js.key_value(bucket=bucket)
        status = await kv.status()

        si = await js.stream_info("KV_TEST")
        config = si.config
        assert config.description == "Explicit Direct KV"
        assert config.subjects == ['$KV.TEST.>']

        # Check server version for some of these.
        assert config.allow_rollup_hdrs == True
        assert config.allow_direct == True
        assert config.deny_delete == True
        assert config.deny_purge == False
        assert config.discard == 'new'
        assert config.duplicate_window == 120.0
        assert config.max_age == 3600.0
        assert config.max_bytes == -1
        assert config.max_consumers == -1
        assert config.max_msg_size == -1
        assert config.max_msgs == -1
        assert config.max_msgs_per_subject == 5
        assert config.mirror == None
        assert config.no_ack == False
        assert config.num_replicas == 1
        assert config.placement == None
        assert config.retention == 'limits'
        assert config.sealed == False
        assert config.sources == None
        assert config.storage == 'file'
        assert config.template_owner == None

        # Nothing from start
        with pytest.raises(KeyNotFoundError):
            await kv.get(f"name")

        # Simple Put
        revision = await kv.put(f"name", b'alice')
        assert revision == 1

        # Simple Get
        result = await kv.get(f"name")
        assert result.revision == 1
        assert result.value == b'alice'

        # Delete
        ok = await kv.delete(f"name")
        assert ok

        # Deleting then getting again should be a not found error still,
        # although internall this is a KeyDeletedError.
        with pytest.raises(KeyNotFoundError):
            await kv.get(f"name")

        # Recreate with different name.
        revision = await kv.create("name", b'bob')
        assert revision == 3

        # Expect last revision to be 4
        with pytest.raises(BadRequestError):
            await kv.delete("name", last=4)

        # Correct revision should work.
        ok = await kv.delete("name", last=3)
        assert ok

        # Conditional Updates.
        revision = await kv.update("name", b"hoge", last=4)
        assert revision == 5

        # Should fail since revision number not the latest.
        with pytest.raises(BadRequestError):
            await kv.update("name", b"hoge", last=3)

        # Update with correct latest.
        revision = await kv.update("name", b"fuga", last=revision)
        assert revision == 6

        # Create a different key.
        revision = await kv.create("age", b'2038')
        assert revision == 7

        # Get current.
        entry = await kv.get("age")
        assert entry.value == b'2038'
        assert entry.revision == 7

        # Update the new key.
        revision = await kv.update("age", b'2039', last=revision)
        assert revision == 8

        # Get latest.
        entry = await kv.get("age")
        assert entry.value == b'2039'
        assert entry.revision == 8

        # Internally uses get msg API instead of get last msg.
        entry = await kv.get("age", revision=7)
        assert entry.value == b'2038'
        assert entry.revision == 7

        # Getting past keys with the wrong expected subject is an error.
        with pytest.raises(KeyNotFoundError) as err:
            entry = await kv.get("age", revision=6)
            assert entry.value == b'fuga'
            assert entry.revision == 6
        assert str(
            err.value
        ) == "nats: key not found: expected '$KV.TEST.age', but got '$KV.TEST.name'"

        with pytest.raises(KeyNotFoundError) as err:
            await kv.get("age", revision=5)

        with pytest.raises(KeyNotFoundError) as err:
            await kv.get("age", revision=4)

        entry = await kv.get("name", revision=3)
        assert entry.value == b'bob'

        with pytest.raises(KeyWrongLastSequenceError,
                           match="nats: wrong last sequence: 8"):
            await kv.create("age", b'1')

        # Now let's delete and recreate.
        await kv.delete("age", last=8)
        await kv.create("age", b'final')

        with pytest.raises(KeyWrongLastSequenceError,
                           match="nats: wrong last sequence: 10"):
            await kv.create("age", b'1')

        entry = await kv.get("age")
        assert entry.revision == 10

        with pytest.raises(Error) as err:
            await js.add_stream(name="mirror", mirror_direct=True)
        assert err.value.err_code == 10052
        assert err.value.description == 'stream has no mirror but does have mirror direct'

        await nc.close()

    @async_test
    async def test_kv_watch(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        kv = await js.create_key_value(bucket="WATCH")
        status = await kv.status()

        # Same as watch all the updates.
        w = await kv.watchall()

        # First update when there are no pending entries will be None
        # to mark that there are no more pending updates.
        e = await w.updates(timeout=1)
        assert e is None

        await kv.create("name", b'alice:1')
        e = await w.updates()
        assert e.delta == 0
        assert e.key == 'name'
        assert e.value == b'alice:1'
        assert e.revision == 1

        await kv.put("name", b'alice:2')
        e = await w.updates()
        assert e.key == 'name'
        assert e.value == b'alice:2'
        assert e.revision == 2

        await kv.put("name", b'alice:3')
        e = await w.updates()
        assert e.key == 'name'
        assert e.value == b'alice:3'
        assert e.revision == 3

        await kv.put("age", b'22')
        e = await w.updates()
        assert e.key == 'age'
        assert e.value == b'22'
        assert e.revision == 4

        await kv.put("age", b'33')
        e = await w.updates()
        assert e.bucket == "WATCH"
        assert e.key == 'age'
        assert e.value == b'33'
        assert e.revision == 5

        await kv.delete("age")
        e = await w.updates()
        assert e.bucket == "WATCH"
        assert e.key == 'age'
        assert e.value == b''
        assert e.revision == 6
        assert e.operation == "DEL"

        await kv.purge("name")
        e = await w.updates()
        assert e.bucket == "WATCH"
        assert e.key == 'name'
        assert e.value == b''
        assert e.revision == 7
        assert e.operation == "PURGE"

        # No new updates at this point...
        with pytest.raises(TimeoutError):
            await w.updates(timeout=0.5)

        # Stop the watcher.
        await w.stop()

        # Now try wildcard matching and make sure we only get last value when starting.
        await kv.create("new", b'hello world')
        await kv.put("t.name", b'a')
        await kv.put("t.name", b'b')
        await kv.put("t.age", b'c')
        await kv.put("t.age", b'd')
        await kv.put("t.a", b'a')
        await kv.put("t.b", b'b')

        # Will only get last values of the matching keys.
        w = await kv.watch("t.*")

        # There are values present so None is _not_ sent to as an update.
        e = await w.updates()
        assert e.bucket == "WATCH"
        assert e.delta == 3
        assert e.key == "t.name"
        assert e.value == b'b'
        assert e.revision == 10
        assert e.operation == None

        e = await w.updates()
        assert e.bucket == "WATCH"
        assert e.delta == 2
        assert e.key == "t.age"
        assert e.value == b'd'
        assert e.revision == 12
        assert e.operation == None

        e = await w.updates()
        assert e.bucket == "WATCH"
        assert e.delta == 1
        assert e.key == "t.a"
        assert e.value == b'a'
        assert e.revision == 13
        assert e.operation == None

        # Consume next pending update.
        e = await w.updates()
        assert e.bucket == "WATCH"
        assert e.delta == 0
        assert e.key == "t.b"
        assert e.value == b'b'
        assert e.revision == 14
        assert e.operation == None

        # There are no more updates so client will be sent a marker to signal
        # that there are no more updates.
        e = await w.updates()
        assert e is None

        # After getting the None marker, subsequent watch attempts will be a timeout error.
        with pytest.raises(TimeoutError):
            await w.updates(timeout=1)

        await kv.put("t.hello", b'hello world')
        e = await w.updates()
        assert e.delta == 0
        assert e.key == 't.hello'
        assert e.revision == 15

        await nc.close()

    @async_test
    async def test_kv_history(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        kv = await js.create_key_value(bucket="WATCHHISTORY", history=10)
        status = await kv.status()

        for i in range(0, 50):
            await kv.put(f"age", f'{i}'.encode())

        vl = await kv.history("age")
        assert len(vl) == 10

        i = 0
        for entry in vl:
            assert entry.key == 'age'
            assert entry.revision == i + 41
            assert int(entry.value) == i + 40
            i += 1

        await nc.close()

    @async_test
    async def test_kv_keys(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        kv = await js.create_key_value(bucket="KVS", history=2)
        status = await kv.status()

        with pytest.raises(NoKeysError):
            await kv.keys()

        await kv.put("a", b'1')
        await kv.put("b", b'2')
        await kv.put("a", b'11')
        await kv.put("b", b'22')
        await kv.put("a", b'111')
        await kv.put("b", b'222')

        keys = await kv.keys()
        assert len(keys) == 2
        assert "a" in keys and "b" in keys

        # Now delete some.
        await kv.delete("a")

        keys = await kv.keys()
        assert "a" not in keys
        assert len(keys) == 1

        await kv.purge("b")

        # No more keys.
        with pytest.raises(NoKeysError):
            await kv.keys()

        await kv.create("c", b'3')
        keys = await kv.keys()
        assert len(keys) == 1
        assert 'c' in keys

        await nc.close()

    @async_test
    async def test_kv_history_too_large(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        with pytest.raises(KeyHistoryTooLargeError):
            await js.create_key_value(bucket="KVS", history=65)

        await nc.close()

    @async_test
    async def test_kv_purge_tombstones(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        kv = await js.create_key_value(bucket="KVS", history=10)

        for i in range(0, 10):
            await kv.put(f"key-{i}", f"{i}".encode())

        for i in range(0, 10):
            await kv.delete(f"key-{i}")

        await kv.put(f"key-last", b'101')
        await kv.purge_deletes(olderthan=-1)

        await asyncio.sleep(0.5)
        info = await js.stream_info("KV_KVS")
        assert info.state.messages == 1

        await nc.close()

    @async_test
    async def test_kv_purge_olderthan(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        kv = await js.create_key_value(bucket="KVS2", history=10)

        await kv.put("foo", f"a".encode())
        await kv.put("bar", f"a".encode())
        await kv.put("bar", f"b".encode())
        await kv.put("foo", f"b".encode())
        await kv.delete("foo")
        await asyncio.sleep(0.2)
        await kv.delete("bar")

        # All messages before purge.
        info = await js.stream_info("KV_KVS2")
        assert info.state.messages == 6

        # Remove almost all of them.
        await kv.purge_deletes(olderthan=0.1)

        await asyncio.sleep(0.5)

        # Only a single message that was already deleted should remain.
        info = await js.stream_info("KV_KVS2")
        assert info.state.messages == 1

        with pytest.raises(nats.js.errors.NoKeysError):
            await kv.history("foo")

        history = await kv.history("bar")
        assert len(history) == 1
        entry = history[0]
        assert entry.key == 'bar'
        assert entry.revision == 6
        assert entry.operation == 'DEL'

        await nc.close()

    @async_test
    async def test_purge_stream(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        async def pub():
            await js.publish("foo.A", b'1')
            await js.publish("foo.C", b'1')
            await js.publish("foo.B", b'1')
            await js.publish("foo.C", b'2')

        await js.add_stream(name="foo", subjects=["foo.A", "foo.B", "foo.C"])
        await pub()

        await js.purge_stream(name="foo", seq=3)
        sub = await js.pull_subscribe("foo.*", "durable")
        info = await js.stream_info("foo")
        assert info.state.messages == 2
        msgs = await sub.fetch(5, timeout=1)
        assert len(msgs) == 2
        assert msgs[0].subject == 'foo.B'
        assert msgs[1].subject == 'foo.C'
        await js.publish(
            "foo.C",
            b'3',
            headers={nats.js.api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE: "4"}
        )

        await nc.close()

    @async_test
    async def test_kv_republish(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        kv = await js.create_key_value(
            bucket="TEST_UPDATE",
            republish=nats.js.api.RePublish(src=">", dest="bar.>")
        )
        status = await kv.status()
        sinfo = await js.stream_info("KV_TEST_UPDATE")
        assert sinfo.config.republish is not None

        sub = await nc.subscribe("bar.>")
        await kv.put("hello.world", b'Hello World!')
        msg = await sub.next_msg()
        assert msg.data == b'Hello World!'
        assert msg.headers.get('Nats-Msg-Size', None) == None
        await sub.unsubscribe()

        kv = await js.create_key_value(
            bucket="TEST_UPDATE_HEADERS",
            republish=nats.js.api.RePublish(
                src=">",
                dest="quux.>",
                headers_only=True,
            )
        )
        sub = await nc.subscribe("quux.>")
        await kv.put("hello.world", b'Hello World!')
        msg = await sub.next_msg()
        assert msg.data == b''
        assert len(msg.headers) == 5
        assert msg.headers['Nats-Msg-Size'] == '12'
        await sub.unsubscribe()


class ObjectStoreTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_object_basics(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        with pytest.raises(nats.js.errors.InvalidBucketNameError):
            await js.create_object_store(bucket="notok!")

        obs = await js.create_object_store(
            bucket="OBJS", description="testing"
        )
        assert obs._name == "OBJS"
        assert obs._stream == f"OBJ_OBJS"

        # Check defaults.
        status = await obs.status()
        sinfo = status.stream_info
        assert sinfo.config.name == "OBJ_OBJS"
        assert sinfo.config.description == "testing"
        assert sinfo.config.subjects == ['$O.OBJS.C.>', '$O.OBJS.M.>']
        assert sinfo.config.retention == "limits"
        assert sinfo.config.max_consumers == -1
        assert sinfo.config.max_msgs == -1
        assert sinfo.config.max_bytes == -1
        assert sinfo.config.discard == "new"
        assert sinfo.config.max_age == 0
        assert sinfo.config.max_msgs_per_subject == -1
        assert sinfo.config.max_msg_size == -1
        assert sinfo.config.storage == "file"
        assert sinfo.config.num_replicas == 1
        assert sinfo.config.allow_rollup_hdrs == True
        assert sinfo.config.allow_direct == True
        assert sinfo.config.mirror_direct == False

        bucketname = ''.join(
            random.SystemRandom().choice(string.ascii_letters)
            for _ in range(10)
        )
        obs = await js.create_object_store(bucket=bucketname)
        assert obs._name == bucketname
        assert obs._stream == f"OBJ_{bucketname}"

        obs = await js.object_store(bucket=bucketname)
        assert obs._name == bucketname
        assert obs._stream == f"OBJ_{bucketname}"

        # Simple example using bytes.
        info = await obs.put("foo", b'bar')
        assert info.name == "foo"
        assert info.size == 3
        assert info.bucket == bucketname

        # Simple example using strings.
        info = await obs.put("plaintext", 'lorem ipsum')
        assert info.name == "plaintext"
        assert info.size == 11
        assert info.bucket == bucketname

        # With custom metadata.
        opts = api.ObjectMetaOptions(max_chunk_size=5)
        info = await obs.put(
            "filename.txt",
            b'filevalue',
            meta=nats.js.api.ObjectMeta(
                description="filedescription",
                headers={"headername": "headerval"},
                options=opts,
            )
        )

        filename = "filename.txt"
        filevalue = b'filevalue'
        filedesc = "filedescription"
        fileheaders = {"headername": "headerval"}
        assert info.name == filename
        assert info.bucket == obs._name
        assert info.nuid != None
        assert info.nuid != ""
        assert info.size == len(filevalue)
        assert info.chunks == 2

        h = sha256()
        h.update(filevalue)
        h.digest()
        expected_digest = f"SHA-256={base64.urlsafe_b64encode(h.digest()).decode('utf-8')}"
        assert info.digest == expected_digest
        assert info.deleted == False
        assert info.description == filedesc
        assert info.headers == fileheaders
        assert info.options == opts

        info = await obs.get_info(name=filename)
        assert info.name == filename
        assert info.bucket == obs._name
        assert info.nuid != None
        assert info.nuid != ""
        assert info.size == len(filevalue)
        assert info.chunks == 2
        assert info.digest == expected_digest
        assert info.deleted == False
        assert info.description == filedesc
        assert info.headers == fileheaders
        assert info.options == opts

        obr = await obs.get(name=filename)
        assert obr.info.name == filename
        assert obr.info.bucket == obs._name
        assert obr.info.nuid != None
        assert obr.info.nuid != ""
        assert obr.info.size == len(filevalue)
        assert obr.info.chunks == 2
        assert obr.info.digest == expected_digest
        assert obr.info.deleted == False
        assert obr.info.description == filedesc
        assert obr.info.headers == fileheaders
        assert obr.info.options == opts
        assert obr.data == filevalue

        # Deleting the object store.
        res = await js.delete_object_store(bucket=bucketname)
        assert res == True

        with pytest.raises(BucketNotFoundError):
            await js.object_store(bucket=bucketname)

        await nc.close()

    @async_test
    async def test_object_big_files(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        # Create an 8MB object.
        obs = await js.create_object_store(bucket="big")
        ls = ''.join("A" for _ in range(0, 1 * 1024 * 1024 + 33))
        w = io.BytesIO(ls.encode())
        info = await obs.put("big", w)
        assert info.name == "big"
        assert info.size == 1048609
        assert info.chunks == 9
        assert info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        # Create actual file and put it in a bucket.
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.write(ls.encode())
        tmp.close()

        with open(tmp.name) as f:
            info = await obs.put("tmp", f.buffer)
            assert info.name == "tmp"
            assert info.size == 1048609
            assert info.chunks == 9
            assert info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        with open(tmp.name) as f:
            info = await obs.put("tmp2", f)
            assert info.name == "tmp2"
            assert info.size == 1048609
            assert info.chunks == 9
            assert info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        # By default this reads the complete data.
        obr = await obs.get("tmp")
        info = obr.info
        assert info.name == "tmp"
        assert info.size == 1048609
        assert info.chunks == 9
        assert info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        # Using a local file.
        with open("pyproject.toml") as f:
            info = await obs.put("pyproject", f.buffer)
            assert info.name == "pyproject"
            assert info.chunks == 1

        # Using a local file but not as a buffered reader.
        with open("pyproject.toml") as f:
            info = await obs.put("pyproject2", f)
            assert info.name == "pyproject2"
            assert info.chunks == 1

        # Write into file without getting complete data.
        w = tempfile.NamedTemporaryFile(delete=False)
        w.close()
        with open(w.name, 'w') as f:
            obr = await obs.get("tmp", writeinto=f)
            assert obr.data == b''
            assert obr.info.size == 1048609
            assert obr.info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        w2 = tempfile.NamedTemporaryFile(delete=False)
        w2.close()
        with open(w2.name, 'w') as f:
            obr = await obs.get("tmp", writeinto=f.buffer)
            assert obr.data == b''
            assert obr.info.size == 1048609
            assert obr.info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        with open(w2.name) as f:
            result = f.read(-1)
            assert len(result) == 1048609

        await nc.close()

    @async_test
    async def test_object_file_basics(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        # Create an 8MB object.
        obs = await js.create_object_store(bucket="sample")
        ls = ''.join("A" for _ in range(0, 2 * 1024 * 1024 + 33))
        w = io.BytesIO(ls.encode())
        info = await obs.put("sample", w)
        assert info.name == "sample"
        assert info.size == 2097185

        # Make sure the stream is saled.
        await obs.seal()

        status = await obs.status()
        assert status.sealed == True

        sinfo = await js.stream_info("OBJ_sample")
        assert sinfo.config.sealed == True

        # Try to replace with another file.
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.write(ls.encode())
        tmp.close()

        # Check simple errors.
        with pytest.raises(nats.js.errors.BadRequestError):
            with open(tmp.name) as f:
                await obs.put("tmp", f.buffer)

        with pytest.raises(nats.js.errors.NotFoundError):
            await obs.get("tmp")

        with pytest.raises(nats.js.errors.NotFoundError):
            await obs.get("")

        res = await js.delete_object_store(bucket="sample")
        assert res == True

        with pytest.raises(nats.js.errors.NotFoundError):
            await obs.get("big")

        with pytest.raises(nats.js.errors.NotFoundError):
            await js.delete_object_store(bucket="sample")

        await nc.close()

    @async_test
    async def test_object_multi_files(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        obs = await js.create_object_store(
            "TEST_FILES",
            config=nats.js.api.ObjectStoreConfig(description="multi_files", )
        )
        await obs.put("A", b'A')
        await obs.put("B", b'B')
        await obs.put("C", b'C')

        res = await obs.get("A")
        assert res.data == b'A'
        assert res.info.digest == "SHA-256=VZrq0IJk1XldOQlxjN0Fq9SVcuhP5VWQ7vMaiKCP3_0="

        res = await obs.get("B")
        assert res.data == b'B'
        assert res.info.digest == "SHA-256=335w5QIVRPSDS77mSp43if68S-gUcN9inK1t2wMyClw="

        res = await obs.get("C")
        assert res.data == b'C'
        assert res.info.digest == "SHA-256=ayPA1fNdGxH5toPwsKYXNV3rESd9ka4JHTmcZVuHlA0="

        with open("README.md") as fp:
            await obs.put("README.md", fp.buffer)

        size = 0
        with open("README.md") as fp:
            data = fp.read(-1)
            size = len(data)

        res = await obs.get("README.md")
        assert res.info.size == size

        await nc.close()

    @async_test
    async def test_object_watch(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        obs = await js.create_object_store(
            "TEST_FILES",
            config=nats.js.api.ObjectStoreConfig(description="multi_files", )
        )

        watcher = await obs.watch()
        e = await watcher.updates()
        assert e == None

        await obs.put("A", b'A')
        await obs.put("B", b'B')
        await obs.put("C", b'C')

        res = await obs.get("A")
        assert res.data == b'A'
        assert res.info.digest == "SHA-256=VZrq0IJk1XldOQlxjN0Fq9SVcuhP5VWQ7vMaiKCP3_0="

        res = await obs.get("B")
        assert res.data == b'B'
        assert res.info.digest == "SHA-256=335w5QIVRPSDS77mSp43if68S-gUcN9inK1t2wMyClw="

        res = await obs.get("C")
        assert res.data == b'C'
        assert res.info.digest == "SHA-256=ayPA1fNdGxH5toPwsKYXNV3rESd9ka4JHTmcZVuHlA0="

        e = await watcher.updates()
        assert e.name == 'A'
        assert e.bucket == 'TEST_FILES'
        assert e.size == 1
        assert e.chunks == 1
        assert e.digest == 'SHA-256=VZrq0IJk1XldOQlxjN0Fq9SVcuhP5VWQ7vMaiKCP3_0='

        e = await watcher.updates()
        assert e.name == 'B'
        assert e.bucket == 'TEST_FILES'
        assert e.size == 1
        assert e.chunks == 1
        assert e.digest == 'SHA-256=335w5QIVRPSDS77mSp43if68S-gUcN9inK1t2wMyClw='

        e = await watcher.updates()
        assert e.name == 'C'
        assert e.bucket == 'TEST_FILES'
        assert e.size == 1
        assert e.chunks == 1
        assert e.digest == 'SHA-256=ayPA1fNdGxH5toPwsKYXNV3rESd9ka4JHTmcZVuHlA0='

        # Expect no more updates.
        with pytest.raises(asyncio.TimeoutError):
            await watcher.updates(timeout=1)

        # Delete a bucket.
        await obs.delete("B")

        e = await watcher.updates()
        assert e.name == "B"
        assert e.deleted == True

        with pytest.raises(ObjectNotFoundError):
            await obs.get("B")

        deleted = await obs.get("B", show_deleted=True)
        assert deleted.info.name == "B"
        assert deleted.info.deleted == True

        info = await obs.put("C", b'CCC')
        assert info.name == 'C'
        assert info.deleted == False

        e = await watcher.updates()
        assert e.name == 'C'
        assert e.deleted == False

        res = await obs.get("C")
        assert res.data == b'CCC'

        # Update meta
        to_update_meta = res.info.meta
        to_update_meta.description = "changed"
        await obs.update_meta("C", to_update_meta)

        e = await watcher.updates()
        assert e.name == 'C'
        assert e.description == 'changed'

        # Try to update meta when it has already been deleted.
        deleted_meta = deleted.info.meta
        deleted_meta.description = "ng"
        with pytest.raises(ObjectDeletedError):
            await obs.update_meta("B", deleted_meta)

        # Try to update object that does not exist.
        with pytest.raises(ObjectDeletedError):
            await obs.update_meta("X", deleted_meta)

        # Update meta
        res = await obs.get("A")
        assert res.data == b'A'
        to_update_meta = res.info.meta
        to_update_meta.name = "Z"
        to_update_meta.description = "changed"
        with pytest.raises(ObjectAlreadyExists):
            await obs.update_meta("A", to_update_meta)

        await nc.close()

    @async_test
    async def test_object_list(self):
        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        obs = await js.create_object_store(
            "TEST_LIST",
            config=nats.js.api.ObjectStoreConfig(description="listing", )
        )
        await asyncio.gather(
            obs.put("A", b'AAA'),
            obs.put("B", b'BBB'),
            obs.put("C", b'CCC'),
            obs.put("D", b'DDD'),
        )
        entries = await obs.list()
        assert len(entries) == 4
        assert entries[0].name == 'A'
        assert entries[1].name == 'B'
        assert entries[2].name == 'C'
        assert entries[3].name == 'D'

        await nc.close()

    @async_test
    async def test_object_aiofiles(self):
        try:
            import aiofiles
        except ImportError:
            pytest.skip("aiofiles not installed")

        errors = []

        async def error_handler(e):
            print("Error:", e, type(e))
            errors.append(e)

        nc = await nats.connect(error_cb=error_handler)
        js = nc.jetstream()

        # Create an 8MB object.
        obs = await js.create_object_store(bucket="big")
        ls = ''.join("A" for _ in range(0, 1 * 1024 * 1024 + 33))
        w = io.BytesIO(ls.encode())
        info = await obs.put("big", w)
        assert info.name == "big"
        assert info.size == 1048609
        assert info.chunks == 9
        assert info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        # Create actual file and put it in a bucket.
        tmp = tempfile.NamedTemporaryFile(delete=False)
        tmp.write(ls.encode())
        tmp.close()

        async with aiofiles.open(tmp.name) as f:
            info = await obs.put("tmp", f)
            assert info.name == "tmp"
            assert info.size == 1048609
            assert info.chunks == 9
            assert info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        async with aiofiles.open(tmp.name) as f:
            info = await obs.put("tmp2", f)
            assert info.name == "tmp2"
            assert info.size == 1048609
            assert info.chunks == 9
            assert info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        obr = await obs.get("tmp")
        info = obr.info
        assert info.name == "tmp"
        assert info.size == 1048609
        assert len(obr.data) == info.size  # no reader reads whole file.
        assert info.chunks == 9
        assert info.digest == 'SHA-256=mhT1pLyi9JlIaqwVmvt0wQp2x09kor_80Lirl4SDblA='

        # Using a local file.
        async with aiofiles.open("pyproject.toml") as f:
            info = await obs.put("pyproject", f.buffer)
            assert info.name == "pyproject"
            assert info.chunks == 1

        async with aiofiles.open("pyproject.toml") as f:
            info = await obs.put("pyproject2", f)
            assert info.name == "pyproject2"
            assert info.chunks == 1

        await nc.close()


class ConsumerReplicasTest(SingleJetStreamServerTestCase):

    @async_test
    async def test_number_of_consumer_replicas(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.add_stream(name="TESTREPLICAS", subjects=["test.replicas"])
        for i in range(0, 10):
            await js.publish("test.replicas", f'{i}'.encode())

        # Create consumer
        config = nats.js.api.ConsumerConfig(
            num_replicas=1, durable_name="mycons"
        )
        cons = await js.add_consumer(stream="TESTREPLICAS", config=config)

        assert cons.config.num_replicas == 1

        await nc.close()


class AccountLimitsTest(SingleJetStreamServerLimitsTestCase):

    @async_test
    async def test_account_limits(self):
        nc = await nats.connect()

        js = nc.jetstream()

        with pytest.raises(BadRequestError) as err:
            await js.add_stream(name="limits", subjects=["limits"])
        assert err.value.err_code == 10113
        assert err.value.description == "account requires a stream config to have max bytes set"

        with pytest.raises(BadRequestError) as err:
            await js.add_stream(
                name="limits", subjects=["limits"], max_bytes=65536
            )
        assert err.value.err_code == 10122
        assert err.value.description == "stream max bytes exceeds account limit max stream bytes"

        si = await js.add_stream(
            name="limits", subjects=["limits"], max_bytes=128
        )
        assert si.config.max_bytes == 128

        await js.publish(
            "limits",
            b'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
        )
        si = await js.stream_info("limits")
        assert si.state.messages == 1

        for i in range(0, 5):
            await js.publish("limits", b'A')

        expected = nats.js.api.AccountInfo(
            memory=0,
            storage=111,
            streams=1,
            consumers=0,
            limits=nats.js.api.AccountLimits(
                max_memory=67108864,  # 64MB
                max_storage=33554432,  # 32MB
                max_streams=10,
                max_consumers=20,
                max_ack_pending=100,
                memory_max_stream_bytes=2048,
                storage_max_stream_bytes=4096,
                max_bytes_required=True
            ),
            api=nats.js.api.APIStats(total=4, errors=2),
            domain='test-domain',
            tiers=None
        )
        info = await js.account_info()
        assert expected == info

        # Messages are limited.
        js = nc.jetstream(domain="test-domain")
        si = await js.stream_info("limits")
        assert si.state.messages == 3

        # Check unmarshalling response with Tiers:
        blob = """{
        "type": "io.nats.jetstream.api.v1.account_info_response",
        "memory": 0,
        "storage": 6829550,
        "streams": 1,
        "consumers": 0,
        "limits": {
                "max_memory": 0,
                "max_storage": 0,
                "max_streams": 0,
                "max_consumers": 0,
                "max_ack_pending": 0,
                "memory_max_stream_bytes": 0,
                "storage_max_stream_bytes": 0,
                "max_bytes_required": false
        },
        "domain": "ngs",
        "api": {
                "total": 6,
                "errors": 0
        },
        "tiers": {
                "R1": {
                        "memory": 0,
                        "storage": 6829550,
                        "streams": 1,
                        "consumers": 0,
                        "limits": {
                                "max_memory": 0,
                                "max_storage": 2000000000000,
                                "max_streams": 100,
                                "max_consumers": 1000,
                                "max_ack_pending": -1,
                                "memory_max_stream_bytes": -1,
                                "storage_max_stream_bytes": -1,
                                "max_bytes_required": true
                        }
                },
                "R3": {
                        "memory": 0,
                        "storage": 0,
                        "streams": 0,
                        "consumers": 0,
                        "limits": {
                                "max_memory": 0,
                                "max_storage": 500000000000,
                                "max_streams": 25,
                                "max_consumers": 250,
                                "max_ack_pending": -1,
                                "memory_max_stream_bytes": -1,
                                "storage_max_stream_bytes": -1,
                                "max_bytes_required": true
                        }
                }
        }}
        """

        expected = nats.js.api.AccountInfo(
            memory=0,
            storage=6829550,
            streams=1,
            consumers=0,
            limits=nats.js.api.AccountLimits(
                max_memory=0,
                max_storage=0,
                max_streams=0,
                max_consumers=0,
                max_ack_pending=0,
                memory_max_stream_bytes=0,
                storage_max_stream_bytes=0,
                max_bytes_required=False
            ),
            api=nats.js.api.APIStats(total=6, errors=0),
            domain='ngs',
            tiers={
                'R1':
                    nats.js.api.Tier(
                        memory=0,
                        storage=6829550,
                        streams=1,
                        consumers=0,
                        limits=nats.js.api.AccountLimits(
                            max_memory=0,
                            max_storage=2000000000000,
                            max_streams=100,
                            max_consumers=1000,
                            max_ack_pending=-1,
                            memory_max_stream_bytes=-1,
                            storage_max_stream_bytes=-1,
                            max_bytes_required=True
                        )
                    ),
                'R3':
                    nats.js.api.Tier(
                        memory=0,
                        storage=0,
                        streams=0,
                        consumers=0,
                        limits=nats.js.api.AccountLimits(
                            max_memory=0,
                            max_storage=500000000000,
                            max_streams=25,
                            max_consumers=250,
                            max_ack_pending=-1,
                            memory_max_stream_bytes=-1,
                            storage_max_stream_bytes=-1,
                            max_bytes_required=True
                        )
                    )
            }
        )
        info = nats.js.api.AccountInfo.from_response(json.loads(blob))
        assert expected == info
        await nc.close()
