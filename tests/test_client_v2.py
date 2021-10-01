import unittest

import nats
from nats.aio.client import __version__
from nats.aio.errors import *
from nats.aio.js.models.streams import Discard, Mirror, Source, Stream
from tests.utils import *


class HeadersTest(SingleServerTestCase):
    @async_test
    async def test_simple_headers(self):
        nc = await nats.connect()

        sub = await nc.subscribe("foo")
        await nc.flush()
        await nc.publish(
            "foo", b'hello world', headers={
                'foo': 'bar',
                'hello': 'world'
            }
        )

        msg = await sub.next_msg()
        self.assertTrue(msg.headers != None)
        self.assertEqual(len(msg.headers), 2)

        self.assertEqual(msg.headers['foo'], 'bar')
        self.assertEqual(msg.headers['hello'], 'world')

        await nc.close()

    @async_test
    async def test_request_with_headers(self):
        nc = await nats.connect()

        async def service(msg):
            # Add another header
            msg.headers['quux'] = 'quuz'
            await msg.respond(b'OK!')

        await nc.subscribe("foo", cb=service)
        await nc.flush()
        msg = await nc.request(
            "foo", b'hello world', headers={
                'foo': 'bar',
                'hello': 'world'
            }
        )

        self.assertTrue(msg.headers != None)
        self.assertEqual(len(msg.headers), 3)
        self.assertEqual(msg.headers['foo'], 'bar')
        self.assertEqual(msg.headers['hello'], 'world')
        self.assertEqual(msg.headers['quux'], 'quuz')
        self.assertEqual(msg.data, b'OK!')

        await nc.close()


class JetStreamTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_account_info(self):
        nc = await nats.connect()

        js = nc.jetstream()
        info = await js.account_info()
        self.assertTrue(isinstance(info.limits.max_streams, int))
        self.assertTrue(isinstance(info.limits.max_consumers, int))
        self.assertTrue(isinstance(info.limits.max_storage, int))
        self.assertTrue(isinstance(info.limits.max_memory, int))

        await nc.close()

    @async_test
    async def test_stream_update(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.stream.add("TEST")

        await js.stream.update("TEST", subjects=["foo"])
        response = await js.stream.info("TEST")
        self.assertEqual(response.config.subjects, ["foo"])

        await js.stream.update("TEST", discard=Discard.new)
        response = await js.stream.info("TEST")
        self.assertEqual(response.config.discard, "new")

        await js.stream.update("TEST", max_msgs=5)
        response = await js.stream.info("TEST")
        self.assertEqual(response.config.max_msgs, 5)

        await js.stream.update("TEST", max_msgs_per_subject=5)
        response = await js.stream.info("TEST")
        self.assertEqual(response.config.max_msgs_per_subject, 5)

        await js.stream.update("TEST", max_bytes=5)
        response = await js.stream.info("TEST")
        self.assertEqual(response.config.max_bytes, 5)

        await js.stream.update("TEST", max_age=1000, duplicate_window=1000)
        response = await js.stream.info("TEST")
        self.assertEqual(response.config.max_age, 1000)

        await js.stream.update("TEST", num_replicas=2)
        response = await js.stream.info("TEST")
        self.assertEqual(response.config.num_replicas, 2)

        await nc.close()

    @async_test
    async def test_stream_delete(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.stream.add("TEST")
        await js.stream.delete("TEST")
        with self.assertRaises(JetStreamAPIError) as context:
            await js.stream.info("TEST")
        assert context.exception.code == 404
        assert context.exception.description == "stream not found"

        await nc.close()

    @async_test
    async def test_stream_purge(self):
        nc = await nats.connect()
        js = nc.jetstream()

        await js.stream.add("TEST", subjects=["foo"])
        for _ in range(10):
            await js.stream.publish("foo")
        await js.stream.purge("TEST")
        with self.assertRaises(JetStreamAPIError) as context:
            await js.stream.msg_get("TEST", last_by_subj="foo")
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.description, "no message found")

        await nc.close()

    @async_test
    async def test_stream_list(self):
        nc = await nats.connect()

        js = nc.jetstream()

        await js.stream.add("TEST", subjects=["foo_", "bar_"])
        await js.stream.add("TEST_mirror", mirror=Mirror("TEST"))
        await js.stream.add("TEST_sources", sources=[Source(name="TEST")])

        response = await js.stream.list()
        self.assertEqual(response.total, 3)
        self.assertEqual(len(response.streams), 3)
        self.assertIsInstance(response.streams[0], Stream)
        response = await js.stream.names(offset=2)
        self.assertEqual(len(response.streams), 1)
        self.assertEqual(response.offset, 2)
        self.assertEqual(response.total, 3)
        self.assertIsInstance(response.streams[0], str)
        # Stream using mirrors or sources are not returned
        response = await js.stream.names(subject="bar_")
        self.assertEqual(response.total, 1)
        response = await js.stream.list(offset=1)
        self.assertEqual(response.total, 3)
        self.assertEqual(response.offset, 1)
        self.assertEqual(len(response.streams), 2)
        await nc.close()

    @async_test
    async def test_stream_create(self):
        nc = await nats.connect()

        js = nc.jetstream()

        await js.stream.add("TEST", subjects=["foo_", "bar_"])
        info = await js.account_info()
        self.assertTrue(info.streams >= 1)

        await js.stream.add("TEST_mirror", mirror=Mirror("TEST"))
        info = await js.stream.info("TEST_mirror")
        self.assertTrue(info.mirror.name == "TEST")
        self.assertTrue(info.config.mirror.name == "TEST")
        await js.stream.add("TEST_sources", sources=[Source(name="TEST")])
        info = await js.stream.info("TEST_sources")
        self.assertTrue(len(info.config.sources) == 1)
        self.assertTrue(len(info.sources) == 1)

        response = await js.stream.list()
        self.assertEqual(response.total, 3)
        self.assertEqual(len(response.streams), 3)
        self.assertIsInstance(response.streams[0], Stream)
        response = await js.stream.names(offset=2)
        self.assertEqual(len(response.streams), 1)
        self.assertEqual(response.offset, 2)
        self.assertEqual(response.total, 3)
        self.assertIsInstance(response.streams[0], str)
        # Stream using mirrors or sources are not returned
        response = await js.stream.names(subject="bar_")
        self.assertEqual(response.total, 1)
        response = await js.stream.list(offset=1)
        self.assertEqual(response.total, 3)
        self.assertEqual(response.offset, 1)
        self.assertEqual(len(response.streams), 2)
        await nc.close()

    @async_test
    async def test_stream_msg_get(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.add("TEST_MSG_GET", subjects=["foo"])
        ack = await js.stream.publish("foo", b"bar", headers={"test": "test"})
        self.assertEqual(ack.stream, "TEST_MSG_GET")
        self.assertEqual(ack.seq, 1)
        self.assertEqual(ack.domain, None)
        self.assertEqual(ack.duplicate, None)

        response = await js.stream.msg_get("TEST_MSG_GET", last_by_subj="foo")
        self.assertEqual(response.message.data, b"bar")
        self.assertEqual(response.message.seq, 1)
        self.assertEqual(response.message.subject, "foo")
        self.assertEqual(response.message.hdrs, {"test": "test"})

        ack = await js.stream.publish("foo", b"baz", headers=None)

        response = await js.stream.msg_get("TEST_MSG_GET", last_by_subj="foo")
        self.assertEqual(response.message.data, b"baz")
        self.assertEqual(response.message.seq, 2)
        self.assertEqual(response.message.subject, "foo")
        self.assertEqual(response.message.hdrs, None)

        await nc.close()

    @async_test
    async def test_stream_msg_get_no_msg(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.add("TEST_MSG_GET", subjects=["missing"])
        with self.assertRaises(JetStreamAPIError) as context:
            await js.stream.msg_get("TEST_MSG_GET", last_by_subj="missing")
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.description, "no message found")

        await nc.close()

    @async_test
    async def test_stream_msg_delete(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.add("TEST", subjects=["foo"])
        await js.stream.publish("foo")
        await js.stream.msg_get("TEST", last_by_subj="foo")
        await js.stream.msg_delete("TEST", 1)
        with self.assertRaises(JetStreamAPIError) as context:
            await js.stream.msg_get("TEST", last_by_subj="foo")
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.description, "no message found")

        await nc.close()

    @async_test
    async def test_consumer_pull_next(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.add("TEST", subjects=["foo", "bar"])

        for i in range(0, 100):
            await nc.publish("foo", f'msg:{i}'.encode())

        msg = await js.consumer.pull_next(
            "dur", subject="foo", deliver_policy="all", auto_ack=False
        )
        self.assertEqual(len(msg.data), 5)
        self.assertEqual(msg.metadata.sequence.stream, 1)
        self.assertEqual(msg.metadata.sequence.consumer, 1)
        self.assertEqual(msg.metadata.num_pending, 99)

        # Get the consumer info from the consumer.
        result = await js.consumer.info("TEST", "dur")

        # Have pulled only one message, inflight acks is 1.
        self.assertEqual(result.num_ack_pending, 1)
        # Messages waiting to be consumed.
        self.assertEqual(result.num_pending, 99)

        await msg.respond(b"")

        result = await js.consumer.info("TEST", "dur")
        self.assertEqual(result.num_ack_pending, 0)
        self.assertEqual(result.num_pending, 99)

        msg = await js.consumer.pull_next(
            "dur", subject="foo", deliver_policy="all", auto_ack=True
        )

        result = await js.consumer.info("TEST", "dur")
        self.assertEqual(result.num_ack_pending, 0)
        self.assertEqual(result.num_pending, 98)

        await nc.close()

    @async_test
    async def test_consumer_pull_bad_consumer(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.add("TEST", subjects=["foo", "bar"])
        await js.consumer.add("TEST", "con", deliver_subject="test")

        with self.assertRaises(JetStreamAPIError) as context:
            await js.consumer.pull_next("con", stream="TEST")
        self.assertEqual(context.exception.code, 500)
        self.assertEqual(
            context.exception.description,
            "cannot pull messages from push consumer"
        )

        await nc.close()

    @async_test
    async def test_consumer_pull_next_no_wait(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.add("TEST", subjects=["foo", "bar"])
        with self.assertRaises(JetStreamAPIError) as context:
            await js.consumer.pull_next("dur", subject="foo", no_wait=True)
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.description, "No Messages")

        await nc.close()

    @async_test
    async def test_consumer_pull_next_no_stream(self):
        nc = await nats.connect()

        js = nc.jetstream()
        with self.assertRaises(JetStreamAPIError) as context:
            await js.consumer.pull_next("dur", subject="foo", no_wait=True)
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.description, "stream not found")

        with self.assertRaises(JetStreamAPIError) as context:
            await js.consumer.pull_next("dur", stream="UNKNOWN", no_wait=True)
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.description, "stream not found")

        await nc.close()

    @async_test
    async def test_consumer_pull_msgs(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.add("TEST", subjects=["foo", "bar"])

        for i in range(0, 3):
            await nc.publish("foo", f'msg:{i}'.encode())

        idx = 0
        async for msg in js.consumer.pull_msgs("dur", subject="foo",
                                               max_msgs=2, auto_ack=True,
                                               deliver_policy="all"):
            self.assertEqual(msg.data, f'msg:{idx}'.encode())
            idx += 1

        self.assertEqual(idx, 2)
        result = await js.consumer.info("TEST", "dur")
        self.assertEqual(result.num_ack_pending, 0)
        self.assertEqual(result.num_pending, 1)

        async for msg in js.consumer.pull_msgs("dur", subject="foo",
                                               max_msgs=1, auto_ack=False,
                                               deliver_policy="all"):
            self.assertEqual(msg.data, f'msg:{idx}'.encode())
            idx += 1

        self.assertEqual(idx, 3)
        result = await js.consumer.info("TEST", "dur")
        self.assertEqual(result.num_ack_pending, 1)
        self.assertEqual(result.num_pending, 0)

        await nc.close()

    @async_test
    async def test_consumer_delete(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.stream.add("TEST", subjects=["foo", "bar"])
        await js.consumer.add("TEST", "dur")
        await js.consumer.delete("TEST", "dur")
        response = await js.consumer.names("TEST")
        self.assertNotIn("dur", response.consumers)
        await nc.close()

    @async_test
    async def test_consumer_list(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.stream.add("TEST", subjects=["foo", "bar"])
        await js.consumer.add("TEST", "dur")
        response = await js.consumer.list("TEST")
        self.assertEqual(response.total, 1)
        self.assertEqual(response.consumers[0].stream_name, "TEST")
        await nc.close()

    @async_test
    async def test_consumer_create(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.stream.add("TEST", subjects=["foo", "bar"])
        await js.stream.publish("foo", b"")
        sub = await nc.subscribe("con", max_msgs=1)
        response = await js.consumer.add(
            "TEST", deliver_subject="con", deliver_policy="all"
        )
        msg = await sub.next_msg()
        self.assertEqual(msg.subject, "foo")
        self.assertEqual(msg.metadata.stream, "TEST")
        self.assertEqual(msg.metadata.consumer, response.name)
        await nc.close()

    @async_test
    async def test_kv_create(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.kv.add("demo")
        response = await js.stream.names(subject="$KV.demo.>")
        self.assertEqual(response.streams[0], "KV_demo")
        await nc.close()

    @async_test
    async def test_kv_get_put(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.kv.add("demo")
        await js.kv.put("demo", "foo", b"bar")
        msg = await js.kv.get("demo", "foo")
        self.assertEqual(msg.data, b"bar")
        await nc.close()

    @async_test
    async def test_kv_delete(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.kv.add("demo")
        await js.kv.put("demo", "foo", b"bar")
        await js.kv.delete("demo", "foo")
        with self.assertRaises(JetStreamAPIError) as context:
            await js.kv.get("demo", "foo")
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.description, "unknown key")
        await nc.close()

    @async_test
    async def test_kv_rm(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.kv.add("demo")
        await js.kv.put("demo", "foo", b"bar")
        await js.kv.rm("demo")
        with self.assertRaises(JetStreamAPIError) as context:
            await js.kv.get("demo", "foo")
        self.assertEqual(context.exception.code, 404)
        self.assertEqual(context.exception.description, "stream not found")

        await nc.close()

    @async_test
    async def test_kv_history(self):
        nc = await nats.connect()

        js = nc.jetstream()
        await js.kv.add("demo", history=2)
        await js.kv.put("demo", "foo", b"bar")
        await js.kv.put("demo", "foo", b"baz")
        history = await js.kv.history("demo", "foo")
        self.assertEqual(history[0].data, b"bar")
        self.assertEqual(history[1].data, b"baz")
        await nc.close()


if __name__ == '__main__':
    import sys
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
