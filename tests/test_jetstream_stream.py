import asyncio
import nats
import nats.jetstream

from nats.jetstream.stream import StreamConfig
from tests.utils import IsolatedJetStreamServerTestCase
from nats.jetstream.consumer import AckPolicy, ConsumerConfig, PullConsumer

class TestJetStreamStream(IsolatedJetStreamServerTestCase):
    # CreateConsumer tests
    async def test_create_durable_pull_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(durable="durable_consumer")
        created_consumer = await test_stream.create_consumer(consumer_config)
        self.assertIsNotNone(created_consumer)
        self.assertEqual(created_consumer.cached_info.name, "durable_consumer")

        await nats_client.close()

    async def test_create_consumer_idempotent(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(durable="durable_consumer")
        first_consumer = await test_stream.create_consumer(consumer_config)
        second_consumer = await test_stream.create_consumer(consumer_config)
        self.assertEqual(first_consumer.cached_info.name, second_consumer.cached_info.name)

        await nats_client.close()

    async def test_create_ephemeral_pull_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(ack_policy=AckPolicy.NONE)
        created_consumer = await test_stream.create_consumer(consumer_config)
        self.assertIsNotNone(created_consumer)

        await nats_client.close()

    async def test_create_consumer_with_filter_subject(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subject="TEST.A")
        created_consumer = await test_stream.create_consumer(consumer_config)
        self.assertIsNotNone(created_consumer)
        self.assertEqual(created_consumer.cached_info.config.filter_subject, "TEST.A")

        await nats_client.close()

    async def test_create_consumer_with_metadata(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(metadata={"foo": "bar", "baz": "quux"})
        created_consumer = await test_stream.create_consumer(consumer_config)
        self.assertEqual(created_consumer.cached_info.config.metadata, {"foo": "bar", "baz": "quux"})

        await nats_client.close()

    async def test_create_consumer_with_multiple_filter_subjects(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subjects=["TEST.A", "TEST.B"])
        created_consumer = await test_stream.create_consumer(consumer_config)
        self.assertIsNotNone(created_consumer)
        self.assertEqual(created_consumer.cached_info.config.filter_subjects, ["TEST.A", "TEST.B"])

        await nats_client.close()

    async def test_create_consumer_with_overlapping_filter_subjects(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subjects=["TEST.*", "TEST.B"])
        with self.assertRaises(Exception):
            await test_stream.create_consumer(consumer_config)

        await nats_client.close()

    async def test_create_consumer_with_filter_subjects_and_filter_subject(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subjects=["TEST.A", "TEST.B"], filter_subject="TEST.C")
        with self.assertRaises(Exception):
            await test_stream.create_consumer(consumer_config)

        await nats_client.close()

    async def test_create_consumer_with_empty_filter_subject(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subjects=["TEST.A", ""])
        with self.assertRaises(Exception):
            await test_stream.create_consumer(consumer_config)

        await nats_client.close()

    async def test_create_consumer_with_invalid_filter_subject_leading_dot(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subject=".TEST")
        with self.assertRaises(Exception):
            await test_stream.create_consumer(consumer_config)

        await nats_client.close()

    async def test_create_consumer_with_invalid_filter_subject_trailing_dot(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subject="TEST.")
        with self.assertRaises(Exception):
            await test_stream.create_consumer(consumer_config)

        await nats_client.close()

    async def test_create_consumer_already_exists(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        await test_stream.create_consumer(ConsumerConfig(durable="durable_consumer"))

        with self.assertRaises(Exception):
          await test_stream.create_consumer(ConsumerConfig(durable="durable_consumer", description="test consumer"))

        await nats_client.close()

    # UpdateConsumer tests
    async def test_update_consumer_with_existing_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        original_config = ConsumerConfig(name="test_consumer", description="original description")
        await test_stream.create_consumer(original_config)
        updated_config = ConsumerConfig(name="test_consumer", description="updated description")
        updated_consumer = await test_stream.update_consumer(updated_config)
        self.assertEqual(updated_consumer.cached_info.config.description, "updated description")

        await nats_client.close()

    async def test_update_consumer_with_metadata(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        original_config = ConsumerConfig(name="test_consumer")
        await test_stream.create_consumer(original_config)
        updated_config = ConsumerConfig(name="test_consumer", metadata={"foo": "bar", "baz": "quux"})
        updated_consumer = await test_stream.update_consumer(updated_config)
        self.assertEqual(updated_consumer.cached_info.config.metadata, {"foo": "bar", "baz": "quux"})

        await nats_client.close()

    async def test_update_consumer_illegal_consumer_update(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        original_config = ConsumerConfig(name="test_consumer", ack_policy=AckPolicy.EXPLICIT)
        await test_stream.create_consumer(original_config)
        illegal_config = ConsumerConfig(name="test_consumer", ack_policy=AckPolicy.NONE)
        with self.assertRaises(Exception):
            await test_stream.update_consumer(illegal_config)

        await nats_client.close()

    async def test_update_non_existent_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        non_existent_config = ConsumerConfig(name="non_existent_consumer")
        with self.assertRaises(Exception):
            await test_stream.update_consumer(non_existent_config)

        await nats_client.close()

    # Consumer tests
    async def test_get_existing_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(durable="durable_consumer")
        await test_stream.create_consumer(consumer_config)
        retrieved_consumer = await test_stream.consumer("durable_consumer")
        self.assertIsNotNone(retrieved_consumer)
        self.assertEqual(retrieved_consumer.cached_info.name, "durable_consumer")

        await nats_client.close()

    async def test_get_non_existent_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        with self.assertRaises(Exception):
            await test_stream.consumer("non_existent_consumer")

        await nats_client.close()

    async def test_get_consumer_with_invalid_name(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        with self.assertRaises(Exception):
            await test_stream.consumer("invalid.consumer.name")

        await nats_client.close()

    # CreateOrUpdateConsumer tests
    async def test_create_or_update_durable_pull_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)

        consumer_config = ConsumerConfig(durable="durable_consumer")
        created_consumer = await test_stream.create_or_update_consumer(consumer_config)
        self.assertIsInstance(created_consumer, PullConsumer)
        self.assertEqual(created_consumer.cached_info.name, "durable_consumer")

        await nats_client.close()

    async def test_create_or_update_ephemeral_pull_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(ack_policy=AckPolicy.NONE)
        created_consumer = await test_stream.create_or_update_consumer(consumer_config)
        self.assertIsInstance(created_consumer, PullConsumer)

        await nats_client.close()

    async def test_create_or_update_consumer_with_filter_subject(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subject="TEST.A")
        created_consumer = await test_stream.create_or_update_consumer(consumer_config)
        self.assertIsInstance(created_consumer, PullConsumer)
        self.assertEqual(created_consumer.cached_info.config.filter_subject, "TEST.A")

        await nats_client.close()

    async def test_create_or_update_consumer_with_multiple_filter_subjects(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subjects=["TEST.A", "TEST.B"])
        created_consumer = await test_stream.create_or_update_consumer(consumer_config)
        self.assertIsInstance(created_consumer, PullConsumer)
        self.assertEqual(created_consumer.cached_info.config.filter_subjects, ["TEST.A", "TEST.B"])

        await nats_client.close()

    async def test_create_or_update_consumer_with_overlapping_filter_subjects(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config=StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subjects=["TEST.*", "TEST.B"])
        with self.assertRaises(Exception):
            await test_stream.create_or_update_consumer(consumer_config)

        await nats_client.close()

    async def test_create_or_update_consumer_with_filter_subjects_and_filter_subject(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subjects=["TEST.A", "TEST.B"], filter_subject="TEST.C")
        with self.assertRaises(Exception):
            await test_stream.create_or_update_consumer(consumer_config)

        await nats_client.close()

    async def test_create_or_update_consumer_with_empty_filter_subject(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(filter_subjects=["TEST.A", ""])
        with self.assertRaises(Exception):
            await test_stream.create_or_update_consumer(consumer_config)

        await nats_client.close()

    async def test_create_or_update_existing_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)

        consumer_config = ConsumerConfig(durable="durable_consumer")
        await test_stream.create_or_update_consumer(consumer_config)

        updated_config = ConsumerConfig(durable="durable_consumer", description="test consumer")
        updated_consumer = await test_stream.create_or_update_consumer(updated_config)
        self.assertEqual(updated_consumer.cached_info.config.description, "test consumer")

        await nats_client.close()

    async def test_create_or_with_update_illegal_update_of_existing_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)

        original_config = ConsumerConfig(durable="durable_consumer_2", ack_policy=AckPolicy.EXPLICIT)
        await test_stream.create_or_update_consumer(original_config)

        updated_config = ConsumerConfig(durable="durable_consumer_2", ack_policy=AckPolicy.NONE)
        with self.assertRaises(Exception):
            await test_stream.create_or_update_consumer(updated_config)

        await nats_client.close()

    async def test_create_or_update_consumer_with_invalid_durable(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        invalid_config = ConsumerConfig(durable="invalid.durable.name")
        with self.assertRaises(Exception):
            await test_stream.create_or_update_consumer(invalid_config)

        await nats_client.close()

    # DeleteConsumer tests
    async def test_delete_existing_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        consumer_config = ConsumerConfig(durable="durable_consumer")
        await test_stream.create_consumer(consumer_config)
        await test_stream.delete_consumer("durable_consumer")
        with self.assertRaises(Exception):
            await test_stream.consumer("durable_consumer")

        await nats_client.close()

    async def test_delete_non_existent_consumer(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        with self.assertRaises(Exception):
            await test_stream.delete_consumer("non_existent_consumer")

        await nats_client.close()

    async def test_delete_consumer_with_invalid_name(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        with self.assertRaises(Exception):
            await test_stream.delete_consumer("invalid.consumer.name")

        await nats_client.close()

    # StreamInfo tests
    async def test_stream_info_without_options(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        stream_info = await test_stream.info()
        self.assertIsNotNone(stream_info)
        self.assertEqual(stream_info.config.name, "test_stream")

        await nats_client.close()

    async def test_stream_info_with_deleted_details(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        for i in range(10):
            await jetstream_context.publish("TEST.A", f"msg {i}".encode())
        await test_stream.delete_message(3)
        await test_stream.delete_message(5)
        stream_info = await test_stream.info(deleted_details=True)
        self.assertEqual(stream_info.state.num_deleted, 2)
        self.assertEqual(stream_info.state.deleted, [3, 5])

        await nats_client.close()

    async def test_stream_info_with_subject_filter(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        for i in range(10):
            await jetstream_context.publish("TEST.A", f"msg A {i}".encode())
            await jetstream_context.publish("TEST.B", f"msg B {i}".encode())
        stream_info = await test_stream.info(subject_filter="TEST.A")
        self.assertEqual(stream_info.state.subjects.get("TEST.A"), 10)
        self.assertNotIn("TEST.B", stream_info.state.subjects)

        await nats_client.close()

    async def test_stream_info_timeout(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        with self.assertRaises(asyncio.TimeoutError):
            await test_stream.info(timeout=0.00001)

        await nats_client.close()

    # # SubjectsFilterPaging test
    # async def test_subjects_filter_paging(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     test_stream = await jetstream_context.create_stream(name="test_stream", subjects=["TEST.*"])
    #     for i in range(110000):
    #         await jetstream_context.publish(f"TEST.{i}", b"data")

    #     stream_info = await test_stream.info(subject_filter="TEST.*")
    #     self.assertEqual(len(stream_info.state.subjects), 110000)

    #     cached_info = test_stream.cached_info()
    #     self.assertEqual(len(cached_info.state.subjects), 0)

    #     await nats_client.close()

    # # StreamCachedInfo test
    async def test_stream_cached_info(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"], description="original")
        test_stream = await jetstream_context.create_stream(stream_config)
        self.assertEqual(test_stream.cached_info.config.name, "test_stream")
        self.assertEqual(test_stream.cached_info.config.description, "original")

        updated_stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"], description="updated")
        await jetstream_context.update_stream(updated_stream_config)
        self.assertEqual(test_stream.cached_info.config.description, "original")

        updated_info = await test_stream.info()
        self.assertEqual(updated_info.config.description, "updated")

        await nats_client.close()
        self.assertEqual(test_stream.cached_info.config.description, "original")

        updated_info = await test_stream.info()
        self.assertEqual(updated_info.config.description, "updated")

        await nats_client.close()

    # # GetMsg tests
    async def test_get_existing_message(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        await jetstream_context.publish("TEST.A", b"test message")
        msg = await test_stream.get_msg(1)
        self.assertEqual(msg.data, b"test message")

        await nats_client.close()

    async def test_get_non_existent_message(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        with self.assertRaises(nats.errors.Error):
            await test_stream.get_msg(100)

        await nats_client.close()

    async def test_get_deleted_message(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        await jetstream_context.publish("TEST.A", b"test message")
        await test_stream.delete_message(1)
        with self.assertRaises(nats.errors.Error):
            await test_stream.get_msg(1)

        await nats_client.close()

    async def test_get_message_with_headers(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        headers = {"X-Test": "test value"}
        await jetstream_context.publish("TEST.A", b"test message", headers=headers)
        msg = await test_stream.get_msg(1)
        self.assertEqual(msg.data, b"test message")
        self.assertEqual(msg.headers.get("X-Test"), "test value")

        await nats_client.close()

    async def test_get_message_context_timeout(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        await jetstream_context.publish("TEST.A", b"test message")
        with self.assertRaises(asyncio.TimeoutError):
            async with asyncio.timeout(0.001):
                await test_stream.get_msg(1)

        await nats_client.close()

    # GetLastMsgForSubject tests
    async def test_get_last_message_for_subject(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        for i in range(5):
            await jetstream_context.publish("TEST.A", f"msg A {i}".encode())
            await jetstream_context.publish("TEST.B", f"msg B {i}".encode())
        msg = await test_stream.get_last_msg_for_subject("TEST.A")
        self.assertEqual(msg.data, b"msg A 4")

        await nats_client.close()

    async def test_get_last_message_for_wildcard_subject(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="test_stream", subjects=["TEST.*"])
        test_stream = await jetstream_context.create_stream(stream_config)
        for i in range(5):
            await jetstream_context.publish(f"TEST.{i}", b"data")
        msg = await test_stream.get_last_msg_for_subject("TEST.*")
        self.assertEqual(msg.data, b"data")

        await nats_client.close()
