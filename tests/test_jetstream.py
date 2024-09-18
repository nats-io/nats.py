import unittest
import asyncio
import time
import nats
import nats.jetstream

from nats.jetstream.stream import *
from nats.jetstream.consumer import *

from .utils import IsolatedJetStreamServerTestCase

class TestJetStream(IsolatedJetStreamServerTestCase):
    # Stream Creation Tests
    async def test_create_stream_ok(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="foo", subjects=["FOO.123"])
        created_stream = await jetstream_context.create_stream(stream_config)
        self.assertIsNotNone(created_stream)
        self.assertEqual(created_stream.cached_info.config.name, "foo")

        await nats_client.close()

    async def test_create_stream_with_metadata(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        metadata = {"foo": "bar", "name": "test"}
        stream_config = StreamConfig(name="foo_meta", subjects=["FOO.meta"], metadata=metadata)
        created_stream = await jetstream_context.create_stream(stream_config)
        self.assertEqual(created_stream.cached_info.config.metadata, metadata)

        await nats_client.close()

    async def test_create_stream_with_metadata_reserved_prefix(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        metadata = {"foo": "bar", "_nats_version": "2.10.0"}
        stream_config = StreamConfig(name="foo_meta1", subjects=["FOO.meta1"], metadata=metadata)
        created_stream = await jetstream_context.create_stream(stream_config)
        self.assertEqual(created_stream.cached_info.config.metadata, metadata)

        await nats_client.close()

    async def test_create_stream_with_empty_context(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="foo_empty_ctx", subjects=["FOO.ctx"])
        created_stream = await jetstream_context.create_stream(stream_config)
        self.assertIsNotNone(created_stream)

        await nats_client.close()

    async def test_create_stream_invalid_name(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(InvalidStreamNameError):
            invalid_stream_config = StreamConfig(name="foo.123", subjects=["FOO.123"])
            await jetstream_context.create_stream(invalid_stream_config)

        await nats_client.close()

    async def test_create_stream_name_required(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(StreamNameRequiredError):
            invalid_stream_config = StreamConfig(name="", subjects=["FOO.123"])
            await jetstream_context.create_stream(invalid_stream_config)

        await nats_client.close()

    async def test_create_stream_name_already_in_use(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="foo", subjects=["FOO.123"])
        created_stream = await jetstream_context.create_stream(stream_config)
        with self.assertRaises(StreamNameAlreadyInUseError):
            await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["BAR.123"]))

        await nats_client.close()

    async def test_create_stream_timeout(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="foo", subjects=["BAR.123"])
        with self.assertRaises(asyncio.TimeoutError):
            await jetstream_context.create_stream(stream_config, timeout=0.00001)

        await nats_client.close()

    # Create or Update Stream Tests
    async def test_create_or_update_stream_create_ok(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="foo", subjects=["FOO.1"])
        created_stream = await jetstream_context.create_stream(stream_config)
        self.assertIsNotNone(created_stream)

        await nats_client.close()


    async def test_create_or_update_stream_invalid_name(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(InvalidStreamNameError):
            invalid_stream_config = StreamConfig(name="foo.123", subjects=["FOO-123"])
            await jetstream_context.create_stream(invalid_stream_config)

        await nats_client.close()

    async def test_create_or_update_stream_name_required(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(StreamNameRequiredError):
            invalid_stream_config = StreamConfig(name="", subjects=["FOO-1234"])
            await jetstream_context.create_stream(invalid_stream_config)

        await nats_client.close()

    async def test_create_or_update_stream_update_ok(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        original_config = StreamConfig(name="foo", subjects=["FOO.1"])
        await jetstream_context.create_stream(original_config)
        updated_config = StreamConfig(name="foo", subjects=["BAR-123"])
        updated_stream = await jetstream_context.update_stream(updated_config)
        self.assertEqual(updated_stream.cached_info.config.subjects, ["BAR-123"])

        await nats_client.close()

    async def test_create_or_update_stream_timeout(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="foo", subjects=["BAR-1234"])
        with self.assertRaises(asyncio.TimeoutError):
            await jetstream_context.create_stream(stream_config, timeout=0.000000001)

        await nats_client.close()

    # Update Stream Tests
    async def test_update_stream_existing(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        original_config = StreamConfig(name="foo", subjects=["FOO.123"])
        await jetstream_context.create_stream(original_config)
        updated_config = StreamConfig(name="foo", subjects=["BAR.123"])
        updated_stream = await jetstream_context.update_stream(updated_config)
        self.assertEqual(updated_stream.cached_info.config.subjects, ["BAR.123"])

        await nats_client.close()

    async def test_update_stream_add_metadata(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        original_config = StreamConfig(name="foo", subjects=["FOO.123"])
        await jetstream_context.create_stream(original_config)
        metadata = {"foo": "bar", "name": "test"}
        updated_config = StreamConfig(name="foo", subjects=["BAR.123"], metadata=metadata)
        updated_stream = await jetstream_context.update_stream(updated_config)
        self.assertEqual(updated_stream.cached_info.config.metadata, metadata)

        await nats_client.close()

    async def test_update_stream_invalid_name(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(InvalidStreamNameError):
            invalid_config = StreamConfig(name="foo.123", subjects=["FOO.123"])
            await jetstream_context.update_stream(invalid_config)

        await nats_client.close()

    async def test_update_stream_name_required(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(StreamNameRequiredError):
            invalid_config = StreamConfig(name="", subjects=["FOO.123"])
            await jetstream_context.update_stream(invalid_config)

        await nats_client.close()

    async def test_update_stream_not_found(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        nonexistent_config = StreamConfig(name="bar", subjects=["FOO.123"])
        with self.assertRaises(StreamNotFoundError):
            await jetstream_context.update_stream(nonexistent_config)

        await nats_client.close()

    # Get Stream Tests
    async def test_get_stream_existing(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="foo", subjects=["FOO.123"])
        await jetstream_context.create_stream(stream_config)
        existing_stream = await jetstream_context.stream("foo")
        self.assertIsNotNone(existing_stream)
        self.assertEqual(existing_stream.cached_info.config.name, "foo")

        await nats_client.close()

    async def test_get_stream_invalid_name(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(InvalidStreamNameError):
            await jetstream_context.stream("foo.123")

        await nats_client.close()

    async def test_get_stream_name_required(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(StreamNameRequiredError):
            await jetstream_context.stream("")

        await nats_client.close()

    async def test_get_stream_not_found(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(StreamNotFoundError):
            await jetstream_context.stream("bar")

        await nats_client.close()

    # Delete Stream Tests
    async def test_delete_stream_existing(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(name="foo", subjects=["FOO.123"])
        await jetstream_context.create_stream(stream_config)
        await jetstream_context.delete_stream("foo")
        with self.assertRaises(StreamNotFoundError):
            await jetstream_context.stream("foo")

        await nats_client.close()

    async def test_delete_stream_invalid_name(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(InvalidStreamNameError):
            await jetstream_context.delete_stream("foo.123")

        await nats_client.close()

    async def test_delete_stream_name_required(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(StreamNameRequiredError):
            await jetstream_context.delete_stream("")

        await nats_client.close()

    async def test_delete_stream_not_found(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        with self.assertRaises(StreamNotFoundError):
            await jetstream_context.delete_stream("foo")

        await nats_client.close()

    # # List Streams Tests
    # async def test_list_streams(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     for i in range(500):
    #         await jetstream_context.create_stream(StreamConfig(name=f"foo{i}", subjects=[f"FOO.{i}"]))
    #     streams = [stream async for stream in jetstream_manager.streams()]
    #     self.assertEqual(len(streams), 500)

    #     await nats_client.close()

    # async def test_list_streams_with_subject_filter(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     for i in range(260):
    #         await jetstream_context.create_stream(StreamConfig(name=f"foo{i}", subjects=[f"FOO.{i}"]))
    #         streams = [stream async for stream in jetstream_context.streams(subject="FOO.123")]
    #         self.assertEqual(len(streams), 1)

    #     await nats_client.close()

    # async def test_list_streams_with_subject_filter_no_match(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     for i in range(100):
    #         await jetstream_context.create_stream(StreamConfig(name=f"foo{i}", subjects=[f"FOO.{i}"]))
    #     streams = [stream async for stream in jetstream_manager.streams(subject="FOO.500")]
    #     self.assertEqual(len(streams), 0)

    #     await nats_client.close()

    # async def test_list_streams_timeout(self):
    #     nats_client = await nats.connect("nats://localhost:4222")

    #     with self.assertRaises(asyncio.TimeoutError):
    #         async with asyncio.timeout(timeout=0.000001):
    #             streams = [stream async for stream in jetstream_manager.streams()]

    #     await nats_client.close()

    # # Stream Names Tests
    # async def test_stream_names(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     for i in range(500):
    #         await jetstream_context.create_stream(StreamConfig(name=f"foo{i}", subjects=[f"FOO.{i}"]))
    #     names = [name async for name in jetstream_manager.stream_names()]
    #     self.assertEqual(len(names), 500)

    #     await nats_client.close()

    # async def test_stream_names_with_subject_filter(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     for i in range(260):
    #         await jetstream_context.create_stream(StreamConfig(name=f"foo{i}", subjects=[f"FOO.{i}"]))
    #     names = [name async for name in jetstream_manager.stream_names(subject="FOO.123")]
    #     self.assertEqual(len(names), 1)

    #     await nats_client.close()

    # async def test_stream_names_with_subject_filter_no_match(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     for i in range(100):
    #         await jetstream_context.create_stream(StreamConfig(name=f"foo{i}", subjects=[f"FOO.{i}"]))
    #     names = [name async for name in jetstream_manager.stream_names(subject="FOO.500")]
    #     self.assertEqual(len(names), 0)

    #     await nats_client.close()

    # async def test_stream_names_timeout(self):
    #     nats_client = await nats.connect("nats://localhost:4222")

    #     with self.assertRaises(asyncio.TimeoutError):
    #         async with asyncio.timeout(timeout=0.000001):
    #             names = [name async for name in jetstream_manager.stream_names()]

    #     await nats_client.close()

    # # Stream by Subject Tests
    # async def test_stream_name_by_subject_explicit(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     stream_name = await jetstream_manager.stream_name_by_subject("FOO.123")
    #     self.assertEqual(stream_name, "foo")

    #     await nats_client.close()

    # async def test_stream_name_by_subject_wildcard(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="bar", subjects=["BAR.*"]))
    #     stream_name = await jetstream_manager.stream_name_by_subject("BAR.*")
    #     self.assertEqual(stream_name, "bar")

    #     await nats_client.close()

    # async def test_stream_name_by_subject_not_found(self):
    #     nats_client = await nats.connect("nats://localhost:4222")

    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_manager.stream_name_by_subject("BAR.XYZ")

    #     await nats_client.close()

    # async def test_stream_name_by_subject_invalid(self):
    #     nats_client = await nats.connect("nats://localhost:4222")

    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_manager.stream_name_by_subject("FOO.>.123")

    #     await nats_client.close()

    # async def test_stream_name_by_subject_timeout(self):
    #     nats_client = await nats.connect("nats://localhost:4222")

    #     with self.assertRaises(asyncio.TimeoutError):
    #         async with asyncio.timeout(timeout=0.000001):
    #             await jetstream_manager.stream_name_by_subject("FOO.123")

    #     await nats_client.close()

    # # Consumer Tests
    # async def test_create_or_update_consumer_create_durable_pull(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     consumer = await jetstream_context.create_consumer("foo", consumer_config)
    #     self.assertIsNotNone(consumer)
    #     self.assertEqual(consumer.name, "dur")

    #     await nats_client.close()

    # async def test_create_or_update_consumer_create_ephemeral_pull(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(ack_policy=ConsumerConfig.AckExplicit)
    #     consumer = await jetstream_context.create_consumer("foo", consumer_config)
    #     self.assertIsNotNone(consumer)

    #     await nats_client.close()

    # async def test_create_or_update_consumer_update(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     await jetstream_context.create_consumer("foo", consumer_config)
    #     updated_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit, description="test consumer")
    #     updated_consumer = await jetstream_context.update_consumer("foo", updated_config)
    #     self.assertEqual(updated_consumer.config.description, "test consumer")

    #     await nats_client.close()

    # async def test_create_or_update_consumer_illegal_update(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     await jetstream_context.create_consumer("foo", consumer_config)
    #     illegal_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckNone)
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.update_consumer("foo", illegal_config)

    #     await nats_client.close()

    # async def test_create_or_update_consumer_stream_not_found(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.create_consumer("nonexistent", consumer_config)

    #     await nats_client.close()

    # async def test_create_or_update_consumer_invalid_stream_name(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.create_consumer("foo.1", consumer_config)

    #     await nats_client.close()

    # async def test_create_or_update_consumer_invalid_durable_name(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur.123", ack_policy=ConsumerConfig.AckExplicit)
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.create_consumer("foo", consumer_config)

    #     await nats_client.close()

    # async def test_create_or_update_consumer_timeout(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     with self.assertRaises(asyncio.TimeoutError):
    #         async with asyncio.timeout(timeout=0.000001):
    #             await jetstream_context.create_consumer("foo", consumer_config)

    #     await nats_client.close()

    # # Get Consumer Tests
    # async def test_get_consumer_existing(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     await jetstream_context.create_consumer("foo", consumer_config)
    #     consumer = await jetstream_context.consumer_info("foo", "dur")
    #     self.assertIsNotNone(consumer)
    #     self.assertEqual(consumer.name, "dur")

    #     await nats_client.close()

    # async def test_get_consumer_not_found(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.consumer_info("foo", "nonexistent")

    #     await nats_client.close()

    # async def test_get_consumer_invalid_name(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.consumer_info("foo", "dur.123")

    #     await nats_client.close()

    # async def test_get_consumer_stream_not_found(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.consumer_info("nonexistent", "dur")

    #     await nats_client.close()

    # async def test_get_consumer_invalid_stream_name(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.consumer_info("foo.1", "dur")

    #     await nats_client.close()

    # async def test_get_consumer_timeout(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     await jetstream_context.create_consumer("foo", consumer_config)
    #     with self.assertRaises(asyncio.TimeoutError):
    #         async with asyncio.timeout(timeout=0.000001):
    #             await jetstream_context.consumer_info("foo", "dur")

    #     await nats_client.close()

    # # Delete Consumer Tests
    # async def test_delete_consumer_existing(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats.jetstream.new(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.ACK_EXPLICIT)
    #     await jetstream_context.create_consumer("foo", consumer_config)
    #     await jetstream_context.delete_consumer("foo", "dur")
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.consumer_info("foo", "dur")

    #     await nats_client.close()

    # async def test_delete_consumer_not_found(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.delete_consumer("foo", "nonexistent")

    #     await nats_client.close()

    # async def test_delete_consumer_invalid_name(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.delete_consumer("foo", "dur.123")

    #     await nats_client.close()

    # async def test_delete_consumer_stream_not_found(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.delete_consumer("nonexistent", "dur")

    #     await nats_client.close()

    # async def test_delete_consumer_invalid_stream_name(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     with self.assertRaises(Exception):  # Replace with specific exception
    #         await jetstream_context.delete_consumer("foo.1", "dur")

    #     await nats_client.close()

    # async def test_delete_consumer_timeout(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     consumer_config = ConsumerConfig(durable_name="dur", ack_policy=ConsumerConfig.AckExplicit)
    #     await jetstream_context.create_consumer("foo", consumer_config)
    #     with self.assertRaises(asyncio.TimeoutError):
    #         async with asyncio.timeout(timeout=0.000001):
    #             await jetstream_context.delete_consumer("foo", "dur")

    #     await nats_client.close()

    # # JetStream Account Info Tests
    # async def test_account_info(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()
    #     jetstream_manager = JetStreamManager(nats_client)

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     info = await jetstream_manager.account_info()
    #     self.assertIsNotNone(info)
    #     self.assertGreaterEqual(info.streams, 1)

    #     await nats_client.close()

    # Stream Config Tests
    async def test_stream_config_matches(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        stream_config = StreamConfig(
            name="stream",
            subjects=["foo.*"],
            retention=RetentionPolicy.LIMITS,
            max_consumers=-1,
            max_msgs=-1,
            max_bytes=-1,
            discard=DiscardPolicy.OLD,
            max_age=0,
            max_msgs_per_subject=-1,
            max_msg_size=-1,
            storage=StorageType.FILE,
            replicas=1,
            no_ack=False,
            discard_new_per_subject=False,
            duplicates=120 * 1000000000,  # 120 seconds in nanoseconds
            placement=None,
            mirror=None,
            sources=None,
            sealed=False,
            deny_delete=False,
            deny_purge=False,
            allow_rollup=False,
            compression=StoreCompression.NONE,
            first_sequence=0,
            subject_transform=None,
            republish=None,
            allow_direct=False,
            mirror_direct=False,
        )

        stream = await jetstream_context.create_stream(stream_config)
        self.assertEqual(stream.cached_info.config, stream_config)

        await nats_client.close()

    # # Consumer Config Tests
    # async def test_consumer_config_matches(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     await jetstream_context.create_stream(StreamConfig(name="FOO", subjects=["foo.*"]))
    #     config = ConsumerConfig(
    #         durable_name="cons",
    #         description="test",
    #         deliver_policy=ConsumerConfig.DeliverAll,
    #         opt_start_seq=0,
    #         opt_start_time=None,
    #         ack_policy=ConsumerConfig.AckExplicit,
    #         ack_wait=30 * 1000000000,  # 30 seconds in nanoseconds
    #         max_deliver=1,
    #         filter_subject="",
    #         replay_policy=ConsumerConfig.ReplayInstant,
    #         rate_limit_bps=0,
    #         sample_freq="",
    #         max_waiting=1,
    #         max_ack_pending=1000,
    #         headers_only=False,
    #         max_batch=0,
    #         max_expires=0,
    #         inactive_threshold=0,
    #         num_replicas=1,
    #     )
    #     consumer = await jetstream_context.create_consumer("FOO", config)
    #     self.assertEqual(consumer.config, config)

    #     await nats_client.close()

    # JetStream Publish Tests
    async def test_publish(self):
        nats_client = await nats.connect("nats://localhost:4222")
        jetstream_context = nats.jetstream.new(nats_client)

        await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
        ack = await jetstream_context.publish("FOO.bar", b"Hello World")
        self.assertIsNotNone(ack)
        self.assertGreater(ack.sequence, 0)

        await nats_client.close()

    # # JetStream Subscribe Tests
    # async def test_subscribe_push(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     sub = await jetstream_context.subscribe("FOO.*")
    #     await jetstream_context.publish("FOO.bar", b"Hello World")
    #     msg = await sub.next_msg()
    #     self.assertEqual(msg.data, b"Hello World")

    #     await nats_client.close()

    # async def test_subscribe_pull(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     await jetstream_context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))
    #     sub = await jetstream_context.pull_subscribe("FOO.*", "consumer")
    #     await jetstream_context.publish("FOO.bar", b"Hello World")
    #     msgs = await sub.fetch(1)
    #     self.assertEqual(len(msgs), 1)
    #     self.assertEqual(msgs[0].data, b"Hello World")

    #     await nats_client.close()

    # # JetStream Stream Transform Tests
    # async def test_stream_transform(self):
    #     nats_client = await nats.connect("nats://localhost:4222")
    #     jetstream_context = nats_client.jetstream()

    #     origin_config = StreamConfig(
    #         name="ORIGIN",
    #         subjects=["test"],
    #         storage=StorageType.MEMORY,
    #         subject_transform=SubjectTransformConfig(source=">", destination="transformed.>")
    #     )
    #     await jetstream_context.create_stream(origin_config)

    #     await nats_client.publish("test", b"1")

    #     sourcing_config = StreamConfig(
    #         name="SOURCING",
    #         storage=StreamConfig.MemoryStorage,
    #         sources=[
    #             StreamSource(
    #                 name="ORIGIN",
    #                 subject_transforms=[
    #                     StreamConfig.SubjectTransform(src=">", dest="fromtest.>")
    #                 ]
    #             )
    #         ]
    #     )
    #     sourcing_stream = await jetstream_context.create_stream(sourcing_config)

    #     consumer_config = ConsumerConfig(
    #         filter_subject="fromtest.>",
    #         max_deliver=1,
    #     )
    #     consumer = await sourcing_stream.create_consumer(consumer_config)

    #     msg = await consumer.next_msg()
    #     self.assertEqual(msg.subject, "fromtest.transformed.test")

    #     await nats_client.close()
