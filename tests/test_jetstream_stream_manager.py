import nats
import nats.jetstream

from tests.utils import SingleServerTestCase, async_test

class TestJetStreamStreamManager(SingleServerTestCase):
    @async_test
    async def test_create_stream_success(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        stream = await js.create_stream(StreamConfig("foo", ["FOO.123"]))
        self.assertEqual(stream.name, "foo")
        self.assertEqual(stream.subjects, ["FOO.123"])

    @async_test
    async def test_create_stream_with_metadata(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        metadata = {"foo": "bar", "name": "test"}
        stream = await js.create_stream(StreamConfig("foo_meta", ["FOO.meta"], metadata))
        self.assertEqual(stream.name, "foo_meta")
        self.assertEqual(stream.subjects, ["FOO.meta"])
        self.assertEqual(stream.metadata, metadata)

    @async_test
    async def test_create_stream_invalid_name(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(InvalidStreamNameError):
            await js.create_stream(StreamConfig("foo.123", ["FOO.123"]))

    @async_test
    async def test_create_stream_name_required(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(StreamNameRequiredError):
            await js.create_stream(StreamConfig("", ["FOO.123"]))

    @async_test
    async def test_create_stream_name_already_in_use(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        await js.create_stream(StreamConfig("foo", ["FOO.123"]))
        with self.assertRaises(StreamNameAlreadyInUseError):
            await js.create_stream(StreamConfig("foo", ["BAR.123"]))

    @async_test
    async def test_update_stream_success(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        await js.create_stream(StreamConfig("foo", ["FOO.123"]))
        stream = await js.update_stream(StreamConfig("foo", ["BAR.123"]))
        info = await stream.info()
        self.assertEqual(info.config.subjects, ["BAR.123"])

    @async_test
    async def test_update_stream_add_metadata(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        await js.create_stream(StreamConfig("foo", ["FOO.123"]))
        metadata = {"foo": "bar", "name": "test"}
        stream = await js.update_stream(StreamConfig("foo", ["BAR.123"], metadata))
        info = await stream.info()
        self.assertEqual(info.config.subjects, ["BAR.123"])
        self.assertEqual(info.config.metadata, metadata)

    @async_test
    async def test_update_stream_invalid_name(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(InvalidStreamNameError):
            await js.update_stream(StreamConfig("foo.123", ["FOO.123"]))

    @async_test
    async def test_update_stream_name_required(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(StreamNameRequiredError):
            await js.update_stream(StreamConfig("", ["FOO.123"]))

    @async_test
    async def test_update_stream_not_found(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(StreamNotFoundError):
            await js.update_stream(StreamConfig("bar", ["FOO.123"]))

    @async_test
    async def test_get_stream_success(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        await js.create_stream(StreamConfig("foo", ["FOO.123"]))
        stream = await js.stream("foo")
        self.assertEqual(stream.cached_info().config.name, "foo")

    @async_test
    async def test_get_stream_invalid_name(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(InvalidStreamNameError):
            await js.stream("foo.123")

    @async_test
    async def test_get_stream_name_required(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(StreamNameRequiredError):
            await js.stream("")

    @async_test
    async def test_get_stream_not_found(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(StreamNotFoundError):
            await js.stream("bar")

    @async_test
    async def test_delete_stream_success(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        await js.create_stream(StreamConfig("foo", ["FOO.123"]))
        await js.delete_stream("foo")
        with self.assertRaises(StreamNotFoundError):
            await js.stream("foo")

    @async_test
    async def test_delete_stream_invalid_name(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(InvalidStreamNameError):
            await js.delete_stream("foo.123")

    @async_test
    async def test_delete_stream_name_required(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(StreamNameRequiredError):
            await js.delete_stream("")

    @async_test
    async def test_delete_stream_not_found(self):
        nc = await nats.connect()
        js = nc.jetstream.new(nc)
        with self.assertRaises(StreamNotFoundError):
            await js.delete_stream("bar")
