from tests.utils import SingleJetStreamServerTestCase

import nats
import nats.jetstream

from nats.jetstream.stream import StreamConfig
from nats.jetstream.consumer import ConsumerConfig, AckPolicy
from nats.jetstream.errors import *

class JetStreamStreamTest(SingleJetStreamServerTestCase):
    async def test_create_or_update_consumer(self):
        tests = [
            {
                "name": "create durable pull consumer",
                "consumer_config": ConsumerConfig(durable="dur"),
                "should_create": True,
                "with_error": None
            },
            {
                "name": "create ephemeral pull consumer",
                "consumer_config": ConsumerConfig(ack_policy=AckPolicy.NONE),
                "should_create": True,
                "with_error": None
            },
            {
                "name": "with filter subject",
                "consumer_config": ConsumerConfig(filter_subject="FOO.A"),
                "should_create": True,
                "with_error": None
            },
            {
                "name": "with multiple filter subjects",
                "consumer_config": ConsumerConfig(filter_subjects=["FOO.A", "FOO.B"]),
                "should_create": True,
                "with_error": None
            },
            {
                "name": "with multiple filter subjects, overlapping subjects",
                "consumer_config": ConsumerConfig(filter_subjects=["FOO.*", "FOO.B"]),
                "should_create": False,
                "with_error": OverlappingFilterSubjectsError
            },
            {
                "name": "with multiple filter subjects and filter subject provided",
                "consumer_config": ConsumerConfig(filter_subjects=["FOO.A", "FOO.B"], filter_subject="FOO.C"),
                "should_create": False,
                "with_error": DuplicateFilterSubjectsError
            },
            {
                "name": "with empty subject in filter subjects",
                "consumer_config": ConsumerConfig(filter_subjects=["FOO.A", ""]),
                "should_create": False,
                "with_error": EmptyFilterError
            },
            {
                "name": "consumer already exists, update",
                "consumer_config": ConsumerConfig(durable="dur", description="test consumer"),
                "should_create": True,
                "with_error": None
            },
            {
                "name": "consumer already exists, illegal update",
                "consumer_config": ConsumerConfig(durable="dur", ack_policy=AckPolicy.NONE),
                "should_create": False,
                "with_error": ConsumerCreateError
            },
            {
                "name": "invalid durable name",
                "consumer_config": ConsumerConfig(durable="dur.123"),
                "should_create": False,
                "with_error": InvalidConsumerNameError
            },
        ]

        client = await nats.connect()
        context = await nats.jetstream.new(client)
        stream = await context.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))

        for test in tests:
            with self.subTest(test=test["name"]):
                try:
                    if test["consumer_config"].filter_subject:
                        subscription = await client.subscribe(f"$JS.API.CONSUMER.CREATE.foo.*.{test['consumer_config'].filter_subject}")
                    else:
                        subscription = await client.subscribe("$JS.API.CONSUMER.CREATE.foo.*")

                    consumer = await stream.create_or_update_consumer(test["consumer_config"])

                    if test["with_error"]:
                        self.fail(f"Expected error: {test['with_error']}; got: None")
                    if test["should_create"]:
                        self.assertIsNotNone(await subscription.next_msg())
                except Exception as e:
                    if not test["with_error"]:
                        self.fail(f"Unexpected error: {e}")
                    if not isinstance(e, test["with_error"]):
                        self.fail(f"Expected error: {test['with_error']}; got: {e}")

async def test_create_consumer(self):
    tests = [
        {
            "name": "create durable pull consumer",
            "consumer_config": ConsumerConfig(durable="dur"),
            "should_create": True,
            "with_error": None
        },
        {
            "name": "idempotent create, no error",
            "consumer_config": ConsumerConfig(durable="dur"),
            "should_create": True,
            "with_error": None
        },
        {
            "name": "create ephemeral pull consumer",
            "consumer_config": ConsumerConfig(ack_policy=AckPolicy.NONE),
            "should_create": True,
            "with_error": None
        },
        {
            "name": "with filter subject",
            "consumer_config": ConsumerConfig(filter_subject="FOO.A"),
            "should_create": True,
            "with_error": None
        },
        {
            "name": "with metadata",
            "consumer_config": ConsumerConfig(metadata={"foo": "bar", "baz": "quux"}),
            "should_create": True,
            "with_error": None
        },
        {
            "name": "with multiple filter subjects",
            "consumer_config": ConsumerConfig(filter_subjects=["FOO.A", "FOO.B"]),
            "should_create": True,
            "with_error": None
        },
        {
            "name": "with multiple filter subjects, overlapping subjects",
            "consumer_config": ConsumerConfig(filter_subjects=["FOO.*", "FOO.B"]),
            "should_create": False,
            "with_error": OverlappingFilterSubjectsError
        },
        {
            "name": "with multiple filter subjects and filter subject provided",
            "consumer_config": ConsumerConfig(filter_subjects=["FOO.A", "FOO.B"], filter_subject="FOO.C"),
            "should_create": False,
            "with_error": DuplicateFilterSubjectsError
        },
        {
            "name": "with empty subject in filter subjects",
            "consumer_config": ConsumerConfig(filter_subjects=["FOO.A", ""]),
            "should_create": False,
            "with_error": EmptyFilterError
        },
        {
            "name": "with invalid filter subject, leading dot",
            "consumer_config": ConsumerConfig(filter_subject=".foo"),
            "should_create": False,
            "with_error": InvalidConsumerNameError
        },
        {
            "name": "with invalid filter subject, trailing dot",
            "consumer_config": ConsumerConfig(filter_subject="foo."),
            "should_create": False,
            "with_error": InvalidConsumerNameError
        },
        {
            "name": "consumer already exists, error",
            "consumer_config": ConsumerConfig(durable="dur", description="test consumer"),
            "should_create": False,
            "with_error": ConsumerExistsError
        },
        {
            "name": "invalid durable name",
            "consumer_config": ConsumerConfig(durable="dur.123"),
            "should_create": False,
            "with_error": InvalidConsumerNameError
        },
    ]

    nc = await nats.connect()
    js = await nats.jetstream.new(nc)
    s = await js.create_stream(StreamConfig(name="foo", subjects=["FOO.*"]))

    for test in tests:
        with self.subTest(test=test["name"]):
            try:
                if test["consumer_config"].filter_subject:
                    sub = await self.nc.subscribe(f"$JS.API.CONSUMER.CREATE.foo.*.{test['consumer_config'].filter_subject}")
                else:
                    sub = await self.nc.subscribe("$JS.API.CONSUMER.CREATE.foo.*")

                c = await s.create_consumer(test["consumer_config"])

                if test["with_error"]:
                    self.fail(f"Expected error: {test['with_error']}; got: None")
                if test["should_create"]:
                    msg = await sub.next_msg()
                    self.assertIsNotNone(msg)
            except Exception as e:
                if not test["with_error"]:
                    self.fail(f"Unexpected error: {e}")
                if not isinstance(e, test["with_error"]):
                    self.fail(f"Expected error: {test['with_error']}; got: {e}")
