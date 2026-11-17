from datetime import datetime, timezone

import nats
from nats.js.api import ConsumerConfig, DeliverPolicy
from tests.utils import SingleJetStreamServerTestCase, async_test


class OrderedConsumerTimeRecoveryTest(SingleJetStreamServerTestCase):
    @async_test
    async def test_time_based_consumer_reset(self):
        errors = []

        async def on_error(error):
            errors.append(error)

        nc = await nats.connect(error_cb=on_error)
        try:
            js = nc.jetstream()
            await js.add_stream(name="TIME_RESET", subjects=["time.reset"])
            sub = await js.subscribe(
                "time.reset",
                ordered_consumer=True,
                config=ConsumerConfig(
                    deliver_policy=DeliverPolicy.BY_START_TIME,
                    opt_start_time=datetime.now(timezone.utc),
                ),
            )
            await js.publish("time.reset", b"first")
            self.assertEqual((await sub.next_msg(timeout=2)).data, b"first")

            # Recovery must clear the start time when switching to a sequence.
            jsi = sub._sub._jsi
            assert jsi is not None
            await jsi.reset_ordered_consumer(2)
            await js.publish("time.reset", b"second")
            self.assertEqual((await sub.next_msg(timeout=2)).data, b"second")
            self.assertEqual(errors, [])
        finally:
            await nc.close()
