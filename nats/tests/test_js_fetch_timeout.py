import unittest
from contextlib import ExitStack
from unittest import mock

import nats
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js.client import JetStreamContext


class FetchDeadlineTest(unittest.IsolatedAsyncioTestCase):
    async def test_expired_lingering_request_raises_nats_timeout(self):
        for remaining in (-0.001, 0.00005):
            with self.subTest(remaining=remaining):
                nc = nats.NATS()
                js = nc.jetstream()
                sub = JetStreamContext.PullSubscription(js, Subscription(nc), "TEST", "consumer", b"_INBOX.test")
                expired = Msg(nc, headers={"Status": "408"})
                with ExitStack() as stack:
                    publish = stack.enter_context(mock.patch.object(nc, "publish", new_callable=mock.AsyncMock))
                    stack.enter_context(
                        mock.patch.object(sub, "_next_msg", new_callable=mock.AsyncMock, return_value=expired)
                    )
                    stack.enter_context(mock.patch.object(JetStreamContext, "_time_until", return_value=remaining))
                    with self.assertRaises(nats.errors.TimeoutError):
                        await sub.fetch(10, timeout=0.5)
                    # The initial probe is allowed; an expired lingering pull is not.
                    self.assertEqual(publish.await_count, 1)
