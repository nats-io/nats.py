import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest

import nats
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import api
from nats.js.client import JetStreamContext
from tests.utils import SingleJetStreamServerTestCase, async_test


class PullFetchWaitTest(SingleJetStreamServerTestCase):
    async def connect(self):
        nc = await nats.connect()
        js = nc.jetstream()
        await js.add_stream(name="FETCH", subjects=["fetch"])
        sub = await js.pull_subscribe("fetch", "wait", stream="FETCH")
        return nc, js, sub

    @async_test
    async def test_wait_for_delayed_messages(self):
        nc, js, sub = await self.connect()
        try:
            await js.publish("fetch", b"first")
            task = asyncio.create_task(sub.fetch(2, timeout=1, no_wait=False))
            await nc.flush()
            await asyncio.sleep(0.1)
            assert not task.done()
            await js.publish("fetch", b"second")
            msgs = await task
            assert [msg.data for msg in msgs] == [b"first", b"second"]
        finally:
            await nc.close()

    @async_test
    async def test_return_partial_batch_at_timeout(self):
        nc, js, sub = await self.connect()
        try:
            await js.publish("fetch", b"first")
            task = asyncio.create_task(sub.fetch(2, timeout=0.3, no_wait=False))
            await asyncio.sleep(0.1)
            assert not task.done()
            msgs = await task
            assert [msg.data for msg in msgs] == [b"first"]
            # Expiration must not create another pull that steals later messages.
            await js.publish("fetch", b"second")
            msgs = await sub.fetch(1, timeout=0.5, no_wait=False)
            assert [msg.data for msg in msgs] == [b"second"]
        finally:
            await nc.close()

    @async_test
    async def test_heartbeats_do_not_complete_a_batch(self):
        nc, js, sub = await self.connect()
        try:
            await js.publish("fetch", b"first")
            task = asyncio.create_task(sub.fetch(2, timeout=1, heartbeat=0.05, no_wait=False))
            await asyncio.sleep(0.2)
            assert not task.done()
            await js.publish("fetch", b"second")
            msgs = await task
            assert [msg.data for msg in msgs] == [b"first", b"second"]
        finally:
            await nc.close()

    @async_test
    async def test_empty_batch_raises_timeout(self):
        nc, _, sub = await self.connect()
        try:
            with pytest.raises(nats.errors.TimeoutError):
                await sub.fetch(2, timeout=0.1, no_wait=False)
            with pytest.raises(nats.js.errors.FetchTimeoutError):
                await sub.fetch(2, timeout=0.2, heartbeat=0.03, no_wait=False)
        finally:
            await nc.close()

    @async_test
    async def test_default_behavior_is_preserved(self):
        nc, js, sub = await self.connect()
        try:
            # Each default spelling still returns the available partial batch.
            for kwargs in ({}, {"no_wait": None}, {"no_wait": True}):
                await js.publish("fetch", b"available")
                msgs = await asyncio.wait_for(sub.fetch(2, timeout=1, **kwargs), 0.5)
                assert [msg.data for msg in msgs] == [b"available"]
        finally:
            await nc.close()

    @async_test
    async def test_wait_without_timeout(self):
        nc, js, sub = await self.connect()
        try:
            task = asyncio.create_task(sub.fetch(2, timeout=None, no_wait=False))
            await js.publish("fetch", b"first")
            await asyncio.sleep(0.1)
            assert not task.done()
            await js.publish("fetch", b"second")
            assert [msg.data for msg in await task] == [b"first", b"second"]
        finally:
            await nc.close()

    @async_test
    async def test_cancelled_wait_leaves_subscription_usable(self):
        nc, js, sub = await self.connect()
        try:
            task = asyncio.create_task(sub.fetch(2, timeout=0.2, no_wait=False))
            await asyncio.sleep(0.05)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not sub._sub._pending_next_msgs_calls
            # Cancellation cannot revoke a server pull; it expires on its original deadline.
            await asyncio.sleep(0.2)
            await js.publish("fetch", b"after cancellation")
            msgs = await sub.fetch(1, timeout=0.5, no_wait=False)
            assert [msg.data for msg in msgs] == [b"after cancellation"]
        finally:
            await nc.close()

    @async_test
    async def test_wait_recovers_from_a_stale_pin(self):
        nc, js, sub = await self.connect()
        try:
            if nc.connected_server_version.minor < 11:
                pytest.skip("consumer group pinning requires nats-server v2.11.0 or later")
            await sub.unsubscribe()
            consumer = await js.add_consumer(
                "FETCH",
                api.ConsumerConfig(priority_policy=api.PriorityPolicy.PINNED, priority_groups=["A"]),
            )
            sub = await js.pull_subscribe_bind(consumer.name, "FETCH", priority_group="A")
            for i in range(3):
                await js.publish("fetch", str(i).encode())
            msgs = await sub.fetch(1, timeout=1, no_wait=False)
            await msgs[0].ack_sync()
            old_pin = sub.pin_id
            assert old_pin is not None
            await js._jsm.unpin_consumer("FETCH", consumer.name, "A")
            msgs = await sub.fetch(2, timeout=1, no_wait=False)
            assert [msg.data for msg in msgs] == [b"1", b"2"]
            assert sub.pin_id is not None and sub.pin_id != old_pin
        finally:
            await nc.close()


def pull_subscription(messages=()):
    nc = SimpleNamespace(is_closed=False, publish=AsyncMock())
    js = SimpleNamespace(_nc=nc, _prefix="$JS.API")
    sub = Subscription(nc, subject="inbox")
    for msg in messages:
        sub._pending_queue.put_nowait(msg)
        sub._pending_size += len(msg.data)
    return JetStreamContext.PullSubscription(js, sub, "stream", "consumer", b"inbox")


def status_message(status):
    return Msg(_client=None, headers={api.Header.STATUS: status, api.Header.DESCRIPTION: "test status"})


@pytest.mark.asyncio
async def test_buffered_batch_does_not_publish_or_overfetch():
    psub = pull_subscription(
        [
            status_message("408"),
            Msg(_client=None, data=b"one"),
            Msg(_client=None, data=b"two"),
            Msg(_client=None, data=b"three"),
        ]
    )
    assert [msg.data for msg in await psub.fetch(2, no_wait=False)] == [b"one", b"two"]
    psub._nc.publish.assert_not_awaited()
    assert psub._sub.pending_bytes == len(b"three")
    assert [msg.data for msg in await psub.fetch(1, no_wait=False)] == [b"three"]
    await asyncio.wait_for(psub._sub._pending_queue.join(), 0.1)


@pytest.mark.asyncio
async def test_request_only_the_missing_messages():
    psub = pull_subscription([Msg(_client=None, data=b"one")])
    psub._sub.next_msg = AsyncMock(return_value=Msg(_client=None, data=b"two"))
    assert [msg.data for msg in await psub.fetch(2, no_wait=False)] == [b"one", b"two"]
    request = json.loads(psub._nc.publish.call_args.args[1])
    assert request["batch"] == 1
    assert "no_wait" not in request


@pytest.mark.asyncio
async def test_late_terminal_statuses_do_not_end_a_live_pull():
    psub = pull_subscription()
    psub._sub.next_msg = AsyncMock(
        side_effect=[
            status_message("408"),
            Msg(_client=None, data=b"one"),
            status_message("404"),
            status_message("100"),
            Msg(_client=None, data=b"two"),
        ]
    )
    assert [msg.data for msg in await psub.fetch(2, no_wait=False)] == [b"one", b"two"]
    assert psub._nc.publish.await_count == 1


@pytest.mark.asyncio
async def test_pin_retry_uses_the_remaining_deadline():
    psub = pull_subscription()
    psub._pin_id = "stale"
    psub._group = "group"
    psub._sub.next_msg = AsyncMock(
        side_effect=[
            status_message("423"),
            Msg(_client=None, data=b"one", headers={api.Header.PIN_ID: "new"}),
            Msg(_client=None, data=b"two"),
        ]
    )
    clock = SimpleNamespace(monotonic=Mock(side_effect=[10, 10, 10.1, 10.2, 10.3, 10.4]))
    with patch("nats.js.client.time", clock):
        msgs = await psub.fetch(2, timeout=1, no_wait=False)
    assert [msg.data for msg in msgs] == [b"one", b"two"]
    requests = [json.loads(call.args[1]) for call in psub._nc.publish.call_args_list]
    assert len(requests) == 2
    assert requests[0]["id"] == "stale"
    assert "id" not in requests[1]
    assert requests[1]["expires"] < requests[0]["expires"]
    assert psub.pin_id == "new"


@pytest.mark.asyncio
async def test_pin_retry_is_bounded():
    psub = pull_subscription()
    psub._pin_id = "stale"
    psub._sub.next_msg = AsyncMock(return_value=status_message("423"))
    with pytest.raises(nats.errors.TimeoutError):
        await psub.fetch(2, timeout=1, no_wait=False)
    assert psub._nc.publish.await_count == 2


@pytest.mark.asyncio
async def test_non_temporary_status_is_not_hidden_by_partial_batch():
    psub = pull_subscription()
    psub._sub.next_msg = AsyncMock(side_effect=[Msg(_client=None, data=b"one"), status_message("400")])
    with pytest.raises(nats.js.errors.APIError) as err:
        await psub.fetch(2, no_wait=False)
    assert err.value.code == 400


@pytest.mark.asyncio
async def test_deadline_does_not_send_a_late_request():
    psub = pull_subscription()
    clock = SimpleNamespace(monotonic=Mock(side_effect=[10, 12]))
    with patch("nats.js.client.time", clock):
        with pytest.raises(nats.errors.TimeoutError):
            await psub.fetch(2, timeout=1, no_wait=False)
    psub._nc.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publish_is_bounded_by_the_fetch_deadline():
    psub = pull_subscription()
    blocked = asyncio.Event()

    async def publish(*args):
        await blocked.wait()

    psub._nc.publish.side_effect = publish
    psub._sub.next_msg = AsyncMock()
    with pytest.raises(nats.errors.TimeoutError):
        await asyncio.wait_for(psub.fetch(2, timeout=0.02, no_wait=False), 0.5)
    psub._sub.next_msg.assert_not_awaited()


@pytest.mark.asyncio
async def test_wait_passes_request_options_to_the_server():
    psub = pull_subscription()
    psub._group = "group"
    psub._sub.next_msg = AsyncMock(return_value=Msg(_client=None, data=b"one"))
    await psub.fetch(1, timeout=1, heartbeat=0.1, min_pending=2, min_ack_pending=3, priority=4, no_wait=False)
    request = json.loads(psub._nc.publish.call_args.args[1])
    assert request["group"] == "group"
    assert request["idle_heartbeat"] == 100_000_000
    assert request["min_pending"] == 2
    assert request["min_ack_pending"] == 3
    assert request["priority"] == 4
    assert 0 < request["expires"] < 1_000_000_000
    assert "no_wait" not in request
