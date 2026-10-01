import datetime
from types import SimpleNamespace

import pytest

from nats.js.kv import KeyValue


def make_fake_jetstream(*consumer_infos, delivered=None):
    subscription_delivered = consumer_infos[0][1] if delivered is None else delivered

    class FakeSubscription:
        delivered = subscription_delivered

        def __init__(self):
            self.info_calls = 0

        async def consumer_info(self):
            num_pending, consumer_seq = consumer_infos[min(self.info_calls, len(consumer_infos) - 1)]
            self.info_calls += 1
            return SimpleNamespace(
                num_pending=num_pending,
                delivered=SimpleNamespace(consumer_seq=consumer_seq),
            )

        async def unsubscribe(self):
            pass

    class FakeJetStream:
        def __init__(self):
            self._timeout = 1
            self.callback = None

        async def subscribe(self, subject, *, cb, **kwargs):
            self.callback = cb
            return FakeSubscription()

    return FakeJetStream()


def message(key, revision, pending):
    return SimpleNamespace(
        subject=f"$KV.TEST.{key}",
        data=b"value",
        header=None,
        metadata=SimpleNamespace(
            sequence=SimpleNamespace(stream=revision),
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            num_pending=pending,
        ),
    )


@pytest.mark.asyncio
async def test_watch_initial_marker_ignores_transient_zero_pending():
    js = make_fake_jetstream((2, 1))
    kv = KeyValue(name="TEST", stream="KV_TEST", pre="$KV.TEST.", js=js, direct=False)
    watcher = await kv.watchall()

    await js.callback(message("one", 1, 1))
    await js.callback(message("two", 2, 0))
    await js.callback(message("three", 3, 0))

    assert (await watcher.updates()).key == "one"
    assert (await watcher.updates()).key == "two"
    assert (await watcher.updates()).key == "three"
    assert await watcher.updates() is None


@pytest.mark.asyncio
async def test_watch_initial_marker_waits_for_displaced_update():
    js = make_fake_jetstream((2, 0))
    kv = KeyValue(name="TEST", stream="KV_TEST", pre="$KV.TEST.", js=js, direct=False)
    watcher = await kv.watchall()

    await js.callback(message("B", 2, 2))
    await js.callback(message("C", 3, 1))
    await js.callback(message("A", 4, 0))

    assert (await watcher.updates()).key == "B"
    assert (await watcher.updates()).key == "C"
    third = await watcher.updates()
    assert third is not None, "initial marker emitted before the updated key A"
    assert third.key == "A"
    assert await watcher.updates() is None


@pytest.mark.asyncio
async def test_watch_initial_marker_reconciles_shrinking_pending_count():
    js = make_fake_jetstream((3, 0), (0, 1))
    kv = KeyValue(name="TEST", stream="KV_TEST", pre="$KV.TEST.", js=js, direct=False)
    watcher = await kv.watchall(include_history=True)

    await js.callback(message("A", 4, 0))

    assert (await watcher.updates()).key == "A"
    assert await watcher.updates() is None


@pytest.mark.asyncio
async def test_watch_initial_marker_waits_for_server_deliveries_in_flight():
    js = make_fake_jetstream((0, 2), delivered=0)
    kv = KeyValue(name="TEST", stream="KV_TEST", pre="$KV.TEST.", js=js, direct=False)
    watcher = await kv.watchall()

    await js.callback(message("one", 1, 1))
    await js.callback(message("two", 2, 0))

    assert (await watcher.updates()).key == "one"
    assert (await watcher.updates()).key == "two"
    assert await watcher.updates() is None
