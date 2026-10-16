import asyncio
from unittest import mock

import nats
import pytest
from nats.js.errors import NoKeysError
from nats.js.kv import KeyValue
from tests.utils import SingleJetStreamServerTestCase, async_test


class KVWatcherCleanupTest(SingleJetStreamServerTestCase):
    async def assert_watcher_stopped(self, nc, watcher, subscriptions_before):
        assert len(nc._subs) == subscriptions_before
        assert watcher._sub._id not in nc._subs
        state = watcher._sub._sub._jsi
        tasks = [watcher._sub._wait_for_msgs_task, state._hbtask, state._fctask]
        tasks = [task for task in tasks if task is not None]
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=1)
        assert all(task.done() for task in tasks)

    @async_test
    async def test_repeated_housekeeping_stops_watchers_and_allows_drain(self):
        errors = []

        async def error_cb(error):
            errors.append(error)

        nc = await nats.connect(error_cb=error_cb, drain_timeout=0.5)
        try:
            js = nc.jetstream()
            kv = await js.create_key_value(bucket="CLEANUP", history=2)
            await kv.put("alive", b"value")
            await kv.put("deleted", b"value")
            await kv.delete("deleted")
            await kv.put("purged", b"value")
            await kv.purge("purged")
            subscriptions_before = len(nc._subs)
            watchers = []
            watchall = kv.watchall

            async def track_watcher(**kwargs):
                watcher = await watchall(**kwargs)
                watchers.append(watcher)
                return watcher

            with mock.patch.object(kv, "watchall", side_effect=track_watcher):
                for _ in range(3):
                    assert await kv.purge_deletes(olderthan=0)
                    await self.assert_watcher_stopped(nc, watchers[-1], subscriptions_before)
                    assert await kv.keys() == ["alive"]
                    await self.assert_watcher_stopped(nc, watchers[-1], subscriptions_before)

                with pytest.raises(NoKeysError):
                    await kv.keys(filters=["absent"])
                await self.assert_watcher_stopped(nc, watchers[-1], subscriptions_before)

            assert (await js.stream_info("KV_CLEANUP")).state.messages == 1
            for i in range(300):
                await kv.put("alive", str(i).encode())
            assert (await kv.get("alive")).value == b"299"
            await asyncio.wait_for(nc.drain(), timeout=1)
            assert not errors
        finally:
            await nc.close()

    @async_test
    async def test_housekeeping_stops_watcher_on_iteration_error(self):
        nc = await nats.connect()
        try:
            js = nc.jetstream()
            kv = await js.create_key_value(bucket="ITERATION_ERROR")
            await kv.put("alive", b"value")
            for method in (kv.purge_deletes, kv.keys):
                with self.subTest(method=method.__name__):
                    subscriptions_before = len(nc._subs)
                    watcher = await kv.watchall()
                    error = RuntimeError("watcher iteration failed")
                    with mock.patch.object(kv, "watchall", return_value=watcher):
                        with mock.patch.object(KeyValue.KeyWatcher, "__anext__", side_effect=error):
                            with pytest.raises(RuntimeError) as raised:
                                await method()
                            assert raised.value is error
                    await self.assert_watcher_stopped(nc, watcher, subscriptions_before)
        finally:
            await nc.close()

    @async_test
    async def test_housekeeping_stops_watcher_on_iteration_cancellation(self):
        nc = await nats.connect()
        try:
            js = nc.jetstream()
            kv = await js.create_key_value(bucket="ITERATION_CANCEL")
            await kv.put("alive", b"value")
            for method in (kv.purge_deletes, kv.keys):
                with self.subTest(method=method.__name__):
                    subscriptions_before = len(nc._subs)
                    watcher = await kv.watchall()
                    entered = asyncio.Event()

                    async def blocked_next():
                        entered.set()
                        await asyncio.Future()

                    with mock.patch.object(kv, "watchall", return_value=watcher):
                        with mock.patch.object(KeyValue.KeyWatcher, "__anext__", side_effect=blocked_next):
                            task = asyncio.create_task(method())
                            try:
                                await asyncio.wait_for(entered.wait(), timeout=1)
                            finally:
                                task.cancel()
                                with pytest.raises(asyncio.CancelledError):
                                    await task
                    await self.assert_watcher_stopped(nc, watcher, subscriptions_before)
        finally:
            await nc.close()

    @async_test
    async def test_keys_stops_watcher_on_consumer_info_error(self):
        nc = await nats.connect()
        try:
            js = nc.jetstream()
            kv = await js.create_key_value(bucket="INFO_ERROR")
            await kv.put("alive", b"value")
            subscriptions_before = len(nc._subs)
            watcher = await kv.watchall()
            error = RuntimeError("consumer info failed")
            with mock.patch.object(kv, "watchall", return_value=watcher):
                with mock.patch.object(watcher._sub, "consumer_info", side_effect=error):
                    with pytest.raises(RuntimeError) as raised:
                        await kv.keys()
                    assert raised.value is error
            await self.assert_watcher_stopped(nc, watcher, subscriptions_before)
        finally:
            await nc.close()

    @async_test
    async def test_keys_stops_watcher_on_consumer_info_cancellation(self):
        nc = await nats.connect()
        try:
            js = nc.jetstream()
            kv = await js.create_key_value(bucket="INFO_CANCEL")
            await kv.put("alive", b"value")
            subscriptions_before = len(nc._subs)
            watcher = await kv.watchall()
            entered = asyncio.Event()

            async def blocked_info():
                entered.set()
                await asyncio.Future()

            with mock.patch.object(kv, "watchall", return_value=watcher):
                with mock.patch.object(watcher._sub, "consumer_info", side_effect=blocked_info):
                    task = asyncio.create_task(kv.keys())
                    try:
                        await asyncio.wait_for(entered.wait(), timeout=1)
                    finally:
                        task.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await task
            await self.assert_watcher_stopped(nc, watcher, subscriptions_before)
        finally:
            await nc.close()

    @async_test
    async def test_purge_deletes_stops_watcher_before_purge_error(self):
        nc = await nats.connect()
        try:
            js = nc.jetstream()
            kv = await js.create_key_value(bucket="PURGE_ERROR")
            await kv.put("deleted", b"value")
            await kv.delete("deleted")
            subscriptions_before = len(nc._subs)
            watcher = await kv.watchall()
            error = RuntimeError("purge failed")
            with mock.patch.object(kv, "watchall", return_value=watcher):
                with mock.patch.object(js, "purge_stream", side_effect=error):
                    with pytest.raises(RuntimeError) as raised:
                        await kv.purge_deletes(olderthan=0)
                    assert raised.value is error
            await self.assert_watcher_stopped(nc, watcher, subscriptions_before)
        finally:
            await nc.close()
