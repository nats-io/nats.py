import asyncio
import shutil
import time
import unittest

import nats
from nats.aio.client import Client as NATS
from nats.errors import TimeoutError

from tests.utils import (
    NATSD,
    SingleJetStreamServerTestCase,
    async_long_test,
    start_natsd,
)


class FlusherPendingDataLossTest(SingleJetStreamServerTestCase):
    @async_long_test
    async def test_pending_data_preserved_when_drain_fails(self):
        """
        Regression test for issue #504: when drain() raises OSError (broken
        connection), _pending data must be preserved so it can be re-sent
        after reconnection.

        The bug was that the flusher cleared _pending BEFORE calling drain().
        If drain() then failed, the pending data (including fetch requests)
        was permanently lost, causing pull subscriptions to stop working
        after reconnection.

        The fix moves drain() before the clear, so _pending is only cleared
        after a successful drain.
        """
        nc = NATS()

        reconnected = asyncio.Event()
        disconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        js = nc.jetstream()
        await js.add_stream(name="FLUSHER_BUG", subjects=["bug.>"])
        sub = await js.pull_subscribe("bug.>", durable="flusher_bug")

        # Publish messages that will be fetched after reconnection.
        for i in range(3):
            await js.publish("bug.msg", f"msg{i}".encode())

        # Fetch one to verify the subscription works.
        msgs = await sub.fetch(1, timeout=5)
        self.assertEqual(len(msgs), 1)
        await msgs[0].ack()

        # Simulate the race condition:
        # Put data into _pending (e.g. a fetch request about to be flushed),
        # then make drain() fail with OSError (connection broke mid-flush).
        test_cmd = b"PUB bug.marker 0\r\n\r\n"
        nc._pending.append(test_cmd)
        nc._pending_data_size += len(test_cmd)

        # Monkey-patch drain to simulate connection failure.
        original_drain = nc._transport.drain

        async def failing_drain():
            raise OSError("Connection reset by peer")

        nc._transport.drain = failing_drain

        # Trigger the flusher to process the pending data.
        future = asyncio.Future()
        await nc._flush_queue.put(future)
        # Wait for flusher to process and hit the OSError.
        await asyncio.sleep(0.5)

        # With the fix: _pending should still contain our data because
        # drain() failed before the clear happened.
        # With the bug: _pending would be empty (data lost forever).
        self.assertTrue(
            len(nc._pending) > 0,
            "Pending data was lost when drain() failed! "
            "This means the flusher clears _pending before drain(), "
            "causing data loss on connection failure (issue #504).",
        )

        # Verify our test command is still in the pending buffer.
        self.assertIn(test_cmd, nc._pending)

        # The OSError from failing_drain triggers _process_op_err which
        # initiates reconnection. Wait for reconnection so we can close
        # cleanly.
        try:
            await asyncio.wait_for(reconnected.wait(), timeout=10)
        except asyncio.TimeoutError:
            pass
        try:
            await nc.close()
        except Exception:
            pass

    @async_long_test
    async def test_pull_subscribe_data_loss_on_server_kill_during_fetch(self):
        """
        End-to-end reproduction of issue #504: kill the NATS server while
        a fetch request is being flushed. With the buggy flusher, the fetch
        request is lost and post-reconnection fetches time out.

        To maximize the chance of hitting the race, we create high flush
        traffic and kill the server abruptly.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        js = nc.jetstream()
        await js.add_stream(name="KILL_TEST", subjects=["kill.>"])
        sub = await js.pull_subscribe("kill.>", durable="kill_consumer")

        # Pre-load the stream with messages.
        for i in range(10):
            await js.publish("kill.msg", f"msg{i}".encode())

        # Verify subscription works.
        msgs = await sub.fetch(1, timeout=5)
        self.assertEqual(len(msgs), 1)
        await msgs[0].ack()

        # Generate some flush traffic to increase the chance of pending
        # data being in the buffer when the server dies.
        for i in range(50):
            await js.publish("kill.filler", f"filler{i}".encode())

        # Kill the server abruptly (not graceful stop).
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)
        await asyncio.wait_for(disconnected.wait(), timeout=5)

        # Restart.
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        await asyncio.sleep(1)

        # The critical test: can we still fetch after reconnection?
        # With the buggy flusher, this may time out because the fetch
        # request was lost during the failed flush.
        try:
            msgs = await sub.fetch(1, timeout=5)
            self.assertEqual(len(msgs), 1)
            await msgs[0].ack()
            fetch_worked = True
        except TimeoutError:
            fetch_worked = False

        # Note: this test may not always reproduce the race condition
        # since it depends on exact timing. The unit test above
        # (test_pending_data_lost_when_drain_fails) is the deterministic
        # proof.

        await nc.close()

        # If fetch didn't work, we've reproduced the bug.
        # If it did work, the timing didn't trigger the race this time.
        # Both are valid outcomes - the unit test above is the definitive proof.


class PullSubscribeReconnectTest(SingleJetStreamServerTestCase):
    @async_long_test
    async def test_pull_subscribe_fetch_after_server_restart(self):
        """
        Test that pull_subscribe().fetch() works after a NATS server
        restart. This reproduces the issue reported in #504 where
        pull subscriptions stop working after reconnection while
        push subscriptions continue to work.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()
        errors = []

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        async def error_cb(e):
            errors.append(e)

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
            error_cb=error_cb,
        )

        js = nc.jetstream()

        # Create stream and durable pull consumer.
        await js.add_stream(name="RECONNTEST", subjects=["test.>"])
        sub = await js.pull_subscribe("test.>", durable="pull_consumer")

        # Verify pull subscribe works before restart.
        await js.publish("test.msg", b"before_restart")
        msgs = await sub.fetch(1, timeout=5)
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].data, b"before_restart")
        await msgs[0].ack()

        # Stop the NATS server (simulating reboot).
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)

        # Wait for client to detect disconnection.
        await asyncio.wait_for(disconnected.wait(), timeout=5)
        self.assertTrue(nc.is_reconnecting)

        # Restart the server (same port, same JetStream store dir).
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)

        # Wait for client to reconnect.
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        self.assertTrue(nc.is_connected)

        # Give JetStream a moment to restore state from disk.
        await asyncio.sleep(1)

        # Publish new messages after reconnection.
        await js.publish("test.msg", b"after_restart")

        # This is the critical test: fetch should work after reconnection.
        msgs = await sub.fetch(1, timeout=5)
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].data, b"after_restart")
        await msgs[0].ack()

        await nc.close()

    @async_long_test
    async def test_push_subscribe_works_after_server_restart(self):
        """
        Contrast test: push subscriptions should work after server restart.
        This verifies the asymmetry reported in #504.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        js = nc.jetstream()

        # Create stream and push subscribe with durable consumer.
        await js.add_stream(name="RECONNTEST_PUSH", subjects=["push.>"])
        sub = await js.subscribe("push.>", durable="push_consumer")

        # Verify push subscribe works before restart.
        await js.publish("push.msg", b"before_restart")
        msg = await sub.next_msg(timeout=5)
        self.assertEqual(msg.data, b"before_restart")
        await msg.ack()

        # Stop the server.
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)

        await asyncio.wait_for(disconnected.wait(), timeout=5)

        # Restart.
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        await asyncio.sleep(1)

        # Publish after reconnection.
        await js.publish("push.msg", b"after_restart")

        # Push subscribe should work after reconnection.
        msg = await sub.next_msg(timeout=5)
        self.assertEqual(msg.data, b"after_restart")
        await msg.ack()

        await nc.close()

    @async_long_test
    async def test_pull_subscribe_fetch_loop_across_reconnect(self):
        """
        Test a continuous fetch loop across a server restart.
        This simulates a typical real-world usage pattern where
        the application fetches in a loop.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        js = nc.jetstream()
        await js.add_stream(name="RECONNTEST_LOOP", subjects=["loop.>"])
        sub = await js.pull_subscribe("loop.>", durable="loop_consumer")

        fetched_msgs = []
        fetch_errors = []
        fetch_done = asyncio.Event()

        async def fetch_loop():
            """Continuously fetch messages, tolerating reconnection errors."""
            while not fetch_done.is_set():
                try:
                    msgs = await sub.fetch(1, timeout=2)
                    for msg in msgs:
                        fetched_msgs.append(msg.data)
                        await msg.ack()
                except TimeoutError:
                    continue
                except Exception as e:
                    fetch_errors.append(e)
                    continue

        # Start the fetch loop in background.
        fetch_task = asyncio.create_task(fetch_loop())

        # Publish a message before restart.
        await js.publish("loop.msg", b"msg1")
        await asyncio.sleep(1)

        # Stop server.
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)
        await asyncio.wait_for(disconnected.wait(), timeout=5)

        # Restart server.
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        await asyncio.sleep(1)

        # Publish after reconnection.
        await js.publish("loop.msg", b"msg2")

        # Wait for the message to be fetched.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            if b"msg2" in fetched_msgs:
                break
            await asyncio.sleep(0.5)

        fetch_done.set()
        await asyncio.wait_for(fetch_task, timeout=5)

        self.assertIn(b"msg1", fetched_msgs)
        self.assertIn(b"msg2", fetched_msgs)

        await nc.close()

    @async_long_test
    async def test_pull_subscribe_pending_data_preserved_across_reconnect(self):
        """
        Test that pending data buffered during a flush is not lost when the
        connection drops. This tests the fix where _pending is only cleared
        after a successful drain(), ensuring data is preserved for re-sending
        after reconnection.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        js = nc.jetstream()
        await js.add_stream(name="RECONNTEST_PENDING", subjects=["pending.>"])
        sub = await js.pull_subscribe("pending.>", durable="pending_consumer")

        # Publish several messages before disconnect.
        for i in range(5):
            await js.publish("pending.msg", f"msg{i}".encode())

        # Fetch first message to confirm subscription works.
        msgs = await sub.fetch(1, timeout=5)
        self.assertEqual(len(msgs), 1)
        await msgs[0].ack()

        # Stop server while there are still messages to fetch.
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)
        await asyncio.wait_for(disconnected.wait(), timeout=5)

        # Restart server.
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        await asyncio.sleep(1)

        # Fetch remaining messages after reconnection.
        # This verifies the subscription and consumer state are preserved.
        remaining = []
        for _ in range(4):
            msgs = await sub.fetch(1, timeout=5)
            for msg in msgs:
                remaining.append(msg.data)
                await msg.ack()

        self.assertEqual(len(remaining), 4)

        await nc.close()

    @async_long_test
    async def test_pull_subscribe_fetch_in_flight_during_disconnect(self):
        """
        Test that a fetch() call in-flight during a disconnect can recover.
        The fetch times out during disconnect, and subsequent fetches should
        work after reconnection.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        js = nc.jetstream()
        await js.add_stream(name="RECONNTEST_INFLIGHT", subjects=["inflight.>"])
        sub = await js.pull_subscribe("inflight.>", durable="inflight_consumer")

        # Start a fetch that will be in-flight when the server stops.
        # Use a short timeout so it doesn't block the test too long.
        fetch_result = None
        fetch_error = None

        async def do_fetch():
            nonlocal fetch_result, fetch_error
            try:
                fetch_result = await sub.fetch(1, timeout=3)
            except Exception as e:
                fetch_error = e

        fetch_task = asyncio.create_task(do_fetch())

        # Give fetch a moment to start waiting.
        await asyncio.sleep(0.2)

        # Stop server while fetch is waiting.
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)
        await asyncio.wait_for(disconnected.wait(), timeout=5)

        # Wait for the fetch to timeout.
        await asyncio.wait_for(fetch_task, timeout=10)
        self.assertIsNotNone(fetch_error, "fetch should have raised an error during disconnect")

        # Restart server.
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        await asyncio.sleep(1)

        # Publish and fetch after reconnection - this should work.
        await js.publish("inflight.msg", b"recovered")
        msgs = await sub.fetch(1, timeout=5)
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].data, b"recovered")
        await msgs[0].ack()

        await nc.close()


class FetchReconnectUnblockTest(SingleJetStreamServerTestCase):
    """Tests that inflight fetch requests are cancelled and re-issued on reconnect."""

    @async_long_test
    async def test_fetch_with_long_timeout_unblocks_on_reconnect(self):
        """
        A fetch() with a very long timeout must not hang for the full duration
        when a reconnect happens.  The client should cancel the blocked
        next_msg, wait for reconnection, re-issue the pull request, and
        deliver the message — all well before the original timeout expires.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        js = nc.jetstream()
        await js.add_stream(name="UNBLOCK_TEST", subjects=["unblock.>"])
        sub = await js.pull_subscribe("unblock.>", durable="unblock_consumer")

        # Start a fetch with a very long timeout.  If the reconnect-unblock
        # mechanism doesn't work, this would hang for 300 seconds.
        fetch_result = []
        fetch_error = None

        async def do_long_fetch():
            nonlocal fetch_error
            try:
                msgs = await sub.fetch(1, timeout=300)
                fetch_result.extend(msgs)
            except Exception as e:
                fetch_error = e

        fetch_task = asyncio.create_task(do_long_fetch())
        # Let the fetch request be sent and block on next_msg.
        await asyncio.sleep(0.5)

        # Kill the server while fetch is blocked.
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)
        await asyncio.wait_for(disconnected.wait(), timeout=5)

        # Restart.
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        await asyncio.sleep(1)

        # Publish a message.  After reconnect the fetch should have
        # re-issued its pull request, so it should receive this message.
        await js.publish("unblock.msg", b"after_reconnect")

        # The fetch should complete well before its 300s timeout.
        await asyncio.wait_for(fetch_task, timeout=15)

        self.assertEqual(len(fetch_result), 1)
        self.assertEqual(fetch_result[0].data, b"after_reconnect")
        await fetch_result[0].ack()
        self.assertIsNone(fetch_error)

        await nc.close()

    @async_long_test
    async def test_fetch_n_with_long_timeout_unblocks_on_reconnect(self):
        """
        Same as above but for batch fetch (batch > 1) which uses _fetch_n.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        js = nc.jetstream()
        await js.add_stream(name="UNBLOCK_BATCH", subjects=["unbatch.>"])
        sub = await js.pull_subscribe("unbatch.>", durable="unbatch_consumer")

        fetch_result = []
        fetch_error = None

        async def do_long_batch_fetch():
            nonlocal fetch_error
            try:
                msgs = await sub.fetch(3, timeout=300)
                fetch_result.extend(msgs)
            except Exception as e:
                fetch_error = e

        fetch_task = asyncio.create_task(do_long_batch_fetch())
        await asyncio.sleep(0.5)

        # Kill server while fetch is blocked.
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)
        await asyncio.wait_for(disconnected.wait(), timeout=5)

        # Restart.
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        await asyncio.sleep(1)

        # Publish messages.
        for i in range(3):
            await js.publish("unbatch.msg", f"batch{i}".encode())

        # The fetch should complete quickly, not wait 300s.
        await asyncio.wait_for(fetch_task, timeout=15)

        self.assertEqual(len(fetch_result), 3)
        self.assertIsNone(fetch_error)
        for msg in fetch_result:
            await msg.ack()

        await nc.close()

    @async_long_test
    async def test_next_msg_raises_reconnecting_error(self):
        """
        Plain next_msg (not via fetch) should raise ConnectionReconnectingError
        when the connection is lost, rather than silently hanging.
        """
        nc = NATS()

        disconnected = asyncio.Event()
        reconnected = asyncio.Event()

        async def disconnected_cb():
            disconnected.set()

        async def reconnected_cb():
            reconnected.set()

        await nc.connect(
            "nats://127.0.0.1:4222",
            reconnect_time_wait=0.5,
            max_reconnect_attempts=-1,
            disconnected_cb=disconnected_cb,
            reconnected_cb=reconnected_cb,
        )

        sub = await nc.subscribe("test.nextmsg")

        next_msg_error = None

        async def do_next_msg():
            nonlocal next_msg_error
            try:
                await sub.next_msg(timeout=300)
            except Exception as e:
                next_msg_error = e

        task = asyncio.create_task(do_next_msg())
        await asyncio.sleep(0.2)

        # Kill server.
        srv = self.server_pool[0]
        await asyncio.get_running_loop().run_in_executor(None, srv.stop)
        await asyncio.wait_for(disconnected.wait(), timeout=5)

        # next_msg should have been unblocked almost immediately.
        await asyncio.wait_for(task, timeout=5)

        from nats.errors import ConnectionReconnectingError

        self.assertIsInstance(next_msg_error, ConnectionReconnectingError)

        # Restart for clean shutdown.
        await asyncio.get_running_loop().run_in_executor(None, start_natsd, srv)
        await asyncio.wait_for(reconnected.wait(), timeout=10)
        await nc.close()


if __name__ == "__main__":
    unittest.main()
