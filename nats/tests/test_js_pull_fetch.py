"""Fetch observations measured against unchanged nats.go; expectations stay inline."""

# Reference: nats.go v1.54.0 (Subscription.Fetch in js.go). Python-specific differences
# retain native exception types, explicit-close errors, and cancellation behavior.

import asyncio
import json
import time

import pytest

import nats
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js import errors
from tests.test_js_pull_subscribe import server as server


@pytest.mark.parametrize(
    "label,batch,heartbeat,timeout,expected_messages,expected_outcome,expected_requests",
    [
        ("full", 2, 0.05, 1, ["one", "two"], "ok", [(2, True, 0.05)]),
        ("missing", 1, 0.05, 1, [], "heartbeat", [(1, False, 0.05)]),
        ("partial_missing", 3, 0.05, 1, ["one"], "ok", [(3, True, 0.05)]),
        ("reset", 1, 0.05, 1, ["one"], "ok", [(1, False, 0.05)]),
        ("interleaved", 3, 0.05, 1, ["one", "two", "three"], "ok", [(3, True, 0.05)]),
        ("retry_404", 3, 0.05, 1, ["one", "two", "three"], "ok", [(3, True, 0.05), (3, False, 0.05)]),
        ("retry_408", 3, 0.05, 1, ["one", "two", "three"], "ok", [(3, True, 0.05), (3, False, 0.05)]),
        ("retry_missing", 3, 0.05, 1, [], "heartbeat", [(3, True, 0.05), (3, False, 0.05)]),
        ("suppressed", 1, 0.48, 1, ["one"], "ok", [(1, False, 0)]),
        ("renewal_suppressed", 3, 0.1, 0.5, ["one", "two", "three"], "ok", [(3, True, 0.1), (3, False, 0)]),
        ("healthy_timeout", 1, 0.03, 0.25, [], "timeout", [(1, False, 0.03)]),
        ("waiting_408", 1, 0, 0.15, [], "timeout", [(1, False, 0)]),
        ("waiting_404", 1, 0, 1, [], "status", [(1, False, 0)]),
        ("negative", 1, -0.001, 1, [], "invalid", []),
        ("too_large", 1, 0.5, 1, [], "invalid", []),
        ("zero", 1, 0, 0.15, [], "timeout", [(1, False, 0)]),
        ("status_payload", 1, 0, 1, ["one"], "ok", [(1, False, 0)]),
        ("empty_payload", 1, 0, 1, [""], "ok", [(1, False, 0)]),
        ("consumer_deleted", 1, 0, 1, [], "status", [(1, False, 0)]),
        ("leadership", 3, 0, 1, [], "status", [(3, True, 0)]),
        ("conflict", 1, 0, 1, [], "status", [(1, False, 0)]),
        ("unavailable", 1, 0, 1, [], "unavailable", [(1, False, 0)]),
        ("partial_conflict", 3, 0, 1, ["one"], "ok", [(3, True, 0)]),
        ("partial_unavailable", 3, 0, 1, ["one"], "ok", [(3, True, 0)]),
        ("partial_404", 3, 0, 1, ["one"], "ok", [(3, True, 0)]),
        ("partial_408", 3, 0, 1, ["one"], "ok", [(3, True, 0)]),
        ("partial_timeout", 3, 0, 0.15, ["one"], "ok", [(3, True, 0)]),
        ("buffered", 2, 0, 1, ["one", "two"], "ok", []),
        ("buffered_status", 1, 0, 1, ["one"], "ok", []),
        ("buffered_status_payload", 1, 0, 1, ["one"], "ok", []),
        ("disconnect", 1, 0, 1, [], "disconnected", [(1, False, 0)]),
        ("close", 1, 0, 1, [], "disconnected", [(1, False, 0)]),
        ("unsubscribe", 1, 0, 1, [], "subscription", [(1, False, 0)]),
        ("partial_disconnect", 3, 0, 1, ["one"], "ok", [(3, True, 0)]),
        ("partial_close", 3, 0, 1, ["one"], "ok", [(3, True, 0)]),
        ("partial_unsubscribe", 3, 0, 1, ["one"], "ok", [(3, True, 0)]),
        ("remaining_one", 3, 0, 1, ["one", "two", "three"], "ok", [(1, False, 0)]),
    ],
)
async def test_fetch_observations(
    server, label, batch, heartbeat, timeout, expected_messages, expected_outcome, expected_requests
):
    url = f"nats://127.0.0.1:{server.port}"
    service = await nats.connect(url)
    client = await nats.connect(url)
    prefix = service.new_inbox()
    requests = []
    workers = []

    async def respond(msg):
        request = json.loads(msg.data)
        requests.append(request)
        number = len(requests)

        async def frames():
            async def status(code, description=""):
                await service.publish(msg.reply, headers={"Status": code, "Description": description})

            async def data(value):
                await service.publish(msg.reply, value.encode())

            if label in ("retry_404", "retry_408", "retry_missing", "renewal_suppressed") and number == 1:
                if label == "renewal_suppressed":
                    for _ in range(3):
                        await asyncio.sleep(0.095)
                        await status("100")
                await status("408" if label == "retry_408" else "404")
            elif label in ("full", "retry_404", "retry_408", "renewal_suppressed"):
                for value in ["one", "two", "three"][:batch]:
                    await data(value)
            elif label in ("partial_missing", "partial_timeout", "suppressed"):
                await data("one")
            elif label == "remaining_one":
                await data("three")
            elif label == "reset":
                for _ in range(3):
                    await asyncio.sleep(0.06)
                    await status("100")
                await asyncio.sleep(0.06)
                await data("one")
            elif label == "interleaved":
                for value in ("one", "two", "three"):
                    await status("100")
                    await data(value)
            elif label == "healthy_timeout":
                while True:
                    await asyncio.sleep(0.02)
                    await status("100")
            elif label == "status_payload":
                await service.publish(msg.reply, b"one", headers={"Status": "409"})
            elif label == "empty_payload":
                await service.publish(msg.reply, b"", headers={"App": "empty"})
            elif label.removeprefix("partial_") in ("disconnect", "close", "unsubscribe"):
                if label.startswith("partial_"):
                    await data("one")
                    await service.flush()
                    await asyncio.sleep(0.03)
                await service.publish(prefix + ".action", label.removeprefix("partial_").encode())
            elif label in (
                "consumer_deleted",
                "leadership",
                "conflict",
                "unavailable",
                "waiting_408",
                "waiting_404",
            ) or label.startswith("partial_"):
                code, description = {
                    "consumer_deleted": ("409", "Consumer Deleted"),
                    "leadership": ("409", "Leadership Change"),
                    "conflict": ("409", "Exceeded MaxWaiting"),
                    "unavailable": ("503", "No Responders"),
                    "waiting_408": ("408", "Request Timeout"),
                    "waiting_404": ("404", "No Messages"),
                    "partial_conflict": ("409", "Consumer Deleted"),
                    "partial_unavailable": ("503", "No Responders"),
                    "partial_404": ("404", "No Messages"),
                    "partial_408": ("408", "Request Timeout"),
                }[label]
                if label.startswith("partial_"):
                    await data("one")
                await status(code, description)

        workers.append(asyncio.create_task(frames()))

    try:
        await service.subscribe(prefix + ".CONSUMER.MSG.NEXT.STREAM.worker", cb=respond)
        await service.flush()
        js = client.jetstream(prefix=prefix)
        inbox = prefix + ".reply"
        sub = js.PullSubscription(js, await client.subscribe(inbox), "STREAM", "worker", inbox.encode())

        async def action(msg):
            if msg.data == b"disconnect":
                await client.force_reconnect()
            elif msg.data == b"close":
                await client.close()
            else:
                await sub.unsubscribe()

        await client.subscribe(prefix + ".action", cb=action)
        await client.flush()
        if label.startswith("buffered") or label == "remaining_one":
            if label == "buffered_status":
                await service.publish(inbox, headers={"Status": "409", "Description": "old"})
            for value in (
                ["one", "two", "three"]
                if label == "buffered"
                else ["one", "two"]
                if label == "remaining_one"
                else ["one"]
            ):
                await service.publish(
                    inbox, value.encode(), headers={"Status": "100"} if label == "buffered_status_payload" else None
                )
            await service.flush()
            # A round trip makes the preloaded deliveries observable before fetch.
            await client.flush()
        start = time.monotonic()
        messages, outcome, error = [], "ok", ""
        try:
            messages = [msg.data.decode() for msg in await sub.fetch(batch, timeout, heartbeat)]
        except (nats.errors.Error, ValueError) as exc:
            error = str(exc)
            if isinstance(exc, errors.NoHeartbeatError):
                outcome = "heartbeat"
            elif isinstance(exc, (errors.FetchDisconnectedError, nats.errors.ConnectionClosedError)):
                outcome = "disconnected"
            elif isinstance(exc, nats.errors.BadSubscriptionError):
                outcome = "subscription"
            elif isinstance(exc, nats.errors.TimeoutError):
                outcome = "timeout"
            elif isinstance(exc, ValueError):
                outcome = "invalid"
            elif isinstance(exc, errors.ServiceUnavailableError):
                outcome = "unavailable"
            else:
                outcome = "status"
                assert isinstance(exc, errors.APIError)
                assert exc.code == (404 if label == "waiting_404" else 409)
                assert (
                    exc.description
                    == {
                        "consumer_deleted": "Consumer Deleted",
                        "leadership": "Leadership Change",
                        "conflict": "Exceeded MaxWaiting",
                        "waiting_404": "No Messages",
                    }[label]
                )
        observed = {"messages": messages, "outcome": outcome, "error": error, "pending": sub.pending_msgs}
        elapsed = time.monotonic() - start
        assert not client._disconnect_waiters
        assert not sub._sub._pending_next_msgs_calls
        assert observed["messages"] == expected_messages
        assert observed["outcome"] == expected_outcome, observed
        expected_errors = {
            "ok": "",
            "heartbeat": "nats: no heartbeat received",
            "timeout": "nats: fetch timeout" if label == "healthy_timeout" else "nats: timeout",
            "disconnected": "nats: disconnected during fetch",
            "subscription": "nats: invalid subscription",
            "unavailable": "nats: ServiceUnavailableError: code=None err_code=None description='None'",
        }
        if expected_outcome == "invalid":
            expected_error = {
                "negative": "nats: invalid heartbeat",
                "too_large": "nats: idle heartbeat value too large",
            }[label]
        elif expected_outcome == "status":
            code, description = {
                "consumer_deleted": (409, "Consumer Deleted"),
                "leadership": (409, "Leadership Change"),
                "conflict": (409, "Exceeded MaxWaiting"),
                "waiting_404": (404, "No Messages"),
            }[label]
            expected_error = f"nats: APIError: code={code} err_code=None description='{description}'"
        elif label == "close":
            expected_error = "nats: connection closed"
        else:
            expected_error = expected_errors[expected_outcome]
        assert observed["error"] == expected_error
        assert observed["pending"] == (1 if label == "buffered" else 0)
        assert [
            (r["batch"], r.get("no_wait", False), r.get("idle_heartbeat", 0) / 1e9) for r in requests
        ] == expected_requests
        for request in requests:
            assert 0 < request["expires"] <= timeout * 0.91 * 1e9
        if len(requests) == 2:
            assert requests[1]["expires"] < requests[0]["expires"]
        if label in ("missing", "partial_missing", "retry_missing"):
            assert 0.08 <= elapsed < 0.7
        if label == "reset":
            assert elapsed >= 0.2
        if expected_outcome == "timeout":
            assert elapsed >= timeout * 0.9
        if expected_outcome in ("status", "unavailable", "subscription", "disconnected"):
            assert elapsed < 0.7
    finally:
        for task in workers:
            task.cancel()
        results = await asyncio.gather(*workers, return_exceptions=True)
        assert all(result is None or isinstance(result, asyncio.CancelledError) for result in results), results
        await client.close()
        await service.close()


@pytest.mark.parametrize("partial", [False, True])
@pytest.mark.parametrize("action", ["force_reconnect", "transport_close", "close", "unsubscribe", "cancel"])
async def test_fetch_lifecycle(server, action, partial):
    disconnected = asyncio.Event()
    reconnected = asyncio.Event()

    async def disconnected_cb():
        disconnected.set()

    async def reconnected_cb():
        reconnected.set()

    url = f"nats://127.0.0.1:{server.port}"
    client = await nats.connect(
        url, disconnected_cb=disconnected_cb, reconnected_cb=reconnected_cb, reconnect_time_wait=0.05
    )
    service = await nats.connect(url)
    prefix = service.new_inbox()
    js = client.jetstream(prefix=prefix)
    inbox = client.new_inbox()
    sub = js.PullSubscription(js, await client.subscribe(inbox), "STREAM", "worker", inbox.encode())
    requested = asyncio.Event()
    calls = 0
    task = None

    async def respond(msg):
        nonlocal calls
        calls += 1
        if calls > 1:
            await service.publish(msg.reply, b"recovered")
        elif partial:
            await service.publish(msg.reply, b"one")
        await service.flush()
        requested.set()

    try:
        await service.subscribe(sub._nms, cb=respond)
        await service.flush()
        task = asyncio.create_task(sub.fetch(3 if partial else 1, timeout=5))
        await asyncio.wait_for(requested.wait(), 1)
        # Give the reader time to collect the partial batch and enter its next wait.
        await asyncio.sleep(0.05)
        start = time.monotonic()
        if action == "force_reconnect":
            await client.force_reconnect()
        elif action == "transport_close":
            # Exercise the read-loop error path, as well as force_reconnect above.
            client._transport.close()
        elif action == "close":
            await client.close()
        elif action == "unsubscribe":
            await sub.unsubscribe()
        else:
            task.cancel()
        if action == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await task
        elif partial:
            assert [msg.data for msg in await asyncio.wait_for(task, 1)] == [b"one"]
        else:
            expected = {
                "unsubscribe": nats.errors.BadSubscriptionError,
                "close": nats.errors.ConnectionClosedError,
            }.get(action, errors.FetchDisconnectedError)
            with pytest.raises(expected):
                await asyncio.wait_for(task, 1)
        assert time.monotonic() - start < 0.7
        assert not client._disconnect_waiters
        assert not sub._sub._pending_next_msgs_calls
        assert not sub._sub._pending_queue._getters
        if action in ("force_reconnect", "transport_close"):
            await asyncio.wait_for(reconnected.wait(), 2)
            assert disconnected.is_set()
            assert client._disconnected_cb is disconnected_cb
            assert [msg.data for msg in await sub.fetch(1, timeout=1)] == [b"recovered"]
        elif action == "close":
            assert disconnected.is_set()
    finally:
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await client.close()
        await service.close()


@pytest.mark.parametrize("heartbeat", [-0.1, 0.5, 1])
async def test_heartbeat_validation_preserves_buffered_messages(server, heartbeat):
    nc = await nats.connect(f"nats://127.0.0.1:{server.port}")
    try:
        js = nc.jetstream()
        inbox = nc.new_inbox()
        native = await nc.subscribe(inbox)
        sub = js.PullSubscription(js, native, "STREAM", "worker", inbox.encode())
        await nc.publish(inbox, b"one")
        await nc.flush()

        async def wait_pending():
            while sub.pending_msgs != 1:
                await asyncio.sleep(0.001)

        await asyncio.wait_for(wait_pending(), 1)
        with pytest.raises(ValueError, match="heartbeat"):
            await sub.fetch(1, timeout=1, heartbeat=heartbeat)
        assert sub.pending_msgs == 1
        assert sub.pending_bytes == 3
        assert [msg.data for msg in await sub.fetch(1)] == [b"one"]
        assert sub.pending_msgs == sub.pending_bytes == 0
        await asyncio.wait_for(native._pending_queue.join(), 1)
        assert not nc._disconnect_waiters
    finally:
        await nc.close()


@pytest.mark.parametrize("batch,remaining_frames", [(2, 2), (3, 1)])
async def test_fetch_burst_after_request(monkeypatch, batch, remaining_frames):
    """Frames queued after publishing still obey batch, status, and accounting rules."""
    nc = Client()
    native = Subscription(nc, subject="inbox")
    native._start(nc._error_cb)
    js = nc.jetstream()
    sub = js.PullSubscription(js, native, "STREAM", "worker", b"inbox")
    requests = []

    async def publish(subject, payload, reply):
        requests.append(json.loads(payload))
        for data, headers in (
            (b"one", None),
            (b"", {"Status": "100"}),
            (b"two", {"Status": "409"}),
            (b"", {"Status": "409", "Description": "Consumer Deleted"}),
            (b"three", None),
        ):
            native._pending_queue.put_nowait(Msg(nc, data=data, headers=headers))
            native._pending_size += len(data)

    monkeypatch.setattr(nc, "publish", publish)
    assert [msg.data for msg in await sub.fetch(batch, timeout=1, heartbeat=0.1)] == [b"one", b"two"]
    assert sub.pending_msgs == remaining_frames
    assert sub.pending_bytes == len(b"three")
    assert [msg.data for msg in await sub.fetch(1)] == [b"three"]
    assert len(requests) == 1
    assert sub.pending_msgs == sub.pending_bytes == 0
    await asyncio.wait_for(native._pending_queue.join(), 1)
    assert not nc._disconnect_waiters
    assert not native._pending_next_msgs_calls


@pytest.mark.parametrize(
    "action,expected_error",
    [
        ("close", nats.errors.ConnectionClosedError),
        ("disconnect", errors.FetchDisconnectedError),
        ("unsubscribe", nats.errors.BadSubscriptionError),
    ],
)
async def test_fetch_lifecycle_before_queued_delivery(monkeypatch, action, expected_error):
    """A completed lifecycle notification takes precedence over queued delivery."""
    nc = Client()
    native = Subscription(nc, subject="inbox")
    native._start(nc._error_cb)
    js = nc.jetstream()
    sub = js.PullSubscription(js, native, "STREAM", "worker", b"inbox")

    async def publish(subject, payload, reply):
        native._pending_queue.put_nowait(Msg(nc, data=b"one"))
        native._pending_size = 3
        if action == "close":
            nc._status = Client.CLOSED
            nc._notify_disconnect()
        elif action == "disconnect":
            nc._notify_disconnect()
        else:
            native._closed = True
            native._stop_processing()

    monkeypatch.setattr(nc, "publish", publish)
    with pytest.raises(expected_error):
        await sub.fetch(1)
    assert sub.pending_msgs == 1
    assert sub.pending_bytes == 3
    assert not nc._disconnect_waiters
    assert not native._pending_next_msgs_calls
