import asyncio

import pytest
import nats.errors

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription
from nats.js.client import JetStreamContext
from nats.js.errors import FetchDisconnectedError


def make_pull():
    nc = Client()
    sub = Subscription(nc)
    pull = JetStreamContext.PullSubscription(nc.jetstream(), sub, "STREAM", "CONSUMER", b"_INBOX.repro")
    return nc, sub, pull


def deliver(sub, data):
    sub._pending_size += len(data)
    sub._pending_queue.put_nowait(Msg(_client=sub._conn, data=data))


@pytest.mark.asyncio
async def test_stale_disconnect_does_not_cancel_the_next_wait():
    nc, sub, pull = make_pull()
    loop = asyncio.get_running_loop()
    old_connection_lost = nc._connection_lost()
    received = []

    async def consume():
        received.append((await pull._next_msg(None, old_connection_lost)).data)
        new_connection_lost = nc._connection_lost()
        assert new_connection_lost is not old_connection_lost
        received.append((await pull._next_msg(None, new_connection_lost)).data)

    worker = asyncio.create_task(consume())
    try:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        deliver(sub, b"before")
        loop.call_soon(nc._notify_connection_lost)
        delivery = loop.call_later(0.01, deliver, sub, b"after")
        try:
            await asyncio.wait_for(worker, 1)
        finally:
            delivery.cancel()
        assert received == [b"before", b"after"]
        assert not sub._pending_next_msgs_calls
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_current_disconnect_still_interrupts_wait():
    nc, sub, pull = make_pull()
    worker = asyncio.create_task(pull._next_msg(None, nc._connection_lost()))
    try:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        nc._notify_connection_lost()
        with pytest.raises(FetchDisconnectedError):
            await asyncio.wait_for(worker, 1)
        assert not sub._pending_next_msgs_calls
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_caller_cancellation_still_propagates():
    nc, sub, pull = make_pull()
    worker = asyncio.create_task(pull._next_msg(None, nc._connection_lost()))
    try:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert not sub._pending_next_msgs_calls
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)


@pytest.mark.asyncio
async def test_disconnect_does_not_cancel_a_concurrent_new_generation():
    nc, sub, pull = make_pull()
    old = asyncio.create_task(pull._next_msg(None, nc._connection_lost()))
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    async def new_wait():
        return await pull._next_msg(None, nc._connection_lost())

    new = asyncio.create_task(new_wait())
    nc._notify_connection_lost()
    try:
        with pytest.raises(FetchDisconnectedError):
            await old
        deliver(sub, b"new")
        assert (await asyncio.wait_for(new, 1)).data == b"new"
    finally:
        old.cancel()
        new.cancel()
        await asyncio.gather(old, new, return_exceptions=True)


@pytest.mark.asyncio
async def test_disconnect_interrupts_all_waits_in_the_same_generation():
    nc, sub, pull = make_pull()
    connection_lost = nc._connection_lost()
    workers = [asyncio.create_task(pull._next_msg(None, connection_lost)) for _ in range(3)]
    try:
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        nc._notify_connection_lost()
        for worker in workers:
            with pytest.raises(FetchDisconnectedError):
                await asyncio.wait_for(worker, 1)
        assert not sub._pending_next_msgs_calls
    finally:
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)


@pytest.mark.asyncio
async def test_timeout_removes_the_disconnect_callback():
    nc, sub, pull = make_pull()
    connection_lost = nc._connection_lost()
    with pytest.raises(nats.errors.TimeoutError):
        await pull._next_msg(0.001, connection_lost)
    assert not sub._pending_next_msgs_calls
    nc._notify_connection_lost()
    deliver(sub, b"next")
    assert (await pull._next_msg(1, nc._connection_lost())).data == b"next"
