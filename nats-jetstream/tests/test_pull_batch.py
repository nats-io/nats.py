"""Pull batch status handling and optional warning inspection."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from nats.client import Message as ClientMessage
from nats.client import Status
from nats.jetstream import JetStream
from nats.jetstream.consumer.ordered import _OrderedMessageBatch
from nats.jetstream.consumer.pull import PullMessageBatch
from nats.jetstream.errors import ConsumerDeletedError


@pytest.mark.parametrize(
    "description",
    [
        "Exceeded MaxRequestBatch of 1",
        "Exceeded MaxRequestExpires of 1s",
        "Exceeded MaxRequestMaxBytes of 1024",
        "Exceeded MaxWaiting",
    ],
)
async def test_pull_batch_preserves_limit_warning(description):
    """A rejected pull remains nonfatal but exposes the server's reason."""
    status = Status("409", description)
    subscription = Mock(next=AsyncMock(return_value=ClientMessage("reply", b"", status=status)))
    subscription.unsubscribe = AsyncMock()
    batch = PullMessageBatch(subscription, 2, None, max_wait=1)

    assert batch.warning is None
    assert [message async for message in batch] == []
    assert batch.error is None
    assert batch.warning == status
    assert [message async for message in batch] == []
    assert batch.warning == status
    subscription.unsubscribe.assert_awaited_once()
    subscription.next.assert_awaited_once()


@pytest.mark.parametrize(
    "code,description", [("404", "No Messages"), ("408", "Request Timeout"), ("409", "Message Size Exceeds MaxBytes")]
)
async def test_pull_batch_normal_completion_has_no_warning(code, description):
    """An idle stream or expired request must not look like a rejected pull."""
    subscription = Mock(next=AsyncMock(return_value=ClientMessage("reply", b"", status=Status(code, description))))
    subscription.unsubscribe = AsyncMock()
    batch = PullMessageBatch(subscription, 2, None, max_wait=1)

    assert [message async for message in batch] == []
    assert batch.warning is None
    assert batch.error is None
    subscription.unsubscribe.assert_awaited_once()


@pytest.mark.parametrize(
    "code,description,error_type",
    [
        ("400", "Bad Request", ValueError),
        ("409", "Consumer Deleted", ConsumerDeletedError),
        ("409", "Consumer is Push Based", ValueError),
        ("409", "Unrecognized Conflict", Exception),
        ("503", "Service Unavailable", Exception),
    ],
)
async def test_pull_batch_terminal_error_is_not_a_warning(code, description, error_type):
    """The warning result must not downgrade terminal errors."""
    subscription = Mock(next=AsyncMock(return_value=ClientMessage("reply", b"", status=Status(code, description))))
    subscription.unsubscribe = AsyncMock()
    batch = PullMessageBatch(subscription, 2, None, max_wait=1)

    assert [message async for message in batch] == []
    assert isinstance(batch.error, error_type)
    assert batch.warning is None


async def test_pull_batch_byte_limit_preserves_messages_without_warning():
    """A normal byte boundary keeps payloads and is not an ADR-37 warning."""
    status = Status("409", "Message Size Exceeds MaxBytes")
    subscription = Mock(
        next=AsyncMock(
            side_effect=[
                ClientMessage("reply", b"", status=Status("100", "Idle Heartbeat")),
                ClientMessage("events", b"payload"),
                ClientMessage("reply", b"", status=status),
            ]
        )
    )
    subscription.unsubscribe = AsyncMock()
    batch = PullMessageBatch(subscription, 2, None, max_wait=1)

    assert [message.data async for message in batch] == [b"payload"]
    assert batch.warning is None
    assert batch.error is None


@pytest.mark.parametrize("max_wait", [0, 1])
async def test_pull_batch_client_timeout_has_no_warning(max_wait):
    """A client-side deadline is not a server rejection."""
    subscription = Mock(next=AsyncMock(side_effect=TimeoutError))
    subscription.unsubscribe = AsyncMock()
    batch = PullMessageBatch(subscription, 1, None, max_wait=max_wait)

    assert [message async for message in batch] == []
    assert batch.error is None
    assert batch.warning is None
    subscription.unsubscribe.assert_awaited_once()


async def test_pull_batch_full_delivery_has_no_warning():
    """A successfully filled batch completes without waiting for a status."""
    subscription = Mock(next=AsyncMock(return_value=ClientMessage("events", b"payload")))
    subscription.unsubscribe = AsyncMock()
    batch = PullMessageBatch(subscription, 1, None, max_wait=1)

    assert [message.data async for message in batch] == [b"payload"]
    assert batch.warning is None
    assert batch.error is None
    subscription.next.assert_awaited_once()
    subscription.unsubscribe.assert_awaited_once()


async def test_ordered_batch_exposes_inner_warning():
    """Ordered consumers must preserve the same public batch result."""
    status = Status("409", "Exceeded MaxRequestBatch of 1")
    subscription = Mock(next=AsyncMock(return_value=ClientMessage("reply", b"", status=status)))
    subscription.unsubscribe = AsyncMock()
    inner = PullMessageBatch(subscription, 2, None, max_wait=1)
    batch = _OrderedMessageBatch(Mock(), inner)

    assert batch.warning is None
    assert [message async for message in batch] == []
    assert batch.warning == status
    assert batch.error is None


async def test_fetch_reports_server_batch_limit(jetstream: JetStream):
    """A real broker's batch limit is distinguishable from an empty stream."""
    stream = await jetstream.create_stream(name="batch_limit", subjects=["batch_limit"])
    consumer = await stream.create_consumer(name="worker", max_batch=1)

    rejected = await consumer.fetch(max_messages=2, max_wait=1)
    assert [message async for message in rejected] == []
    assert rejected.error is None
    assert rejected.warning is not None
    assert rejected.warning.code == "409"
    assert rejected.warning.description == "Exceeded MaxRequestBatch of 1"

    idle = await consumer.fetch_nowait(max_messages=1)
    assert [message async for message in idle] == []
    assert idle.error is None
    assert idle.warning is None


async def test_fetch_reports_server_waiting_limit(jetstream: JetStream):
    """Waiting-capacity pressure does not become a fatal batch error."""
    stream = await jetstream.create_stream(name="waiting_limit", subjects=["waiting_limit"])
    consumer = await stream.create_consumer(name="worker", max_waiting=1, ack_policy="explicit")
    first = await consumer.fetch(max_messages=1, max_wait=5)
    try:
        async with asyncio.timeout(2):
            while (await jetstream.get_consumer_info(stream.name, consumer.name)).num_waiting != 1:
                await asyncio.sleep(0.001)
        rejected = await consumer.fetch(max_messages=1, max_wait=1)
        assert [message async for message in rejected] == []
        assert rejected.error is None
        assert rejected.warning == Status("409", "Exceeded MaxWaiting")
    finally:
        await jetstream.publish("waiting_limit", b"release")
        async for message in first:
            await message.double_ack()
