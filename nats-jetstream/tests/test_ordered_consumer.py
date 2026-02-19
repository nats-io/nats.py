"""Tests for JetStream ordered consumer functionality.

Ported from Go's jetstream/test/ordered_test.go.
"""

import asyncio
from datetime import timedelta

import pytest
from nats.jetstream import JetStream, OrderedConsumerConfig

# ---------------------------------------------------------------------------
# messages() (iterator-based)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordered_consumer_messages(jetstream: JetStream):
    """Basic messages() usage: publish, iterate, get all messages."""
    stream = await jetstream.create_stream(name="test_oc_msgs", subjects=["OC.MSGS.*"])

    for i in range(10):
        await jetstream.publish(f"OC.MSGS.{i}", f"message {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.MSGS.*"])
    messages = await consumer.messages(max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 10:
            break

    assert len(received) == 10
    for i in range(10):
        assert received[i] == f"message {i}"

    await messages.stop()


@pytest.mark.asyncio
async def test_ordered_consumer_messages_delete_consumer(jetstream: JetStream):
    """Messages recovers after the underlying consumer is deleted."""
    stream = await jetstream.create_stream(name="test_oc_msgs_del", subjects=["OC.MDEL.*"])

    for i in range(5):
        await jetstream.publish(f"OC.MDEL.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.MDEL.*"])
    messages = await consumer.messages(max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 5:
            break

    assert len(received) == 5

    # Delete the underlying server-side consumer
    await stream.delete_consumer(consumer.name)

    # Publish more messages
    for i in range(5):
        await jetstream.publish(f"OC.MDEL.more{i}", f"more {i}".encode())

    # Should recover and deliver the new messages
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 10:
            break

    assert len(received) == 10
    await messages.stop()


@pytest.mark.asyncio
async def test_ordered_consumer_messages_in_order(jetstream: JetStream):
    """Messages are delivered in strict stream sequence order."""
    stream = await jetstream.create_stream(name="test_oc_msgs_ord", subjects=["OC.MORD"])

    for i in range(20):
        await jetstream.publish("OC.MORD", f"{i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.MORD"])
    messages = await consumer.messages(max_wait=5.0)

    received = []
    prev_stream_seq = 0
    async for msg in messages:
        received.append(int(msg.data.decode()))
        assert msg.metadata.sequence.stream > prev_stream_seq
        prev_stream_seq = msg.metadata.sequence.stream
        if len(received) == 20:
            break

    assert received == list(range(20))
    await messages.stop()


# ---------------------------------------------------------------------------
# fetch()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordered_consumer_fetch(jetstream: JetStream):
    """Basic fetch: publish and fetch a batch."""
    stream = await jetstream.create_stream(name="test_oc_fetch", subjects=["OC.FETCH.*"])

    for i in range(5):
        await jetstream.publish(f"OC.FETCH.{i}", f"message {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FETCH.*"])
    batch = await consumer.fetch(max_messages=5, max_wait=5.0)

    received = []
    async for msg in batch:
        received.append(msg.data.decode())

    assert len(received) == 5
    for i in range(5):
        assert received[i] == f"message {i}"


@pytest.mark.asyncio
async def test_ordered_consumer_fetch_delete_consumer(jetstream: JetStream):
    """Fetch recovers after the underlying consumer is deleted between fetches."""
    stream = await jetstream.create_stream(name="test_oc_fetch_del", subjects=["OC.FDEL.*"])

    for i in range(5):
        await jetstream.publish(f"OC.FDEL.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FDEL.*"])

    # First fetch
    batch = await consumer.fetch(max_messages=5, max_wait=5.0)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())
    assert len(received) == 5

    # Delete the consumer
    await stream.delete_consumer(consumer.name)

    # Publish more messages
    for i in range(5):
        await jetstream.publish(f"OC.FDEL.more{i}", f"more {i}".encode())

    # Second fetch should recover and get new messages
    batch2 = await consumer.fetch(max_messages=5, max_wait=5.0)
    received2 = []
    async for msg in batch2:
        received2.append(msg.data.decode())

    assert len(received2) == 5
    assert len(received) + len(received2) == 10


@pytest.mark.asyncio
async def test_ordered_consumer_fetch_deliver_last_per_subject(jetstream: JetStream):
    """Fetch with deliver_policy='last_per_subject'."""
    stream = await jetstream.create_stream(name="test_oc_fetch_lps", subjects=["OC.FLPS.*"])

    # Publish 5 messages to each of two subjects
    for i in range(5):
        await jetstream.publish("OC.FLPS.A", f"a-{i}".encode())
        await jetstream.publish("OC.FLPS.B", f"b-{i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.FLPS.*"],
        deliver_policy="last_per_subject",
    )

    batch = await consumer.fetch(max_messages=10, max_wait=5.0)
    received = []
    async for msg in batch:
        received.append((msg.subject, msg.data.decode()))

    # Should get exactly 2 messages (last per subject)
    assert len(received) == 2
    subjects = [r[0] for r in received]
    assert "OC.FLPS.A" in subjects
    assert "OC.FLPS.B" in subjects


@pytest.mark.asyncio
async def test_ordered_consumer_fetch_multiple(jetstream: JetStream):
    """Multiple sequential fetch calls get consecutive messages."""
    stream = await jetstream.create_stream(name="test_oc_fetch_multi", subjects=["OC.FMULTI"])

    for i in range(10):
        await jetstream.publish("OC.FMULTI", f"message {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FMULTI"])

    batch1 = await consumer.fetch(max_messages=5, max_wait=5.0)
    received1 = []
    async for msg in batch1:
        received1.append(msg.data.decode())

    batch2 = await consumer.fetch(max_messages=5, max_wait=5.0)
    received2 = []
    async for msg in batch2:
        received2.append(msg.data.decode())

    assert len(received1) + len(received2) == 10
    # Verify ordering across fetches
    all_msgs = received1 + received2
    for i in range(10):
        assert all_msgs[i] == f"message {i}"


# ---------------------------------------------------------------------------
# fetch_nowait()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordered_consumer_fetch_nowait(jetstream: JetStream):
    """Basic fetch_nowait: returns immediately available messages."""
    stream = await jetstream.create_stream(name="test_oc_fnw", subjects=["OC.FNW.*"])

    for i in range(3):
        await jetstream.publish(f"OC.FNW.{i}", f"message {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FNW.*"])

    batch = await consumer.fetch_nowait(max_messages=10)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())

    assert len(received) == 3


@pytest.mark.asyncio
async def test_ordered_consumer_fetch_nowait_no_messages(jetstream: JetStream):
    """fetch_nowait returns empty when no messages available."""
    stream = await jetstream.create_stream(name="test_oc_fnw_empty", subjects=["OC.FNWE.*"])
    consumer = await stream.ordered_consumer(filter_subjects=["OC.FNWE.*"])

    batch = await consumer.fetch_nowait(max_messages=10)
    received = []
    async for msg in batch:
        received.append(msg)

    assert len(received) == 0


@pytest.mark.asyncio
async def test_ordered_consumer_fetch_nowait_delete_consumer(jetstream: JetStream):
    """fetch_nowait recovers after consumer deletion between fetches."""
    stream = await jetstream.create_stream(name="test_oc_fnw_del", subjects=["OC.FNWD.*"])

    for i in range(5):
        await jetstream.publish(f"OC.FNWD.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FNWD.*"])

    batch = await consumer.fetch_nowait(max_messages=5)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())
    assert len(received) == 5

    # Delete the consumer
    await stream.delete_consumer(consumer.name)

    # Publish more messages
    for i in range(5):
        await jetstream.publish(f"OC.FNWD.more{i}", f"more {i}".encode())

    batch2 = await consumer.fetch_nowait(max_messages=5)
    received2 = []
    async for msg in batch2:
        received2.append(msg.data.decode())

    assert len(received2) == 5
    assert len(received) + len(received2) == 10


# ---------------------------------------------------------------------------
# next()
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordered_consumer_next(jetstream: JetStream):
    """Basic next: get a single message."""
    stream = await jetstream.create_stream(name="test_oc_next", subjects=["OC.NEXT.*"])
    await jetstream.publish("OC.NEXT.1", b"hello")

    consumer = await stream.ordered_consumer(filter_subjects=["OC.NEXT.*"])
    msg = await consumer.next(max_wait=5.0)

    assert msg.data == b"hello"
    assert msg.subject == "OC.NEXT.1"


@pytest.mark.asyncio
async def test_ordered_consumer_next_delete_consumer(jetstream: JetStream):
    """next() recovers after the underlying consumer is deleted."""
    stream = await jetstream.create_stream(name="test_oc_next_del", subjects=["OC.NDEL.*"])
    await jetstream.publish("OC.NDEL.1", b"first")

    consumer = await stream.ordered_consumer(filter_subjects=["OC.NDEL.*"])

    msg = await consumer.next(max_wait=5.0)
    assert msg.data == b"first"

    # Delete consumer
    await stream.delete_consumer(consumer.name)

    # Publish another message
    await jetstream.publish("OC.NDEL.2", b"second")

    # next() should recover and return the new message
    msg2 = await consumer.next(max_wait=5.0)
    assert msg2.data == b"second"


@pytest.mark.asyncio
async def test_ordered_consumer_next_timeout(jetstream: JetStream):
    """next() times out when no messages are available."""
    stream = await jetstream.create_stream(name="test_oc_next_to", subjects=["OC.NTO.*"])
    consumer = await stream.ordered_consumer(filter_subjects=["OC.NTO.*"])

    with pytest.raises(asyncio.TimeoutError):
        await consumer.next(max_wait=1.0)


@pytest.mark.asyncio
async def test_ordered_consumer_next_preserve_sequence_after_timeout(jetstream: JetStream):
    """Sequence is preserved after a timeout (not reset)."""
    stream = await jetstream.create_stream(name="test_oc_next_seq", subjects=["OC.NSEQ"])

    await jetstream.publish("OC.NSEQ", b"msg1")

    consumer = await stream.ordered_consumer(filter_subjects=["OC.NSEQ"])

    # Get first message (seq 1)
    msg1 = await consumer.next(max_wait=5.0)
    assert msg1.metadata.sequence.stream == 1

    # Timeout - no more messages
    with pytest.raises(asyncio.TimeoutError):
        await consumer.next(max_wait=1.0)

    # Publish another and get it
    await jetstream.publish("OC.NSEQ", b"msg2")
    msg2 = await consumer.next(max_wait=5.0)
    assert msg2.metadata.sequence.stream == 2


@pytest.mark.asyncio
async def test_ordered_consumer_next_order(jetstream: JetStream):
    """Messages via next() arrive in strict sequence order."""
    stream = await jetstream.create_stream(name="test_oc_next_order", subjects=["OC.NORD"])

    for i in range(100):
        await jetstream.publish("OC.NORD", f"{i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.NORD"])

    for i in range(100):
        msg = await consumer.next(max_wait=5.0)
        assert msg.metadata.sequence.stream == i + 1
        assert msg.data.decode() == str(i)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordered_consumer_config_default(jetstream: JetStream):
    """Default config produces correct server-side consumer."""
    stream = await jetstream.create_stream(name="test_oc_cfg_def", subjects=["OC.CDEF.*"])
    consumer = await stream.ordered_consumer()

    info = consumer.info
    assert info.config.deliver_policy == "all"
    assert info.config.ack_policy == "none"
    assert info.config.mem_storage is True
    assert info.config.num_replicas == 1
    assert info.config.inactive_threshold == timedelta(minutes=5)


@pytest.mark.asyncio
async def test_ordered_consumer_config_custom_inactive_threshold(jetstream: JetStream):
    """Custom inactive_threshold is reflected."""
    stream = await jetstream.create_stream(name="test_oc_cfg_it", subjects=["OC.CIT.*"])
    consumer = await stream.ordered_consumer(
        inactive_threshold=timedelta(seconds=10),
    )

    info = consumer.info
    assert info.config.inactive_threshold == timedelta(seconds=10)


@pytest.mark.asyncio
async def test_ordered_consumer_config_custom_start_seq(jetstream: JetStream):
    """Custom opt_start_seq with by_start_sequence deliver policy."""
    stream = await jetstream.create_stream(name="test_oc_cfg_seq", subjects=["OC.CSEQ.*"])

    for i in range(10):
        await jetstream.publish(f"OC.CSEQ.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.CSEQ.*"],
        deliver_policy="by_start_sequence",
        opt_start_seq=6,
    )

    info = consumer.info
    assert info.config.deliver_policy == "by_start_sequence"
    assert info.config.opt_start_seq == 6


@pytest.mark.asyncio
async def test_ordered_consumer_config_all_fields(jetstream: JetStream):
    """All supported config fields are reflected in server-side consumer."""
    stream = await jetstream.create_stream(name="test_oc_cfg_all", subjects=["OC.CALL.*"])

    config = OrderedConsumerConfig(
        filter_subjects=["OC.CALL.*"],
        deliver_policy="by_start_sequence",
        opt_start_seq=1,
        replay_policy="instant",
        inactive_threshold=timedelta(seconds=10),
        headers_only=True,
        metadata={"foo": "bar"},
    )
    consumer = await stream.ordered_consumer(config)

    info = consumer.info
    assert info.config.deliver_policy == "by_start_sequence"
    assert info.config.opt_start_seq == 1
    assert info.config.replay_policy == "instant"
    assert info.config.inactive_threshold == timedelta(seconds=10)
    assert info.config.headers_only is True
    assert info.config.ack_policy == "none"
    assert info.config.mem_storage is True
    assert info.config.num_replicas == 1


@pytest.mark.asyncio
async def test_ordered_consumer_config_deliver_policy_new(jetstream: JetStream):
    """deliver_policy='new' only delivers messages published after consumer creation."""
    stream = await jetstream.create_stream(name="test_oc_cfg_new", subjects=["OC.CNEW.*"])

    # Publish before consumer
    for i in range(5):
        await jetstream.publish(f"OC.CNEW.{i}", f"old {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.CNEW.*"],
        deliver_policy="new",
    )

    info = consumer.info
    assert info.config.deliver_policy == "new"

    # Publish after consumer
    for i in range(3):
        await jetstream.publish(f"OC.CNEW.new{i}", f"new {i}".encode())

    batch = await consumer.fetch(max_messages=10, max_wait=2.0)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())

    assert len(received) == 3
    for i in range(3):
        assert received[i] == f"new {i}"


@pytest.mark.asyncio
async def test_ordered_consumer_config_deliver_policy_last(jetstream: JetStream):
    """deliver_policy='last' delivers only the last message."""
    stream = await jetstream.create_stream(name="test_oc_cfg_last", subjects=["OC.CLAST"])

    for i in range(5):
        await jetstream.publish("OC.CLAST", f"message {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.CLAST"],
        deliver_policy="last",
    )

    msg = await consumer.next(max_wait=5.0)
    assert msg.data == b"message 4"


# ---------------------------------------------------------------------------
# Custom prefix
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordered_consumer_custom_prefix(jetstream: JetStream):
    """Consumer name uses the custom prefix."""
    stream = await jetstream.create_stream(name="test_oc_pfx", subjects=["OC.PFX.*"])
    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.PFX.*"],
        name_prefix="test",
    )

    assert consumer.name == "test_1"


@pytest.mark.asyncio
async def test_ordered_consumer_custom_prefix_increments_on_recreation(jetstream: JetStream):
    """Consumer serial increments when the consumer is recreated."""
    stream = await jetstream.create_stream(name="test_oc_pfx_inc", subjects=["OC.PFXI.*"])

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.PFXI.*"],
        name_prefix="test",
    )
    assert consumer.name == "test_1"

    # Publish a message so fetch works
    await jetstream.publish("OC.PFXI.1", b"hello")

    # Fetch to consume from initial consumer
    batch = await consumer.fetch(max_messages=1, max_wait=5.0)
    async for _ in batch:
        pass

    # Delete the consumer
    await stream.delete_consumer(consumer.name)

    # Publish another message
    await jetstream.publish("OC.PFXI.2", b"world")

    # Next fetch will trigger reset -> new consumer with incremented serial
    batch2 = await consumer.fetch(max_messages=1, max_wait=5.0)
    async for _ in batch2:
        pass

    assert consumer.name == "test_2"


# ---------------------------------------------------------------------------
# Info
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordered_consumer_info(jetstream: JetStream):
    """info reflects the current underlying consumer."""
    stream = await jetstream.create_stream(name="test_oc_info", subjects=["OC.INFO.*"])
    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.INFO.*"],
        name_prefix="info",
    )

    info = consumer.info
    assert info.name == "info_1"
    assert info.stream_name == "test_oc_info"


@pytest.mark.asyncio
async def test_ordered_consumer_info_changes_after_reset(jetstream: JetStream):
    """After consumer reset, info reflects the new consumer name."""
    stream = await jetstream.create_stream(name="test_oc_info_rst", subjects=["OC.IRST.*"])

    await jetstream.publish("OC.IRST.1", b"msg1")

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.IRST.*"],
        name_prefix="info",
    )

    initial_name = consumer.info.name
    assert initial_name == "info_1"

    # Fetch to consume
    batch = await consumer.fetch(max_messages=1, max_wait=5.0)
    async for _ in batch:
        pass

    # Delete consumer to force reset
    await stream.delete_consumer(consumer.name)

    # Publish more and fetch to trigger reset
    await jetstream.publish("OC.IRST.2", b"msg2")
    batch2 = await consumer.fetch(max_messages=1, max_wait=5.0)
    async for _ in batch2:
        pass

    # Consumer name should have changed
    assert consumer.info.name != initial_name


# ---------------------------------------------------------------------------
# Via JetStream convenience method
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ordered_consumer_via_jetstream(jetstream: JetStream):
    """Creating ordered consumer via JetStream works."""
    await jetstream.create_stream(name="test_oc_js", subjects=["OC.JS.*"])

    for i in range(3):
        await jetstream.publish(f"OC.JS.{i}", f"message {i}".encode())

    consumer = await jetstream.ordered_consumer("test_oc_js", filter_subjects=["OC.JS.*"])

    batch = await consumer.fetch(max_messages=3, max_wait=5.0)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())

    assert len(received) == 3


@pytest.mark.asyncio
async def test_ordered_consumer_via_jetstream_with_config(jetstream: JetStream):
    """Creating ordered consumer via JetStream with config object works."""
    await jetstream.create_stream(name="test_oc_js_cfg", subjects=["OC.JSC.*"])

    for i in range(3):
        await jetstream.publish(f"OC.JSC.{i}", f"message {i}".encode())

    config = OrderedConsumerConfig(
        filter_subjects=["OC.JSC.*"],
        deliver_policy="all",
    )
    consumer = await jetstream.ordered_consumer("test_oc_js_cfg", config)

    batch = await consumer.fetch(max_messages=3, max_wait=5.0)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())

    assert len(received) == 3
