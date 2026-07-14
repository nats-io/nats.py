"""Tests for JetStream ordered consumer functionality.

Ported from Go's jetstream/test/ordered_test.go.
"""

import asyncio
from datetime import timedelta

import pytest
from nats.client import ClientStatus, connect
from nats.jetstream import JetStream, OrderedConsumerClosedError, OrderedConsumerConfig, OrderedConsumerResetError
from nats.jetstream import new as new_jetstream
from nats.server import Server, run


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


async def test_ordered_consumer_fetch_nowait_no_messages(jetstream: JetStream):
    """fetch_nowait returns empty when no messages available."""
    stream = await jetstream.create_stream(name="test_oc_fnw_empty", subjects=["OC.FNWE.*"])
    consumer = await stream.ordered_consumer(filter_subjects=["OC.FNWE.*"])

    batch = await consumer.fetch_nowait(max_messages=10)
    received = []
    async for msg in batch:
        received.append(msg)

    assert len(received) == 0


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


async def test_ordered_consumer_next(jetstream: JetStream):
    """Basic next: get a single message."""
    stream = await jetstream.create_stream(name="test_oc_next", subjects=["OC.NEXT.*"])
    await jetstream.publish("OC.NEXT.1", b"hello")

    consumer = await stream.ordered_consumer(filter_subjects=["OC.NEXT.*"])
    msg = await consumer.next(max_wait=5.0)

    assert msg.data == b"hello"
    assert msg.subject == "OC.NEXT.1"


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


async def test_ordered_consumer_next_timeout(jetstream: JetStream):
    """next() times out when no messages are available."""
    stream = await jetstream.create_stream(name="test_oc_next_to", subjects=["OC.NTO.*"])
    consumer = await stream.ordered_consumer(filter_subjects=["OC.NTO.*"])

    with pytest.raises(asyncio.TimeoutError):
        await consumer.next(max_wait=1.0)


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


async def test_ordered_consumer_config_custom_inactive_threshold(jetstream: JetStream):
    """Custom inactive_threshold is reflected."""
    stream = await jetstream.create_stream(name="test_oc_cfg_it", subjects=["OC.CIT.*"])
    consumer = await stream.ordered_consumer(
        inactive_threshold=timedelta(seconds=10),
    )

    info = consumer.info
    assert info.config.inactive_threshold == timedelta(seconds=10)


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
    assert info.config.metadata is not None
    assert info.config.metadata["foo"] == "bar"
    assert info.config.ack_policy == "none"
    assert info.config.mem_storage is True
    assert info.config.num_replicas == 1


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


async def test_ordered_consumer_custom_prefix(jetstream: JetStream):
    """Consumer name uses the custom prefix."""
    stream = await jetstream.create_stream(name="test_oc_pfx", subjects=["OC.PFX.*"])
    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.PFX.*"],
        name_prefix="test",
    )

    assert consumer.name == "test_1"


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


async def test_ordered_consumer_close(jetstream: JetStream):
    """close() prevents further operations."""
    stream = await jetstream.create_stream(name="test_oc_close", subjects=["OC.CLOSE.*"])
    await jetstream.publish("OC.CLOSE.1", b"hello")

    consumer = await stream.ordered_consumer(filter_subjects=["OC.CLOSE.*"])

    msg = await consumer.next(max_wait=5.0)
    assert msg.data == b"hello"

    await consumer.close()

    with pytest.raises(OrderedConsumerClosedError):
        await consumer.fetch(max_messages=1, max_wait=1.0)

    with pytest.raises(OrderedConsumerClosedError):
        await consumer.next(max_wait=1.0)

    with pytest.raises(OrderedConsumerClosedError):
        await consumer.messages(max_wait=1.0)


async def test_ordered_consumer_close_idempotent(jetstream: JetStream):
    """Calling close() multiple times is safe."""
    stream = await jetstream.create_stream(name="test_oc_close_idem", subjects=["OC.CIDEM.*"])
    consumer = await stream.ordered_consumer(filter_subjects=["OC.CIDEM.*"])

    await consumer.close()
    await consumer.close()


async def test_ordered_consumer_context_manager(jetstream: JetStream):
    """OrderedConsumer works as an async context manager."""
    stream = await jetstream.create_stream(name="test_oc_ctx", subjects=["OC.CTX.*"])
    await jetstream.publish("OC.CTX.1", b"hello")

    async with await stream.ordered_consumer(filter_subjects=["OC.CTX.*"]) as consumer:
        msg = await consumer.next(max_wait=5.0)
        assert msg.data == b"hello"

    # After exiting context, consumer should be closed
    with pytest.raises(OrderedConsumerClosedError):
        await consumer.next(max_wait=1.0)


async def test_ordered_consumer_messages_with_custom_start_seq(jetstream: JetStream):
    """messages() with custom start sequence skips earlier messages."""
    stream = await jetstream.create_stream(name="test_oc_msgs_seq", subjects=["OC.MSEQ"])

    for i in range(10):
        await jetstream.publish("OC.MSEQ", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.MSEQ"],
        deliver_policy="by_start_sequence",
        opt_start_seq=6,
    )
    messages = await consumer.messages(max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 5:
            break

    await messages.stop()

    assert len(received) == 5
    assert received[0] == "msg 5"
    assert received[4] == "msg 9"


async def test_ordered_consumer_fetch_max_bytes_delete_consumer(jetstream: JetStream):
    """fetch with max_bytes recovers after consumer deletion."""
    stream = await jetstream.create_stream(name="test_oc_fb_del", subjects=["OC.FBDEL.*"])

    for i in range(5):
        await jetstream.publish(f"OC.FBDEL.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FBDEL.*"])

    # First fetch by bytes (large enough to get all messages)
    batch = await consumer.fetch(max_bytes=1024 * 1024, max_wait=5.0)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())
    assert len(received) == 5

    # Delete the consumer
    await stream.delete_consumer(consumer.name)

    # Publish more messages
    for i in range(5):
        await jetstream.publish(f"OC.FBDEL.more{i}", f"more {i}".encode())

    # Second fetch should recover
    batch2 = await consumer.fetch(max_bytes=1024 * 1024, max_wait=5.0)
    received2 = []
    async for msg in batch2:
        received2.append(msg.data.decode())

    assert len(received2) == 5
    assert len(received) + len(received2) == 10


async def test_ordered_consumer_reset_before_receiving_messages(jetstream: JetStream):
    """Consumer recovers when reset occurs before any messages are received."""
    stream = await jetstream.create_stream(name="test_oc_rst_early", subjects=["OC.RSTE.*"])

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.RSTE.*"],
        name_prefix="early",
    )
    assert consumer.name == "early_1"

    # Delete the underlying consumer before any messages are consumed
    await stream.delete_consumer(consumer.name)

    # Publish messages
    for i in range(3):
        await jetstream.publish(f"OC.RSTE.{i}", f"msg {i}".encode())

    # First fetch uses the (now-deleted) initial consumer, gets nothing
    batch = await consumer.fetch(max_messages=3, max_wait=1.0)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())
    assert len(received) == 0
    assert batch.error is not None

    # Second fetch triggers reset and recovers
    batch2 = await consumer.fetch(max_messages=3, max_wait=5.0)
    async for msg in batch2:
        received.append(msg.data.decode())

    assert len(received) == 3
    # Consumer should have been recreated with incremented serial
    assert consumer.name == "early_2"


async def test_ordered_consumer_close_connection(server: Server):
    """Closing the NATS connection terminates ordered consumer iteration."""
    client = await connect(server.client_url)
    js = new_jetstream(client)

    stream = await js.create_stream(name="test_oc_closeconn", subjects=["OC.CC.*"])

    for i in range(3):
        await js.publish(f"OC.CC.{i}", f"msg {i}".encode())

    # Use max_reset_attempts=0 so the consumer doesn't retry forever
    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.CC.*"],
        max_reset_attempts=0,
    )
    messages = await consumer.messages(max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 3:
            break

    assert len(received) == 3

    # Close the connection — the reset loop should fail immediately
    await client.close()

    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) > 10:
            break

    # No new messages should have been received
    assert len(received) == 3


async def test_ordered_consumer_messages_server_restart(server: Server, store_dir: str):
    """messages() recovers after a server restart."""
    client = await connect(server.client_url, reconnect_max_attempts=0)
    js = new_jetstream(client)

    stream = await js.create_stream(name="test_oc_srv_rst", subjects=["OC.SRV.*"])

    for i in range(5):
        await js.publish(f"OC.SRV.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.SRV.*"])
    messages = await consumer.messages(max_wait=10.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 5:
            break

    assert len(received) == 5

    port = server.port

    # Shutdown the server
    await server.shutdown()

    # Restart on the same port and store_dir
    async with await run(port=port, jetstream=True, store_dir=store_dir):
        # Wait for client to reconnect
        for _ in range(50):
            if client.status == ClientStatus.CONNECTED:
                break
            await asyncio.sleep(0.1)
        assert client.status == ClientStatus.CONNECTED

        # Publish more messages
        for i in range(5):
            await js.publish(f"OC.SRV.more{i}", f"more {i}".encode())

        # Should recover and deliver new messages
        async for msg in messages:
            received.append(msg.data.decode())
            if len(received) == 10:
                break

        assert len(received) == 10
        await messages.stop()
        await client.close()


async def test_ordered_consumer_max_reset_attempts_exceeded(server: Server):
    """OrderedConsumerResetError raised when max_reset_attempts is exceeded."""
    client = await connect(server.client_url)
    js = new_jetstream(client)

    stream = await js.create_stream(name="test_oc_max_reset", subjects=["OC.MR.*"])

    for i in range(3):
        await js.publish(f"OC.MR.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.MR.*"],
        max_reset_attempts=1,
        name_prefix="maxreset",
    )

    # Consume first batch to advance the cursor
    batch = await consumer.fetch(max_messages=3, max_wait=5.0)
    async for _ in batch:
        pass

    # Delete the stream so reset cannot create a new consumer
    await js.delete_stream("test_oc_max_reset")

    # Publish to trigger a fetch that requires reset
    # The reset should fail since the stream is gone
    with pytest.raises(OrderedConsumerResetError):
        await consumer.fetch(max_messages=1, max_wait=2.0)

    await client.close()


async def test_ordered_consumer_max_reset_attempts_exceeded_messages(server: Server):
    """OrderedConsumerResetError terminates messages() iteration when max_reset_attempts exceeded."""
    client = await connect(server.client_url)
    js = new_jetstream(client)

    stream = await js.create_stream(name="test_oc_max_reset_msgs", subjects=["OC.MRM.*"])

    for i in range(3):
        await js.publish(f"OC.MRM.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.MRM.*"],
        max_reset_attempts=1,
    )

    messages = await consumer.messages(max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 3:
            # Delete the stream to make the next reset fail
            await js.delete_stream("test_oc_max_reset_msgs")
            # Delete the underlying consumer to trigger reset
            try:
                await stream.delete_consumer(consumer.name)
            except Exception:
                pass
            break

    assert len(received) == 3

    # The next iteration should terminate (StopAsyncIteration from the reset failure)
    more = []
    async for msg in messages:
        more.append(msg)

    assert len(more) == 0
    await client.close()


async def test_ordered_consumer_close_stops_messages_iteration(jetstream: JetStream):
    """Closing the consumer stops an active messages() iteration."""
    stream = await jetstream.create_stream(name="test_oc_close_msgs", subjects=["OC.CM.*"])

    for i in range(3):
        await jetstream.publish(f"OC.CM.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.CM.*"],
        max_reset_attempts=0,
    )
    messages = await consumer.messages(max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 3:
            break

    assert len(received) == 3

    # Close consumer and verify iteration terminates
    await consumer.close()
    more = []
    async for msg in messages:
        more.append(msg)
    assert len(more) == 0


async def test_ordered_consumer_fetch_close_raises(jetstream: JetStream):
    """fetch() raises OrderedConsumerClosedError after close()."""
    stream = await jetstream.create_stream(name="test_oc_fetch_close", subjects=["OC.FC.*"])

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FC.*"])
    await consumer.close()

    with pytest.raises(OrderedConsumerClosedError):
        await consumer.fetch(max_messages=1, max_wait=1.0)


async def test_ordered_consumer_fetch_nowait_close_raises(jetstream: JetStream):
    """fetch_nowait() raises OrderedConsumerClosedError after close()."""
    stream = await jetstream.create_stream(name="test_oc_fnw_close", subjects=["OC.FNWC.*"])

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FNWC.*"])
    await consumer.close()

    with pytest.raises(OrderedConsumerClosedError):
        await consumer.fetch_nowait(max_messages=1)


async def test_ordered_consumer_messages_deliver_policy_new(jetstream: JetStream):
    """messages() with deliver_policy='new' only gets new messages."""
    stream = await jetstream.create_stream(name="test_oc_msgs_new", subjects=["OC.MNEW"])

    # Publish before creating consumer
    for i in range(5):
        await jetstream.publish("OC.MNEW", f"old {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.MNEW"],
        deliver_policy="new",
    )
    messages = await consumer.messages(max_wait=2.0)

    # Publish new messages after consumer creation
    for i in range(3):
        await jetstream.publish("OC.MNEW", f"new {i}".encode())

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 3:
            break

    await messages.stop()

    assert len(received) == 3
    for i in range(3):
        assert received[i] == f"new {i}"


async def test_ordered_consumer_messages_deliver_policy_last(jetstream: JetStream):
    """messages() with deliver_policy='last' starts from last message."""
    stream = await jetstream.create_stream(name="test_oc_msgs_last", subjects=["OC.MLAST"])

    for i in range(5):
        await jetstream.publish("OC.MLAST", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.MLAST"],
        deliver_policy="last",
    )
    messages = await consumer.messages(max_wait=2.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 1:
            break

    await messages.stop()

    assert len(received) == 1
    assert received[0] == "msg 4"


async def test_ordered_consumer_messages_deliver_last_per_subject(jetstream: JetStream):
    """messages() with deliver_policy='last_per_subject' gets last message per subject."""
    stream = await jetstream.create_stream(name="test_oc_msgs_lps", subjects=["OC.MLPS.*"])

    for i in range(5):
        await jetstream.publish("OC.MLPS.A", f"a-{i}".encode())
        await jetstream.publish("OC.MLPS.B", f"b-{i}".encode())
        await jetstream.publish("OC.MLPS.C", f"c-{i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.MLPS.*"],
        deliver_policy="last_per_subject",
    )
    messages = await consumer.messages(max_wait=2.0)

    received = []
    async for msg in messages:
        received.append((msg.subject, msg.data.decode()))
        if len(received) == 3:
            break

    await messages.stop()

    assert len(received) == 3
    subjects = {r[0] for r in received}
    assert subjects == {"OC.MLPS.A", "OC.MLPS.B", "OC.MLPS.C"}
    # Each should be the last message for that subject
    data = {r[0]: r[1] for r in received}
    assert data["OC.MLPS.A"] == "a-4"
    assert data["OC.MLPS.B"] == "b-4"
    assert data["OC.MLPS.C"] == "c-4"


async def test_ordered_consumer_messages_multiple_filter_subjects(jetstream: JetStream):
    """messages() with multiple filter_subjects works correctly."""
    stream = await jetstream.create_stream(name="test_oc_msgs_mf", subjects=["OC.MF.*"])

    await jetstream.publish("OC.MF.A", b"alpha")
    await jetstream.publish("OC.MF.B", b"beta")
    await jetstream.publish("OC.MF.C", b"gamma")  # Not in filter
    await jetstream.publish("OC.MF.A", b"alpha2")

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.MF.A", "OC.MF.B"],
    )
    messages = await consumer.messages(max_wait=2.0)

    received = []
    async for msg in messages:
        received.append((msg.subject, msg.data.decode()))
        if len(received) == 3:
            break

    await messages.stop()

    assert len(received) == 3
    subjects = [r[0] for r in received]
    assert "OC.MF.C" not in subjects
    assert subjects.count("OC.MF.A") == 2
    assert subjects.count("OC.MF.B") == 1


async def test_ordered_consumer_headers_only(jetstream: JetStream):
    """headers_only=True delivers messages without bodies."""
    stream = await jetstream.create_stream(name="test_oc_hdr_only", subjects=["OC.HDR"])

    await jetstream.publish("OC.HDR", b"this body should not appear")

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.HDR"],
        headers_only=True,
    )
    msg = await consumer.next(max_wait=5.0)

    # With headers_only, data should be empty
    assert msg.data == b""
    # Should have Nats-Msg-Size header indicating original payload size
    assert msg.headers is not None
    assert msg.headers.get("Nats-Msg-Size") is not None


async def test_ordered_consumer_stream_delete_stops_messages(server: Server):
    """Deleting the underlying stream terminates ordered consumer."""
    client = await connect(server.client_url)
    js = new_jetstream(client)

    stream = await js.create_stream(name="test_oc_sdel", subjects=["OC.SDEL.*"])

    for i in range(3):
        await js.publish(f"OC.SDEL.{i}", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.SDEL.*"],
        max_reset_attempts=1,
    )
    messages = await consumer.messages(max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 3:
            break

    assert len(received) == 3

    # Delete the entire stream
    await js.delete_stream("test_oc_sdel")

    # Delete underlying consumer to trigger reset (which should fail since stream is gone)
    # The messages iterator should terminate
    more = []
    async for msg in messages:
        more.append(msg)

    assert len(more) == 0
    await client.close()


async def test_ordered_consumer_messages_resume_after_delete_consumer(jetstream: JetStream):
    """messages() resumes from correct position after consumer is deleted."""
    stream = await jetstream.create_stream(name="test_oc_msgs_res", subjects=["OC.MRES"])

    for i in range(5):
        await jetstream.publish("OC.MRES", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.MRES"],
        name_prefix="resume",
    )
    assert consumer.name == "resume_1"

    messages = await consumer.messages(max_wait=5.0)

    # Consume all initial messages
    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 5:
            break

    assert received == [f"msg {i}" for i in range(5)]
    last_stream_seq = consumer._cursor.stream_seq

    # Delete consumer to force reset on next iteration
    await stream.delete_consumer(consumer.name)

    # Publish more messages
    for i in range(5):
        await jetstream.publish("OC.MRES", f"more {i}".encode())

    # Continue the same iterator — should reset and resume from stream_seq+1
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 10:
            break

    await messages.stop()

    assert len(received) == 10
    # Verify the new messages start after where we left off
    assert received[5:] == [f"more {i}" for i in range(5)]
    # Verify the consumer was recreated (serial incremented)
    assert consumer.name == "resume_2"
    # Verify cursor advanced past the original messages
    assert consumer._cursor.stream_seq > last_stream_seq


async def test_ordered_consumer_fetch_no_args_raises(jetstream: JetStream):
    """fetch() raises ValueError when neither max_messages nor max_bytes provided."""
    stream = await jetstream.create_stream(name="test_oc_f_noargs", subjects=["OC.FNA.*"])
    consumer = await stream.ordered_consumer(filter_subjects=["OC.FNA.*"])

    with pytest.raises(ValueError):
        await consumer.fetch(max_wait=1.0)


async def test_ordered_consumer_fetch_nowait_no_args_raises(jetstream: JetStream):
    """fetch_nowait() raises ValueError when neither max_messages nor max_bytes provided."""
    stream = await jetstream.create_stream(name="test_oc_fnw_noargs", subjects=["OC.FNWNA.*"])
    consumer = await stream.ordered_consumer(filter_subjects=["OC.FNWNA.*"])

    with pytest.raises(ValueError):
        await consumer.fetch_nowait()


async def test_ordered_consumer_messages_sequence_tracking_across_resets(jetstream: JetStream):
    """Stream sequence tracking is maintained across multiple resets."""
    stream = await jetstream.create_stream(name="test_oc_seq_track", subjects=["OC.SEQT"])

    for i in range(15):
        await jetstream.publish("OC.SEQT", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.SEQT"],
        name_prefix="seq",
    )

    # Fetch first batch
    batch1 = await consumer.fetch(max_messages=5, max_wait=5.0)
    received = []
    async for msg in batch1:
        received.append(msg.data.decode())
    assert len(received) == 5

    # Delete and force reset
    await stream.delete_consumer(consumer.name)

    # Fetch second batch - should continue from where we left off
    batch2 = await consumer.fetch(max_messages=5, max_wait=5.0)
    async for msg in batch2:
        received.append(msg.data.decode())
    assert len(received) == 10

    # Delete again
    await stream.delete_consumer(consumer.name)

    # Third batch
    batch3 = await consumer.fetch(max_messages=5, max_wait=5.0)
    async for msg in batch3:
        received.append(msg.data.decode())
    assert len(received) == 15

    # Verify all messages in order
    for i in range(15):
        assert received[i] == f"msg {i}"


async def test_ordered_consumer_metadata_propagated(jetstream: JetStream):
    """Custom metadata is propagated to the server-side consumer."""
    stream = await jetstream.create_stream(name="test_oc_meta", subjects=["OC.META.*"])

    metadata = {"app": "test", "version": "1.0", "environment": "staging"}
    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.META.*"],
        metadata=metadata,
    )

    info = consumer.info
    assert info.config.metadata is not None
    assert info.config.metadata["app"] == "test"
    assert info.config.metadata["version"] == "1.0"
    assert info.config.metadata["environment"] == "staging"


async def test_ordered_consumer_name_property_before_and_after_close(jetstream: JetStream):
    """name property returns fallback name after consumer is closed."""
    stream = await jetstream.create_stream(name="test_oc_name_close", subjects=["OC.NC.*"])

    consumer = await stream.ordered_consumer(
        filter_subjects=["OC.NC.*"],
        name_prefix="nc",
    )
    assert consumer.name == "nc_1"

    await consumer.close()

    # After close, _current_consumer is None, should return prefix_0
    assert consumer.name == "nc_0"


async def test_ordered_consumer_info_raises_after_close(jetstream: JetStream):
    """info property raises OrderedConsumerClosedError after close."""
    stream = await jetstream.create_stream(name="test_oc_info_close", subjects=["OC.IC.*"])

    consumer = await stream.ordered_consumer(filter_subjects=["OC.IC.*"])
    await consumer.close()

    with pytest.raises(OrderedConsumerClosedError):
        _ = consumer.info

    with pytest.raises(OrderedConsumerClosedError):
        await consumer.get_info()


async def test_ordered_consumer_stream_name_property(jetstream: JetStream):
    """stream_name property returns the correct stream name."""
    stream = await jetstream.create_stream(name="test_oc_sname", subjects=["OC.SN.*"])
    consumer = await stream.ordered_consumer(filter_subjects=["OC.SN.*"])
    assert consumer.stream_name == "test_oc_sname"


async def test_ordered_consumer_fetch_max_bytes(jetstream: JetStream):
    """fetch with max_bytes returns messages within byte limit."""
    stream = await jetstream.create_stream(name="test_oc_fetch_mb", subjects=["OC.FMB"])

    # Publish messages with known sizes
    for i in range(10):
        await jetstream.publish("OC.FMB", f"message-{i:04d}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.FMB"])

    # Fetch with a large enough byte limit to get all messages
    batch = await consumer.fetch(max_bytes=1024 * 1024, max_wait=5.0)
    received = []
    async for msg in batch:
        received.append(msg.data.decode())

    assert len(received) == 10
    for i in range(10):
        assert received[i] == f"message-{i:04d}"


async def test_ordered_consumer_context_manager_on_exception(jetstream: JetStream):
    """Context manager cleans up even when an exception occurs."""
    stream = await jetstream.create_stream(name="test_oc_ctx_exc", subjects=["OC.CTXE.*"])
    await jetstream.publish("OC.CTXE.1", b"hello")

    consumer = None
    with pytest.raises(RuntimeError, match="test error"):
        async with await stream.ordered_consumer(filter_subjects=["OC.CTXE.*"]) as c:
            consumer = c
            msg = await c.next(max_wait=5.0)
            assert msg.data == b"hello"
            raise RuntimeError("test error")

    # Consumer should be closed despite the exception
    assert consumer is not None
    with pytest.raises(OrderedConsumerClosedError):
        await consumer.next(max_wait=1.0)


async def test_ordered_consumer_messages_stop_is_idempotent(jetstream: JetStream):
    """Calling stop() multiple times on a message stream is safe."""
    stream = await jetstream.create_stream(name="test_oc_msgs_stop", subjects=["OC.MSTOP"])
    await jetstream.publish("OC.MSTOP", b"hello")

    consumer = await stream.ordered_consumer(filter_subjects=["OC.MSTOP"])
    messages = await consumer.messages(max_wait=2.0)

    async for msg in messages:
        assert msg.data == b"hello"
        break

    await messages.stop()
    await messages.stop()  # Should not raise


async def test_ordered_consumer_messages_stop_terminates_iteration(jetstream: JetStream):
    """Calling stop() on a message stream terminates iteration."""
    stream = await jetstream.create_stream(name="test_oc_msgs_stop2", subjects=["OC.MSTOP2"])

    for i in range(10):
        await jetstream.publish("OC.MSTOP2", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.MSTOP2"])
    messages = await consumer.messages(max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 3:
            await messages.stop()

    # Should have stopped after 3 messages
    assert len(received) == 3


async def test_ordered_consumer_fetch_batch_error_property(jetstream: JetStream):
    """MessageBatch.error is None for a successful batch."""
    stream = await jetstream.create_stream(name="test_oc_batch_err", subjects=["OC.BERR"])

    for i in range(3):
        await jetstream.publish("OC.BERR", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.BERR"])
    batch = await consumer.fetch(max_messages=3, max_wait=5.0)

    received = []
    async for msg in batch:
        received.append(msg.data.decode())

    assert len(received) == 3
    assert batch.error is None


async def test_ordered_consumer_messages_max_messages_continues(jetstream: JetStream):
    """messages() with max_messages continues past the batch limit."""
    stream = await jetstream.create_stream(name="test_oc_mm_cont", subjects=["OC.MMCONT"])

    for i in range(10):
        await jetstream.publish("OC.MMCONT", f"msg {i}".encode())

    consumer = await stream.ordered_consumer(filter_subjects=["OC.MMCONT"])
    # max_messages=3 is the internal batch size, not a total limit
    messages = await consumer.messages(max_messages=3, max_wait=5.0)

    received = []
    async for msg in messages:
        received.append(msg.data.decode())
        if len(received) == 10:
            break

    await messages.stop()

    # All 10 messages should be received despite max_messages=3 per batch
    assert len(received) == 10
    for i in range(10):
        assert received[i] == f"msg {i}"
