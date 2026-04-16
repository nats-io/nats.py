"""Integration tests for nats-key-value."""

import asyncio
from datetime import timedelta

import pytest
from nats.client import Client
from nats.jetstream import JetStream
from nats.jetstream.stream import Republish
from nats.key_value import (
    BucketExistsError,
    BucketNotFoundError,
    HistoryTooLargeError,
    InvalidBucketNameError,
    InvalidKeyError,
    KeyExistsError,
    KeyNotFoundError,
    KeyValueConfig,
    KeyValueError,
    KeyValueOp,
    WrongLastRevisionError,
    create_key_value,
    delete_key_value,
    key_value,
    update_key_value,
)


async def test_put_and_get(jetstream: JetStream):
    """Simple put and get operations."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=5))

    # Simple put
    rev = await kv.put("name", b"derek")
    assert rev == 1

    # Simple get
    entry = await kv.get("name")
    assert entry.value == b"derek"
    assert entry.revision == 1
    assert entry.key == "name"
    assert entry.bucket == "TEST"
    assert entry.operation == KeyValueOp.PUT


async def test_invalid_key_put(jetstream: JetStream):
    """Put with invalid key raises InvalidKeyError."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    with pytest.raises(InvalidKeyError):
        await kv.put(".invalid", b"value")


async def test_invalid_key_get(jetstream: JetStream):
    """Get with invalid key raises InvalidKeyError."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    with pytest.raises(InvalidKeyError):
        await kv.get(".invalid")


async def test_invalid_key_delete(jetstream: JetStream):
    """Delete with invalid key raises InvalidKeyError."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    with pytest.raises(InvalidKeyError):
        await kv.delete(".invalid")


async def test_delete_and_recreate(jetstream: JetStream):
    """Delete a key and then re-create it."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=5))

    await kv.put("name", b"derek")
    await kv.delete("name")

    # Get after delete should raise KeyNotFoundError
    with pytest.raises(KeyNotFoundError):
        await kv.get("name")

    # Re-create should work
    rev = await kv.create("name", b"derek")
    assert rev == 3


async def test_delete_with_last_revision(jetstream: JetStream):
    """Conditional delete with last_revision."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=5))

    await kv.put("name", b"derek")
    await kv.delete("name")
    rev = await kv.create("name", b"derek")
    assert rev == 3

    # Wrong revision should fail
    with pytest.raises(WrongLastRevisionError):
        await kv.delete("name", last_revision=4)

    # Correct revision should succeed
    await kv.delete("name", last_revision=3)


async def test_conditional_update(jetstream: JetStream):
    """Update with expected revision (CAS)."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=5))

    await kv.put("name", b"derek")
    await kv.delete("name")
    await kv.create("name", b"derek")

    # Update with correct revision
    rev = await kv.update("name", b"rip", 3)

    # Update with stale revision should fail
    with pytest.raises(WrongLastRevisionError):
        await kv.update("name", b"ik", 2)

    # Update with correct revision should succeed
    await kv.update("name", b"ik", rev)


async def test_status(jetstream: JetStream):
    """Status reports correct bucket information."""
    kv = await create_key_value(
        jetstream,
        KeyValueConfig(
            bucket="TEST",
            history=5,
            ttl=timedelta(hours=1),
            metadata={"foo": "bar"},
        ),
    )

    await kv.put("name", b"derek")
    await kv.create("age", b"22")
    await kv.update("age", b"33", 2)

    status = await kv.status()
    assert status.bucket == "TEST"
    assert status.history == 5
    assert status.ttl == timedelta(hours=1)
    assert status.values == 3
    assert status.metadata is not None
    assert status.metadata["foo"] == "bar"


async def test_bucket_property(jetstream: JetStream):
    """Bucket name is accessible via property."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))
    assert kv.bucket == "TEST"


async def test_create_invalid_bucket_name(jetstream: JetStream):
    """Invalid bucket names are rejected."""
    with pytest.raises(InvalidBucketNameError):
        await create_key_value(jetstream, KeyValueConfig(bucket="invalid.name"))

    with pytest.raises(InvalidBucketNameError):
        await create_key_value(jetstream, KeyValueConfig(bucket=""))


async def test_create_history_too_large(jetstream: JetStream):
    """History exceeding 64 is rejected."""
    with pytest.raises(HistoryTooLargeError):
        await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=65))


async def test_create_idempotent(jetstream: JetStream):
    """Creating a bucket with identical config succeeds (idempotent)."""
    await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))
    await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))


async def test_create_duplicate_bucket(jetstream: JetStream):
    """Creating a bucket with different config raises BucketExistsError."""
    await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    with pytest.raises(BucketExistsError):
        await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=10))


async def test_update_bucket(jetstream: JetStream):
    """Updating an existing bucket changes its configuration."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=1))
    await kv.put("key", b"value")

    await update_key_value(jetstream, KeyValueConfig(bucket="TEST", history=10))

    info = await jetstream.get_stream_info("KV_TEST")
    assert info.config.max_msgs_per_subject == 10


async def test_update_nonexistent_bucket(jetstream: JetStream):
    """Updating a nonexistent bucket raises BucketNotFoundError."""
    with pytest.raises(BucketNotFoundError):
        await update_key_value(jetstream, KeyValueConfig(bucket="NOPE"))


async def test_create_stream_config(jetstream: JetStream):
    """Verify the backing stream has correct KV configuration."""
    await create_key_value(
        jetstream,
        KeyValueConfig(
            bucket="TEST",
            description="Test KV",
            max_value_size=128,
            history=10,
            ttl=timedelta(hours=1),
            max_bytes=1024,
            storage="file",
        ),
    )

    info = await jetstream.get_stream_info("KV_TEST")
    assert info.config.name == "KV_TEST"
    assert info.config.description == "Test KV"
    assert info.config.subjects == ["$KV.TEST.>"]
    assert info.config.max_msgs_per_subject == 10
    assert info.config.max_age == timedelta(hours=1)
    assert info.config.max_bytes == 1024
    assert info.config.max_msg_size == 128
    assert info.config.storage == "file"
    assert info.config.discard == "new"
    assert info.config.deny_delete is True
    assert info.config.allow_rollup_hdrs is True
    assert info.config.allow_direct is True


async def test_create_new_key(jetstream: JetStream):
    """Create a new key succeeds."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    rev = await kv.create("key", b"1")
    assert rev == 1


async def test_create_existing_key(jetstream: JetStream):
    """Create a key that already exists raises KeyExistsError."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    await kv.create("key", b"1")

    with pytest.raises(KeyExistsError):
        await kv.create("key", b"2")


async def test_create_after_delete(jetstream: JetStream):
    """Create succeeds on a deleted key (tombstone recovery)."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=5))

    await kv.create("key", b"1")
    await kv.delete("key")

    rev = await kv.create("key", b"2")
    assert rev == 3

    entry = await kv.get("key")
    assert entry.value == b"2"


async def test_create_with_ttl(jetstream: JetStream):
    """Create a key with per-key TTL expires the key after the duration."""
    kv = await create_key_value(
        jetstream,
        KeyValueConfig(bucket="TEST", limit_marker_ttl=timedelta(seconds=1)),
    )

    await kv.create("key", b"value", ttl=timedelta(seconds=1))

    entry = await kv.get("key")
    assert entry.value == b"value"

    for _ in range(10):
        await asyncio.sleep(0.5)
        try:
            await kv.get("key")
        except KeyNotFoundError:
            break
    else:
        pytest.fail("Key did not expire")


async def test_get_specific_revision(jetstream: JetStream):
    """Retrieve a specific historical revision."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=5))

    await kv.put("key", b"value1")
    await kv.put("key", b"value2")

    entry = await kv.get("key", revision=1)
    assert entry.value == b"value1"
    assert entry.revision == 1

    entry = await kv.get("key", revision=2)
    assert entry.value == b"value2"
    assert entry.revision == 2


async def test_get_deleted_revision(jetstream: JetStream):
    """Getting a deleted revision raises KeyNotFoundError."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST", history=5))

    await kv.put("key", b"value1")
    await kv.put("key", b"value2")
    await kv.delete("key")

    # Revision 3 is the delete marker
    with pytest.raises(KeyNotFoundError):
        await kv.get("key", revision=3)


async def test_get_nonexistent(jetstream: JetStream):
    """Get a key that was never created."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    with pytest.raises(KeyNotFoundError):
        await kv.get("nonexistent")


async def test_delete_preserves_history(jetstream: JetStream):
    """Delete adds a marker but preserves history."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="KVS", history=10))

    await kv.put("name", b"derek")
    await kv.put("age", b"22")
    await kv.put("name", b"ivan")
    await kv.put("age", b"33")
    await kv.put("name", b"rip")
    await kv.put("age", b"44")

    await kv.delete("age")

    entries = [entry async for entry in await kv.history("age")]
    # Expect 3 values + delete marker = 4 entries
    assert len(entries) == 4


async def test_purge_removes_history(jetstream: JetStream):
    """Purge removes history, leaving only the purge marker."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="KVS", history=10))

    await kv.put("name", b"derek")
    await kv.put("age", b"22")
    await kv.put("name", b"ivan")
    await kv.put("age", b"33")
    await kv.put("name", b"rip")
    await kv.put("age", b"44")

    await kv.purge("name")

    # Get should fail (purge marker)
    with pytest.raises(KeyNotFoundError):
        await kv.get("name")

    # History should only have the purge marker
    entries = [entry async for entry in await kv.history("name")]
    assert len(entries) == 1
    assert entries[0].operation == KeyValueOp.PURGE


async def test_purge_with_last_revision(jetstream: JetStream):
    """Purge with wrong last_revision fails."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="KVS", history=10))

    await kv.put("name", b"derek")
    await kv.put("name", b"ivan")
    await kv.put("name", b"rip")

    with pytest.raises(WrongLastRevisionError):
        await kv.purge("name", last_revision=2)

    await kv.purge("name", last_revision=3)


async def test_purge_with_ttl(jetstream: JetStream):
    """Purge marker with TTL is removed after the duration."""
    kv = await create_key_value(
        jetstream,
        KeyValueConfig(bucket="KVS", limit_marker_ttl=timedelta(seconds=1)),
    )

    await kv.put("age", b"22")
    await kv.purge("age", ttl=timedelta(seconds=1))

    with pytest.raises(KeyNotFoundError):
        await kv.get("age")

    # Purge marker should exist initially
    info = await jetstream.get_stream_info("KV_KVS")
    assert info.state.messages == 1

    # Wait for purge marker TTL to expire
    for _ in range(10):
        await asyncio.sleep(0.5)
        info = await jetstream.get_stream_info("KV_KVS")
        if info.state.messages == 0:
            break
    else:
        pytest.fail("Purge marker did not expire")


async def test_history_with_limit(jetstream: JetStream):
    """History respects the configured limit."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="LIST", history=10))

    for i in range(50):
        await kv.put("age", str(i + 22).encode())

    entries = [entry async for entry in await kv.history("age")]
    assert len(entries) == 10

    for i, entry in enumerate(entries):
        assert entry.key == "age"
        assert entry.revision == i + 41  # history=10, sent 50
        assert int(entry.value) == i + 62


async def test_keys_empty_bucket(jetstream: JetStream):
    """Keys on empty bucket yields nothing."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="KVS", history=2))

    keys = [key async for key in await kv.keys()]
    assert keys == []


async def test_keys_list(jetstream: JetStream):
    """Keys returns unique active keys."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="KVS", history=2))

    await kv.put("name", b"derek")
    await kv.put("age", b"22")
    await kv.put("country", b"US")
    await kv.put("name", b"ivan")
    await kv.put("age", b"33")
    await kv.put("country", b"US")
    await kv.put("name", b"rip")
    await kv.put("age", b"44")
    await kv.put("country", b"MT")

    keys = [key async for key in await kv.keys()]
    assert sorted(keys) == ["age", "country", "name"]


async def test_keys_excludes_deleted(jetstream: JetStream):
    """Deleted and purged keys are excluded from listing."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="KVS", history=2))

    await kv.put("name", b"derek")
    await kv.put("age", b"22")
    await kv.put("country", b"US")

    await kv.delete("name")
    await kv.purge("country")

    keys = [key async for key in await kv.keys()]
    assert keys == ["age"]


async def test_watch_default(jetstream: JetStream):
    """Default watcher receives live updates, at_eod() signals init complete."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH"))

    watcher = await kv.watch_all()

    # Empty bucket: at_eod() is true immediately
    assert watcher.at_eod() is True

    # Live updates still delivered
    await kv.create("name", b"derek")
    entry = await asyncio.wait_for(anext(watcher), timeout=5.0)
    assert entry.key == "name"
    assert entry.value == b"derek"
    assert entry.revision == 1

    await kv.put("name", b"rip")
    entry = await asyncio.wait_for(anext(watcher), timeout=5.0)
    assert entry.key == "name"
    assert entry.value == b"rip"
    assert entry.revision == 2

    await kv.put("age", b"22")
    entry = await asyncio.wait_for(anext(watcher), timeout=5.0)
    assert entry.key == "age"
    assert entry.value == b"22"
    assert entry.revision == 3

    await kv.delete("age")
    entry = await asyncio.wait_for(anext(watcher), timeout=5.0)
    assert entry.key == "age"
    assert entry.operation == KeyValueOp.DELETE
    assert entry.revision == 4

    await watcher.stop()


async def test_watch_existing_data(jetstream: JetStream):
    """Watcher delivers last value per subject for existing data, then init done."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH"))

    await kv.put("name", b"derek")
    await kv.put("name", b"rip")
    await kv.put("age", b"22")
    await kv.put("age", b"33")

    watcher = await kv.watch_all()

    # Should get last per subject
    entries = []
    async for entry in watcher:
        entries.append(entry)
        if watcher.at_eod():
            break

    await watcher.stop()

    assert len(entries) == 2
    keys = {e.key: e for e in entries}
    assert keys["name"].value == b"rip"
    assert keys["age"].value == b"33"


async def test_watch_include_history(jetstream: JetStream):
    """Watcher with include_history delivers all historical values."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH", history=64))

    await kv.create("name", b"derek")
    await kv.put("name", b"rip")
    await kv.put("name", b"ik")
    await kv.put("age", b"22")
    await kv.put("age", b"33")
    await kv.delete("age")

    watcher = await kv.watch_all(include_history=True)

    entries = []
    async for entry in watcher:
        entries.append(entry)
        if watcher.at_eod():
            break

    await watcher.stop()

    assert len(entries) == 6
    assert entries[0].key == "name" and entries[0].value == b"derek" and entries[0].revision == 1
    assert entries[1].key == "name" and entries[1].value == b"rip" and entries[1].revision == 2
    assert entries[2].key == "name" and entries[2].value == b"ik" and entries[2].revision == 3
    assert entries[3].key == "age" and entries[3].value == b"22" and entries[3].revision == 4
    assert entries[4].key == "age" and entries[4].value == b"33" and entries[4].revision == 5
    assert entries[5].key == "age" and entries[5].operation == KeyValueOp.DELETE and entries[5].revision == 6


async def test_watch_updates_only(jetstream: JetStream):
    """Watcher with updates_only skips existing values."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH", history=64))

    await kv.create("name", b"derek")
    await kv.put("name", b"rip")
    await kv.put("age", b"22")

    watcher = await kv.watch_all(updates_only=True)

    # at_eod() is true immediately for updates_only
    assert watcher.at_eod() is True

    # Publish new updates
    await kv.put("name", b"pp")
    entry = await asyncio.wait_for(anext(watcher), timeout=5.0)
    assert entry.key == "name"
    assert entry.value == b"pp"
    assert entry.revision == 4

    await kv.put("age", b"44")
    entry = await asyncio.wait_for(anext(watcher), timeout=5.0)
    assert entry.key == "age"
    assert entry.value == b"44"
    assert entry.revision == 5

    await kv.delete("age")
    entry = await asyncio.wait_for(anext(watcher), timeout=5.0)
    assert entry.key == "age"
    assert entry.operation == KeyValueOp.DELETE
    assert entry.revision == 6

    await watcher.stop()


async def test_watch_include_history_and_updates_only_conflict(jetstream: JetStream):
    """include_history and updates_only are mutually exclusive."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH"))

    with pytest.raises(KeyValueError):
        await kv.watch_all(include_history=True, updates_only=True)


async def test_watch_wildcard(jetstream: JetStream):
    """Watch with wildcard pattern filters keys."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH"))

    await kv.put("t.name", b"rip")
    await kv.put("t.name", b"ik")
    await kv.put("t.age", b"22")
    await kv.put("t.age", b"44")
    await kv.put("other", b"value")

    watcher = await kv.watch("t.*")

    entries = []
    async for entry in watcher:
        entries.append(entry)
        if watcher.at_eod():
            break

    await watcher.stop()

    assert len(entries) == 2
    keys = {e.key: e for e in entries}
    assert keys["t.name"].value == b"ik"
    assert keys["t.age"].value == b"44"


async def test_watch_purge_operation(jetstream: JetStream):
    """Watcher delivers purge operations."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH"))

    await kv.put("name", b"derek")

    watcher = await kv.watch_all()

    # Collect initial values
    entries = []
    async for entry in watcher:
        entries.append(entry)
        if watcher.at_eod():
            break

    assert len(entries) == 1

    # Purge the key
    await kv.purge("name")

    entry = await asyncio.wait_for(anext(watcher), timeout=5.0)
    assert entry.key == "name"
    assert entry.operation == KeyValueOp.PURGE

    await watcher.stop()


async def test_watch_resume_from_revision(jetstream: JetStream):
    """Watcher with resume_from_revision starts from a specific sequence."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH", history=64))

    await kv.put("name", b"derek")
    await kv.put("name", b"rip")
    await kv.put("name", b"ik")
    await kv.put("age", b"22")
    await kv.put("age", b"33")

    # Resume from revision 3
    watcher = await kv.watch_all(resume_from_revision=3)

    entries = []
    async for entry in watcher:
        entries.append(entry)
        if watcher.at_eod():
            break

    await watcher.stop()

    assert len(entries) == 3
    assert entries[0].key == "name" and entries[0].value == b"ik" and entries[0].revision == 3
    assert entries[1].key == "age" and entries[1].value == b"22" and entries[1].revision == 4
    assert entries[2].key == "age" and entries[2].value == b"33" and entries[2].revision == 5


async def test_watch_ignore_deletes(jetstream: JetStream):
    """Watcher with ignore_deletes skips delete and purge markers."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH", history=64))

    await kv.put("name", b"derek")
    await kv.put("age", b"22")
    await kv.delete("age")

    watcher = await kv.watch_all(include_history=True, ignore_deletes=True)

    entries = []
    async for entry in watcher:
        entries.append(entry)

    await watcher.stop()

    assert len(entries) == 2
    assert entries[0].key == "name" and entries[0].value == b"derek"
    assert entries[1].key == "age" and entries[1].value == b"22"
    assert watcher.at_eod()


async def test_watch_stop_does_not_block(jetstream: JetStream):
    """Calling stop() returns promptly and subsequent iteration ends."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH"))

    watcher = await kv.watch_all()

    # Empty bucket: at_eod() is true immediately
    assert watcher.at_eod() is True

    # Stop should complete quickly
    await asyncio.wait_for(watcher.stop(), timeout=5.0)

    # Iteration after stop should end immediately
    async for entry in watcher:
        pytest.fail("Should not receive entries after stop")


async def test_watch_stop_with_large_pending(jetstream: JetStream):
    """Stopping a watcher with many pending messages does not hang."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="WATCH", history=64))

    for i in range(100):
        await kv.put(f"key.{i}", f"value-{i}".encode())

    watcher = await kv.watch_all(include_history=True)

    # Stop immediately without draining
    await asyncio.wait_for(watcher.stop(), timeout=5.0)


async def test_bind_existing(jetstream: JetStream):
    """Bind to an existing bucket."""
    await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    kv = await key_value(jetstream, "TEST")
    assert kv.bucket == "TEST"

    # Operations should work
    await kv.put("key", b"value")
    entry = await kv.get("key")
    assert entry.value == b"value"


async def test_bind_nonexistent(jetstream: JetStream):
    """Bind to a non-existent bucket raises BucketNotFoundError."""
    with pytest.raises(BucketNotFoundError):
        await key_value(jetstream, "NONEXISTENT")


async def test_bind_invalid_name(jetstream: JetStream):
    """Bind with invalid bucket name raises InvalidBucketNameError."""
    with pytest.raises(InvalidBucketNameError):
        await key_value(jetstream, "invalid.name")


async def test_delete_bucket(jetstream: JetStream):
    """Delete a bucket."""
    await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    result = await delete_key_value(jetstream, "TEST")
    assert result is True

    # Should not be accessible anymore
    with pytest.raises(BucketNotFoundError):
        await key_value(jetstream, "TEST")


async def test_delete_nonexistent(jetstream: JetStream):
    """Delete a non-existent bucket raises BucketNotFoundError."""
    with pytest.raises(BucketNotFoundError):
        await delete_key_value(jetstream, "NONEXISTENT")


async def test_purge_deletes(jetstream: JetStream):
    """PurgeDeletes removes old delete markers."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="KVS", history=10))

    # Create and delete many keys
    for i in range(100):
        await kv.put(f"key-{i}", b"ABC" * 33)
    for i in range(100):
        await kv.delete(f"key-{i}")

    # Purge all markers regardless of age (negative value)
    await kv.purge_deletes(older_than=timedelta(seconds=-1))

    # Verify stream is empty
    info = await jetstream.get_stream_info("KV_KVS")
    assert info.state.messages == 0


async def test_status_bytes(jetstream: JetStream):
    """Status reports correct byte count."""
    kv = await create_key_value(jetstream, KeyValueConfig(bucket="TEST"))

    await kv.put("key", b"value")

    status = await kv.status()
    assert status.bytes > 0
    assert status.values == 1


async def test_compression(jetstream: JetStream):
    """Verify compression config maps to S2 on backing stream."""
    kv = await create_key_value(
        jetstream,
        KeyValueConfig(bucket="TEST_COMPRESS", compression=True),
    )

    status = await kv.status()
    assert status.compressed is True

    info = await jetstream.get_stream_info("KV_TEST_COMPRESS")
    assert info.config.compression == "s2"


async def test_republish(client: Client, jetstream: JetStream):
    """Verify republish config forwards KV messages to another subject."""
    # Create a bucket without republish first
    await create_key_value(jetstream, KeyValueConfig(bucket="TEST_UPDATE"))

    # Re-creating with different config (adding republish) should fail
    with pytest.raises(BucketExistsError):
        await create_key_value(
            jetstream,
            KeyValueConfig(
                bucket="TEST_UPDATE",
                republish=Republish(src=">", dest="bar.>"),
            ),
        )

    kv = await create_key_value(
        jetstream,
        KeyValueConfig(
            bucket="TEST",
            republish=Republish(src=">", dest="bar.>"),
        ),
    )

    info = await jetstream.get_stream_info("KV_TEST")
    assert info.config.republish is not None

    sub = await client.subscribe("bar.>")
    await kv.put("foo", b"value")

    msg = await sub.next(timeout=5.0)
    assert msg.data == b"value"
    assert msg.headers.get("Nats-Subject") == "$KV.TEST.foo"


async def test_limit_marker_ttl(jetstream: JetStream):
    """Verify limit_marker_ttl maps to subject_delete_marker_ttl and allow_msg_ttl on stream."""
    await create_key_value(
        jetstream,
        KeyValueConfig(
            bucket="TEST_MARKER_TTL",
            limit_marker_ttl=timedelta(seconds=1),
        ),
    )

    info = await jetstream.get_stream_info("KV_TEST_MARKER_TTL")
    assert info.config.allow_msg_ttl is True
    assert info.config.subject_delete_marker_ttl == timedelta(seconds=1)


async def test_no_limit_marker_ttl(jetstream: JetStream):
    """Verify allow_msg_ttl is not set when limit_marker_ttl is omitted."""
    await create_key_value(
        jetstream,
        KeyValueConfig(bucket="TEST_NO_TTL"),
    )

    info = await jetstream.get_stream_info("KV_TEST_NO_TTL")
    assert info.config.allow_msg_ttl is not True
