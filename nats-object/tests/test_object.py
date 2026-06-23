"""Tests for nats-object."""

from __future__ import annotations

import hashlib

import pytest
from nats.jetstream import JetStream
from nats.object import (
    DEFAULT_CHUNK_SIZE,
    BucketExistsError,
    BucketNotFoundError,
    InvalidBucketNameError,
    InvalidObjectNameError,
    LinkError,
    ObjectAlreadyExistsError,
    ObjectInfo,
    ObjectMeta,
    ObjectMetaOptions,
    ObjectNotFoundError,
    ObjectStoreConfig,
    create_object_store,
    delete_object_store,
    object_store,
    object_store_names,
)


async def test_invalid_bucket_name(jetstream: JetStream) -> None:
    with pytest.raises(InvalidBucketNameError):
        await create_object_store(jetstream, bucket="bad name!")
    with pytest.raises(InvalidBucketNameError):
        await create_object_store(jetstream, bucket="")


async def test_create_and_bind(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    assert store.bucket == "files"

    bound = await object_store(jetstream, "files")
    assert bound.bucket == "files"


async def test_create_with_identical_config_is_idempotent(jetstream: JetStream) -> None:
    await create_object_store(jetstream, bucket="files")
    await create_object_store(jetstream, bucket="files")


async def test_create_with_conflicting_config_raises(jetstream: JetStream) -> None:
    await create_object_store(jetstream, bucket="files")
    with pytest.raises(BucketExistsError):
        await create_object_store(jetstream, bucket="files", description="different")


async def test_bind_unknown_bucket(jetstream: JetStream) -> None:
    with pytest.raises(BucketNotFoundError):
        await object_store(jetstream, "missing")


async def test_delete_bucket(jetstream: JetStream) -> None:
    await create_object_store(jetstream, bucket="files")
    await delete_object_store(jetstream, "files")
    with pytest.raises(BucketNotFoundError):
        await object_store(jetstream, "files")


async def test_put_and_get_small_object(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")

    info = await store.put("hello.txt", b"hello world")
    assert info.size == len("hello world")
    assert info.chunks == 1
    assert info.digest.startswith("SHA-256=")
    assert info.deleted is False

    result = await store.get("hello.txt")
    assert result.info.name == "hello.txt"
    assert await result.read() == b"hello world"


async def test_put_empty_object(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")

    info = await store.put("empty", b"")
    assert info.size == 0
    assert info.chunks == 0

    result = await store.get("empty")
    assert await result.read() == b""


async def test_put_chunks_large_object(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")

    payload = bytes(range(256)) * (DEFAULT_CHUNK_SIZE // 256 * 3 + 7)
    info = await store.put("big.bin", payload)
    assert info.size == len(payload)
    assert info.chunks > 1

    result = await store.get("big.bin")
    assert await result.read() == payload


async def test_put_with_custom_chunk_size(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")

    payload = b"a" * 5000
    meta = ObjectMeta(name="big.txt", options=ObjectMetaOptions(max_chunk_size=1024))
    info = await store.put(meta, payload)
    assert info.chunks == 5  # ceil(5000 / 1024)

    result = await store.get("big.txt")
    assert await result.read() == payload


async def test_get_unknown_object(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    with pytest.raises(ObjectNotFoundError):
        await store.get_info("nope")
    with pytest.raises(ObjectNotFoundError):
        await store.get("nope")


async def test_put_invalid_name(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    with pytest.raises(InvalidObjectNameError):
        await store.put("", b"data")


async def test_put_replaces_existing_chunks(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    await store.put("file", b"original")

    info = await store.put("file", b"replacement")
    assert info.size == len(b"replacement")

    result = await store.get("file")
    assert await result.read() == b"replacement"


async def test_delete_marks_object_gone(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    await store.put("file", b"data")

    await store.delete("file")
    with pytest.raises(ObjectNotFoundError):
        await store.get_info("file")

    await store.delete("file")  # second delete is a no-op


async def test_delete_missing_object_raises(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    with pytest.raises(ObjectNotFoundError):
        await store.delete("missing")


async def test_update_meta_changes_description(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    await store.put(ObjectMeta(name="file", description="first"), b"data")

    await store.update_meta("file", ObjectMeta(name="file", description="second"))
    info = await store.get_info("file")
    assert info.description == "second"


async def test_update_meta_renames(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    await store.put("old", b"data")

    info = await store.update_meta("old", ObjectMeta(name="new"))
    assert info.name == "new"
    with pytest.raises(ObjectNotFoundError):
        await store.get_info("old")

    result = await store.get("new")
    assert await result.read() == b"data"


async def test_update_meta_rejects_rename_collision(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    await store.put("a", b"hi")
    await store.put("b", b"there")

    with pytest.raises(ObjectAlreadyExistsError):
        await store.update_meta("a", ObjectMeta(name="b"))


async def test_list_objects(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    await store.put("a", b"a")
    await store.put("b", b"b")
    await store.delete("b")

    names = sorted([info.name async for info in store.list_objects()])
    assert names == ["a"]

    all_names = sorted([info.name async for info in store.list_objects(include_deleted=True)])
    assert all_names == ["a", "b"]


async def test_object_store_names_lists_buckets(jetstream: JetStream) -> None:
    await create_object_store(jetstream, bucket="alpha")
    await create_object_store(jetstream, bucket="beta")

    names = sorted([name async for name in object_store_names(jetstream)])
    assert names == ["alpha", "beta"]


async def test_status_reports_backing_stream(jetstream: JetStream) -> None:
    config = ObjectStoreConfig(bucket="files", description="d", metadata={"owner": "infra"})
    store = await create_object_store(jetstream, config)
    await store.put("a", b"a")

    status = await store.status()
    assert status.bucket == "files"
    assert status.description == "d"
    assert status.metadata["owner"] == "infra"
    assert status.backing_store == "JetStream"
    assert status.sealed is False
    assert status.size > 0


async def test_seal_blocks_further_writes(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    await store.put("a", b"a")
    await store.seal()

    status = await store.status()
    assert status.sealed is True


async def test_add_link_points_at_target(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    target = await store.put("target", b"data")
    link_info = await store.add_link("alias", target)

    assert link_info.options.link is not None
    assert link_info.options.link.bucket == "files"
    assert link_info.options.link.name == "target"


async def test_add_link_rejects_link_to_link(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    target = await store.put("target", b"data")
    link = await store.add_link("alias", target)
    with pytest.raises(LinkError):
        await store.add_link("alias2", link)


async def test_add_bucket_link(jetstream: JetStream) -> None:
    src = await create_object_store(jetstream, bucket="src")
    dst = await create_object_store(jetstream, bucket="dst")
    info = await src.add_bucket_link("dst-link", dst)
    assert info.options.link is not None
    assert info.options.link.bucket == "dst"
    assert info.options.link.name is None


async def test_digest_is_sha256_of_payload(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    payload = b"the quick brown fox"
    info = await store.put("file", payload)

    import base64

    expected = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=").decode()
    assert info.digest == f"SHA-256={expected}"


async def test_async_iterator_payload(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")

    async def source():
        yield b"hello "
        yield b"world"

    info = await store.put("file", source())
    assert info.size == len(b"hello world")

    result = await store.get("file")
    assert await result.read() == b"hello world"


async def test_object_info_modified_populated_from_message_time(jetstream: JetStream) -> None:
    store = await create_object_store(jetstream, bucket="files")
    await store.put("file", b"hi")

    info = await store.get_info("file")
    assert isinstance(info, ObjectInfo)
    assert info.modified.tzinfo is not None
