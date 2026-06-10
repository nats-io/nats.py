"""NATS Object Store client (ADR-20).

Stores arbitrary-sized objects on top of JetStream by sharding their payloads
into chunked messages and recording per-object metadata on a separate, rolled-up
subject. Buckets map onto a single backing stream named ``OBJ_<bucket>``.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import re
import uuid
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Literal

from nats.jetstream import (
    JetStream,
    Stream,
    StreamConfig,
    StreamInfo,
)
from nats.jetstream.consumer import OrderedConsumerConfig
from nats.jetstream.errors import (
    MessageNotFoundError,
    StreamNameAlreadyInUseError,
    StreamNotFoundError,
)
from nats.jetstream.stream import Placement
from nats.object.errors import (
    BucketExistsError,
    BucketNotFoundError,
    DigestMismatchError,
    InvalidBucketNameError,
    InvalidObjectNameError,
    LinkError,
    ObjectAlreadyExistsError,
    ObjectNotFoundError,
    ObjectStoreError,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


DEFAULT_CHUNK_SIZE = 128 * 1024
"""Default chunk size used by put() when the meta does not override it."""

DIGEST_PREFIX = "SHA-256="
NATS_ROLLUP_HEADER = "Nats-Rollup"
NATS_ROLLUP_SUBJECT = "sub"

_BUCKET_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def _validate_bucket(bucket: str) -> None:
    if not bucket or not _BUCKET_RE.match(bucket):
        raise InvalidBucketNameError(bucket)


def _validate_object_name(name: str) -> None:
    if not name:
        raise InvalidObjectNameError("object name cannot be empty")


def _encode_name(name: str) -> str:
    """Encode an object name as a URL-safe base64 string with no padding."""
    return base64.urlsafe_b64encode(name.encode("utf-8")).rstrip(b"=").decode("ascii")


def _b64_no_pad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


@dataclass(slots=True)
class ObjectLink:
    """Link to another bucket or object."""

    bucket: str
    name: str | None = None


@dataclass(slots=True)
class ObjectMetaOptions:
    """Per-object options bundled with :class:`ObjectMeta`."""

    link: ObjectLink | None = None
    max_chunk_size: int | None = None


@dataclass(slots=True)
class ObjectMeta:
    """User-facing object metadata accepted by put() and update_meta()."""

    name: str
    description: str | None = None
    headers: dict[str, list[str]] | None = None
    metadata: dict[str, str] | None = None
    options: ObjectMetaOptions = field(default_factory=ObjectMetaOptions)


@dataclass(slots=True)
class ObjectInfo:
    """Object metadata as returned by the server.

    The ``modified`` field is filled in from the meta message timestamp at read
    time; it is never persisted as part of the JSON payload.
    """

    bucket: str
    name: str
    nuid: str
    size: int
    chunks: int
    modified: datetime
    digest: str = ""
    deleted: bool = False
    description: str | None = None
    headers: dict[str, list[str]] | None = None
    metadata: dict[str, str] | None = None
    options: ObjectMetaOptions = field(default_factory=ObjectMetaOptions)


@dataclass(slots=True)
class ObjectStoreConfig:
    """Configuration for a new Object Store bucket."""

    bucket: str
    description: str | None = None
    metadata: dict[str, str] | None = None
    ttl: timedelta | None = None
    max_bytes: int | None = None
    storage: Literal["file", "memory"] | None = None
    replicas: int = 1
    placement: Placement | None = None
    compression: bool = False


@dataclass(slots=True)
class ObjectStoreStatus:
    """Run-time status of an Object Store bucket."""

    bucket: str
    description: str
    metadata: dict[str, str]
    ttl: timedelta | None
    storage: str
    replicas: int
    sealed: bool
    size: int
    backing_store: str
    compressed: bool
    stream_info: StreamInfo


class ObjectResult(AsyncIterable[bytes]):
    """Streaming result of a :meth:`ObjectStore.get`.

    Yields object chunks via async iteration; :meth:`read` collects them into a
    single ``bytes`` instance.
    """

    __slots__ = ("info", "_source")

    def __init__(self, info: ObjectInfo, source: AsyncIterator[bytes]) -> None:
        self.info = info
        self._source = source

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self._source

    async def read(self) -> bytes:
        buf = bytearray()
        async for chunk in self._source:
            buf.extend(chunk)
        return bytes(buf)


class ObjectStore:
    """An Object Store bucket backed by a single JetStream stream."""

    def __init__(self, bucket: str, stream: Stream, js: JetStream) -> None:
        self._bucket = bucket
        self._stream = stream
        self._js = js
        self._stream_name = f"OBJ_{bucket}"
        self._chunk_prefix = f"$O.{bucket}.C"
        self._meta_prefix = f"$O.{bucket}.M"

    @property
    def bucket(self) -> str:
        return self._bucket

    def _chunk_subject(self, nuid: str) -> str:
        return f"{self._chunk_prefix}.{nuid}"

    def _meta_subject(self, name: str) -> str:
        return f"{self._meta_prefix}.{_encode_name(name)}"

    async def put(self, meta: ObjectMeta | str, data: bytes | AsyncIterator[bytes] | None = None) -> ObjectInfo:
        """Store an object's bytes under the given name and return its :class:`ObjectInfo`."""
        if isinstance(meta, str):
            meta = ObjectMeta(name=meta)
        _validate_object_name(meta.name)
        if meta.options.link is not None:
            raise LinkError("put() rejects meta containing a link — use add_link or add_bucket_link")

        previous = await self._maybe_get_info(meta.name)
        nuid = uuid.uuid4().hex
        chunk_subject = self._chunk_subject(nuid)
        chunk_size = meta.options.max_chunk_size or DEFAULT_CHUNK_SIZE

        size = 0
        chunks = 0
        digest = hashlib.sha256()

        async for chunk in _rechunk(data, chunk_size):
            digest.update(chunk)
            size += len(chunk)
            chunks += 1
            await self._js.publish(chunk_subject, chunk)

        info = ObjectInfo(
            bucket=self._bucket,
            name=meta.name,
            nuid=nuid,
            size=size,
            chunks=chunks,
            modified=datetime.now(timezone.utc),
            digest=f"{DIGEST_PREFIX}{_b64_no_pad(digest.digest())}",
            deleted=False,
            description=meta.description,
            headers=meta.headers,
            metadata=meta.metadata,
            options=meta.options,
        )
        await self._publish_meta(info)

        if previous is not None and previous.nuid != nuid:
            await self._stream.purge(filter=self._chunk_subject(previous.nuid))

        return info

    async def get(self, name: str) -> ObjectResult:
        """Retrieve an object by name, returning an :class:`ObjectResult` that streams its chunks."""
        info = await self.get_info(name)
        if info.options.link is not None:
            raise LinkError("cannot get a link directly; resolve the link target first")
        return ObjectResult(info, self._read_chunks(info))

    async def _read_chunks(self, info: ObjectInfo) -> AsyncIterator[bytes]:
        expected = info.digest.removeprefix(DIGEST_PREFIX) if info.digest else ""

        if info.chunks == 0:
            actual = _b64_no_pad(hashlib.sha256(b"").digest())
            if expected and expected != actual:
                raise DigestMismatchError(f"digest mismatch for {info.name}: expected {expected}, got {actual}")
            return

        config = OrderedConsumerConfig(filter_subjects=[self._chunk_subject(info.nuid)])
        consumer = await self._js.ordered_consumer(self._stream_name, config)
        digest = hashlib.sha256()
        received = 0
        try:
            messages = await consumer.messages()
            async for msg in messages:
                digest.update(msg.data)
                received += 1
                yield msg.data
                if received >= info.chunks:
                    break
        finally:
            await consumer.close()

        actual = _b64_no_pad(digest.digest())
        if expected and expected != actual:
            raise DigestMismatchError(f"digest mismatch for {info.name}: expected {expected}, got {actual}")

    async def get_info(self, name: str) -> ObjectInfo:
        """Return the current :class:`ObjectInfo` for ``name`` (excluding deleted objects)."""
        info = await self._get_info(name)
        if info.deleted:
            raise ObjectNotFoundError(name)
        return info

    async def _get_info(self, name: str) -> ObjectInfo:
        _validate_object_name(name)
        try:
            msg = await self._stream.get_last_message_for_subject(self._meta_subject(name))
        except MessageNotFoundError as e:
            raise ObjectNotFoundError(name) from e
        return _decode_info(msg.data, msg.time)

    async def _maybe_get_info(self, name: str) -> ObjectInfo | None:
        try:
            return await self._get_info(name)
        except ObjectNotFoundError:
            return None

    async def update_meta(self, name: str, meta: ObjectMeta) -> ObjectInfo:
        """Update name/description/headers/metadata of an existing object.

        Renaming is permitted as long as the destination name is not currently
        held by a non-deleted object.
        """
        _validate_object_name(name)
        _validate_object_name(meta.name)

        info = await self._get_info(name)
        if info.deleted:
            raise ObjectNotFoundError(name)

        if meta.name != name:
            existing = await self._maybe_get_info(meta.name)
            if existing is not None and not existing.deleted:
                raise ObjectAlreadyExistsError(meta.name)

            tombstone = ObjectInfo(
                bucket=self._bucket,
                name=name,
                nuid=info.nuid,
                size=0,
                chunks=0,
                modified=datetime.now(timezone.utc),
                deleted=True,
            )
            await self._publish_meta(tombstone, subject=self._meta_subject(name))

        updated = ObjectInfo(
            bucket=self._bucket,
            name=meta.name,
            nuid=info.nuid,
            size=info.size,
            chunks=info.chunks,
            modified=datetime.now(timezone.utc),
            digest=info.digest,
            deleted=False,
            description=meta.description if meta.description is not None else info.description,
            headers=meta.headers if meta.headers is not None else info.headers,
            metadata=meta.metadata if meta.metadata is not None else info.metadata,
            options=info.options,
        )
        await self._publish_meta(updated)
        return updated

    async def delete(self, name: str) -> None:
        """Soft-delete an object and purge its chunks."""
        info = await self._get_info(name)
        if info.deleted:
            return

        tombstone = ObjectInfo(
            bucket=self._bucket,
            name=info.name,
            nuid=info.nuid,
            size=0,
            chunks=0,
            modified=datetime.now(timezone.utc),
            deleted=True,
        )
        await self._publish_meta(tombstone)
        await self._stream.purge(filter=self._chunk_subject(info.nuid))

    async def seal(self) -> None:
        """Seal the backing stream so no further changes can be made to the bucket."""
        info = await self._js.get_stream_info(self._stream_name)
        config_dict = info.config.to_request()
        config_dict["sealed"] = True
        await self._js.update_stream(**config_dict)

    async def status(self) -> ObjectStoreStatus:
        """Return :class:`ObjectStoreStatus` describing the backing stream."""
        info = await self._js.get_stream_info(self._stream_name)
        return ObjectStoreStatus(
            bucket=self._bucket,
            description=info.config.description or "",
            metadata=info.config.metadata or {},
            ttl=info.config.max_age,
            storage=info.config.storage or "file",
            replicas=info.config.num_replicas or 1,
            sealed=bool(info.config.sealed),
            size=info.state.bytes,
            backing_store="JetStream",
            compressed=info.config.compression == "s2",
            stream_info=info,
        )

    async def list_objects(self, *, include_deleted: bool = False) -> AsyncIterator[ObjectInfo]:
        """Yield each live object's :class:`ObjectInfo`. Set ``include_deleted=True`` to also yield tombstones."""
        config = OrderedConsumerConfig(
            filter_subjects=[f"{self._meta_prefix}.>"],
            deliver_policy="last_per_subject",
        )
        consumer = await self._js.ordered_consumer(self._stream_name, config)
        try:
            info = await consumer.get_info()
            if info.num_pending == 0:
                return
            messages = await consumer.messages()
            async for msg in messages:
                obj = _decode_info(msg.data, msg.metadata.timestamp)
                done = msg.metadata.num_pending == 0
                if obj.deleted and not include_deleted:
                    if done:
                        break
                    continue
                yield obj
                if done:
                    break
        finally:
            await consumer.close()

    async def add_link(self, name: str, target: ObjectInfo) -> ObjectInfo:
        """Create an object that links to another object."""
        _validate_object_name(name)
        if target.options.link is not None:
            raise LinkError("cannot link to a link")
        if target.deleted:
            raise LinkError("cannot link to a deleted object")
        return await self._add_link(name, ObjectLink(bucket=target.bucket, name=target.name))

    async def add_bucket_link(self, name: str, target: ObjectStore) -> ObjectInfo:
        """Create an object that links to another bucket."""
        _validate_object_name(name)
        return await self._add_link(name, ObjectLink(bucket=target.bucket))

    async def _add_link(self, name: str, link: ObjectLink) -> ObjectInfo:
        existing = await self._maybe_get_info(name)
        if existing is not None and not existing.deleted and existing.options.link is None:
            raise ObjectAlreadyExistsError(name)

        info = ObjectInfo(
            bucket=self._bucket,
            name=name,
            nuid=uuid.uuid4().hex,
            size=0,
            chunks=0,
            modified=datetime.now(timezone.utc),
            options=ObjectMetaOptions(link=link),
        )
        await self._publish_meta(info)
        return info

    async def _publish_meta(self, info: ObjectInfo, *, subject: str | None = None) -> None:
        target = subject if subject is not None else self._meta_subject(info.name)
        payload = _encode_info_payload(info)
        await self._js.publish(target, payload, headers={NATS_ROLLUP_HEADER: NATS_ROLLUP_SUBJECT})


async def _rechunk(data: bytes | AsyncIterator[bytes] | None, chunk_size: int) -> AsyncIterator[bytes]:
    """Yield ``data`` as fixed-size chunks, accepting either a single ``bytes`` or an async iterator of pieces."""
    if data is None or data == b"":
        return
    if isinstance(data, bytes):
        for i in range(0, len(data), chunk_size):
            yield data[i : i + chunk_size]
        return

    buffer = bytearray()
    async for piece in data:
        buffer.extend(piece)
        while len(buffer) >= chunk_size:
            yield bytes(buffer[:chunk_size])
            del buffer[:chunk_size]
    if buffer:
        yield bytes(buffer)


def _encode_info_payload(info: ObjectInfo) -> bytes:
    payload: dict[str, object] = {
        "name": info.name,
        "bucket": info.bucket,
        "nuid": info.nuid,
        "size": info.size,
        "chunks": info.chunks,
    }
    if info.digest:
        payload["digest"] = info.digest
    if info.deleted:
        payload["deleted"] = True
    if info.description is not None:
        payload["description"] = info.description
    if info.headers:
        payload["headers"] = info.headers
    if info.metadata:
        payload["metadata"] = info.metadata

    options_payload: dict[str, object] = {}
    if info.options.link is not None:
        link_payload: dict[str, object] = {"bucket": info.options.link.bucket}
        if info.options.link.name:
            link_payload["name"] = info.options.link.name
        options_payload["link"] = link_payload
    if info.options.max_chunk_size is not None:
        options_payload["max_chunk_size"] = info.options.max_chunk_size
    if options_payload:
        payload["options"] = options_payload

    return json.dumps(payload).encode("utf-8")


def _decode_info(payload: bytes, modified: datetime) -> ObjectInfo:
    data = json.loads(payload)

    options_data = data.get("options") or {}
    link_data = options_data.get("link")
    link = ObjectLink(bucket=link_data["bucket"], name=link_data.get("name")) if link_data else None
    options = ObjectMetaOptions(link=link, max_chunk_size=options_data.get("max_chunk_size"))

    return ObjectInfo(
        bucket=data["bucket"],
        name=data["name"],
        nuid=data["nuid"],
        size=data.get("size", 0),
        chunks=data.get("chunks", 0),
        modified=modified,
        digest=data.get("digest", ""),
        deleted=data.get("deleted", False),
        description=data.get("description"),
        headers=data.get("headers"),
        metadata=data.get("metadata"),
        options=options,
    )


def _config_to_stream_config(config: ObjectStoreConfig) -> StreamConfig:
    compression = "s2" if config.compression else None
    return StreamConfig(
        name=f"OBJ_{config.bucket}",
        description=config.description,
        subjects=[f"$O.{config.bucket}.C.>", f"$O.{config.bucket}.M.>"],
        max_age=config.ttl,
        max_bytes=config.max_bytes,
        storage=config.storage or "file",
        num_replicas=config.replicas,
        placement=config.placement,
        discard="new",
        allow_rollup_hdrs=True,
        allow_direct=True,
        compression=compression,
        metadata=config.metadata,
    )


async def create_object_store(
    js: JetStream,
    config: ObjectStoreConfig | None = None,
    /,
    **kwargs: object,
) -> ObjectStore:
    """Create a new Object Store bucket and return an :class:`ObjectStore` bound to it."""
    if config is None:
        config = ObjectStoreConfig(**kwargs)  # type: ignore[arg-type]
    _validate_bucket(config.bucket)

    stream_config = _config_to_stream_config(config)
    try:
        stream = await js.create_stream(stream_config)
    except StreamNameAlreadyInUseError as e:
        raise BucketExistsError(config.bucket) from e

    return ObjectStore(config.bucket, stream, js)


async def object_store(js: JetStream, bucket: str) -> ObjectStore:
    """Bind to an existing Object Store bucket."""
    _validate_bucket(bucket)
    try:
        stream = await js.get_stream(f"OBJ_{bucket}")
    except StreamNotFoundError as e:
        raise BucketNotFoundError(bucket) from e
    return ObjectStore(bucket, stream, js)


async def delete_object_store(js: JetStream, bucket: str) -> None:
    """Delete the backing stream for an Object Store bucket."""
    _validate_bucket(bucket)
    try:
        await js.delete_stream(f"OBJ_{bucket}")
    except StreamNotFoundError as e:
        raise BucketNotFoundError(bucket) from e


async def object_store_names(js: JetStream) -> AsyncIterator[str]:
    """Yield every Object Store bucket name found on the server."""
    async for stream_name in js.stream_names(subject="$O.*.M.>"):
        if stream_name.startswith("OBJ_"):
            yield stream_name[4:]


__all__ = [
    "DEFAULT_CHUNK_SIZE",
    "ObjectLink",
    "ObjectMeta",
    "ObjectMetaOptions",
    "ObjectInfo",
    "ObjectResult",
    "ObjectStore",
    "ObjectStoreConfig",
    "ObjectStoreStatus",
    "create_object_store",
    "object_store",
    "delete_object_store",
    "object_store_names",
    "ObjectStoreError",
    "InvalidBucketNameError",
    "InvalidObjectNameError",
    "BucketExistsError",
    "BucketNotFoundError",
    "ObjectNotFoundError",
    "ObjectAlreadyExistsError",
    "DigestMismatchError",
    "LinkError",
]
