"""NATS Key-Value Store package."""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Literal

from nats.client.errors import StatusError
from nats.jetstream import JetStream, Stream, StreamConfig, StreamInfo
from nats.jetstream.consumer import Consumer, OrderedConsumerConfig
from nats.jetstream.errors import JetStreamError, MessageNotFoundError, StreamNotFoundError
from nats.jetstream.stream import Placement, Republish, StreamSource
from nats.key_value.errors import (
    BadBucketError,
    BucketExistsError,
    BucketNotFoundError,
    HistoryTooLargeError,
    InvalidBucketNameError,
    InvalidKeyError,
    KeyExistsError,
    KeyNotFoundError,
    KeyValueError,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Header constants
KV_OP = "KV-Operation"
KV_DEL = "DEL"
KV_PURGE = "PURGE"
MSG_ROLLUP = "Nats-Rollup"
MSG_ROLLUP_SUBJECT = "sub"
EXPECTED_LAST_SUBJECT_SEQUENCE = "Nats-Expected-Last-Subject-Sequence"
MSG_TTL = "Nats-TTL"
MARKER_REASON = "Nats-Marker-Reason"

# Validation
KV_MAX_HISTORY = 64
VALID_BUCKET_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
VALID_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9]+$")
VALID_SEARCH_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9*]*[>]?$")


def _bucket_valid(bucket: str) -> bool:
    if len(bucket) == 0:
        return False
    return bool(VALID_BUCKET_RE.match(bucket))


def _key_valid(key: str) -> bool:
    if len(key) == 0 or key[0] == "." or key[-1] == ".":
        return False
    return bool(VALID_KEY_RE.match(key))


def _search_key_valid(key: str) -> bool:
    if len(key) == 0 or key[0] == "." or key[-1] == ".":
        return False
    return bool(VALID_SEARCH_KEY_RE.match(key))


def _op_from_headers(headers) -> KeyValueOp:
    if headers:
        kv_op = headers.get(KV_OP)
        if kv_op:
            if kv_op == KV_DEL:
                return KeyValueOp.DELETE
            if kv_op == KV_PURGE:
                return KeyValueOp.PURGE
        else:
            reason = headers.get(MARKER_REASON)
            if reason in ("MaxAge", "Purge"):
                return KeyValueOp.PURGE
            if reason == "Remove":
                return KeyValueOp.DELETE
    return KeyValueOp.PUT


def _format_ttl(ttl: timedelta) -> str:
    total_ns = int(ttl.total_seconds() * 1_000_000_000)
    if total_ns % 1_000_000_000 == 0:
        return f"{total_ns // 1_000_000_000}s"
    if total_ns % 1_000_000 == 0:
        return f"{total_ns // 1_000_000}ms"
    if total_ns % 1_000 == 0:
        return f"{total_ns // 1_000}us"
    return f"{total_ns}ns"


class KeyValueOp(Enum):
    """Key-Value operation type."""

    PUT = "PUT"
    DELETE = "DEL"
    PURGE = "PURGE"


@dataclass
class KeyValueEntry:
    """An entry from a Key-Value store."""

    bucket: str
    """The bucket name."""

    key: str
    """The key."""

    value: bytes
    """The value."""

    revision: int
    """The revision number (stream sequence)."""

    created: datetime
    """When the entry was stored."""

    delta: int
    """Difference between this revision and the latest."""

    operation: KeyValueOp
    """The type of operation (PUT, DELETE, PURGE)."""


@dataclass
class KeyValueConfig:
    """Configuration for a Key-Value bucket."""

    bucket: str
    """The bucket name."""

    description: str | None = None
    """A short description of the purpose of this bucket."""

    max_value_size: int | None = None
    """Maximum size of a value in bytes. None for unlimited."""

    history: int = 1
    """Number of historical values to keep per key. Max 64."""

    ttl: timedelta | None = None
    """Time-to-live for values. None for no expiration."""

    max_bytes: int | None = None
    """Maximum total size of the bucket in bytes. None for unlimited."""

    storage: Literal["file", "memory"] | None = None
    """Storage backend. None defaults to file."""

    replicas: int = 1
    """Number of replicas."""

    compression: bool = False
    """Enable S2 compression."""

    placement: Placement | None = None
    """Placement directives to consider when placing replicas of this stream."""

    republish: Republish | None = None
    """Rules for republishing messages from a stream with subject mapping."""

    mirror: StreamSource | None = None
    """Maintains a 1:1 mirror of another stream. When set, subjects and sources must be empty."""

    sources: list[StreamSource] | None = None
    """List of stream sources to aggregate."""

    limit_marker_ttl: timedelta | None = None
    """TTL for delete/purge markers. Enables per-key TTL support."""

    metadata: dict[str, str] | None = None
    """Additional metadata for the bucket."""


@dataclass
class KeyValueStatus:
    """Status information about a Key-Value bucket."""

    bucket: str
    """The bucket name."""

    values: int
    """Number of stored values."""

    history: int
    """History depth per key."""

    ttl: timedelta | None
    """Time-to-live for values."""

    bytes: int
    """Total bytes used."""

    backing_store: str
    """The backing stream name."""

    compressed: bool
    """Whether compression is enabled."""

    limit_marker_ttl: timedelta | None
    """TTL for delete/purge markers."""

    metadata: dict[str, str] | None
    """Additional metadata."""

    stream_info: StreamInfo
    """The underlying stream info."""


class KeyWatcher(AsyncIterator[KeyValueEntry], AbstractAsyncContextManager):
    """Watches for key-value updates.

    Use as an async iterator to receive updates. Use at_eod() to check
    whether all initial values have been delivered (End Of Data).
    """

    def __init__(
        self,
        consumer: Consumer,
        messages: AsyncIterator,
        prefix: str,
        bucket: str,
        *,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        init_pending: int = 0,
    ) -> None:
        self._consumer = consumer
        self._messages = messages
        self._prefix = prefix
        self._bucket = bucket
        self._ignore_deletes = ignore_deletes
        self._meta_only = meta_only
        self._init_pending = init_pending
        self._received = 0
        self._stopped = False

    def at_eod(self) -> bool:
        """Check whether all initial values have been delivered (End Of Data)."""
        return self._received >= self._init_pending

    async def values(self) -> AsyncIterator[KeyValueEntry]:
        """Iterate initial values, stopping when end of data is reached."""
        while not self.at_eod():
            yield await self.__anext__()

    async def updates(self) -> AsyncIterator[KeyValueEntry]:
        """Skip initial values, then yield live updates."""
        while not self.at_eod():
            await self.__anext__()
        async for entry in self:
            yield entry

    async def stop(self) -> None:
        """Stop this watcher."""
        if self._stopped:
            return
        self._stopped = True
        await self._consumer.close()

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def __anext__(self) -> KeyValueEntry:
        while not self._stopped:
            msg = await self._messages.__anext__()

            msg_subject = msg.subject
            if not msg_subject.startswith(self._prefix):
                continue
            key = msg_subject[len(self._prefix) :]

            op = _op_from_headers(msg.headers)

            if self._received < self._init_pending:
                self._received += 1
                if msg.metadata.num_pending == 0:
                    self._init_pending = self._received

            if self._ignore_deletes and op in (KeyValueOp.DELETE, KeyValueOp.PURGE):
                if self.at_eod() and msg.metadata.num_pending == 0:
                    raise StopAsyncIteration
                continue

            return KeyValueEntry(
                bucket=self._bucket,
                key=key,
                value=msg.data if not self._meta_only else b"",
                revision=msg.metadata.sequence.stream,
                created=msg.metadata.timestamp,
                delta=msg.metadata.num_pending,
                operation=op,
            )

        raise StopAsyncIteration


class KeyHistory(AsyncIterator[KeyValueEntry]):
    """Iterates all historical revisions for a key. Finite — stops when all entries are delivered."""

    def __init__(
        self, consumer: Consumer, messages: AsyncIterator, prefix: str, bucket: str, *, done: bool = False
    ) -> None:
        self._consumer = consumer
        self._messages = messages
        self._prefix = prefix
        self._bucket = bucket
        self._done = done

    async def __anext__(self) -> KeyValueEntry:
        if self._done:
            raise StopAsyncIteration

        while True:
            msg = await self._messages.__anext__()

            msg_subject = msg.subject
            if not msg_subject.startswith(self._prefix):
                continue
            key = msg_subject[len(self._prefix) :]

            op = _op_from_headers(msg.headers)

            if msg.metadata.num_pending == 0:
                self._done = True

            return KeyValueEntry(
                bucket=self._bucket,
                key=key,
                value=msg.data,
                revision=msg.metadata.sequence.stream,
                created=msg.metadata.timestamp,
                delta=msg.metadata.num_pending,
                operation=op,
            )


class KeyLister(AsyncIterator[str]):
    """Iterates active (non-deleted) key names. Finite — stops when all keys are delivered."""

    def __init__(self, consumer: Consumer, messages: AsyncIterator, prefix: str, *, done: bool = False) -> None:
        self._consumer = consumer
        self._messages = messages
        self._prefix = prefix
        self._done = done

    async def __anext__(self) -> str:
        if self._done:
            raise StopAsyncIteration

        while True:
            msg = await self._messages.__anext__()

            msg_subject = msg.subject
            if not msg_subject.startswith(self._prefix):
                continue
            key = msg_subject[len(self._prefix) :]

            is_delete = False
            if msg.headers:
                kv_op = msg.headers.get(KV_OP)
                if kv_op in (KV_DEL, KV_PURGE):
                    is_delete = True

            if msg.metadata.num_pending == 0:
                self._done = True

            if is_delete:
                if self._done:
                    raise StopAsyncIteration
                continue

            return key


class KeyValue:
    """Key-Value store backed by a JetStream stream."""

    def __init__(self, name: str, stream: Stream, js: JetStream) -> None:
        self._name = name
        self._stream_name = stream.name
        self._stream = stream
        self._pre = f"$KV.{name}."
        self._js = js

    @property
    def bucket(self) -> str:
        """Get the bucket name."""
        return self._name

    async def get(self, key: str, *, revision: int | None = None) -> KeyValueEntry:
        """Get the value for a key.

        Args:
            key: The key to get.
            revision: Get a specific revision instead of the latest.

        Returns:
            The key-value entry.

        Raises:
            InvalidKeyError: If the key is invalid.
            KeyNotFoundError: If the key does not exist or has been deleted.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        entry = await self._get(key, revision)
        if entry.operation != KeyValueOp.PUT:
            raise KeyNotFoundError(key)

        return entry

    async def _get(self, key: str, revision: int | None = None) -> KeyValueEntry:
        """Internal get — returns the entry regardless of operation."""
        subject = f"{self._pre}{key}"

        try:
            if revision is not None:
                msg = await self._stream.get_message(revision)
            else:
                msg = await self._stream.get_last_message_for_subject(subject)
        except MessageNotFoundError as e:
            raise KeyNotFoundError(key) from e
        except StatusError as e:
            if e.status == "404":
                raise KeyNotFoundError(key) from e
            raise

        if revision is not None and msg.subject != subject:
            raise KeyNotFoundError(key)

        return KeyValueEntry(
            bucket=self._name,
            key=key,
            value=msg.data,
            revision=msg.sequence,
            created=msg.time,
            delta=0,
            operation=_op_from_headers(msg.headers),
        )

    async def put(self, key: str, value: bytes) -> int:
        """Put a value for a key.

        Args:
            key: The key to put.
            value: The value to store.

        Returns:
            The revision number.

        Raises:
            InvalidKeyError: If the key is invalid.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        ack = await self._js.publish(f"{self._pre}{key}", value)
        return ack.sequence

    async def create(self, key: str, value: bytes, *, ttl: timedelta | None = None) -> int:
        """Create a key-value pair only if the key does not exist.

        Args:
            key: The key to create.
            value: The value to store.
            ttl: Per-key time-to-live. Requires limit_marker_ttl on the bucket.

        Returns:
            The revision number.

        Raises:
            InvalidKeyError: If the key is invalid.
            KeyExistsError: If the key already exists.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        try:
            return await self._update(key, value, 0, ttl=ttl)
        except KeyExistsError:
            pass

        # Check if the key was deleted — if so, update with its last revision
        entry = await self._get(key)
        if entry.operation == KeyValueOp.PUT:
            raise KeyExistsError(key)
        return await self._update(key, value, entry.revision, ttl=ttl)

    async def update(self, key: str, value: bytes, revision: int) -> int:
        """Update a key's value only if the current revision matches.

        Args:
            key: The key to update.
            value: The new value.
            revision: The expected current revision.

        Returns:
            The new revision number.

        Raises:
            InvalidKeyError: If the key is invalid.
            KeyExistsError: If the revision does not match.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        return await self._update(key, value, revision)

    async def _update(self, key: str, value: bytes, revision: int, *, ttl: timedelta | None = None) -> int:
        """Internal update with expected last subject sequence."""
        headers: dict[str, str] = {EXPECTED_LAST_SUBJECT_SEQUENCE: str(revision)}
        if ttl is not None:
            headers[MSG_TTL] = _format_ttl(ttl)

        try:
            ack = await self._js.publish(f"{self._pre}{key}", value, headers=headers)
        except JetStreamError as e:
            if e.error_code == 10071:
                raise KeyExistsError(key) from e
            raise

        return ack.sequence

    async def delete(self, key: str, *, last_revision: int | None = None) -> None:
        """Soft delete a key by placing a delete marker.

        Args:
            key: The key to delete.
            last_revision: Optional expected revision for optimistic concurrency.

        Raises:
            InvalidKeyError: If the key is invalid.
            KeyExistsError: If last_revision does not match.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        hdrs: dict[str, str] = {KV_OP: KV_DEL}

        if last_revision is not None:
            hdrs[EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last_revision)

        try:
            await self._js.publish(f"{self._pre}{key}", b"", headers=hdrs)
        except JetStreamError as e:
            if e.error_code == 10071:
                raise KeyExistsError(key) from e
            raise

    async def purge(self, key: str, *, last_revision: int | None = None, ttl: timedelta | None = None) -> None:
        """Purge a key, removing the key and all prior revisions.

        Args:
            key: The key to purge.
            last_revision: Optional expected revision for optimistic concurrency.
            ttl: TTL for the purge marker. Requires limit_marker_ttl on the bucket.

        Raises:
            InvalidKeyError: If the key is invalid.
            KeyExistsError: If last_revision does not match.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        hdrs: dict[str, str] = {
            KV_OP: KV_PURGE,
            MSG_ROLLUP: MSG_ROLLUP_SUBJECT,
        }

        if ttl is not None:
            hdrs[MSG_TTL] = _format_ttl(ttl)

        if last_revision is not None:
            hdrs[EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last_revision)

        try:
            await self._js.publish(f"{self._pre}{key}", b"", headers=hdrs)
        except JetStreamError as e:
            if e.error_code == 10071:
                raise KeyExistsError(key) from e
            raise

    async def purge_deletes(self, *, older_than: timedelta = timedelta(minutes=30)) -> None:
        """Remove old delete and purge markers.

        Args:
            older_than: Only remove markers older than this duration.
                       Use a negative value to remove all markers regardless of age.
        """
        subject = f"{self._pre}>"
        config = OrderedConsumerConfig(
            filter_subjects=[subject],
            deliver_policy="all",
        )
        consumer = await self._js.ordered_consumer(self._stream_name, config)
        info = await consumer.get_info()

        delete_markers: list[KeyValueEntry] = []
        if info.num_pending > 0:
            hist = KeyHistory(consumer, (await consumer.messages()).__aiter__(), self._pre, self._name)
            async for entry in hist:
                if entry.operation in (KeyValueOp.DELETE, KeyValueOp.PURGE):
                    delete_markers.append(entry)
        else:
            await consumer.close()

        now = datetime.now(timezone.utc)
        for entry in delete_markers:
            subject = f"{self._pre}{entry.key}"
            keep = 0
            if older_than.total_seconds() > 0:
                age = now - entry.created
                if age.total_seconds() < older_than.total_seconds():
                    keep = 1
            await self._stream.purge(filter=subject, keep=keep)

    async def status(self) -> KeyValueStatus:
        """Get the status of this bucket.

        Returns:
            Bucket status information.
        """
        info = await self._js.get_stream_info(self._stream_name)

        ttl = info.config.max_age

        compressed = info.config.compression == "s2"

        return KeyValueStatus(
            bucket=self._name,
            values=info.state.messages,
            history=info.config.max_msgs_per_subject or 1,
            ttl=ttl,
            bytes=info.state.bytes,
            backing_store="JetStream",
            compressed=compressed,
            limit_marker_ttl=info.config.subject_delete_marker_ttl,
            metadata=info.config.metadata,
            stream_info=info,
        )

    async def watch(
        self,
        keys: str = ">",
        *,
        include_history: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        updates_only: bool = False,
        resume_from_revision: int | None = None,
    ) -> KeyWatcher:
        """Watch for changes to keys matching a pattern.

        Args:
            keys: Key pattern to watch. Use ">" for all keys.
            include_history: Include all historical values, not just latest per subject.
            ignore_deletes: Don't deliver delete/purge markers.
            meta_only: Only deliver headers, not values.
            updates_only: Only deliver new updates, skip existing values.
            resume_from_revision: Start watching from a specific stream sequence.

        Returns:
            A KeyWatcher that delivers updates.
        """
        if not _search_key_valid(keys):
            raise InvalidKeyError(keys)

        if include_history and updates_only:
            raise KeyValueError("include_history and updates_only are mutually exclusive")

        subject = f"{self._pre}{keys}"

        if resume_from_revision is not None:
            deliver_policy = "by_start_sequence"
        elif updates_only:
            deliver_policy = "new"
        elif include_history:
            deliver_policy = "all"
        else:
            deliver_policy = "last_per_subject"

        config = OrderedConsumerConfig(
            filter_subjects=[subject],
            deliver_policy=deliver_policy,
            opt_start_seq=resume_from_revision,
            headers_only=True if meta_only else None,
        )

        consumer = await self._js.ordered_consumer(self._stream_name, config)
        info = await consumer.get_info()
        messages = await consumer.messages()
        pending = 0 if updates_only else info.num_pending

        return KeyWatcher(
            consumer=consumer,
            messages=messages.__aiter__(),
            prefix=self._pre,
            bucket=self._name,
            ignore_deletes=ignore_deletes,
            meta_only=meta_only,
            init_pending=pending,
        )

    async def watch_all(
        self,
        *,
        include_history: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        updates_only: bool = False,
        resume_from_revision: int | None = None,
    ) -> KeyWatcher:
        """Watch all keys. Shorthand for watch(">", ...).

        Returns:
            A KeyWatcher that delivers updates.
        """
        return await self.watch(
            ">",
            include_history=include_history,
            ignore_deletes=ignore_deletes,
            meta_only=meta_only,
            updates_only=updates_only,
            resume_from_revision=resume_from_revision,
        )

    async def keys(self) -> KeyLister:
        """Get all active (non-deleted) keys in the bucket.

        Returns:
            A KeyLister that yields key names.
        """
        subject = f"{self._pre}>"
        config = OrderedConsumerConfig(
            filter_subjects=[subject],
            deliver_policy="last_per_subject",
            headers_only=True,
        )
        consumer = await self._js.ordered_consumer(self._stream_name, config)
        info = await consumer.get_info()
        done = info.num_pending == 0

        if done:
            await consumer.close()
            return KeyLister(consumer, iter([]), self._pre, done=True)

        messages = await consumer.messages()
        return KeyLister(consumer, messages.__aiter__(), self._pre)

    async def history(self, key: str) -> KeyHistory:
        """Get all historical revisions for a key.

        Args:
            key: The key to get history for.

        Returns:
            A KeyHistory that yields entries in chronological order.

        Raises:
            InvalidKeyError: If the key is invalid.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        subject = f"{self._pre}{key}"
        config = OrderedConsumerConfig(
            filter_subjects=[subject],
            deliver_policy="all",
        )
        consumer = await self._js.ordered_consumer(self._stream_name, config)
        info = await consumer.get_info()
        done = info.num_pending == 0

        if done:
            await consumer.close()
            return KeyHistory(consumer, iter([]), self._pre, self._name, done=True)

        messages = await consumer.messages()
        return KeyHistory(consumer, messages.__aiter__(), self._pre, self._name)


async def create_key_value(js: JetStream, config: KeyValueConfig) -> KeyValue:
    """Create a new Key-Value bucket.

    Args:
        js: JetStream context.
        config: Bucket configuration.

    Returns:
        A KeyValue instance for the created bucket.

    Raises:
        InvalidBucketNameError: If the bucket name is invalid.
        HistoryTooLargeError: If history exceeds 64.
        BucketExistsError: If a bucket with this name already exists.
    """
    if not _bucket_valid(config.bucket):
        raise InvalidBucketNameError(config.bucket)

    if config.history > KV_MAX_HISTORY:
        raise HistoryTooLargeError(f"history {config.history} exceeds maximum of {KV_MAX_HISTORY}")

    stream_config = _kv_config_to_stream_config(config)

    try:
        stream = await js.create_stream(stream_config)
    except JetStreamError as e:
        if e.error_code == 10058:
            raise BucketExistsError(config.bucket) from e
        raise

    return KeyValue(config.bucket, stream, js)


async def update_key_value(js: JetStream, config: KeyValueConfig) -> KeyValue:
    """Update an existing Key-Value bucket's configuration.

    Args:
        js: JetStream context.
        config: Bucket configuration.

    Returns:
        A KeyValue instance for the updated bucket.

    Raises:
        InvalidBucketNameError: If the bucket name is invalid.
        HistoryTooLargeError: If history exceeds 64.
        BucketNotFoundError: If the bucket does not exist.
    """
    if not _bucket_valid(config.bucket):
        raise InvalidBucketNameError(config.bucket)

    if config.history > KV_MAX_HISTORY:
        raise HistoryTooLargeError(f"history {config.history} exceeds maximum of {KV_MAX_HISTORY}")

    stream_config = _kv_config_to_stream_config(config)

    try:
        await js.update_stream(**stream_config.to_request())
    except StreamNotFoundError as e:
        raise BucketNotFoundError(config.bucket) from e

    stream = await js.get_stream(f"KV_{config.bucket}")
    return KeyValue(config.bucket, stream, js)


async def create_or_update_key_value(js: JetStream, config: KeyValueConfig) -> KeyValue:
    """Create a Key-Value bucket or update it if it already exists.

    Args:
        js: JetStream context.
        config: Bucket configuration.

    Returns:
        A KeyValue instance for the bucket.

    Raises:
        InvalidBucketNameError: If the bucket name is invalid.
        HistoryTooLargeError: If history exceeds 64.
    """
    try:
        return await create_key_value(js, config)
    except BucketExistsError:
        return await update_key_value(js, config)


async def key_value(js: JetStream, bucket: str) -> KeyValue:
    """Bind to an existing Key-Value bucket.

    Args:
        js: JetStream context.
        bucket: The bucket name.

    Returns:
        A KeyValue instance for the existing bucket.

    Raises:
        InvalidBucketNameError: If the bucket name is invalid.
        BucketNotFoundError: If the bucket does not exist.
    """
    if not _bucket_valid(bucket):
        raise InvalidBucketNameError(bucket)

    stream_name = f"KV_{bucket}"

    try:
        stream = await js.get_stream(stream_name)
    except StreamNotFoundError as e:
        raise BucketNotFoundError(bucket) from e

    info = stream.info
    if info is not None and info.config.max_msgs_per_subject is not None and info.config.max_msgs_per_subject < 1:
        raise BadBucketError(stream_name)

    return KeyValue(bucket, stream, js)


async def key_value_bucket_names(js: JetStream) -> AsyncIterator[str]:
    """List all Key-Value bucket names.

    Args:
        js: JetStream context.

    Yields:
        Bucket names.
    """
    async for name in js.stream_names(subject="$KV.*.>"):
        if name.startswith("KV_"):
            yield name[3:]


async def key_value_buckets(js: JetStream) -> AsyncIterator[KeyValueStatus]:
    """List all Key-Value bucket statuses.

    Args:
        js: JetStream context.

    Yields:
        KeyValueStatus for each bucket.
    """
    async for info in js.list_streams(subject="$KV.*.>"):
        if not info.config.name or not info.config.name.startswith("KV_"):
            continue
        bucket = info.config.name[3:]
        yield KeyValueStatus(
            bucket=bucket,
            values=info.state.messages,
            history=info.config.max_msgs_per_subject or 1,
            ttl=info.config.max_age,
            bytes=info.state.bytes,
            backing_store="JetStream",
            compressed=info.config.compression == "s2",
            limit_marker_ttl=info.config.subject_delete_marker_ttl,
            metadata=info.config.metadata,
            stream_info=info,
        )


async def delete_key_value(js: JetStream, bucket: str) -> bool:
    """Delete a Key-Value bucket.

    Args:
        js: JetStream context.
        bucket: The bucket name to delete.

    Returns:
        True if the bucket was deleted.

    Raises:
        InvalidBucketNameError: If the bucket name is invalid.
        BucketNotFoundError: If the bucket does not exist.
    """
    if not _bucket_valid(bucket):
        raise InvalidBucketNameError(bucket)

    stream_name = f"KV_{bucket}"
    try:
        return await js.delete_stream(stream_name)
    except StreamNotFoundError as e:
        raise BucketNotFoundError(bucket) from e


def _kv_config_to_stream_config(config: KeyValueConfig) -> StreamConfig:
    """Convert a KeyValueConfig to a StreamConfig for the backing stream."""
    if config.limit_marker_ttl is not None and config.limit_marker_ttl <= timedelta(seconds=1):
        raise KeyValueError("limit_marker_ttl must be longer than 1 second")

    # Duplicate window: default 2 min, or TTL if shorter
    duplicate_window = timedelta(minutes=2)
    if config.ttl is not None and config.ttl < duplicate_window:
        duplicate_window = config.ttl

    compression = None
    if config.compression:
        compression = "s2"

    return StreamConfig(
        name=f"KV_{config.bucket}",
        description=config.description,
        subjects=[f"$KV.{config.bucket}.>"],
        max_msgs_per_subject=config.history,
        max_bytes=config.max_bytes,
        max_age=config.ttl,
        max_msg_size=config.max_value_size,
        storage=config.storage or "file",
        num_replicas=config.replicas,
        discard="new",
        allow_rollup_hdrs=True,
        allow_msg_ttl=config.limit_marker_ttl is not None,
        deny_delete=True,
        allow_direct=True,
        duplicate_window=duplicate_window,
        max_msgs=None,
        max_consumers=None,
        compression=compression,
        placement=config.placement,
        republish=config.republish,
        mirror=config.mirror,
        sources=config.sources,
        subject_delete_marker_ttl=config.limit_marker_ttl,
        metadata=config.metadata,
    )


__all__ = [
    # Core types
    "KeyValue",
    "KeyValueConfig",
    "KeyValueEntry",
    "KeyValueOp",
    "KeyValueStatus",
    "KeyWatcher",
    "KeyHistory",
    "KeyLister",
    # Factory functions
    "create_key_value",
    "update_key_value",
    "create_or_update_key_value",
    "key_value",
    "key_value_bucket_names",
    "key_value_buckets",
    "delete_key_value",
    # Errors
    "KeyValueError",
    "BadBucketError",
    "KeyNotFoundError",
    "KeyExistsError",
    "InvalidKeyError",
    "InvalidBucketNameError",
    "BucketNotFoundError",
    "BucketExistsError",
    "HistoryTooLargeError",
]
