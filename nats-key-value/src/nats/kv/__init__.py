"""NATS Key-Value Store package."""

from __future__ import annotations

import asyncio
import base64
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Literal

from nats.client.protocol.message import parse_headers
from nats.jetstream import JetStream, StreamConfig, StreamInfo
from nats.jetstream.consumer import Consumer, OrderedConsumerConfig
from nats.jetstream.errors import JetStreamError, MessageNotFoundError, StreamNotFoundError
from nats.kv.errors import (
    BucketExistsError,
    BucketNotFoundError,
    HistoryTooLargeError,
    InvalidBucketNameError,
    InvalidKeyError,
    KeyDeletedError,
    KeyExistsError,
    KeyNotFoundError,
    KeyValueError,
    NoKeysFoundError,
    WrongLastRevisionError,
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

# Validation
KV_MAX_HISTORY = 64
VALID_BUCKET_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
VALID_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9]+$")


def _bucket_valid(bucket: str) -> bool:
    if len(bucket) == 0:
        return False
    return bool(VALID_BUCKET_RE.match(bucket))


def _key_valid(key: str) -> bool:
    if len(key) == 0 or key[0] == "." or key[-1] == ".":
        return False
    return bool(VALID_KEY_RE.match(key))


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

    max_value_size: int = -1
    """Maximum size of a value in bytes. -1 for unlimited."""

    history: int = 1
    """Number of historical values to keep per key. Max 64."""

    ttl: timedelta | None = None
    """Time-to-live for values. None for no expiration."""

    max_bytes: int = -1
    """Maximum total size of the bucket in bytes. -1 for unlimited."""

    storage: Literal["file", "memory"] | None = None
    """Storage backend. None defaults to file."""

    replicas: int = 1
    """Number of replicas."""

    compression: bool = False
    """Enable S2 compression."""

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

    metadata: dict[str, str] | None
    """Additional metadata."""

    stream_info: StreamInfo
    """The underlying stream info."""


class KeyWatcher:
    """Watches for key-value updates.

    Use as an async iterator to receive updates. A None entry signals
    that all initial values have been delivered.
    """

    def __init__(self) -> None:
        self._updates: asyncio.Queue[KeyValueEntry | None] = asyncio.Queue(maxsize=256)
        self._consumer: Consumer | None = None
        self._task: asyncio.Task | None = None
        self._init_done = False
        self._init_pending: int = 0
        self._received: int = 0
        self._stopped = False

    async def stop(self) -> None:
        """Stop this watcher."""
        if self._stopped:
            return
        self._stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer is not None:
            await self._consumer.close()

    async def updates(self, timeout: float = 5.0) -> KeyValueEntry | None:
        """Get the next update.

        Returns None when initial values are done being delivered.

        Args:
            timeout: How long to wait for the next update.

        Returns:
            The next entry update, or None as the initial-done marker.
        """
        return await asyncio.wait_for(self._updates.get(), timeout)

    def __aiter__(self):
        return self

    async def __aenter__(self) -> KeyWatcher:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.stop()

    async def __anext__(self) -> KeyValueEntry | None:
        if self._stopped:
            raise StopAsyncIteration
        try:
            entry = await self._updates.get()
            if self._stopped:
                raise StopAsyncIteration
            return entry
        except asyncio.CancelledError:
            raise StopAsyncIteration


class KeyValue:
    """Key-Value store backed by a JetStream stream."""

    def __init__(self, name: str, stream_name: str, js: JetStream) -> None:
        self._name = name
        self._stream_name = stream_name
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

        try:
            entry = await self._get(key, revision)
        except KeyDeletedError:
            raise KeyNotFoundError(key)

        return entry

    async def _get(self, key: str, revision: int | None = None) -> KeyValueEntry:
        """Internal get that raises KeyDeletedError for deleted keys."""
        subject = f"{self._pre}{key}"

        try:
            if revision is not None:
                response = await self._js._api.stream_msg_get(self._stream_name, seq=revision)
            else:
                response = await self._js._api.stream_msg_get(self._stream_name, last_by_subj=subject)
        except MessageNotFoundError:
            raise KeyNotFoundError(key)

        message = response["message"]

        # If a specific revision was requested, verify the subject matches
        if revision is not None and message["subject"] != subject:
            raise KeyNotFoundError(key)

        # Decode data
        data = b""
        if "data" in message and message["data"]:
            data = base64.b64decode(message["data"])

        # Decode headers
        op = KeyValueOp.PUT
        if "hdrs" in message and message["hdrs"]:
            try:
                headers_bytes = base64.b64decode(message["hdrs"])
                parsed_headers, _status_code, _status_description = parse_headers(headers_bytes)
                if parsed_headers:
                    kv_op = None
                    if KV_OP in parsed_headers:
                        kv_op = parsed_headers[KV_OP][0]
                    if kv_op == KV_DEL:
                        op = KeyValueOp.DELETE
                    elif kv_op == KV_PURGE:
                        op = KeyValueOp.PURGE
            except Exception:
                logger.warning("Failed to decode message headers for key %s", key, exc_info=True)

        created = datetime.fromisoformat(message["time"])

        entry = KeyValueEntry(
            bucket=self._name,
            key=key,
            value=data,
            revision=message["seq"],
            created=created,
            delta=0,
            operation=op,
        )

        if op != KeyValueOp.PUT:
            raise KeyDeletedError(entry, op.value)

        return entry

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

    async def create(self, key: str, value: bytes) -> int:
        """Create a key-value pair only if the key does not exist.

        Args:
            key: The key to create.
            value: The value to store.

        Returns:
            The revision number.

        Raises:
            InvalidKeyError: If the key is invalid.
            KeyExistsError: If the key already exists.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        try:
            return await self._update(key, value, 0)
        except WrongLastRevisionError:
            pass

        # Check if the key was deleted — if so, update with its last revision
        try:
            await self._get(key)
            # Key exists and is not deleted
            raise KeyExistsError(key)
        except KeyDeletedError as e:
            return await self._update(key, value, e.entry.revision)

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
            WrongLastRevisionError: If the revision does not match.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        return await self._update(key, value, revision)

    async def _update(self, key: str, value: bytes, revision: int) -> int:
        """Internal update with expected last subject sequence."""
        headers = {EXPECTED_LAST_SUBJECT_SEQUENCE: str(revision)}

        try:
            ack = await self._js.publish(f"{self._pre}{key}", value, headers=headers)
        except JetStreamError as e:
            if e.error_code == 10071:
                raise WrongLastRevisionError(e.description)
            raise

        return ack.sequence

    async def delete(self, key: str, *, last_revision: int | None = None) -> None:
        """Soft delete a key by placing a delete marker.

        Args:
            key: The key to delete.
            last_revision: Optional expected revision for optimistic concurrency.

        Raises:
            InvalidKeyError: If the key is invalid.
            WrongLastRevisionError: If last_revision does not match.
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
                raise WrongLastRevisionError(e.description)
            raise

    async def purge(self, key: str, *, last_revision: int | None = None) -> None:
        """Purge a key, removing the key and all prior revisions.

        Args:
            key: The key to purge.
            last_revision: Optional expected revision for optimistic concurrency.

        Raises:
            InvalidKeyError: If the key is invalid.
            WrongLastRevisionError: If last_revision does not match.
        """
        if not _key_valid(key):
            raise InvalidKeyError(key)

        hdrs: dict[str, str] = {
            KV_OP: KV_PURGE,
            MSG_ROLLUP: MSG_ROLLUP_SUBJECT,
        }

        if last_revision is not None:
            hdrs[EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last_revision)

        try:
            await self._js.publish(f"{self._pre}{key}", b"", headers=hdrs)
        except JetStreamError as e:
            if e.error_code == 10071:
                raise WrongLastRevisionError(e.description)
            raise

    async def purge_deletes(self, *, older_than: timedelta = timedelta(minutes=30)) -> None:
        """Remove old delete and purge markers.

        Args:
            older_than: Only remove markers older than this duration.
                       Use a negative value to remove all markers regardless of age.
        """
        watcher = await self.watch_all()

        delete_markers: list[KeyValueEntry] = []
        try:
            async for entry in watcher:
                if entry is None:
                    break
                if entry.operation in (KeyValueOp.DELETE, KeyValueOp.PURGE):
                    delete_markers.append(entry)
        finally:
            await watcher.stop()

        now = datetime.now(timezone.utc)
        for entry in delete_markers:
            subject = f"{self._pre}{entry.key}"
            keep = 0
            if older_than.total_seconds() > 0:
                age = now - entry.created
                if age.total_seconds() < older_than.total_seconds():
                    keep = 1
            await self._js._api.stream_purge(self._stream_name, filter=subject, keep=keep)

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
            backing_store=self._stream_name,
            compressed=compressed,
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
        if include_history and updates_only:
            raise KeyValueError("include_history and updates_only are mutually exclusive")

        subject = f"{self._pre}{keys}"

        watcher = KeyWatcher()

        # Build ordered consumer config
        if updates_only:
            deliver_policy = "new"
        elif include_history:
            deliver_policy = "all"
        elif resume_from_revision is not None:
            deliver_policy = "by_start_sequence"
        else:
            deliver_policy = "last_per_subject"

        config = OrderedConsumerConfig(
            filter_subjects=[subject],
            deliver_policy=deliver_policy,
            opt_start_seq=resume_from_revision,
            headers_only=True if meta_only else None,
        )

        consumer = await self._js.ordered_consumer(self._stream_name, config)

        async def _watch_loop():
            try:
                # Get initial consumer info to determine pending count
                info = await consumer.get_info()
                initial_pending = info.num_pending

                if updates_only:
                    watcher._init_done = True
                elif initial_pending == 0:
                    watcher._init_done = True
                    await watcher._updates.put(None)
                else:
                    watcher._init_pending = initial_pending

                async for msg in await consumer.messages():
                    if watcher._stopped:
                        break

                    # Extract key from subject
                    msg_subject = msg.subject
                    if not msg_subject.startswith(self._pre):
                        continue
                    key = msg_subject[len(self._pre) :]

                    # Determine operation
                    op = KeyValueOp.PUT
                    if msg.headers:
                        kv_op = msg.headers.get(KV_OP)
                        if kv_op == KV_DEL:
                            op = KeyValueOp.DELETE
                        elif kv_op == KV_PURGE:
                            op = KeyValueOp.PURGE

                    # Skip deletes if requested
                    if ignore_deletes and op in (KeyValueOp.DELETE, KeyValueOp.PURGE):
                        # Still count for init tracking
                        if not watcher._init_done:
                            watcher._received += 1
                            if watcher._received >= watcher._init_pending:
                                watcher._init_done = True
                                await watcher._updates.put(None)
                        continue

                    entry = KeyValueEntry(
                        bucket=self._name,
                        key=key,
                        value=msg.data if not meta_only else b"",
                        revision=msg.metadata.sequence.stream,
                        created=msg.metadata.timestamp,
                        delta=msg.metadata.num_pending,
                        operation=op,
                    )

                    await watcher._updates.put(entry)

                    if not watcher._init_done:
                        watcher._received += 1
                        if watcher._received >= watcher._init_pending or msg.metadata.num_pending == 0:
                            watcher._init_done = True
                            await watcher._updates.put(None)

            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("Error in watch loop")

        watcher._consumer = consumer
        watcher._task = asyncio.create_task(_watch_loop())

        return watcher

    async def watch_all(self, **kwargs) -> KeyWatcher:
        """Watch all keys. Shorthand for watch(">", ...).

        Args:
            **kwargs: Additional options passed to watch().

        Returns:
            A KeyWatcher that delivers updates.
        """
        return await self.watch(">", **kwargs)

    async def keys(self) -> list[str]:
        """Get all active (non-deleted) keys in the bucket.

        Returns:
            List of key names.

        Raises:
            NoKeysFoundError: If no keys are found.
        """
        watcher = await self.watch_all(ignore_deletes=True, meta_only=True)

        result: list[str] = []
        try:
            async for entry in watcher:
                if entry is None:
                    break
                result.append(entry.key)
        finally:
            await watcher.stop()

        if not result:
            raise NoKeysFoundError()

        return result

    async def history(self, key: str) -> list[KeyValueEntry]:
        """Get all historical revisions for a key.

        Args:
            key: The key to get history for.

        Returns:
            List of entries in chronological order.

        Raises:
            KeyNotFoundError: If the key has no history.
        """
        watcher = await self.watch(key, include_history=True)

        entries: list[KeyValueEntry] = []
        try:
            async for entry in watcher:
                if entry is None:
                    break
                entries.append(entry)
        finally:
            await watcher.stop()

        if not entries:
            raise KeyNotFoundError(key)

        return entries


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
        await js.create_stream(stream_config)
    except JetStreamError as e:
        if e.error_code == 10058:
            raise BucketExistsError(config.bucket) from e
        raise

    return KeyValue(config.bucket, f"KV_{config.bucket}", js)


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
        info = await js.get_stream_info(stream_name)
    except StreamNotFoundError:
        raise BucketNotFoundError(bucket)

    # Sanity check: max_msgs_per_subject should be > 0 for KV streams
    if info.config.max_msgs_per_subject is not None and info.config.max_msgs_per_subject < 1:
        raise BucketNotFoundError(f"stream {stream_name} is not a valid KV bucket")

    return KeyValue(bucket, stream_name, js)


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
    except StreamNotFoundError:
        raise BucketNotFoundError(bucket)


def _kv_config_to_stream_config(config: KeyValueConfig) -> StreamConfig:
    """Convert a KeyValueConfig to a StreamConfig for the backing stream."""
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
        max_bytes=config.max_bytes if config.max_bytes != -1 else None,
        max_age=config.ttl,
        max_msg_size=config.max_value_size if config.max_value_size != -1 else None,
        storage=config.storage or "file",
        num_replicas=config.replicas,
        discard="new",
        allow_rollup_hdrs=True,
        allow_msg_ttl=True,
        deny_delete=True,
        allow_direct=True,
        duplicate_window=duplicate_window,
        max_msgs=None,
        max_consumers=None,
        compression=compression,
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
    # Factory functions
    "create_key_value",
    "key_value",
    "delete_key_value",
    # Errors
    "KeyValueError",
    "KeyNotFoundError",
    "KeyDeletedError",
    "KeyExistsError",
    "InvalidKeyError",
    "InvalidBucketNameError",
    "BucketNotFoundError",
    "BucketExistsError",
    "HistoryTooLargeError",
    "WrongLastRevisionError",
    "NoKeysFoundError",
]
