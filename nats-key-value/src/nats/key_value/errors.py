"""Key-Value store errors."""

from __future__ import annotations


class KeyValueError(Exception):
    """Base error for Key-Value store operations."""

    pass


class KeyNotFoundError(KeyValueError):
    """Key does not exist or has been deleted."""

    pass


class KeyExistsError(KeyValueError):
    """Key already exists (create failed)."""

    pass


class InvalidKeyError(KeyValueError):
    """Key name is invalid."""

    pass


class InvalidBucketNameError(KeyValueError):
    """Bucket name is invalid."""

    pass


class BucketNotFoundError(KeyValueError):
    """Bucket does not exist."""

    pass


class BadBucketError(KeyValueError):
    """Stream is not a valid Key-Value bucket."""

    pass


class BucketExistsError(KeyValueError):
    """Bucket already exists."""

    pass


class HistoryTooLargeError(KeyValueError):
    """History value exceeds the maximum (64)."""

    pass
