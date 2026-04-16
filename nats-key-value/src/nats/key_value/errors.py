"""Key-Value store errors."""

from __future__ import annotations


class KeyValueError(Exception):
    """Base error for Key-Value store operations."""

    pass


class KeyNotFoundError(KeyValueError):
    """Key does not exist or has been deleted."""

    pass


class KeyDeletedError(KeyValueError):
    """Key has been deleted (has a delete/purge marker).

    This is an internal error used during get operations.
    Users will see KeyNotFoundError instead.
    """

    def __init__(self, entry: object | None = None, op: str | None = None):
        self.entry = entry
        self.op = op
        super().__init__(f"key deleted: op={op}")


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


class WrongLastRevisionError(KeyValueError):
    """Expected last revision did not match (CAS failure)."""

    pass


class NoKeysFoundError(KeyValueError):
    """No keys found in the bucket."""

    pass
