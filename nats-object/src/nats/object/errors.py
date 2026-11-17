"""Object Store errors."""

from __future__ import annotations


class ObjectStoreError(Exception):
    """Base error for Object Store operations."""


class InvalidBucketNameError(ObjectStoreError):
    """Bucket name does not match the required pattern."""


class InvalidObjectNameError(ObjectStoreError):
    """Object name is empty or otherwise invalid."""


class BucketNotFoundError(ObjectStoreError):
    """Bucket does not exist."""


class BucketExistsError(ObjectStoreError):
    """Bucket already exists."""


class ObjectNotFoundError(ObjectStoreError):
    """Object does not exist or has been deleted."""


class ObjectAlreadyExistsError(ObjectStoreError):
    """Cannot rename to an existing, non-deleted object."""


class DigestMismatchError(ObjectStoreError):
    """The retrieved object digest did not match the expected digest."""


class LinkError(ObjectStoreError):
    """Invalid link operation (linking to a link, linking with an existing reader, ...)."""
