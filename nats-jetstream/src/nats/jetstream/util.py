"""Utility functions for JetStream."""

import uuid


def new_inbox() -> str:
    """Generate a new NATS inbox name.

    This is an internal utility function that creates a unique inbox subject
    by generating a UUID and formatting it in the NATS inbox format.

    Returns:
        A unique inbox subject string in the format "_INBOX.{uuid}"
    """
    return f"_INBOX.{uuid.uuid4().hex}"
