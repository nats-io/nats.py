"""Formatting server addresses for diagnostics."""

from urllib.parse import urlparse

_REDACTED = "!!!REDACTED!!!"


def _redact_url(url: str | bytes) -> str:
    """Redact a NATS URL to hide passwords. Used for logging."""
    if isinstance(url, bytes):
        url = url.decode("utf-8", errors="replace")
    if not url:
        return url

    try:
        u = urlparse(url)

        if u.scheme == "" or u.netloc == "":
            if u.geturl()[:2] == "//":
                u = urlparse(f"nats:{u.geturl()}")
            else:
                u = urlparse(f"nats://{u.geturl()}")

        if u.username is None:
            return url

        netloc_redacted = ""

        if u.password is None:
            netloc_redacted += f"{_REDACTED}@"
        else:
            netloc_redacted += f"{u.username}:{_REDACTED}@"

        if u.hostname is not None:
            netloc_redacted += f"[{u.hostname}]" if ":" in u.hostname else u.hostname

        if u.port is not None:
            netloc_redacted += f":{u.port}"

    except ValueError:
        return _REDACTED

    return u._replace(netloc=netloc_redacted).geturl()
