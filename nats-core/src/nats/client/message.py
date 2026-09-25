"""NATS message types and utilities."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, MutableMapping
from dataclasses import dataclass
from typing import cast, overload

_MISSING = object()
"""Sentinel distinguishing "no default given" from an explicit ``None`` default."""


class Headers(MutableMapping[str, str]):
    """NATS message headers.

    Case-insensitive ``MutableMapping`` of header names to values that also
    supports multi-valued headers. Lookups, containment checks, and deletes
    fold case for comparison while iteration preserves the original casing
    of each key (similar to ``httpx.Headers``).

    The ``[]`` accessor and ``get`` return a single value (the most recently
    set value when a key has multiple values). Use ``get_all`` and ``append``
    for explicit multi-value access.
    """

    __slots__ = ("_data",)

    def __init__(
        self,
        headers: Mapping[str, str | list[str]] | Iterable[tuple[str, str | list[str]]] | None = None,
    ) -> None:
        # _data maps lowercase key -> (original_case_key, list[str]).
        self._data: dict[str, tuple[str, list[str]]] = {}
        if headers is None:
            return

        items: Iterable[tuple[str, str | list[str]]]
        if isinstance(headers, Headers):
            # Copy full value lists so multi-valued headers survive; the
            # inherited Mapping.items() would collapse each key to its last value.
            items = headers.asdict().items()
        elif isinstance(headers, Mapping):
            # isinstance narrows to the unparameterized Mapping, so items()
            # widens to ItemsView[object, object]; restore the declared types.
            items = cast("Iterable[tuple[str, str | list[str]]]", headers.items())
        else:
            items = headers

        for key, value in items:
            if isinstance(value, str):
                values = [value]
            elif isinstance(value, list):
                if not all(isinstance(v, str) for v in value):
                    msg = "All items in header value list must be strings"
                    raise ValueError(msg)
                values = list(value)
                if not values:
                    msg = f"Header value list for {key!r} must not be empty"
                    raise ValueError(msg)
            else:
                msg = "Header values must be strings or lists of strings"
                raise TypeError(msg)
            self._data[key.lower()] = (key, values)

    @overload
    def get(self, key: str, /) -> str | None: ...

    @overload
    def get(self, key: str, default: str, /) -> str: ...

    @overload
    def get[T](self, key: str, default: T, /) -> str | T: ...

    def get(self, key: str, default: object = None) -> object:
        """Return the last value for ``key`` or ``default`` if missing.

        Lookup is case-insensitive.
        """
        entry = self._data.get(key.lower())
        if entry is None or not entry[1]:
            return default
        return entry[1][-1]

    def get_all(self, key: str) -> list[str]:
        """Return all values for ``key``, or ``[]`` if missing.

        Lookup is case-insensitive.
        """
        entry = self._data.get(key.lower())
        if entry is None:
            return []
        return list(entry[1])

    def set(self, key: str, value: str) -> None:
        """Set ``key`` to ``value``, replacing any existing values.

        Lookup is case-insensitive; the casing of ``key`` is preserved
        for subsequent iteration.
        """
        self._data[key.lower()] = (key, [value])

    @overload
    def delete(self, key: str, /) -> str: ...

    @overload
    def delete[T](self, key: str, default: T, /) -> str | T: ...

    def delete(self, key: str, default: object = _MISSING) -> object:
        """Remove ``key`` and return its last value, mirroring ``dict.pop``.

        Lookup is case-insensitive. Raises ``KeyError`` if the key is missing
        and no ``default`` is given.
        """
        if default is _MISSING:
            return self.pop(key)
        return self.pop(key, default)

    def append(self, key: str, value: str) -> None:
        """Append ``value`` to ``key``, preserving existing values.

        Lookup is case-insensitive. If the key already exists, its
        original casing is retained; otherwise the casing of ``key`` is
        used for subsequent iteration.
        """
        lower = key.lower()
        entry = self._data.get(lower)
        if entry is None:
            self._data[lower] = (key, [value])
        else:
            entry[1].append(value)

    def multi_items(self) -> Iterator[tuple[str, str]]:
        """Iterate every ``(original_case_key, value)`` pair, including
        each value of multi-valued headers."""
        for original_key, values in self._data.values():
            for value in values:
                yield original_key, value

    def asdict(self) -> dict[str, list[str]]:
        """Return a ``dict`` mapping original-case header names to value lists."""
        return {original_key: list(values) for original_key, values in self._data.values()}

    def __getitem__(self, key: str) -> str:
        entry = self._data.get(key.lower())
        if entry is None or not entry[1]:
            raise KeyError(key)
        return entry[1][-1]

    def __setitem__(self, key: str, value: str) -> None:
        self.set(key, value)

    def __delitem__(self, key: str) -> None:
        lower = key.lower()
        if lower not in self._data:
            raise KeyError(key)
        del self._data[lower]

    def __iter__(self) -> Iterator[str]:
        for original_key, _ in self._data.values():
            yield original_key

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return key.lower() in self._data

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Headers):
            # Case-insensitive comparison: keys compared via lowercase,
            # values compared as lists (order matters within a key).
            if self._data.keys() != other._data.keys():
                return False
            for lower, (_, values) in self._data.items():
                if other._data[lower][1] != values:
                    return False
            return True
        return NotImplemented

    def __repr__(self) -> str:
        return f"Headers({self.asdict()!r})"


@dataclass(slots=True)
class Status:
    """NATS message status information.

    Attributes:
        code: The status code (e.g., "503")
        description: Human-readable description (e.g., "No Responders")
    """

    code: str
    description: str | None = None

    def __str__(self) -> str:
        """String representation of the status."""
        if self.description:
            return f"{self.code}: {self.description}"
        return self.code


@dataclass(slots=True)
class Message:
    """A NATS message.

    Attributes:
        subject: The subject the message was published to
        data: The message payload as bytes
        reply: Optional reply subject for request-reply messaging
        headers: Optional message headers
        status: Optional NATS status information
    """

    subject: str
    data: bytes
    reply: str | None = None
    headers: Headers | None = None
    status: Status | None = None
