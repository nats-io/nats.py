"""A module providing a utility class for handling JSON-related operations."""

import json
from typing import Any

try:
    import orjson
except ImportError:
    orjson = None


class JsonUtil:
    """A utility class for handling JSON-related operations.
    This class provides static methods for formatting JSON strings, and
    for converting between Python objects and JSON strings/files. It uses
    the `orjson` library where possible for its speed advantages, but reverts
    to the standard `json` library where `orjson` does not support the required
    functionality.
    """

    @staticmethod
    def _handle_sort_keys(kwargs):
        """Internal helper to handle sort_keys parameter for orjson compatibility.
        Args:
            kwargs: The keyword arguments dictionary to modify
        Returns:
            Modified kwargs dictionary
        """
        if kwargs.pop("sort_keys", False):
            option = kwargs.get("option", 0) | orjson.OPT_SORT_KEYS
            kwargs["option"] = option
        return kwargs

    @staticmethod
    def dumps(obj, *args, **kwargs) -> str:
        """Convert a Python object into a json string.
        Args:
            obj: The data to be converted
            *args: Extra arguments to pass to the dumps() function
            **kwargs: Extra keyword arguments to pass to the dumps() function.
                     Special handling for 'sort_keys' which is translated to
                     orjson.OPT_SORT_KEYS when using orjson.
        Returns:
            The json string representation of obj
        """
        if orjson is None:
            return json.dumps(obj, *args, **kwargs)
        else:
            kwargs = JsonUtil._handle_sort_keys(kwargs)
            return orjson.dumps(obj, *args, **kwargs).decode("utf-8")

    @staticmethod
    def dump_bytes(obj, *args, **kwargs) -> bytes:
        """Convert a Python object into a bytes string.
        Args:
            obj: The data to be converted
            *args: Extra arguments to pass to the dumps() function
            **kwargs: Extra keyword arguments to pass to the dumps() function.
                     Special handling for 'sort_keys' which is translated to
                     orjson.OPT_SORT_KEYS when using orjson.
        Returns:
            The json string representation of obj as bytes
        """
        if orjson is None:
            return json.dumps(obj, *args, **kwargs).encode("utf-8")
        else:
            kwargs = JsonUtil._handle_sort_keys(kwargs)
            return orjson.dumps(obj, *args, **kwargs)

    @staticmethod
    def loads(s: str, *args, **kwargs) -> Any:
        """Parse a JSON string into a Python object.
        Args:
            s: The JSON string to be parsed
            *args: Extra arguments to pass to the orjson.loads() function
            **kwargs: Extra keyword arguments to pass to the orjson.loads() function
        Returns:
            The Python representation of s
        """
        if orjson is None:
            return json.loads(s, *args, **kwargs)
        else:
            return orjson.loads(s, *args, **kwargs)
