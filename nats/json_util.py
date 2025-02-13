"""A module providing a utility class for handling JSON-related operations."""

import json
from typing import Any

try:
    import orjson
except ImportError:
    orjson = None


class JsonUtil:
    """A utility class for handling JSON serialization operations.
    This class provides static methods for converting between Python objects and JSON
    strings/bytes. It uses the `orjson` library when available for its performance
    advantages, falling back to the standard `json` library when `orjson` is not
    installed.

    The class handles compatibility between the two libraries, particularly for options
    like 'sort_keys' which have different implementations in each library. All methods
    maintain consistent behavior regardless of which JSON library is being used.

    Methods:
        dumps(obj, *args, **kwargs) -> str: Converts object to JSON string
        dump_bytes(obj, *args, **kwargs) -> bytes: Converts object to JSON bytes
        loads(s, *args, **kwargs) -> Any: Parses JSON string into Python object
    """

    @staticmethod
    def _handle_sort_keys(kwargs):
        """Internal helper to handle sort_keys parameter for orjson compatibility.
        Args:
            kwargs: The keyword arguments dictionary to modify
        Returns:
            Modified kwargs dictionary with orjson-compatible options
        """
        if kwargs.pop("sort_keys", False):
            option = kwargs.get("option", 0) | orjson.OPT_SORT_KEYS
            kwargs["option"] = option
        return kwargs

    @staticmethod
    def dumps(obj, *args, **kwargs) -> str:
        """Convert a Python object into a JSON string.
        Args:
            obj: The data to be converted
            *args: Extra arguments to pass to the dumps() function
            **kwargs: Extra keyword arguments to pass to the dumps() function.
                     Special handling for 'sort_keys' which is translated to
                     orjson.OPT_SORT_KEYS when using orjson.
        Returns:
            str: A JSON string representation of obj
        """
        if orjson is None:
            return json.dumps(obj, *args, **kwargs)
        else:
            kwargs = JsonUtil._handle_sort_keys(kwargs)
            return orjson.dumps(obj, *args, **kwargs).decode("utf-8")

    @staticmethod
    def dump_bytes(obj, *args, **kwargs) -> bytes:
        """Convert a Python object into a JSON bytes string.
        Args:
            obj: The data to be converted
            *args: Extra arguments to pass to the dumps() function
            **kwargs: Extra keyword arguments to pass to the dumps() function.
                     Special handling for 'sort_keys' which is translated to
                     orjson.OPT_SORT_KEYS when using orjson.
        Returns:
            bytes: A JSON bytes string representation of obj
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
