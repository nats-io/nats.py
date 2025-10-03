import copy
from dataclasses import _is_dataclass_instance, fields
from typing import List, Mapping, Optional, Union, get_args, get_origin

# from nats.jwt.flatten_model import FlatteningModel


def bytes_serializer(obj):
    if isinstance(obj, bytes):
        return obj.decode()
    raise TypeError(
        f"Object of type {obj.__class__.__name__} is not JSON serializable"
    )


def is_optional_annotation(annotation):
    """
    Checks if a type annotation represents an Optional type.
    """
    # Optional[T] is a shortcut for Union[T, NoneType]
    if get_origin(annotation) is Union:
        return type(None) in get_args(annotation)
    return False


def asdict(obj, *, dict_factory=dict, omitempty=False):
    """Return the fields of a dataclass instance as a new dictionary mapping
    field names to field values.

    Example usage::

      @dataclass
      class C:
          x: int
          y: int

      c = C(1, 2)
      assert asdict(c) == {'x': 1, 'y': 2}

    If given, 'dict_factory' will be used instead of built-in dict.
    The function applies recursively to field values that are
    dataclass instances. This will also look into built-in containers:
    tuples, lists, and dicts.
    """
    if not _is_dataclass_instance(obj):
        raise TypeError("asdict() should be called on dataclass instances")
    return _asdict_inner(obj, dict_factory, omitempty)


def _asdict_inner(obj, dict_factory, omitempty=False):
    if _is_dataclass_instance(obj):
        result = []
        for f in fields(obj):
            value = getattr(obj, f.name)
            if omitempty and is_optional_annotation(f.type) and value is None:
                continue
            value = _asdict_inner(value, dict_factory)
            result.append((f.name, value))
        return dict_factory(result)
    elif isinstance(obj, tuple) and hasattr(obj, '_fields'):
        # obj is a namedtuple.  Recurse into it, but the returned
        # object is another namedtuple of the same type.  This is
        # similar to how other list- or tuple-derived classes are
        # treated (see below), but we just need to create them
        # differently because a namedtuple's __init__ needs to be
        # called differently (see bpo-34363).

        # I'm not using namedtuple's _asdict()
        # method, because:
        # - it does not recurse in to the namedtuple fields and
        #   convert them to dicts (using dict_factory).
        # - I don't actually want to return a dict here.  The main
        #   use case here is json.dumps, and it handles converting
        #   namedtuples to lists.  Admittedly we're losing some
        #   information here when we produce a json list instead of a
        #   dict.  Note that if we returned dicts here instead of
        #   namedtuples, we could no longer call asdict() on a data
        #   structure where a namedtuple was used as a dict key.

        return type(obj)(*[_asdict_inner(v, dict_factory) for v in obj])
    elif isinstance(obj, (list, tuple)):
        # Assume we can create an object of this type by passing in a
        # generator (which is not true for namedtuples, handled
        # above).
        return type(obj)(_asdict_inner(v, dict_factory) for v in obj)
    elif isinstance(obj, dict):
        return type(obj)(
            (_asdict_inner(k, dict_factory), _asdict_inner(v, dict_factory))
            for k, v in obj.items()
        )
    else:
        return copy.deepcopy(obj)
