from dataclasses import dataclass, is_dataclass, fields, MISSING
from typing import get_origin, get_args, Any, Type, TypeVar, Generic, Dict, List, Callable
from datetime import datetime, timedelta
from enum import Enum

T = TypeVar('T', bound='Model')

def asdict(obj: Any) -> Any:
    """
    Convert a dataclass object to a dictionary, handling nested dataclasses,
    lists, dictionaries, datetime, timedelta objects, and enums.

    Args:
        obj (Any): The dataclass object to convert.

    Returns:
        dict: A dictionary representation of the dataclass object.
    """
    def serialize(value):
        """
        Recursively serialize values.

        Args:
            value (Any): The value to serialize.

        Returns:
            Any: The serialized value.
        """
        if is_dataclass(value):
            return asdict(value)

        if isinstance(value, list):
            return [serialize(item) for item in value]
        elif isinstance(value, dict):
            return {str(key): serialize(val) for key, val in value.items()}
        elif isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, timedelta):
            return value.total_seconds() * 1e9  # Convert to nanoseconds
        else:
            return value

    return {
        field.metadata.get('rename', field.name): serialize(getattr(obj, field.name)) for field in fields(obj)
    }

def fromdict(data: dict, cls: Type) -> Any:
    """
    Convert a dictionary to a dataclass object, handling nested dataclasses,
    lists, dictionaries, datetime, timedelta objects, and enums.

    Args:
        data (dict): The dictionary to convert.
        cls (Type[T]): The dataclass type to instantiate.

    Returns:
        T: An instance of the dataclass.
    """

    def deserialize(value, type):
        """
        Recursively deserialize values.

        Args:
            value (Any): The value to deserialize.
            type (Type): The type to deserialize into.

        Returns:
            Any: The deserialized value.
        """
        if is_dataclass(type):
            return fromdict(value, type)

        if get_origin(type) is list:
            item_type = get_args(type)[0]
            return [deserialize(item, item_type) for item in value]
        elif get_origin(type) is dict:
            _, value_type = get_args(type)
            return {key: deserialize(item, value_type) for key, item in value.items()}
        elif type == datetime:
            return datetime.fromisoformat(value)
        elif type == timedelta:
            return timedelta(microseconds=value / 1000)  # Convert nanoseconds to microseconds
        else:
            return value

    kwargs = {}
    for field in fields(cls):
        name = field.metadata.get('rename', field.name)
        if name in data:
            kwargs[name] = deserialize(data[field.name], field.type)
        elif field.default is not MISSING:
            kwargs[name] = field.default
        elif field.default_factory is not MISSING:  # for fields with default_factory
            kwargs[name] = field.default_factory()

    return cls(**kwargs)

@dataclass
class Model(Generic[T]):
    """
    Base model class with to_dict and from_dict methods for converting
    to and from dictionary representations.
    """
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the model instance to a dictionary.

        Returns:
            dict: A dictionary representation of the model instance.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """
        Create a model instance from a dictionary.

        Args:
            data (dict): The dictionary to convert.
            cls (Type[T]): The dataclass type to instantiate.

        Returns:
            T: An instance of the model.
        """
        return fromdict(data, cls)
