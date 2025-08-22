import json
from dataclasses import is_dataclass
from typing import List, Mapping, Optional, Union

from nats.contrib.utils import asdict, bytes_serializer, is_optional_annotation


class FlatteningModel:

    def __init__(self, **kwargs):
        for field_name, field_type in self.__annotations__.items():
            field_value = kwargs.get(field_name)
            if field_value is not None:
                setattr(field_name, kwargs.get(field_name))

    class Meta:
        unflatten_fields = []

    def get_fields(self, ):
        for key in self.__dict__.keys():
            if not key.startswith("__"):
                if not callable(getattr(self, key)):
                    yield key

    def singleton(
        self, field, field_value, exclude_null_values, omitempty
    ) -> Mapping:
        # Returns Value of a field to be added or updated in the dict
        if issubclass(type(field_value), FlatteningModel):
            # Is a FlatteningModel
            return field_value.to_dict(
                exclude_null_values=exclude_null_values, omitempty=omitempty
            )
        elif is_dataclass(field_value):
            # Is a Dataclass
            return asdict(field_value, omitempty=omitempty)
        elif field:
            # return {field: field_value}
            return field_value
        else:
            raise NotImplementedError()

    def not_dataclass_or_model(
        self, field, field_value, field_annotation
    ) -> bool:
        if issubclass(type(field_value), FlatteningModel):
            # Is a FlatteningModel
            return False
        elif is_dataclass(field_value):
            # Is a Dataclass
            return False
        elif field:
            return True
        else:
            raise NotImplementedError()

    def to_dict(self, exclude_null_values=False, omitempty=True):
        result = {}
        unflatten_fields = dict(self.Meta.unflatten_fields)

        for field in self.get_fields():
            field_value = getattr(self, field)
            field_annotation = self.__annotations__[field]
            if omitempty:
                if is_optional_annotation(field_annotation
                                          ) and field_value is None:
                    continue

            singleton_element = self.singleton(
                field, field_value, exclude_null_values, omitempty
            )

            if unflatten_fields.get(field,
                                    False) or self.not_dataclass_or_model(
                                        field, field_value, field_annotation):
                result[unflatten_fields.get(field, field)] = singleton_element
            else:
                # result[field] = singleton_element
                result.update(singleton_element)

        if not exclude_null_values:
            return result

        return {k: v for k, v in result.items() if v}

    def to_json(
        self, exclude_null_values=False, omitempty=True, serializer=None
    ):
        return json.dumps(
            self.to_dict(
                exclude_null_values=exclude_null_values, omitempty=omitempty
            ),
            default=serializer if serializer else bytes_serializer,
            separators=(',', ':')
        )
