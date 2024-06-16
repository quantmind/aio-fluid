import types
from copy import deepcopy
from typing import Any, Optional, Tuple, Type, TypeVar, Union, get_args, get_origin

from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo


def is_subclass(value: Any, cls: type) -> bool:
    try:
        return issubclass(value, cls)
    except TypeError:
        return False


def make_field_optional(field: FieldInfo, default: Any = None) -> Tuple[Any, FieldInfo]:
    new = deepcopy(field)
    if get_origin(field.annotation) is types.UnionType:
        as_none = False
        new_args = []
        for arg in get_args(field.annotation):
            if is_subclass(arg, BaseModel):
                arg = make_partial_model(arg)
            elif arg == type(None):
                as_none = True
            new_args.append(arg)
        if not as_none:
            new_args.append(type(None))
        annotation = Union[tuple(new_args)]  # type: ignore[valid-type]
    elif is_subclass(field.annotation, BaseModel):
        annotation = make_partial_model(field.annotation)  # type: ignore[arg-type]
    else:
        annotation = Optional[field.annotation]
    new.default = default
    new.annotation = annotation  # type: ignore[assignment]
    return (new.annotation, new)


BaseModelT = TypeVar("BaseModelT", bound=BaseModel)


def make_partial_model(model: Type[BaseModelT]) -> Type[BaseModelT]:
    return create_model(  # type: ignore
        f"Partial{model.__name__}",
        __base__=model,
        __module__=model.__module__,
        **{
            field_name: make_field_optional(field_info)
            for field_name, field_info in model.model_fields.items()
        },
    )
