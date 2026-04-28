from __future__ import annotations

import enum
import types as _types
from typing import Any, Union, get_args, get_origin

import click
from pydanclick import from_pydantic as _from_pydantic
from pydantic import BaseModel
from pydantic_core import PydanticUndefined

_UNION_TYPES = {Union, _types.UnionType}


def _unwrap_optional(annotation: Any) -> Any:
    origin = get_origin(annotation)
    if origin in _UNION_TYPES:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _require_enum_callback(
    ctx: click.Context, param: click.Parameter, value: Any
) -> Any:
    if value is None:
        raise click.MissingParameter(ctx=ctx, param=param)
    return value


def _enum_extra_options(model: type[BaseModel]) -> dict[str, Any]:
    extra: dict[str, Any] = {}
    for name, field in model.model_fields.items():
        annotation = _unwrap_optional(field.annotation)
        if (
            isinstance(annotation, type)
            and issubclass(annotation, enum.Enum)
            and issubclass(annotation, str)
        ):
            opts: dict[str, Any] = {"type": click.Choice([e.value for e in annotation])}
            if field.default is PydanticUndefined:
                # pydanclick sets default=None for required fields, which causes
                # Click to pass None through without raising MissingParameter.
                # A callback that raises before pydanclick's validator runs fixes this.
                opts["callback"] = _require_enum_callback
            elif field.default is not None:
                # Override PydanclickDefault which click.Choice cannot handle.
                opts["default"] = field.default.value
            extra[name] = opts
    return extra


def from_pydantic(model: type[BaseModel], **kwargs: Any) -> Any:
    """Drop-in replacement for pydanclick.from_pydantic with Enum support."""
    extra = _enum_extra_options(model)
    if extra:
        existing = kwargs.get("extra_options") or {}
        kwargs["extra_options"] = {**extra, **existing}
    return _from_pydantic(model, **kwargs)
