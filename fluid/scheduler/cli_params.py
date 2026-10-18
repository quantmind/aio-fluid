from __future__ import annotations

import enum
import json
import types
from typing import Any, Literal, Sequence, TypeVar, Union, get_args, get_origin

import click
from click.core import ParameterSource
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined

C = TypeVar("C", bound=click.Command)

# names of the options exec defines itself
RESERVED_NAMES = frozenset(("log", "run_id", "params"))
_UNION_TYPES = {Union, types.UnionType}
_SEQUENCE_TYPES = {list, set, frozenset, tuple}
_GIVEN = {ParameterSource.COMMANDLINE, ParameterSource.ENVIRONMENT}


class JsonParamType(click.ParamType):
    """A value passed as a JSON string"""

    name = "json"

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> Any:
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            self.fail(f"{value!r} is not valid JSON: {e}", param, ctx)


class ValueChoice(click.Choice):
    """A choice among enum members or literal values, matched by their value"""

    def __init__(self, values: Sequence[Any]) -> None:
        self.values = {_label(value): value for value in values}
        super().__init__(list(self.values))

    def convert(
        self, value: Any, param: click.Parameter | None, ctx: click.Context | None
    ) -> Any:
        return self.values[super().convert(value, param, ctx)]


JSON = JsonParamType()


class ModelOptions:
    """Click options for the top level fields of a pydantic model

    Scalar fields are passed as plain strings and validated by the model,
    booleans are `--name/--no-name` flags, a list of scalars is passed by
    repeating the option, and only structured values are passed as JSON.
    """

    def __init__(self, model: type[BaseModel]) -> None:
        self.model = model
        self.options = [
            _field_option(name, field)
            for name, field in model.model_fields.items()
            if name not in RESERVED_NAMES
        ]

    def __call__(self, command: C) -> C:
        """Add the options to a click command"""
        command.params.extend(self.options)
        return command

    def collect(self, ctx: click.Context, values: dict[str, Any]) -> dict[str, Any]:
        """The values given on the command line, keyed as the model expects

        Options left out are not included, so the model defaults apply
        """
        params: dict[str, Any] = {}
        for option in self.options:
            name = option.name or ""
            if ctx.get_parameter_source(name) in _GIVEN:
                field = self.model.model_fields[name]
                params[field.alias or name] = values[name]
        return params

    def check_required(self, ctx: click.Context, params: dict[str, Any]) -> None:
        """Raise a missing parameter error for a required field not in params"""
        options = {option.name: option for option in self.options}
        for name, field in self.model.model_fields.items():
            if field.is_required() and (field.alias or name) not in params:
                option = options.get(name)
                if option is None:
                    raise click.UsageError(
                        f"Missing parameter {name!r}, pass it with --params", ctx
                    )
                raise click.MissingParameter(ctx=ctx, param=option)


def _field_option(name: str, field: FieldInfo) -> click.Option:
    annotation = _unwrap_optional(field.annotation)
    param_type = _scalar_type(annotation)
    multiple = False
    if param_type is None:
        item = _sequence_item(annotation)
        if item is not None and (item_type := _scalar_type(item)) is not None:
            param_type, multiple = item_type, True
        else:
            param_type = JSON
    flag = name.replace("_", "-")
    decls = [f"--{flag}/--no-{flag}" if param_type is click.BOOL else f"--{flag}"]
    # the click default stays None so options left out can be told apart,
    # the model default is only shown in the help
    text = field.description or ""
    if field.is_required():
        text = f"{text} [required]"
    elif (default := _default_label(field)) is not None:
        text = f"{text} [default: {default}]"
    return click.Option(
        [*decls, name],
        type=None if param_type is click.BOOL else param_type,
        multiple=multiple,
        default=None,
        help=text.strip(),
    )


def _scalar_type(annotation: Any) -> click.ParamType | None:
    """The click type of a field passed as a plain value, None if structured"""
    if get_origin(annotation) is Literal:
        return ValueChoice(get_args(annotation))
    if isinstance(annotation, type) and get_origin(annotation) is None:
        if issubclass(annotation, enum.Enum):
            return ValueChoice(list(annotation))
        if issubclass(annotation, bool):
            return click.BOOL
        if issubclass(annotation, int):
            return click.INT
        if issubclass(annotation, float):
            return click.FLOAT
        if issubclass(annotation, (BaseModel, dict, *_SEQUENCE_TYPES)):
            return None
        return click.STRING
    if get_origin(annotation) in _UNION_TYPES:
        # a union of plain values, such as int | str, is left to the model
        args = [a for a in get_args(annotation) if a is not type(None)]
        if all(_scalar_type(a) is not None for a in args):
            return click.STRING
        return None
    if get_origin(annotation) is not None:
        return None
    # NewType and other annotations which are not classes
    return click.STRING


def _sequence_item(annotation: Any) -> Any:
    """The item type of a homogeneous list, set or tuple, None otherwise"""
    if get_origin(annotation) not in _SEQUENCE_TYPES:
        return None
    args = get_args(annotation)
    if len(args) == 1 or (len(args) == 2 and args[1] is Ellipsis):
        return _unwrap_optional(args[0])
    return None


def _unwrap_optional(annotation: Any) -> Any:
    if get_origin(annotation) in _UNION_TYPES:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _label(value: Any) -> str:
    return str(value.value if isinstance(value, enum.Enum) else value)


def _default_label(field: FieldInfo) -> str | None:
    default = field.default
    if default is PydanticUndefined or default is None:
        return None
    if isinstance(default, BaseModel):
        return default.model_dump_json()
    if isinstance(default, (list, tuple, set, frozenset)):
        return ", ".join(_label(value) for value in default)
    return _label(default)
