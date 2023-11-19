from functools import partial
from typing import Any, Sequence, cast

import sqlalchemy as sa
from pydantic import BaseModel, create_model
from pydantic.fields import FieldInfo


def model_from_table(
    model_name: str,
    table: sa.Table,
    *,
    exclude: Sequence[str] | None = None,
    include: Sequence[str] | None = None,
    default: bool | Sequence[str] = False,
    required: bool | Sequence[str] = False,
    ops: dict[str, Sequence[str]] | None = None,
) -> type[BaseModel]:
    """Create a dataclass from an :class:`sqlalchemy.schema.Table`

    :param name: dataclass name
    :param table: sqlalchemy table
    :param exclude: fields to exclude from the dataclass
    :param include: fields to include in the dataclass
    :param default: use columns defaults in the dataclass
    :param required: set non nullable columns without a default as
        required fields in the dataclass
    :param ops: additional operation for fields
    """
    includes = set(include or table.columns.keys()) - set(exclude or ())
    default_set = column_info(includes, default)
    required_set = column_info(includes, required)
    column_ops = cast(dict[str, Sequence[str]], ops or {})
    fields = {}
    for column in table.columns:
        name = str(column.name)
        if name not in includes:
            continue
        python_type: type | None = None
        if hasattr(column.type, "impl"):
            if hasattr(column.type.impl, "python_type"):
                python_type = column.type.impl.python_type
        elif hasattr(column.type, "python_type"):
            python_type = column.type.python_type
        if not python_type:  # pragma:   no cover
            raise NotImplementedError(f"Cannot convert column {name}")
        required = name in required_set
        use_default = name in default_set
        fields[name] = (
            python_type,
            field_info(column, required, use_default, column_ops.get(name, ())),
        )
    return create_model(model_name, **fields)  # type: ignore


def column_info(columns: set[str], value: bool | Sequence[str]) -> set[str]:
    if value is False:
        return set()
    elif value is True:
        return columns.copy()
    else:
        return set(value if value is not None else columns)


def field_info(
    col: sa.Column, required: bool, use_default: bool, ops: Sequence[str]
) -> FieldInfo:
    data: dict[str, Any] = {"ops": ops}
    data.clear()
    if use_default and col.default is not None:
        default = col.default.arg  # type: ignore
        if col.default.is_callable:
            data.update(default_factory=partial(default, None))
            required = False
        elif col.default.is_sequence:
            data.update(default_factory=lambda: default.copy())
            required = False
        else:
            data.update(default=default)
            if required and (col.nullable or default is not None):
                required = False
    elif required and col.nullable:
        required = False
    # data.update(required=required)
    if col.doc:
        data.update(description=col.doc)
    data.update(col.info or ())
    return FieldInfo(**data)
