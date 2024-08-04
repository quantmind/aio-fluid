from typing import Any, Container

from pydantic import BaseConfig, BaseModel, create_model
from pydantic.fields import FieldInfo
from sqlalchemy.inspection import inspect


class OrmConfig(BaseConfig):
    from_attributes = True


def sqlalchemy_to_pydantic(
    db_model: Any,
    model_name: str,
    *,
    config: type = OrmConfig,
    exclude: Container[str] = (),
    optional: bool = False,
) -> type[BaseModel]:
    mapper = inspect(db_model)
    if mapper is None:
        raise ValueError(f"Could not inspect {db_model}")
    fields = {}
    for column in mapper.columns:
        name = str(column.key)
        if name in exclude:
            continue
        python_type: Any = None
        if hasattr(column.type, "impl"):
            if hasattr(column.type.impl, "python_type"):
                python_type = column.type.impl.python_type
        elif hasattr(column.type, "python_type"):
            python_type = column.type.python_type
        assert python_type, f"Could not infer python_type for {column}"
        kwargs: dict = {}
        if optional:
            kwargs["default"] = None
            python_type = python_type | None
        elif column.default:
            if not column.default.is_callable:
                kwargs["default"] = column.default.arg
        fields[name] = (python_type, FieldInfo(**kwargs))
    return create_model(model_name, __config__=config, **fields)  # type: ignore
