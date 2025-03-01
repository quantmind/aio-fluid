from typing import Any


class FluidError(Exception):
    """Base class for all Fluid Trading errors."""


class FluidValueError(ValueError, FluidError):
    """Quant Value Error"""


class ValidationError(FluidValueError):
    """Validation Error"""

    def __init__(
        self, field: str, msg: str = "", field_type: str = "string", value: Any = None
    ):
        self.field = field
        self.field_type = field_type
        self.msg = msg or "validation error"
        self.value = value

    def __str__(self) -> str:
        return self.msg


class WorkerStartError(FluidError):
    """Worker start error"""


class FlamegraphError(FluidError):
    pass
