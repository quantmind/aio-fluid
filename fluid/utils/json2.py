import json
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from functools import partial
from typing import Any
from uuid import UUID

from .dates import isoformat


def encoder(obj: Any) -> str:
    if isinstance(obj, UUID):
        return obj.hex
    elif isinstance(obj, Decimal):
        return str(obj)
    elif isinstance(obj, datetime):
        return isoformat(obj)
    elif isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, Enum):
        return obj.name
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


loads = json.loads
dumps = partial(json.dumps, default=encoder)
