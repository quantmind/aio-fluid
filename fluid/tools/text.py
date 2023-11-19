import json
import os
import uuid
from typing import Any

TRUE_VALUES = frozenset(("yes", "true", "t", "1"))

String = bytes | str


def to_bytes(s: String) -> bytes:
    return s.encode("utf-8") if isinstance(s, str) else s


def to_string(s: String) -> str:
    return s.decode("utf-8") if isinstance(s, bytes) else s


def to_bool(v: str | bool | int | None) -> bool:
    return str(v).lower() in TRUE_VALUES if v else False


def as_uuid(uid: Any) -> str | None:
    if uid:
        if hasattr(uid, "hex"):
            uid = uid.hex
        try:
            return uuid.UUID(uid).hex
        except ValueError:
            return None
    return None


def nice_env_str(space: int = 4, trim_length: int = 100):
    lt = max(len(k) for k in os.environ) + space
    values = []
    for key, value in os.environ.items():
        if len(value) > trim_length + 3:
            value = f"{value[:trim_length]}..."
        k = f"{key}:".ljust(lt)
        values.append(f"{k}{value}")
    return "\n".join(values)


def nice_json(data: Any) -> str:
    if not isinstance(data, str):
        return json.dumps(data, indent=4)
    return data
