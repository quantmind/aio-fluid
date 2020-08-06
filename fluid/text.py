import json
import os
from typing import Any


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
