from __future__ import annotations

import base64
from typing import Any, NamedTuple, Self

from fluid.utils import json2
from fluid.utils.errors import ValidationError
from fluid.utils.text import to_bytes


class CursorEntry(NamedTuple):
    field: str
    value: Any


class Cursor(NamedTuple):
    entries: tuple[CursorEntry, ...]
    limit: int
    filters: dict[str, Any]
    search_text: str

    @classmethod
    def decode(cls, cursor: str, field_names: tuple[str, ...]) -> Self:
        try:
            base64_bytes = cursor.encode("ascii")
            cursor_bytes = base64.b64decode(base64_bytes)
            values, limit, filters, search_text = json2.loads(cursor_bytes)
            if len(values) == len(field_names):
                return cls(
                    entries=tuple(map(CursorEntry, field_names, values)),
                    limit=limit,
                    filters=filters,
                    search_text=search_text,
                )
            raise ValueError
        except Exception as e:
            raise ValidationError("invalid cursor") from e

    def encode(self) -> str:
        data = tuple(e.value for e in self.entries)
        cursor_bytes = to_bytes(
            json2.dumps((data, self.limit, self.filters, self.search_text))
        )
        base64_bytes = base64.b64encode(cursor_bytes)
        return base64_bytes.decode("ascii")
