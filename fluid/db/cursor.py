from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Any, NamedTuple, Self

from fluid.utils import json2
from fluid.utils.errors import ValidationError
from fluid.utils.text import to_bytes


class CursorEntry(NamedTuple):
    field: str
    value: Any


class Cursor(NamedTuple):
    """The position of the next page, as a client sends it back.

    The cursor carries the ordering it was built with, the field names of
    its entries and the direction, so it can only resume the ordering that
    produced it.
    """

    entries: tuple[CursorEntry, ...]
    limit: int
    filters: dict[str, Any]
    search_text: str
    desc: bool = False

    @property
    def field_names(self) -> tuple[str, ...]:
        """The ordering fields the cursor was built with"""
        return tuple(entry.field for entry in self.entries)

    @classmethod
    def decode(cls, cursor: str, secret: str = "") -> Self:
        """Decode a cursor, checking its signature when a secret is given.

        A cursor that is unsigned, changed or signed with another secret
        raises a ValidationError.
        """
        try:
            if secret:
                cursor, dot, signature = cursor.rpartition(".")
                if not dot or not hmac.compare_digest(
                    signature, _signature(cursor, secret)
                ):
                    raise ValueError
            base64_bytes = cursor.encode("ascii")
            cursor_bytes = base64.b64decode(base64_bytes)
            values, limit, filters, search_text, field_names, desc = json2.loads(
                cursor_bytes
            )
            if (
                isinstance(values, list)
                and isinstance(field_names, list)
                and len(values) == len(field_names) > 0
                and all(isinstance(name, str) for name in field_names)
                and isinstance(limit, int)
                and isinstance(filters, dict)
                and isinstance(search_text, str)
                and isinstance(desc, bool)
            ):
                return cls(
                    entries=tuple(map(CursorEntry, field_names, values)),
                    limit=limit,
                    filters=filters,
                    search_text=search_text,
                    desc=desc,
                )
            raise ValueError
        except Exception as e:
            raise ValidationError("invalid cursor") from e

    def encode(self, secret: str = "") -> str:
        """Encode the cursor, signed with the secret when one is given."""
        data = tuple(e.value for e in self.entries)
        cursor_bytes = to_bytes(
            json2.dumps(
                (
                    data,
                    self.limit,
                    self.filters,
                    self.search_text,
                    self.field_names,
                    self.desc,
                )
            )
        )
        payload = base64.b64encode(cursor_bytes).decode("ascii")
        if secret:
            return f"{payload}.{_signature(payload, secret)}"
        return payload


def _signature(payload: str, secret: str) -> str:
    digest = hmac.new(to_bytes(secret), to_bytes(payload), hashlib.sha256).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii")
