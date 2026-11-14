from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Any, NamedTuple, Self

from fluid.utils import json2
from fluid.utils.errors import ValidationError
from fluid.utils.text import to_bytes


class Cursor(NamedTuple):
    """The position of the next page, as a client sends it back.

    The cursor carries the sort field and the direction it was built with,
    so it can only resume the ordering that produced it. The values are those
    of the sort field followed by those of the unique fields of the ordering,
    whose names are not stored since the server always provides them.
    """

    sort_by: str
    values: tuple[Any, ...]
    limit: int
    filters: dict[str, Any]
    search_text: str
    desc: bool = False

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
            values, limit, filters, search_text, sort_by, desc = json2.loads(
                cursor_bytes
            )
            if (
                isinstance(values, list)
                and len(values) > 0
                and isinstance(sort_by, str)
                and sort_by
                and isinstance(limit, int)
                and isinstance(filters, dict)
                and isinstance(search_text, str)
                and isinstance(desc, bool)
            ):
                return cls(
                    sort_by=sort_by,
                    values=tuple(values),
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
        cursor_bytes = to_bytes(
            json2.dumps(
                (
                    self.values,
                    self.limit,
                    self.filters,
                    self.search_text,
                    self.sort_by,
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
