import base64

import pytest

from fluid.db.cursor import Cursor, CursorEntry
from fluid.utils import json2
from fluid.utils.errors import ValidationError


def make_cursor(**kwargs) -> Cursor:
    defaults: dict = dict(
        entries=(CursorEntry("id", 1), CursorEntry("created", "2024-01-01")),
        limit=10,
        filters={},
        search_text="",
    )
    defaults.update(kwargs)
    return Cursor(**defaults)


# ---------------------------------------------------------------------------
# encode / decode round-trip
# ---------------------------------------------------------------------------


def test_encode_decode_roundtrip() -> None:
    cursor = make_cursor()
    encoded = cursor.encode()
    decoded = Cursor.decode(encoded)
    assert decoded.entries == cursor.entries
    assert decoded.limit == cursor.limit
    assert decoded.filters == cursor.filters
    assert decoded.search_text == cursor.search_text


def test_encode_decode_with_filters() -> None:
    cursor = make_cursor(filters={"status": "active"}, search_text="hello")
    decoded = Cursor.decode(cursor.encode())
    assert decoded.filters == {"status": "active"}
    assert decoded.search_text == "hello"


# ---------------------------------------------------------------------------
# Cursor.decode — ValidationError cases
# ---------------------------------------------------------------------------


def test_decode_invalid_base64_raises() -> None:
    with pytest.raises(ValidationError):
        Cursor.decode("not-valid-base64!!!")


def test_decode_invalid_json_raises() -> None:
    bad = base64.b64encode(b"this is not json").decode("ascii")
    with pytest.raises(ValidationError):
        Cursor.decode(bad)


def test_decode_ordering() -> None:
    cursor = make_cursor(desc=True)
    decoded = Cursor.decode(cursor.encode())
    assert decoded.field_names == ("id", "created")
    assert decoded.desc is True


def raw_cursor(*payload: object) -> str:
    return base64.b64encode(json2.dumps(payload).encode()).decode("ascii")


def test_decode_bad_ordering_raises() -> None:
    payload: tuple[object, ...]
    for payload in (
        # the format before the ordering was stored
        ([1, "2024-01-01"], 10, {}, ""),
        # a field name for every value
        ([1, "2024-01-01"], 10, {}, "", ["id"], False),
        ([1], 10, {}, "", [1], False),
        ([], 10, {}, "", [], False),
        ([1], 10, {}, "", ["id"], "yes"),
    ):
        with pytest.raises(ValidationError):
            Cursor.decode(raw_cursor(*payload))


async def test_decode_wrong_types_raises() -> None:
    for kwargs in (
        dict(limit="10"),
        dict(filters=["status"]),
        dict(search_text=1),
    ):
        with pytest.raises(ValidationError):
            Cursor.decode(make_cursor(**kwargs).encode())


async def test_signed_roundtrip() -> None:
    cursor = make_cursor(filters={"org_id": "mine"})
    encoded = cursor.encode(secret="secret")
    assert encoded.startswith(cursor.encode() + ".")
    decoded = Cursor.decode(encoded, secret="secret")
    assert decoded == cursor


async def test_signed_decode_rejects() -> None:
    signed = make_cursor().encode(secret="secret")
    payload, signature = signed.split(".")
    forged = make_cursor(filters={}, limit=0).encode()
    for cursor in (
        make_cursor().encode(),
        f"{forged}.{signature}",
        f"{payload}.{signature[:-2]}",
        make_cursor().encode(secret="other"),
    ):
        with pytest.raises(ValidationError):
            Cursor.decode(cursor, secret="secret")


def test_decode_empty_string_raises() -> None:
    with pytest.raises(ValidationError):
        Cursor.decode("")
