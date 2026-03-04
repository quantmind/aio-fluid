import pytest

from fluid.db.cursor import Cursor, CursorEntry
from fluid.utils.errors import ValidationError

FIELD_NAMES = ("id", "created")


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
    decoded = Cursor.decode(encoded, FIELD_NAMES)
    assert decoded.entries == cursor.entries
    assert decoded.limit == cursor.limit
    assert decoded.filters == cursor.filters
    assert decoded.search_text == cursor.search_text


def test_encode_decode_with_filters() -> None:
    cursor = make_cursor(filters={"status": "active"}, search_text="hello")
    decoded = Cursor.decode(cursor.encode(), FIELD_NAMES)
    assert decoded.filters == {"status": "active"}
    assert decoded.search_text == "hello"


# ---------------------------------------------------------------------------
# Cursor.decode — ValidationError cases
# ---------------------------------------------------------------------------


def test_decode_invalid_base64_raises() -> None:
    with pytest.raises(ValidationError):
        Cursor.decode("not-valid-base64!!!", FIELD_NAMES)


def test_decode_invalid_json_raises() -> None:
    import base64

    bad = base64.b64encode(b"this is not json").decode("ascii")
    with pytest.raises(ValidationError):
        Cursor.decode(bad, FIELD_NAMES)


def test_decode_wrong_field_count_raises() -> None:
    # cursor encoded with 2 fields, decoded with 3 field names
    cursor = make_cursor()
    encoded = cursor.encode()
    with pytest.raises(ValidationError):
        Cursor.decode(encoded, ("id", "created", "extra"))


def test_decode_empty_string_raises() -> None:
    with pytest.raises(ValidationError):
        Cursor.decode("", FIELD_NAMES)
