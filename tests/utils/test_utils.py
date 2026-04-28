from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from uuid import UUID

import pytest

from fluid.utils.dates import as_utc, isoformat
from fluid.utils.errors import ValidationError
from fluid.utils.json2 import encoder

pytestmark = pytest.mark.asyncio(loop_scope="module")


# ---------------------------------------------------------------------------
# dates
# ---------------------------------------------------------------------------


async def test_as_utc_with_date():
    d = date(2024, 1, 15)
    result = as_utc(d)
    assert result == datetime(2024, 1, 15, tzinfo=timezone.utc)


async def test_isoformat_non_utc():
    tz = timezone(timedelta(hours=2))
    dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=tz)
    result = isoformat(dt)
    assert "+02:00" in result


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


async def test_validation_error_str():
    err = ValidationError("field", msg="bad value")
    assert str(err) == "bad value"


async def test_validation_error_default_msg():
    err = ValidationError("field")
    assert str(err) == "validation error"


# ---------------------------------------------------------------------------
# json2 encoder
# ---------------------------------------------------------------------------


async def test_encoder_uuid():
    uid = UUID("12345678-1234-5678-1234-567812345678")
    assert encoder(uid) == uid.hex


async def test_encoder_decimal():
    assert encoder(Decimal("3.14")) == "3.14"


async def test_encoder_date():
    assert encoder(date(2024, 1, 15)) == "2024-01-15"


async def test_encoder_enum():
    class Color(Enum):
        red = 1

    assert encoder(Color.red) == "red"


async def test_encoder_unknown_type_raises():
    with pytest.raises(TypeError):
        encoder(object())
