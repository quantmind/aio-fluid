from uuid import uuid4

from fluid.utils.text import as_uuid


def test_as_uuid() -> None:
    assert as_uuid(None) is None
    assert as_uuid("a") is None
    uid = uuid4()
    assert as_uuid(uid) == uid.hex
    assert as_uuid(uid.hex) == uid.hex
