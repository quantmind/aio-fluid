from fluid.utils.text import as_uuid, create_uid
from uuid import uuid4


def test_as_uuid() -> None:
    assert as_uuid(None) is None
    assert as_uuid("a") == None
    uid = uuid4()
    assert as_uuid(uid) == uid.hex
    assert as_uuid(uid.hex) == uid.hex
