import pytest
from openapi.data.fields import VALIDATOR, ValidationError

from fluid import fields


def test_country_field() -> None:
    field = fields.country_field()
    validator = field.metadata.get(VALIDATOR)
    assert validator(field, "it") == "it"
    assert validator(field, "us") == "us"
    with pytest.raises(ValidationError):
        validator(field, "ita")
    with pytest.raises(ValidationError):
        validator(field, ";[")
