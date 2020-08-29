import pycountry
from openapi.data import fields
from openapi.data.validate import ValidationError
from slugify import slugify

a2 = frozenset((c.alpha_2.lower() for c in pycountry.countries))


class SlugValidator(fields.StrValidator):
    def __call__(self, field, value):
        value = super().__call__(field, value)
        if slugify(value) != value:
            raise ValidationError(field.name, "Not a valid value")
        return value


class UUIDorSlugValidator(fields.UUIDValidator):
    def __call__(self, field, value):
        try:
            return super().__call__(field, value)
        except fields.ValidationError:
            if not isinstance(value, str):
                raise
            return value


def slug_field(max_length=None, min_length=None, **kw):
    kw.setdefault(
        "validator", SlugValidator(min_length=min_length, max_length=max_length)
    )
    return fields.data_field(**kw)


def uuid_field_or_slug(format="uuid", **kw):  # noqa
    """A UUID field with validation"""
    kw.setdefault("validator", UUIDorSlugValidator())
    return fields.data_field(format=format, **kw)


def country_field(**kw):
    kw["validator"] = fields.Choice(a2)
    return fields.str_field(**kw)
