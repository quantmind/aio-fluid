from dataclasses import dataclass

from aiohttp import web
from openapi import json
from openapi.data.validate import ValidationErrors
from openapi.db import CrudDB


class ExpectedOneOnly(RuntimeError):
    pass


@dataclass
class DbTools:
    db: CrudDB

    def raise_validation_error(self, message: str = "", errors=None) -> None:
        raw = self.as_errors(message, errors)
        data = self.dump(ValidationErrors, raw)
        raise web.HTTPUnprocessableEntity(
            body=json.dumps(data), content_type="application/json"
        )
