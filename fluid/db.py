from dataclasses import dataclass
from typing import Any, List

from aiohttp import web
from openapi import json
from openapi.data.validate import ValidationErrors
from openapi.db import CrudDB, compile_query
from sqlalchemy import Table


class ExpectedOneOnly(RuntimeError):
    pass


def one_only(data: List, *, Error: type = ExpectedOneOnly) -> Any:
    n = len(data)
    if not n == 1:
        raise Error
    return data[0]


@dataclass
class DbTools:
    db: CrudDB

    def raise_validation_error(self, message: str = "", errors=None) -> None:
        raw = self.as_errors(message, errors)
        data = self.dump(ValidationErrors, raw)
        raise web.HTTPUnprocessableEntity(
            body=json.dumps(data), content_type="application/json"
        )


async def batch_select(db: CrudDB, table: Table, limit: int = 50, **filters):
    offset = 0
    while True:
        query = (
            db.get_query(table, table.select(), params=filters)
            .limit(limit)
            .offset(offset)
        )
        sql, args = compile_query(query)
        async with db.ensure_connection() as conn:
            rows = await conn.fetch(sql, *args)
        yield rows
        offset += limit
        if len(rows) < limit:
            break
