from dataclasses import dataclass
from typing import Iterator, List

from aiohttp import web
from openapi import json
from openapi.data.validate import ValidationErrors
from openapi.db import CrudDB
from openapi.types import Record
from sqlalchemy import Table


@dataclass
class DbTools:
    db: CrudDB

    def raise_validation_error(self, message: str = "", errors=None) -> None:
        raw = self.as_errors(message, errors)
        data = self.dump(ValidationErrors, raw)
        raise web.HTTPUnprocessableEntity(
            body=json.dumps(data), content_type="application/json"
        )


async def batch_select(
    db: CrudDB,
    table: Table,
    *,
    batch_size: int = 50,
    **filters,
) -> Iterator[List[Record]]:
    query = db.get_query(table, table.select(), params=filters)
    async with db.transaction() as conn:
        result = await conn.stream(query)
        async for rows in result.partitions(batch_size):
            yield rows
