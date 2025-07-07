from __future__ import annotations

from typing import Any, NamedTuple, Sequence, cast

from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import FromClause, tuple_
from sqlalchemy.sql.expression import ColumnElement

from fluid import settings
from fluid.utils.errors import ValidationError

from .crud import CrudDB, Row, Select, column_value_to_python
from .cursor import Cursor, CursorEntry


class Search(NamedTuple):
    search_fields: tuple[str, ...]
    search_text: str


class Pagination(NamedTuple):
    order_by_fields: tuple[str, ...]
    limit: int
    filters: dict[str, Any]
    search: Search | None
    cursor: Cursor | None
    desc: bool = False

    @classmethod
    def create(
        cls,
        *order_by_fields: str,
        cursor: str = "",
        limit: int = 0,
        filters: dict[str, Any] | None = None,
        search: Search | None = None,
        desc: bool = False,
    ) -> Pagination:
        if limit < 0:
            raise ValidationError("limit must be greater than or equal to 0")
        if cursor:
            if limit:
                raise ValidationError("limit cannot be provided with cursor")
            if filters:
                raise ValidationError("filters cannot be provided with cursor")
            if search and search.search_text:
                raise ValidationError("search text cannot be provided with cursor")
            decoded_cursor = Cursor.decode(cursor, order_by_fields)
            limit = decoded_cursor.limit
            filters = decoded_cursor.filters
            if search:
                search = search._replace(search_text=decoded_cursor.search_text)
        else:
            decoded_cursor = None
            if limit >= 0:
                limit = limit or settings.DEFAULT_PAGINATION_LIMIT
        return cls(
            order_by_fields=order_by_fields,
            cursor=decoded_cursor,
            limit=limit,
            filters=filters or {},
            search=search,
            desc=desc,
        )

    @property
    def order_by_fields_sign(self) -> tuple[str, ...]:
        if self.desc:
            return tuple(f"-{field}" for field in self.order_by_fields)
        return self.order_by_fields

    async def execute(
        self,
        db: CrudDB,
        table: FromClause,
        *,
        conn: AsyncConnection | None = None,
    ) -> tuple[Sequence[Row], str]:
        sql_query = self.query(db, table)
        async with db.ensure_connection(conn) as conn:
            result = await conn.execute(sql_query)
        data = result.all()
        cursor = ""
        if self.limit > 0 and len(data) > self.limit:
            cursor = self._encode_cursor(data[-1])
            data = data[:-1]
        return data, cursor

    def query(self, db: CrudDB, table: FromClause) -> Select:
        sql_query = cast(
            Select,
            db.get_query(table, table.select(), params=self.filters),
        )
        if self.search:
            sql_query = db.search_query(
                table,
                sql_query,
                self.search.search_fields,
                self.search.search_text,
            )
        start_clause = self._start_clause(table)
        if start_clause is not None:
            sql_query = sql_query.where(start_clause)
        columns = db.order_by_columns(table, self.order_by_fields_sign)
        ordered = sql_query.order_by(*columns)
        return ordered.limit(self.limit + 1) if self.limit > 0 else ordered

    def _start_clause(self, table: FromClause) -> ColumnElement[bool] | None:
        if self.cursor:
            columns = []
            values = []
            for name, entry in zip(
                self.order_by_fields, self.cursor.entries, strict=False
            ):
                column = getattr(table.c, name)
                values.append(column_value_to_python(column, entry.value))
                columns.append(column)
            if self.desc:
                if len(columns) == 1:
                    return columns[0] <= values[0]
                else:
                    return tuple_(*columns) <= values
            else:
                if len(columns) == 1:
                    return columns[0] >= values[0]
                else:
                    return tuple_(*columns) >= values
        return None

    def _encode_cursor(self, row: Row) -> str:
        fields = (
            field[1:] if field.startswith("-") else field
            for field in self.order_by_fields
        )
        return Cursor(
            entries=tuple(CursorEntry(field, getattr(row, field)) for field in fields),
            limit=self.limit,
            filters=self.filters,
            search_text=self.search.search_text if self.search else "",
        ).encode()
