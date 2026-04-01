from datetime import date, datetime
from typing import Any, Set, TypeAlias, cast

from dateutil.parser import parse as parse_date
from sqlalchemy import Column, Table, func, insert, select
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import FromClause, Select, and_, or_
from sqlalchemy.sql.dml import Delete, Insert, Update
from typing_extensions import Annotated, Doc

from fluid.utils.errors import ValidationError

from .container import Database

QueryType: TypeAlias = Delete | Select | Update


class CrudDB(Database):
    """A Database with additional methods for CRUD operations"""

    async def db_select(
        self,
        table: Annotated[FromClause, Doc("The table to select from")],
        filters: Annotated[
            dict,
            Doc(
                "Key-value pairs for filtering rows; supports 'field:op' syntax "
                "for operators (eq, ne, gt, ge, lt, le)"
            ),
        ],
        *,
        order_by: Annotated[
            tuple[str, ...] | None,
            Doc("Column names to order by; prefix with '-' for descending"),
        ] = None,
        conn: Annotated[
            AsyncConnection | None, Doc("Optional existing connection to reuse")
        ] = None,
    ) -> CursorResult:
        """Select rows from a given table"""
        sql_query = self.get_query(table, Select(table), params=filters)
        if order_by:
            sql_query = self.order_by_query(table, cast(Select, sql_query), order_by)
        async with self.ensure_transaction(conn) as conn:
            return await conn.execute(sql_query)

    async def db_insert(
        self,
        table: Annotated[Table, Doc("The table to insert into")],
        data: Annotated[
            list[dict] | dict,
            Doc(
                "A single row dict or a list of row dicts; missing columns in "
                "a multi-row insert are filled with None"
            ),
        ],
        *,
        conn: Annotated[
            AsyncConnection | None, Doc("Optional existing connection to reuse")
        ] = None,
    ) -> CursorResult:
        """Insert one or more rows into a table, returning the inserted rows"""
        async with self.ensure_transaction(conn) as conn:
            sql_query = self.insert_query(table, data)
            return await conn.execute(sql_query)

    async def db_update(
        self,
        table: Annotated[Table, Doc("The table to update")],
        filters: Annotated[
            dict,
            Doc(
                "Key-value pairs identifying rows to update; supports 'field:op' syntax"
            ),
        ],
        data: Annotated[dict, Doc("Column values to set on the matching rows")],
        *,
        conn: Annotated[
            AsyncConnection | None, Doc("Optional existing connection to reuse")
        ] = None,
    ) -> CursorResult:
        """Update rows matching the filters, returning all updated rows"""
        update = (
            cast(
                Update,
                self.get_query(table, table.update(), params=filters),
            )
            .values(**data)
            .returning(*table.columns)
        )
        async with self.ensure_transaction(conn) as conn:
            return await conn.execute(update)

    async def db_upsert(
        self,
        table: Annotated[Table, Doc("The table to upsert into")],
        filters: Annotated[
            dict, Doc("Key-value pairs used to look up the existing row")
        ],
        data: Annotated[
            dict | None,
            Doc(
                "Column values to set; if None, the row is fetched or inserted "
                "using only the filters"
            ),
        ] = None,
        *,
        conn: Annotated[
            AsyncConnection | None, Doc("Optional existing connection to reuse")
        ] = None,
    ) -> Row:
        """Update a single row if it exists, otherwise insert it, returning the row"""
        if data:
            result = await self.db_update(table, filters, data, conn=conn)
        else:
            result = await self.db_select(table, filters, conn=conn)
        record = result.one_or_none()
        if record is None:
            insert_data = data.copy() if data else {}
            insert_data.update(filters)
            result = await self.db_insert(table, insert_data, conn=conn)
            record = result.one()
        return record

    async def db_delete(
        self,
        table: Annotated[Table, Doc("The table to delete from")],
        filters: Annotated[
            dict,
            Doc(
                "Key-value pairs identifying rows to delete; supports 'field:op' syntax"
            ),
        ],
        *,
        conn: Annotated[
            AsyncConnection | None, Doc("Optional existing connection to reuse")
        ] = None,
    ) -> CursorResult:
        """Delete rows matching the filters, returning the deleted rows"""
        sql_query = self.get_query(
            table,
            table.delete().returning(*table.columns),
            params=filters,
        )
        async with self.ensure_transaction(conn) as conn:
            return await conn.execute(sql_query)

    async def db_count(
        self,
        table: Annotated[FromClause, Doc("The table to count rows in")],
        filters: Annotated[
            dict, Doc("Key-value pairs for filtering rows; supports 'field:op' syntax")
        ],
        *,
        conn: Annotated[
            AsyncConnection | None, Doc("Optional existing connection to reuse")
        ] = None,
    ) -> int:
        """Count rows in a table matching the given filters"""
        count_query = self.db_count_query(
            cast(
                Select,
                self.get_query(
                    table,
                    table.select(),
                    params=filters,
                ),
            ),
        )
        async with self.ensure_connection(conn) as conn:
            result: CursorResult = await conn.execute(count_query)
            return cast(int, result.scalar())

    # Query methods

    def insert_query(
        self,
        table: Annotated[Table, Doc("The table to insert into")],
        records: Annotated[
            list[dict] | dict, Doc("A single row dict or a list of row dicts")
        ],
    ) -> Insert:
        if isinstance(records, dict):
            records = [records]
        else:
            cols: Set[str] = set()
            for record in records:
                cols.update(record)
            new_records = []
            for record in records:
                if len(record) < len(cols):
                    record = record.copy()
                    missing = cols.difference(record)
                    for col in missing:
                        record[col] = None
                new_records.append(record)
            records = new_records
        return insert(table).values(records).returning(*table.columns)

    def get_query(
        self,
        table: Annotated[FromClause, Doc("The table the query targets")],
        sql_query: Annotated[
            QueryType, Doc("The base SQLAlchemy query to apply filters to")
        ],
        *,
        params: Annotated[
            dict | None, Doc("Key-value filter pairs; keys may use 'field:op' syntax")
        ] = None,
    ) -> QueryType:
        """Apply filters from params to a SQLAlchemy query and return it"""
        filters: list = []
        columns = table.c
        params = params or {}

        for key, value in params.items():
            bits = key.split(":")
            field = bits[0]
            op = bits[1] if len(bits) == 2 else "eq"
            field = getattr(columns, field)
            result = self.default_filter_column(field, op, value)
            if result is not None:
                if not isinstance(result, (list, tuple)):
                    result = (result,)
                filters.extend(result)
        if filters:
            whereclause = and_(*filters) if len(filters) > 1 else filters[0]
            sql_query = cast(Select, sql_query).where(whereclause)
        return sql_query

    def db_count_query(
        self,
        sql_query: Annotated[
            Select, Doc("The filtered SELECT query to wrap in a COUNT")
        ],
    ) -> Select:
        return select(func.count()).select_from(sql_query.alias("inner"))

    def order_by_query(
        self,
        table: Annotated[FromClause, Doc("The table the query targets")],
        sql_query: Annotated[Select, Doc("The SELECT query to add ordering to")],
        order_by: Annotated[
            tuple[str, ...],
            Doc("Column names to order by; prefix with '-' for descending"),
        ],
    ) -> Select:
        """Apply ordering to a SELECT query"""
        return sql_query.order_by(*self.order_by_columns(table, order_by))

    def order_by_columns(
        self,
        table: Annotated[FromClause, Doc("The table whose columns are referenced")],
        order_by: Annotated[
            tuple[str, ...],
            Doc("Column names to order by; prefix with '-' for descending"),
        ],
    ) -> list[Column]:
        """Return a list of SQLAlchemy column expressions for the given
        order_by fields"""
        columns = []
        for name in order_by:
            if name.startswith("-"):
                order_by_column = getattr(table.c, name[1:], None)
                if order_by_column is not None:
                    columns.append(order_by_column.desc())
            else:
                order_by_column = getattr(table.c, name, None)
                if order_by_column is not None:
                    columns.append(order_by_column)
        return columns

    def search_query(
        self,
        table: Annotated[FromClause, Doc("The table whose columns are searched")],
        sql_query: Annotated[
            Select, Doc("The SELECT query to add the search filter to")
        ],
        search_fields: Annotated[
            tuple[str, ...], Doc("Column names to search across using ILIKE")
        ],
        search: Annotated[str, Doc("Search text; empty string is a no-op")],
    ) -> Select:
        """Apply a case-insensitive substring search across the given columns"""
        if search and search_fields:
            columns = [getattr(table.c, col) for col in search_fields]
            return sql_query.where(or_(*(col.ilike(f"%{search}%") for col in columns)))
        return sql_query

    def default_filter_column(
        self,
        column: Annotated[Column, Doc("The SQLAlchemy column to filter on")],
        op: Annotated[
            str, Doc("Comparison operator: `eq`, `ne`, `gt`, `ge`, `lt`, or `le`")
        ],
        value: Annotated[
            Any, Doc("Comparison value; a list triggers IN / NOT IN for `eq` / `ne`")
        ],
    ) -> Any:
        """Build a SQLAlchemy WHERE clause expression for a single column filter"""
        if multiple := isinstance(value, (list, tuple)):
            value = tuple(column_value_to_python(column, v) for v in value)
        else:
            value = column_value_to_python(column, value)

        if multiple and op in ("eq", "ne"):
            if op == "eq":
                return column.in_(value)
            elif op == "ne":
                return ~column.in_(value)
        else:
            if multiple:
                assert len(value) > 0
                value = value[0]

            if op == "eq":
                return column == value
            elif op == "ne":
                return column != value
            elif op == "gt":
                return column > value
            elif op == "ge":
                return column >= value
            elif op == "lt":
                return column < value
            elif op == "le":
                return column <= value


def column_value_to_python(column: Column, value: Any) -> Any:
    py_type = column.type.python_type
    try:
        if py_type is datetime:
            return parse_date(value) if isinstance(value, str) else value
        elif py_type is date:
            return parse_date(value).date() if isinstance(value, str) else value
        elif py_type is int:
            return int(value)
        else:
            return value
    except Exception as e:
        raise ValidationError("cursor", "invalid cursor") from e
