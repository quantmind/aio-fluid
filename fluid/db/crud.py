from datetime import date, datetime
from typing import Any, Set, TypeAlias, cast

from dateutil.parser import parse as parse_date
from sqlalchemy import Column, Table, func, insert, select
from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.engine.row import Row
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import FromClause, Select, and_, or_
from sqlalchemy.sql.dml import Delete, Insert, Update

from fluid.utils.errors import ValidationError

from .container import Database

QueryType: TypeAlias = Delete | Select | Update


class CrudDB(Database):
    """A Database with additional methods for CRUD operations"""

    async def db_select(
        self,
        table: FromClause,
        filters: dict,
        *,
        order_by: tuple[str, ...] | None = None,
        conn: AsyncConnection | None = None,
    ) -> CursorResult:
        """Select rows from a given table
        :param table: sqlalchemy Table
        :param filters: key-value pairs for filtering rows
        :param conn: optional db connection
        :param consumer: optional consumer (see :meth:`.get_query`)
        """
        sql_query = self.get_query(table, Select(table), params=filters)
        if order_by:
            sql_query = self.order_by_query(table, cast(Select, sql_query), order_by)
        async with self.ensure_transaction(conn) as conn:
            return await conn.execute(sql_query)

    async def db_insert(
        self,
        table: Table,
        data: list[dict] | dict,
        *,
        conn: AsyncConnection | None = None,
    ) -> CursorResult:
        """Perform an insert into a table
        :param table: sqlalchemy Table
        :param data: key-value pairs for columns values
        :param conn: optional db connection
        """
        async with self.ensure_transaction(conn) as conn:
            sql_query = self.insert_query(table, data)
            return await conn.execute(sql_query)

    async def db_update(
        self,
        table: Table,
        filters: dict,
        data: dict,
        *,
        conn: AsyncConnection | None = None,
    ) -> CursorResult:
        """Perform an update of rows

        :param table: sqlalchemy Table
        :param filters: key-value pairs for filtering rows to update
        :param data: key-value pairs for updating columns values of selected rows
        :param conn: optional db connection
        :param consumer: optional consumer (see :meth:`.get_query`)
        """
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
        table: Table,
        filters: dict,
        data: dict | None = None,
        *,
        conn: AsyncConnection | None = None,
    ) -> Row:
        """Perform an upsert for a single record

        :param table: sqlalchemy Table
        :param filters: key-value pairs for filtering rows to update
        :param data: key-value pairs for updating columns values of selected rows
        :param conn: optional db connection
        :param consumer: optional consumer (see :meth:`.get_query`)
        """
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
        table: Table,
        filters: dict,
        *,
        conn: AsyncConnection | None = None,
    ) -> CursorResult:
        """Delete rows from a given table
        :param table: sqlalchemy Table
        :param filters: key-value pairs for filtering rows
        :param conn: optional db connection
        :param consumer: optional consumer (see :meth:`.get_query`)
        """
        sql_query = self.get_query(
            table,
            table.delete().returning(*table.columns),
            params=filters,
        )
        async with self.ensure_transaction(conn) as conn:
            return await conn.execute(sql_query)

    async def db_count(
        self,
        table: FromClause,
        filters: dict,
        *,
        conn: AsyncConnection | None = None,
    ) -> int:
        """Count rows in a table
        :param table: sqlalchemy Table
        :param filters: key-value pairs for filtering rows
        :param conn: optional db connection
        :param consumer: optional consumer (see :meth:`.get_query`)
        """
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

    def insert_query(self, table: Table, records: list[dict] | dict) -> Insert:
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
        table: FromClause,
        sql_query: QueryType,
        *,
        params: dict | None = None,
    ) -> QueryType:
        """Build an SqlAlchemy query
        :param table: sqlalchemy Table
        :param sql_query: sqlalchemy query type
        :param params: key-value pairs for the query
        :param consumer: optional consumer for manipulating parameters
        """
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

    def db_count_query(self, sql_query: Select) -> Select:
        return select(func.count()).select_from(sql_query.alias("inner"))

    def order_by_query(
        self,
        table: FromClause,
        sql_query: Select,
        order_by: tuple[str, ...],
    ) -> Select:
        """Apply ordering to a sql_query"""
        return sql_query.order_by(*self.order_by_columns(table, order_by))

    def order_by_columns(
        self,
        table: FromClause,
        order_by: tuple[str, ...],
    ) -> list[Column]:
        """Apply ordering to a sql_query"""
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
        table: FromClause,
        sql_query: Select,
        search_fields: tuple[str, ...],
        search: str,
    ) -> Select:
        """Apply search to a sql_query"""
        if search and search_fields:
            columns = [getattr(table.c, col) for col in search_fields]
            return sql_query.where(or_(*(col.ilike(f"%{search}%") for col in columns)))
        return sql_query

    def default_filter_column(self, column: Column, op: str, value: Any) -> Any:
        """
        Applies a filter on a field.
        Notes on 'ne' op:
        Example data: [None, 'john', 'roger']
        ne:john would return only roger (i.e. nulls excluded)
        ne:     would return john and roger
        Notes on  'search' op:
        For some reason, SQLAlchemy uses to_tsquery rather than
        plainto_tsquery for the match operator
        to_tsquery uses operators (&, |, ! etc.) while
        plainto_tsquery tokenises the input string and uses AND between
        tokens, hence plainto_tsquery is what we want here
        For other database back ends, the behaviour of the match
        operator is completely different - see:
        http://docs.sqlalchemy.org/en/rel_1_0/core/sqlelement.html
        :param field: field name
        :param op: 'eq', 'ne', 'gt', 'lt', 'ge', 'le' or 'search'
        :param value: comparison value, string or list/tuple
        :return:
        """
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
