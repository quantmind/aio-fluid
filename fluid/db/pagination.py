from __future__ import annotations

import warnings
from typing import Any, NamedTuple, Self, Sequence, cast

from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql import FromClause, select, tuple_
from sqlalchemy.sql.expression import ColumnElement
from typing_extensions import Annotated, Doc

from fluid import settings
from fluid.utils.errors import ValidationError

from .crud import CrudDB, Row, Select, column_value_to_python
from .cursor import Cursor


class Search(NamedTuple):
    search_fields: Annotated[tuple[str, ...], Doc("Fields to search in")]
    search_text: Annotated[str, Doc("Text to search for")]


class OrderBy(NamedTuple):
    """The orderings a paginated query supports.

    A page is sorted by one of the `fields`, the first one unless another is
    requested, followed by the `unique_fields`. The unique fields break the
    ties of the sort field, so together they must uniquely identify a row,
    usually with the primary key:

    ```python
    OrderBy(("published_at", "title"), unique_fields=("id",), desc=True)
    ```

    When the ordering is not unique, rows sharing the same ordering values can
    be repeated across consecutive pages, and rows can be missed entirely
    because the database is free to order tied rows differently between the
    two queries.
    """

    fields: Annotated[
        tuple[str, ...],
        Doc("Fields a page can be sorted by, at least one; the first is the default"),
    ]
    unique_fields: Annotated[
        tuple[str, ...],
        Doc(
            "Fields always appended to the sort field, in the same direction, "
            "so the ordering is unique"
        ),
    ] = ()
    desc: Annotated[
        bool,
        Doc("Direction of the default ordering, when no sort field is requested"),
    ] = False

    def sort(
        self,
        sort_by: Annotated[
            str,
            Doc(
                "Requested sort field, prefixed by `-` for descending order; "
                "the first field in the default direction when empty"
            ),
        ] = "",
    ) -> tuple[str, bool]:
        """Return the sort field and whether it is descending.

        A sort field which is not one of the `fields` raises a ValidationError.
        """
        if not sort_by:
            return self.fields[0], self.desc
        desc = sort_by.startswith("-")
        field = sort_by[1:] if desc else sort_by
        if field not in self.fields:
            raise ValidationError("sort_by", f"cannot sort by '{field}'")
        return field, desc

    @classmethod
    def from_legacy(cls, fields: tuple[str | Self, ...], desc: bool | None) -> Self:
        names: list[str] = []
        for field in fields:
            if isinstance(field, OrderBy):
                if len(fields) > 1:
                    raise ValueError("Cannot mix OrderBy with other fields")
                if desc is not None:
                    raise ValueError("Cannot specify desc when using OrderBy instance")
                return field
            names.append(field)
        if not names:
            return cls(fields=(), desc=desc or False)
        warnings.warn(
            "Passing order by fields and desc to Pagination.create is deprecated, "
            "pass an OrderBy instead",
            DeprecationWarning,
            stacklevel=3,
        )
        return cls(
            fields=tuple(names[:1]),
            unique_fields=tuple(names[1:]),
            desc=desc or False,
        )


class Pagination(NamedTuple):
    """Cursor-based pagination over a database table.

    The rows are sorted by `sort_by` followed by the unique fields of
    `order_by`, all in the same direction. The cursor stores the sort field,
    the direction and the values of the ordering fields for the first row of
    the next page, so it can only resume the ordering that produced it.
    """

    order_by: Annotated[OrderBy, Doc("Orderings supported by the query")]
    sort_by: Annotated[str, Doc("Field the results are sorted by")]
    limit: Annotated[int, Doc("Maximum number of results per page")]
    filters: Annotated[dict[str, Any], Doc("Filters applied to the query")]
    search: Annotated[Search | None, Doc("Full-text search configuration")]
    cursor: Annotated[Cursor | None, Doc("Decoded pagination cursor")]
    desc: Annotated[bool, Doc("Order results in descending order")] = False

    @classmethod
    def create(
        cls,
        *order_by_fields: Annotated[
            OrderBy | str, Doc("Orderings supported by the query")
        ],
        sort_by: Annotated[
            str,
            Doc(
                "Sort field requested by the client, one of the `order_by` "
                "fields, prefixed by `-` for descending order; the default "
                "ordering of `order_by` when empty"
            ),
        ] = "",
        cursor: Annotated[
            str,
            Doc("Encoded pagination cursor from a previous response"),
        ] = "",
        limit: Annotated[
            int | None,
            Doc(
                "Maximum number of results per page, capped at "
                "settings.DEFAULT_PAGINATION_MAX_LIMIT; defaults to "
                "settings.DEFAULT_PAGINATION_LIMIT when not given or 0"
            ),
        ] = None,
        filters: Annotated[
            dict[str, Any] | None,
            Doc(
                "Filters to apply to the query; with a cursor they are applied "
                "on top of the filters the cursor carries"
            ),
        ] = None,
        search: Annotated[
            Search | None,
            Doc("Full-text search configuration"),
        ] = None,
        desc: Annotated[
            bool | None,
            Doc("Order results in descending order, supported but deprecated"),
        ] = None,
    ) -> Self:
        """Factory method to create a Pagination instance,
        decoding the cursor if provided.

        Without a cursor, a limit above settings.DEFAULT_PAGINATION_MAX_LIMIT
        is replaced by the maximum and a negative limit raises a
        ValidationError, so every page is bounded.

        If the cursor is provided, limit and search text are extracted from it,
        and the provided values for these parameters are ignored. The same
        applies to the sort field and the direction, which replace `sort_by`.
        A cursor whose sort field is not one of the `order_by` fields raises a
        ValidationError. A cursor limit outside 1 to
        settings.DEFAULT_PAGINATION_MAX_LIMIT is replaced by the maximum.

        Cursors are signed with settings.PAGINATION_SECRET when it is set, and
        a cursor that does not match the signature raises a ValidationError.
        Without a secret a client can change the cursor. Either way, with a
        cursor the provided filters are applied on top of the cursor filters
        and win over them on the same key, so pass every filter the caller
        must enforce, such as the owner of the rows, on every page.
        """
        order_by = OrderBy.from_legacy(order_by_fields, desc)
        if not order_by.fields:
            raise ValueError("order_by requires at least one field")
        if cursor:
            decoded_cursor = Cursor.decode(cursor, secret=settings.PAGINATION_SECRET)
            if decoded_cursor.sort_by not in order_by.fields or len(
                decoded_cursor.values
            ) != 1 + len(order_by.unique_fields):
                raise ValidationError("cursor", "invalid cursor")
            sort_field, desc = decoded_cursor.sort_by, decoded_cursor.desc
            max_limit = settings.DEFAULT_PAGINATION_MAX_LIMIT
            limit = decoded_cursor.limit
            if limit < 1 or limit > max_limit:
                limit = max_limit
            filters = {**decoded_cursor.filters, **(filters or {})}
            if search:
                search = search._replace(search_text=decoded_cursor.search_text)
        else:
            decoded_cursor = None
            sort_field, desc = order_by.sort(sort_by)
            if limit is not None and limit < 0:
                raise ValidationError("limit", "limit cannot be negative")
            limit = min(
                limit or settings.DEFAULT_PAGINATION_LIMIT,
                settings.DEFAULT_PAGINATION_MAX_LIMIT,
            )
        return cls(
            order_by=order_by,
            sort_by=sort_field,
            cursor=decoded_cursor,
            limit=limit,
            filters=filters or {},
            search=search,
            desc=desc,
        )

    @property
    def order_by_fields(self) -> tuple[str, ...]:
        """The fields the results are ordered by, the sort field first"""
        return (self.sort_by, *self.order_by.unique_fields)

    async def execute(
        self,
        db: Annotated[CrudDB, Doc("Database instance to execute the query on")],
        table: Annotated[FromClause, Doc("SQLAlchemy table to query")],
        *,
        conn: Annotated[
            AsyncConnection | None,
            Doc("Optional existing connection to reuse"),
        ] = None,
        columns: Annotated[
            Sequence[ColumnElement],
            Doc(
                "Columns to select, every column of the table when empty; "
                "they must include the order_by_fields, which the cursor is "
                "built from"
            ),
        ] = (),
    ) -> tuple[Sequence[Row], str]:
        """Execute the paginated query and return the results
        along with the next cursor.
        """
        sql_query = self.query(db, table, columns)
        async with db.ensure_connection(conn) as conn:
            result = await conn.execute(sql_query)
        data = result.all()
        cursor = ""
        if len(data) > self.limit:
            cursor = self._encode_cursor(data[-1])
            data = data[:-1]
        return data, cursor

    def query(
        self,
        db: CrudDB,
        table: FromClause,
        columns: Sequence[ColumnElement] = (),
    ) -> Select:
        select_query = select(*columns) if columns else table.select()
        sql_query = cast(
            Select,
            db.get_query(table, select_query, params=self.filters),
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
        sign = "-" if self.desc else ""
        columns = db.order_by_columns(
            table, tuple(f"{sign}{field}" for field in self.order_by_fields)
        )
        return sql_query.order_by(*columns).limit(self.limit + 1)

    def _start_clause(self, table: FromClause) -> ColumnElement[bool] | None:
        if self.cursor:
            columns = []
            values = []
            for name, value in zip(
                self.order_by_fields, self.cursor.values, strict=True
            ):
                column = getattr(table.c, name)
                try:
                    values.append(column_value_to_python(column, value))
                except ValidationError as e:
                    raise ValidationError("cursor", "invalid cursor") from e
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
        return Cursor(
            sort_by=self.sort_by,
            values=tuple(getattr(row, field) for field in self.order_by_fields),
            limit=self.limit,
            filters=self.filters,
            search_text=self.search.search_text if self.search else "",
            desc=self.desc,
        ).encode(secret=settings.PAGINATION_SECRET)
