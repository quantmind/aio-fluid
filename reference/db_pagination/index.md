# DB Pagination

The Pagination class is a tool for managing paginated rows from the database.

It requires the `db` extra to be installed:

```bash
pip install aio-fluid[db]
```

It can be imported from `fluid.db`:

```python
from fluid.db import Pagination, Search
```

## fluid.db.Pagination

Bases: `NamedTuple`

Cursor-based pagination over a database table.

The `order_by_fields` must uniquely identify a row: the values of those fields, taken together, must be distinct for every row matched by the query. The cursor stores nothing but those values, so it can only resume from an unambiguous position. Add a unique column, usually the primary key, as the last ordering field when the other fields can tie:

```python
Pagination.create("published_at", "id", limit=20)
```

When the ordering is not unique, rows sharing the same ordering values can be repeated across consecutive pages, and rows can be missed entirely because the database is free to order tied rows differently between the two queries.

### order_by_fields

```python
order_by_fields
```

Fields to order results by

### limit

```python
limit
```

Maximum number of results per page

### filters

```python
filters
```

Filters applied to the query

### search

```python
search
```

Full-text search configuration

### cursor

```python
cursor
```

Decoded pagination cursor

### desc

```python
desc = False
```

Order results in descending order

### order_by_fields_sign

```python
order_by_fields_sign
```

### create

```python
create(
    *order_by_fields,
    cursor="",
    limit=None,
    filters=None,
    search=None,
    desc=False
)
```

Factory method to create a Pagination instance, decoding the cursor if provided.

If the cursor is provided, limit and search text are extracted from it, and the provided values for these parameters are ignored. A cursor limit outside 1 to settings.DEFAULT_PAGINATION_MAX_LIMIT is replaced by the maximum.

Cursors are signed with settings.PAGINATION_SECRET when it is set, and a cursor that does not match the signature raises a ValidationError. Without a secret a client can change the cursor. Either way, with a cursor the provided filters are applied on top of the cursor filters and win over them on the same key, so pass every filter the caller must enforce, such as the owner of the rows, on every page.

The `order_by_fields` must uniquely identify a row, otherwise pages can repeat or miss rows.

| PARAMETER          | DESCRIPTION                                                                                                                       |
| ------------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `*order_by_fields` | Fields to order results by **TYPE:** `str` **DEFAULT:** `()`                                                                      |
| `cursor`           | Encoded pagination cursor from a previous response **TYPE:** `str` **DEFAULT:** `''`                                              |
| `limit`            | Maximum number of results per page; defaults to settings.DEFAULT_PAGINATION_LIMIT **TYPE:** \`int                                 |
| `filters`          | Filters to apply to the query; with a cursor they are applied on top of the filters the cursor carries **TYPE:** \`dict[str, Any] |
| `search`           | Full-text search configuration **TYPE:** \`Search                                                                                 |
| `desc`             | Order results in descending order **TYPE:** `bool` **DEFAULT:** `False`                                                           |

Source code in `fluid/db/pagination.py`

```python
@classmethod
def create(
    cls,
    *order_by_fields: Annotated[str, Doc("Fields to order results by")],
    cursor: Annotated[
        str,
        Doc("Encoded pagination cursor from a previous response"),
    ] = "",
    limit: Annotated[
        int | None,
        Doc(
            "Maximum number of results per page; "
            "defaults to settings.DEFAULT_PAGINATION_LIMIT"
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
        bool,
        Doc("Order results in descending order"),
    ] = False,
) -> Self:
    """Factory method to create a Pagination instance,
    decoding the cursor if provided.

    If the cursor is provided, limit and search text are extracted from it,
    and the provided values for these parameters are ignored. A cursor
    limit outside 1 to settings.DEFAULT_PAGINATION_MAX_LIMIT is replaced
    by the maximum.

    Cursors are signed with settings.PAGINATION_SECRET when it is set, and
    a cursor that does not match the signature raises a ValidationError.
    Without a secret a client can change the cursor. Either way, with a
    cursor the provided filters are applied on top of the cursor filters
    and win over them on the same key, so pass every filter the caller
    must enforce, such as the owner of the rows, on every page.

    The `order_by_fields` must uniquely identify a row, otherwise pages can
    repeat or miss rows.
    """
    if cursor:
        decoded_cursor = Cursor.decode(
            cursor, order_by_fields, secret=settings.PAGINATION_SECRET
        )
        max_limit = settings.DEFAULT_PAGINATION_MAX_LIMIT
        limit = decoded_cursor.limit
        if limit < 1 or limit > max_limit:
            limit = max_limit
        filters = {**decoded_cursor.filters, **(filters or {})}
        if search:
            search = search._replace(search_text=decoded_cursor.search_text)
    else:
        decoded_cursor = None
        limit = limit or settings.DEFAULT_PAGINATION_LIMIT
    return cls(
        order_by_fields=order_by_fields,
        cursor=decoded_cursor,
        limit=limit,
        filters=filters or {},
        search=search,
        desc=desc,
    )
```

### execute

```python
execute(db, table, *, conn=None, columns=())
```

Execute the paginated query and return the results along with the next cursor.

| PARAMETER | DESCRIPTION                                                                                                                                                                          |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `db`      | Database instance to execute the query on **TYPE:** `CrudDB`                                                                                                                         |
| `table`   | SQLAlchemy table to query **TYPE:** `FromClause`                                                                                                                                     |
| `conn`    | Optional existing connection to reuse **TYPE:** \`AsyncConnection                                                                                                                    |
| `columns` | Columns to select, every column of the table when empty; they must include the order_by_fields, which the cursor is built from **TYPE:** `Sequence[ColumnElement]` **DEFAULT:** `()` |

Source code in `fluid/db/pagination.py`

```python
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
    if self.limit > 0 and len(data) > self.limit:
        cursor = self._encode_cursor(data[-1])
        data = data[:-1]
    return data, cursor
```

### query

```python
query(db, table, columns=())
```

Source code in `fluid/db/pagination.py`

```python
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
    columns = db.order_by_columns(table, self.order_by_fields_sign)
    ordered = sql_query.order_by(*columns)
    return ordered.limit(self.limit + 1) if self.limit > 0 else ordered
```

## fluid.db.Search

Bases: `NamedTuple`

### search_fields

```python
search_fields
```

Fields to search in

### search_text

```python
search_text
```

Text to search for
