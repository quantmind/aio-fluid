# DB Migration

The migration object is accessed via Database.migration or CrudDB and is used to create and manage database migrations.

It requires the `db` extra to be installed:

```bash
pip install aio-fluid[db]
```

## fluid.db.Migration

```python
Migration(db)
```

A wrapper around Alembic commands to perform database migrations

### db

```python
db
```

### cfg

```python
cfg = field(init=False, repr=False)
```

### metadata

```python
metadata
```

### sync_engine

```python
sync_engine
```

### init

```python
init()
```

Source code in `fluid/db/migration.py`

```python
def init(self) -> str:
    dirname = self.cfg.get_main_option("script_location") or ""
    alembic_cmd.init(self.cfg, dirname)
    _wire_metadata(Path(dirname) / "env.py")
    return self.message()
```

### show

```python
show(revision)
```

Source code in `fluid/db/migration.py`

```python
def show(self, revision: str) -> str:
    alembic_cmd.show(self.cfg, revision)
    return self.message()
```

### history

```python
history()
```

Source code in `fluid/db/migration.py`

```python
def history(self) -> str:
    alembic_cmd.history(self.cfg)
    return self.message()
```

### revision

```python
revision(message, autogenerate=False, branch_label=None)
```

Source code in `fluid/db/migration.py`

```python
def revision(
    self,
    message: str,
    autogenerate: bool = False,
    branch_label: str | None = None,
) -> str:
    alembic_cmd.revision(
        self.cfg,
        autogenerate=autogenerate,
        message=message,
        branch_label=branch_label,
    )
    return self.message()
```

### upgrade

```python
upgrade(revision)
```

Source code in `fluid/db/migration.py`

```python
def upgrade(self, revision: str) -> str:
    alembic_cmd.upgrade(self.cfg, revision)
    return self.message()
```

### downgrade

```python
downgrade(revision)
```

Source code in `fluid/db/migration.py`

```python
def downgrade(self, revision: str) -> str:
    alembic_cmd.downgrade(self.cfg, revision)
    return self.message()
```

### current

```python
current(verbose=False)
```

Source code in `fluid/db/migration.py`

```python
def current(self, verbose: bool = False) -> str:
    alembic_cmd.current(self.cfg, verbose=verbose)
    return self.message()
```

### message

```python
message()
```

Source code in `fluid/db/migration.py`

```python
def message(self) -> str:
    msg = cast(StringIO, self.cfg.stdout).getvalue()
    self.cfg.stdout.seek(0)
    self.cfg.stdout.truncate()
    return msg
```

### db_exists

```python
db_exists(dbname='')
```

Source code in `fluid/db/migration.py`

```python
def db_exists(self, dbname: str = "") -> bool:
    url = self.sync_engine.url
    if dbname:
        url = url.set(database=dbname)
    return database_exists(url)
```

### db_create

```python
db_create(dbname='')
```

Creates a new database if it does not exist

Source code in `fluid/db/migration.py`

```python
def db_create(self, dbname: str = "") -> bool:
    """Creates a new database if it does not exist"""
    url = self.sync_engine.url
    if dbname:
        url = url.set(database=dbname)
    if database_exists(url):
        return False
    create_database(url)
    return True
```

### db_drop

```python
db_drop(dbname='')
```

Source code in `fluid/db/migration.py`

```python
def db_drop(self, dbname: str = "") -> bool:
    url = self.sync_engine.url
    if dbname:
        url = url.set(database=dbname)
    if database_exists(url):
        drop_database(url)
        return True
    return False
```

### create_all

```python
create_all()
```

Create all tables from :attr:`metadata` in database.

A schema is created before the tables that live in it. SQLAlchemy's `MetaData.create_all` only emits `CREATE TABLE` and does not create the schema itself, so tables registered on a non-`public` schema would otherwise fail with `InvalidSchemaName`.

Source code in `fluid/db/migration.py`

```python
def create_all(self) -> None:
    """Create all tables from :attr:`metadata` in database.

    A schema is created before the tables that live in it. SQLAlchemy's
    ``MetaData.create_all`` only emits ``CREATE TABLE`` and does not create
    the schema itself, so tables registered on a non-``public`` schema
    would otherwise fail with ``InvalidSchemaName``.
    """
    schemas = {
        table.schema
        for table in self.metadata.sorted_tables
        if table.schema is not None
    }
    with self.sync_engine.begin() as conn:
        for schema in sorted(schemas):
            conn.execute(sa.schema.CreateSchema(schema, if_not_exists=True))
        self.metadata.create_all(conn)
```

### truncate

```python
truncate(table, *, cascade=False)
```

Truncate a specific table in the database

Source code in `fluid/db/migration.py`

```python
def truncate(self, table: str, *, cascade: bool = False) -> None:
    """Truncate a specific table in the database"""
    cascade_sql = " cascade" if cascade else ""
    with self.sync_engine.begin() as conn:
        conn.execute(sa.text(f"truncate table {table}{cascade_sql}"))
```

### truncate_all

```python
truncate_all()
```

Truncate all tables in the database

Source code in `fluid/db/migration.py`

```python
def truncate_all(self) -> None:
    """Truncate all tables in the database"""
    with self.sync_engine.begin() as conn:
        conn.execute(sa.text(f'truncate {", ".join(self.metadata.tables)}'))
```

### schemas

```python
schemas()
```

Return the non-system schemas in the database.

System schemas (`pg_*` and `information_schema`) are excluded.

Source code in `fluid/db/migration.py`

```python
def schemas(self) -> list[str]:
    """Return the non-system schemas in the database.

    System schemas (``pg_*`` and ``information_schema``) are excluded.
    """
    with self.sync_engine.connect() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT nspname FROM pg_namespace "
                "WHERE nspname NOT LIKE 'pg_%' "
                "AND nspname <> 'information_schema'"
            )
        )
        return [row[0] for row in rows]
```

### drop_all_schemas

```python
drop_all_schemas(schemas=None)
```

Drop the given schemas, or `public` when none are given.

When `schemas` is `None` only `public` is dropped, for backwards compatibility. Pass an explicit sequence to drop additional schemas, for example the application schemas created by multi-schema setups.

Source code in `fluid/db/migration.py`

```python
def drop_all_schemas(self, schemas: Sequence[str] | None = None) -> None:
    """Drop the given schemas, or ``public`` when none are given.

    When ``schemas`` is ``None`` only ``public`` is dropped, for backwards
    compatibility. Pass an explicit sequence to drop additional schemas,
    for example the application schemas created by multi-schema setups.
    """
    names: tuple[str, ...] = ("public",) if schemas is None else tuple(schemas)
    with self.sync_engine.begin() as conn:
        for name in names:
            conn.execute(sa.schema.DropSchema(name, cascade=True, if_exists=True))
        conn.execute(sa.schema.CreateSchema("public", if_not_exists=True))
```

### create_ro_user

```python
create_ro_user(
    username, password, role="", schema="public"
)
```

Creates a read-only user

Source code in `fluid/db/migration.py`

```python
def create_ro_user(
    self,
    username: str,
    password: str,
    role: str = "",
    schema: str = "public",
) -> bool:
    """Creates a read-only user"""
    engine = self.sync_engine
    role = role or f"{engine.url.username}_ro"
    database = engine.url.database
    created = True
    with engine.begin() as conn:
        try:
            conn.execute(sa.text(f"CREATE ROLE {role};"))
        except sa.exc.ProgrammingError:
            created = False
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                f"GRANT CONNECT ON DATABASE {database} TO {role};"
                f"GRANT USAGE ON SCHEMA {schema} TO {role};"
                f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {role};"
                f"GRANT SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {role};",
            ),
        )
        conn.execute(
            sa.text(
                f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
                f"GRANT SELECT ON TABLES TO {role};",
            ),
        )
    with engine.begin() as conn:
        try:
            conn.execute(
                sa.text(
                    f"CREATE USER {username} WITH PASSWORD '{password}';"
                    f"GRANT {role} TO {username};",
                ),
            )
        except sa.exc.ProgrammingError:
            created = False
    return created
```

### drop_role

```python
drop_role(role)
```

Drop a role

Source code in `fluid/db/migration.py`

```python
def drop_role(
    self,
    role: str,
) -> bool:
    """Drop a role"""
    try:
        with self.sync_engine.begin() as conn:
            conn.execute(sa.text(f"DROP OWNED BY {role};"))
        with self.sync_engine.begin() as conn:
            conn.execute(sa.text(f"DROP ROLE IF EXISTS {role};"))
    except sa.exc.ProgrammingError as exc:
        if f'role "{role}" does not exist' not in str(exc):
            raise
        return False
    return True
```
