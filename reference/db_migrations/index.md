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

Create all tables from :attr:`metadata` in database

Source code in `fluid/db/migration.py`

```python
def create_all(self) -> None:
    """Create all tables from :attr:`metadata` in database"""
    with self.sync_engine.begin() as conn:
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

### drop_all_schemas

```python
drop_all_schemas()
```

Drop all schema in database

Source code in `fluid/db/migration.py`

```python
def drop_all_schemas(self) -> None:
    """Drop all schema in database"""
    with self.sync_engine.begin() as conn:
        conn.execute(sa.text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(sa.text("CREATE SCHEMA IF NOT EXISTS public"))
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
