from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Union

import click
from sqlalchemy import func, inspect, select, text

if TYPE_CHECKING:
    from .container import Database


class DbGroup(click.Group):
    """A click group for database commands

    This class provides a CLI for a Database Application.

    It requires to install the `cli` extra dependencies.
    """

    def __init__(
        self,
        db: Database,
        name: str = "db",
        help: str = "Manage database and migrations",  # noqa: A002
        **kwargs: Any,
    ) -> None:
        super().__init__(name=name, help=help, **kwargs)
        self.db = db
        for command in _db.commands.values():
            self.add_command(command)

    def get_command(self, ctx: click.Context, name: str) -> Optional[click.Command]:
        ctx.obj = {"db": self.db}
        return super().get_command(ctx, name)

    def list_commands(self, ctx: click.Context) -> list[str]:
        ctx.obj = {"db": self.db}
        return super().list_commands(ctx)


def get_db(ctx: click.Context) -> Database:
    return ctx.obj["db"]


@click.group()
def _db() -> None:
    """Perform database migrations and utilities"""


@_db.command()
@click.pass_context
def dsn(ctx: click.Context) -> None:
    """Display data source name"""
    click.echo(get_db(ctx).engine.url.render_as_string(hide_password=False))


@_db.command()
@click.pass_context
def init(ctx: click.Context) -> None:
    """Creates a new migration repository."""
    get_db(ctx).migration().init()


@_db.command()
@click.option("-m", "--message", help="Revision message", required=True)
@click.option(
    "--branch-label",
    help="Specify a branch label to apply to the new revision",
)
@click.pass_context
def migrate(ctx: click.Context, message: str, branch_label: str) -> None:
    """Autogenerate a new revision file
    alias for 'revision --autogenerate'
    """
    get_db(ctx).migration().revision(
        message, autogenerate=True, branch_label=branch_label
    )


@_db.command()
@click.option("-m", "--message", help="Revision message", required=True)
@click.option(
    "--branch-label",
    help="Specify a branch label to apply to the new revision",
)
@click.option(
    "--autogenerate",
    default=False,
    is_flag=True,
    help=(
        "Populate revision script with candidate migration "
        "operations, based on comparison of database to model"
    ),
)
@click.pass_context
def revision(
    ctx: click.Context,
    message: str,
    branch_label: str,
    autogenerate: bool,
) -> None:
    """Autogenerate a new revision file"""
    get_db(ctx).migration().revision(
        message,
        autogenerate=autogenerate,
        branch_label=branch_label,
    )


@_db.command()
@click.argument("revision", default="heads")
@click.pass_context
def upgrade(ctx: click.Context, revision: Union[str, int]) -> None:
    """Upgrade to a later version"""
    get_db(ctx).migration().upgrade(str(revision))
    click.echo(f"upgraded successfully to {revision}")


@_db.command()
@click.argument("revision")
@click.pass_context
def downgrade(ctx: click.Context, revision: Union[str, int]) -> None:
    """Downgrade to a previous REVISION identifier"""
    get_db(ctx).migration().downgrade(str(revision))
    click.echo(f"downgraded successfully to {revision}")


@_db.command()
@click.option("--revision", default="heads")
@click.pass_context
def show(ctx: click.Context, revision: str) -> None:
    """Show revision ID and creation date"""
    click.echo(get_db(ctx).migration().show(revision))


@_db.command()
@click.pass_context
def history(ctx: click.Context) -> None:
    """List changeset scripts in chronological order"""
    click.echo(get_db(ctx).migration().history())


@_db.command()
@click.option("--verbose/--quiet", default=False)
@click.pass_context
def current(ctx: click.Context, verbose: bool) -> None:
    """Show revision ID and creation date"""
    click.echo(get_db(ctx).migration().current(verbose))


@_db.command()
@click.argument("dbname", nargs=1)
@click.pass_context
def create(ctx: click.Context, dbname: str) -> None:
    """Creates a new database"""
    if get_db(ctx).migration().db_create(dbname):
        click.echo(f"database '{dbname}' created")
    else:
        click.echo(f"database '{dbname}' already available")


@_db.command()
@click.argument("dbname", nargs=1)
@click.option("-y", "--yes", is_flag=True, help="confirm")
@click.pass_context
def drop(ctx: click.Context, dbname: str, yes: bool) -> None:
    """Drop an existing database"""
    if not yes:  # pragma: no cover
        click.echo(f"Are you sure you want to drop database '{dbname}'?")
        click.confirm("Please confirm", abort=True)
    if get_db(ctx).migration().db_drop(dbname):
        click.echo(f"database '{dbname}' dropped")
    else:
        click.echo(f"database '{dbname}' not found")


@_db.command()
@click.argument("name", nargs=1)
@click.pass_context
def create_schema(ctx: click.Context, name: str) -> None:
    """Creates a new database schema"""
    with get_db(ctx).sync_engine.connect() as conn:
        conn.execute(text(f"create schema {name}"))
    click.echo(f"schema {name} created")


@_db.command()
@click.argument("username", nargs=1)
@click.argument("password", nargs=1)
@click.option("--dbname", help="database name")
@click.option("--role", help="role name")
@click.option("--schema", default="public", show_default=True)
@click.pass_context
def create_ro_user(
    ctx: click.Context,
    username: str,
    password: str,
    dbname: str,
    role: str,
    schema: str,
) -> None:
    """Creates or updates a database user"""
    d = get_db(ctx)
    if dbname:
        url = d.engine.url.set(database=dbname)
        d = type(d).from_env(dsn=str(url))
    mig = d.migration()
    mig.create_ro_user(username, password, role=role, schema=schema)
    click.echo(f"schema {username} created")


@_db.command()
@click.argument("role", nargs=1)
@click.pass_context
def drop_role(ctx: click.Context, role: str) -> None:
    """Drop a role or a user"""
    mig = get_db(ctx).migration()
    if mig.drop_role(role):
        click.echo(f"role {role} dropped")
    else:
        click.echo(f"role {role} not found")


@_db.command()
@click.option(
    "--db",
    default=False,
    is_flag=True,
    help="List tables in database rather than in sqlalchemy metadata",
)
@click.pass_context
def tables(ctx: click.Context, db: bool) -> None:
    """List all tables managed by the app"""
    d = get_db(ctx)
    if db:
        tables = inspect(d.sync_engine).get_table_names()
    else:
        tables = list(d.tables)
    for name in sorted(tables):
        click.echo(name)


@_db.command()
@click.pass_context
def databases(ctx: click.Context) -> None:
    """List databases"""
    sql = text("SELECT datname FROM pg_database WHERE datistemplate = false;")
    engine = get_db(ctx).sync_engine
    with engine.begin() as conn:
        for row in conn.execute(sql):
            click.echo(f"{row[0]}")


@_db.command()
@click.argument("table", nargs=1)
@click.option("--dry", help="dry run", is_flag=True, default=False)
@click.pass_context
def delete_rows(ctx: click.Context, table: str, dry: bool) -> None:
    """Delete all rows in a table"""
    db = get_db(ctx)
    engine = db.sync_engine
    try:
        db_table = db.metadata.tables[table]
    except KeyError:
        click.echo(f"table {table} not found in {engine}", err=True)
        raise click.Abort() from None
    with engine.begin() as conn:
        count_query = select(func.count()).select_from(db_table)
        rows = conn.execute(count_query).scalar()
        click.echo(f"removing {rows} rows")
        if dry:
            click.echo("nothing done, dry mode")
        else:
            conn.execute(db_table.delete())
