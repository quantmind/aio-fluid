from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

import click
import uvicorn
from fastapi import FastAPI
from rich.console import Console
from rich.table import Table
from uvicorn.importer import import_from_string

from fluid.utils import log

if TYPE_CHECKING:
    from .consumer import TaskManager
    from .models import TaskRun


TaskManagerApp = FastAPI | Callable[..., Any] | str


class TaskManagerCLI(click.Group):
    def __init__(self, task_manager_app: TaskManagerApp, **kwargs: Any):
        kwargs.setdefault("commands", DEFAULT_COMMANDS)
        super().__init__(**kwargs)
        self.task_manager_app = task_manager_app


def ctx_task_manager_app(ctx: click.Context) -> TaskManagerApp:
    return ctx.parent.command.task_manager_app  # type: ignore


def ctx_app(ctx: click.Context) -> FastAPI:
    app = ctx_task_manager_app(ctx)  # type: ignore
    if isinstance(app, str):
        return import_from_string(app)()
    elif isinstance(app, FastAPI):
        return app
    else:
        return app()


def ctx_task_manager(ctx: click.Context) -> TaskManager:
    return ctx_app(ctx).state.task_manager


@click.command()
@click.pass_context
def ls(ctx: click.Context) -> None:
    """list all tasks"""
    task_manager = ctx_task_manager(ctx)
    table = Table(title="Tasks")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Schedule", style="magenta")
    table.add_column("CPU bound", style="magenta")
    table.add_column("Timeout secs", style="green")
    table.add_column("Priority", style="magenta")
    table.add_column("Description", style="green")
    for name in sorted(task_manager.registry):
        task = task_manager.registry[name]
        table.add_row(
            name,
            str(task.schedule),
            "yes" if task.cpu_bound else "no",
            str(task.timeout_seconds),
            str(task.priority),
            task.short_description,
        )
    console = Console()
    console.print(table)


@click.command()
@click.pass_context
@click.argument("task")
@click.option(
    "--dry-run",
    is_flag=True,
    help="dry run (if the tasks supports it)",
    default=False,
)
def execute(ctx: click.Context, task: str, dry_run: bool) -> None:
    """execute a task"""
    task_manager = ctx_task_manager(ctx)
    run = task_manager.execute_sync(task, dry_run=dry_run)
    console = Console()
    console.print(task_run_table(run))


@click.command("serve", short_help="Start app server.")
@click.option(
    "--host",
    "-h",
    default="0.0.0.0",
    help="The interface to bind to",
    show_default=True,
)
@click.option(
    "--port",
    "-p",
    default=8080,
    help="The port to bind to",
    show_default=True,
)
@click.option(
    "--reload",
    is_flag=True,
    default=False,
    help="Enable auto-reload",
    show_default=True,
)
@click.pass_context
def serve(ctx: click.Context, host: str, port: int, reload: bool) -> None:
    """Run the service."""
    task_manager_app = ctx_task_manager_app(ctx)
    uvicorn.run(
        task_manager_app,
        port=port,
        host=host,
        log_level="info",
        reload=reload,
        log_config=log.config(),
    )


DEFAULT_COMMANDS = (ls, execute, serve)


def task_run_table(task_run: TaskRun) -> Table:
    table = Table(title="Task Run", show_header=False)
    color = "red" if task_run.state.is_failure else "green"
    table.add_column("Name", style="cyan")
    table.add_column("Description", style=color)
    table.add_row("name", task_run.task.name)
    table.add_row("description", task_run.task.description)
    table.add_row("run_id", task_run.id)
    table.add_row("state", task_run.state)
    if task_run.start:
        table.add_row("started", task_run.start.isoformat())
    if task_run.end:
        table.add_row("completed", task_run.end.isoformat())
    table.add_row("duration ms", str(task_run.duration_ms))
    return table
