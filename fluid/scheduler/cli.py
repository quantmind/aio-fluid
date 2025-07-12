from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any, Callable

import click
import uvicorn
from fastapi import FastAPI
from pydanclick import from_pydantic
from pydantic import BaseModel
from rich.console import Console
from rich.table import Table
from uvicorn.importer import import_from_string

from fluid.utils import log as log_
from fluid.utils.lazy import LazyGroup

from .common import is_in_cpu_process
from .errors import UnknownTaskError

if TYPE_CHECKING:
    from .consumer import TaskManager
    from .models import TaskRun


TaskManagerApp = FastAPI | Callable[..., Any] | str


class TaskManagerCLI(LazyGroup):
    """CLI for TaskManager

    This class provides a CLI for a TaskManager Application.

    It requires to install the `cli` extra dependencies.
    """

    def __init__(
        self,
        task_manager_app: TaskManagerApp,
        **kwargs: Any,
    ):
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


class ExecuteTasks(click.Group):
    def list_commands(self, ctx: click.Context) -> list[str]:
        task_manager = ctx_task_manager(ctx)
        return sorted(task_manager.registry)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        task_manager = ctx_task_manager(ctx)
        task = task_manager.registry.get(cmd_name)
        if task is None:
            raise click.ClickException(f"Task {cmd_name} not found")

        @click.command(cmd_name, help=task.short_description)
        @click.option("--log", is_flag=True, help="Show logs")
        @click.option("--run-id", help="run id")
        @click.option("--params", help="optional parameters as JSON string")
        @from_pydantic(task.params_model)
        def execute_task(log: bool, run_id: str, params: str, **extra: Any) -> None:
            if log:
                log_.config()
            kwargs = json.loads(params or "{}")
            for value in extra.values():
                if isinstance(value, BaseModel):
                    kwargs.update(value.model_dump())
            if run_id:
                kwargs.update(run_id=run_id)
            run = task_manager.execute_sync(cmd_name, **kwargs)
            if is_in_cpu_process():
                return
            console = Console()
            console.print(task_run_table(run))

        return execute_task


@click.command()
@click.pass_context
def ls(ctx: click.Context) -> None:
    """List all tasks with their schedules"""
    task_manager = ctx_task_manager(ctx)
    table = asyncio.run(tasks_table(task_manager))
    console = Console()
    console.print(table)


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
    """Run the service"""
    task_manager_app = ctx_task_manager_app(ctx)
    uvicorn.run(
        task_manager_app,
        port=port,
        host=host,
        log_level="info",
        reload=reload,
        log_config=log_.config(),
    )


@click.command()
@click.argument("task")
@click.option(
    "--disable",
    is_flag=True,
    default=False,
    help="Disable the task",
    show_default=True,
)
@click.pass_context
def enable(ctx: click.Context, task: str, disable: bool) -> None:
    """Enable or disable a task"""
    task_manager = ctx_task_manager(ctx)
    asyncio.run(enable_task(task_manager, task, not disable))


execute = ExecuteTasks(name="exec", help="Execute a registered task")

DEFAULT_COMMANDS = (ls, execute, serve, enable)


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


async def enable_task(task_manager: TaskManager, task: str, enable: bool) -> None:
    try:
        await task_manager.broker.enable_task(task, enable=enable)
    except UnknownTaskError as e:
        raise click.ClickException(f"Task {task} not found") from e


async def tasks_table(task_manager: TaskManager) -> Table:
    task_info = await task_manager.broker.get_tasks_info()
    dynamic = {t.name: t for t in task_info}
    table = Table(title="Tasks")
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Enabled")
    table.add_column("Schedule", style="magenta")
    table.add_column("CPU bound", style="magenta")
    table.add_column("Timeout secs", style="green")
    table.add_column("Priority", style="magenta")
    table.add_column("Description", style="green")
    for name in sorted(task_manager.registry):
        task = task_manager.registry[name]
        table.add_row(
            name,
            ":white_check_mark:" if dynamic[name].enabled else "[red]:x:",
            str(task.schedule),
            ":white_check_mark:" if task.cpu_bound else "[red]:x:",
            str(task.timeout_seconds),
            str(task.priority),
            task.short_description,
        )
    return table
