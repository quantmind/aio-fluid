"""Execution of cpu bound tasks in a separate process.

A cpu bound task does not run in the consumer process, it is executed by the
`exec` command of the task manager command line client. The command is derived
from the one which started the consumer, the same way a kubernetes job derives
its command from the task consumer deployment, so a task behaves the same
locally and in a cluster.
"""

from __future__ import annotations

import os
import sys
from typing import TYPE_CHECKING, Any, Sequence

from pydantic_core import to_json

from fluid.utils import kernel

from .common import cpu_env, params_dump
from .errors import TaskAbortedError, TaskRunError

if TYPE_CHECKING:  # pragma: no cover
    from .models import TaskRun


class RemoteLog:
    """Stream the output of the task process back to the consumer."""

    def __init__(self, out: Any) -> None:
        self.out = out

    def __call__(self, data: bytes) -> None:
        self.out.write(data.decode("utf-8"))


SERVE_COMMAND = "serve"


def strip_serve(command: Sequence[str]) -> list[str]:
    """Strip the `serve` command from a task manager command line.

    What remains is the entry point itself, ready for the `exec` command to be
    appended to it. `serve` takes options of its own (`--host`, `--port`,
    `--reload`), and they belong to `serve` rather than to the entry point, so
    everything from `serve` onwards is dropped.
    """
    entry_point = list(command)
    if SERVE_COMMAND in entry_point:
        return entry_point[: entry_point.index(SERVE_COMMAND)]
    return entry_point


def exec_args(ctx: TaskRun) -> list[str]:
    """Build the `exec` arguments which run a single task run to completion.

    Params are serialised with secret values revealed because the process
    executing the task re-validates them.
    """
    return [
        "exec",
        ctx.name,
        "--log",
        "--run-id",
        ctx.id,
        "--params",
        to_json(params_dump(ctx.params)).decode(),
    ]


def cpu_bound_command(ctx: TaskRun) -> list[str]:
    """Build the command which executes a cpu bound task.

    It is the command which started this process, with the `serve` command
    removed and the `exec` command appended, therefore the entry point must be
    a task manager command line client.
    """
    return [*strip_serve(sys.orig_argv), *exec_args(ctx)]


async def run_in_subprocess(ctx: TaskRun) -> None:
    """Execute a cpu bound task in a subprocess."""
    env = dict(os.environ)
    env.update(cpu_env())
    env.update(ctx.task.env)
    executable, *args = cpu_bound_command(ctx)
    result = await kernel.run(
        executable,
        *args,
        result_callback=RemoteLog(sys.stdout),
        error_callback=RemoteLog(sys.stderr),
        env=env,
        stream_output=True,
        stream_error=True,
    )
    if reason := await ctx.task_manager.broker.get_task_aborted(ctx.id):
        raise TaskAbortedError(reason)
    if result:
        raise TaskRunError(result)
