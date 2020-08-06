import asyncio
from typing import Any, Coroutine

import click
from openapi.exc import ImproperlyConfigured


class RunnerError(ImproperlyConfigured):
    pass


def sync_run(coro: Coroutine) -> Any:
    try:
        return asyncio.get_event_loop().run_until_complete(coro)
    except ImproperlyConfigured as e:
        click.secho(str(e), err=True, color="red")
        raise click.Abort from None
