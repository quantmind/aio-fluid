from importlib import import_module
from typing import Any

import click


class LazyGroup(click.Group):
    """A click Group that can lazily load subcommands

    This class extends the click.Group class to allow for subcommands to be
    lazily loaded from a module path.

    It is useful when you have a large number of subcommands that you don't
    want to load until they are actually needed.

    Available with the `cli` extra dependencies.
    """

    def __init__(
        self,
        *,
        lazy_subcommands: dict[str, str] | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.lazy_subcommands = lazy_subcommands or {}

    def list_commands(self, ctx: click.Context) -> list[str]:
        commands = super().list_commands(ctx)
        commands.extend(self.lazy_subcommands)
        return sorted(commands)

    def get_command(self, ctx: click.Context, cmd_name: str) -> click.Command | None:
        if cmd_name in self.lazy_subcommands:
            return self._lazy_load(cmd_name)
        return super().get_command(ctx, cmd_name)

    def _lazy_load(self, cmd_name: str) -> click.Command:
        # lazily loading a command, first get the module name and attribute name
        import_path = self.lazy_subcommands[cmd_name]
        modname, cmd_object_name = import_path.rsplit(":", 1)
        # do the import
        mod = import_module(modname)
        # get the Command object from that module
        cmd_object = getattr(mod, cmd_object_name)
        # check the result to make debugging easier
        if not isinstance(cmd_object, click.Command):
            raise ValueError(
                f"Lazy loading of {import_path} failed by returning "
                "a non-command object"
            )
        return cmd_object
