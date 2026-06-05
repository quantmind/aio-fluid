from __future__ import annotations

import abc
from enum import Enum
from typing import TYPE_CHECKING

from fastapi import FastAPI
from typing_extensions import Annotated, Doc

if TYPE_CHECKING:
    from .consumer import TaskManager


class TaskManagerPlugin(abc.ABC):
    """Plugin for a task Manager"""

    @abc.abstractmethod
    def register(self, task_manager: TaskManager) -> None:
        """Register the plugin with the task manager"""

    def register_routes(  # noqa: B027
        self,
        app: Annotated[
            FastAPI,
            Doc("FastAPI app instance."),
        ],
        prefix: Annotated[
            str,
            Doc("The URL prefix for the routes."),
        ] = "/tasks",
        tags: Annotated[
            list[str | Enum] | None,
            Doc("The tags for the routes."),
        ] = None,
    ) -> None:
        """Register routes with the FastAPI app"""
