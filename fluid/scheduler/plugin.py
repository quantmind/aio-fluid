import abc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .consumer import TaskManager


class TaskManagerPlugin(abc.ABC):
    """Plugin for a task Manager"""

    @abc.abstractmethod
    def register(self, task_manager: TaskManager) -> None:
        """Register the plugin with the task manager"""
