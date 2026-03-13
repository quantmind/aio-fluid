import sqlalchemy as sa
from typing_extensions import Annotated, Doc

from fluid.db.crud import CrudDB
from fluid.utils.dispatcher import Event

from .consumer import TaskManager
from .models import TaskRun, TaskState
from .plugin import TaskManagerPlugin


class TaskDbPlugin(TaskManagerPlugin):
    """A plugin to store task runs in a database.

    This plugin listens to task state changes and updates the database accordingly.
    It requires a CrudDB instance to perform database operations and allows
    customization of the table name and event tags.

    You can use the `skip_db` tag to prevent database operations for specific tasks.

    It can be used if the `db` extra is installed, and requires a compatible
    database backend supported by CrudDB.
    """

    def __init__(
        self,
        db: CrudDB,
        *,
        table_name: Annotated[
            str,
            Doc("The name of the table to store task runs"),
        ] = "fluid_tasks",
        tag: Annotated[
            str,
            Doc("The tag for the plugin event registration"),
        ] = "db",
        skip_db_tag: Annotated[
            str,
            Doc("The tag to skip database operations"),
        ] = "skip_db",
    ) -> None:
        task_meta(db.metadata, table_name=table_name)
        self.table_name = table_name
        self.db = db
        self.tag = tag
        self.skip_db_tag = skip_db_tag

    def register(self, task_manager: TaskManager) -> None:
        task_manager.register_async_handler(
            Event(TaskState.queued, self.tag),
            self._handle_queued,
        )
        task_manager.register_async_handler(
            Event(TaskState.running, self.tag),
            self._handle_update,
        )
        task_manager.register_async_handler(
            Event(TaskState.success, self.tag),
            self._handle_update,
        )
        task_manager.register_async_handler(
            Event(TaskState.failure, self.tag),
            self._handle_update,
        )
        task_manager.register_async_handler(
            Event(TaskState.aborted, self.tag),
            self._handle_update,
        )
        task_manager.register_async_handler(
            Event(TaskState.rate_limited, self.tag),
            self._handle_update,
        )

    async def _handle_queued(self, task_run: TaskRun) -> None:
        if self.skip_db_tag in task_run.task.tags:
            return
        await self.db.db_insert(
            self.db.tables[self.table_name],
            {
                "id": task_run.id,
                "name": task_run.name,
                "priority": task_run.priority,
                "state": task_run.state,
                "queued": task_run.queued,
                "params": task_run.params.model_dump(),
            },
        )

    async def _handle_update(self, task_run: TaskRun) -> None:
        if self.skip_db_tag in task_run.task.tags:
            return
        await self.db.db_update(
            self.db.tables[self.table_name],
            dict(id=task_run.id),
            dict(state=task_run.state, start=task_run.start, end=task_run.end),
        )


def task_meta(meta: sa.MetaData, table_name: str = "tasks") -> None:
    """Add task runs related"""
    sa.Table(
        table_name,
        meta,
        sa.Column(
            "id",
            sa.String(32),
            primary_key=True,
        ),
        sa.Column(
            "name",
            sa.String(64),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "priority",
            sa.String(64),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "state",
            sa.Enum(TaskState),
            nullable=False,
            index=True,
        ),
        sa.Column("queued", sa.DateTime(timezone=True), nullable=False),
        sa.Column("start", sa.DateTime(timezone=True)),
        sa.Column("end", sa.DateTime(timezone=True)),
        sa.Column("params", sa.JSON),
    )
