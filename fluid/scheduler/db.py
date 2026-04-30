from __future__ import annotations

from datetime import datetime
from typing import Any, ClassVar

import sqlalchemy as sa
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.exc import NoResultFound
from typing_extensions import Annotated, Doc

from fluid.db.crud import CrudDB
from fluid.db.pagination import Pagination
from fluid.utils.dispatcher import Event

from .common import is_in_cpu_process
from .consumer import TaskManager
from .endpoints import TaskManagerDep
from .models import TaskPriority, TaskRun, TaskState
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
        if table_name not in db.tables:
            task_meta(db.metadata, table_name=table_name)
        self.table_name = table_name
        self.db = db
        self.tag = tag
        self.skip_db_tag = skip_db_tag

    def register(self, task_manager: TaskManager) -> None:
        task_manager.state.task_db_plugin = self

        if is_in_cpu_process():
            return

        task_manager.register_async_handler(
            Event(TaskState.queued, self.tag),
            self._handle_update,
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
        task_manager.register_async_handler(
            Event(TaskState.interrupted, self.tag),
            self._handle_update,
        )

    async def get_history(
        self,
        q: Annotated[
            HistoryQuery, Doc("Query parameters for fetching task run history")
        ],
    ) -> TaskRunHistoryPage:
        """Get task run history based on the provided query parameters."""
        table = self.db.tables[self.table_name]
        pagination = Pagination.create(
            "queued",
            filters=q.filters(),
            limit=q.limit,
            cursor=q.cursor,
            desc=True,
        )
        rows, cursor = await pagination.execute(self.db, table)
        return TaskRunHistoryPage(
            data=[_row_to_task_run(row) for row in rows],
            cursor=cursor,
        )

    async def get_run(self, run_id: str) -> TaskRunHistory:
        """Get a specific task run by its ID."""
        table = self.db.tables[self.table_name]
        result = await self.db.db_select(table, {"id": run_id})
        rows = result.fetchall()
        if not rows:
            raise NoResultFound(f"Task run with id {run_id} not found")
        return _row_to_task_run(rows[0])

    async def _handle_update(self, task_run: TaskRun) -> None:
        if self.skip_db_tag in task_run.task.tags:
            return
        await self.db.db_upsert(
            self.db.tables[self.table_name],
            dict(id=task_run.id),
            dict(
                state=task_run.state,
                name=task_run.name,
                priority=task_run.priority,
                queued=task_run.queued,
                start=task_run.start,
                end=task_run.end,
                params=task_run.params.model_dump(mode="json"),
            ),
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


class TaskRunHistory(BaseModel):
    """A model representing the history of a task run,
    including its parameters and timing information."""  # noqa: E501

    id: str = Field(..., description="The unique ID of the task run")
    task: str = Field(..., description="The name of the task")
    priority: TaskPriority = Field(..., description="The priority of the task")
    state: TaskState = Field(..., description="The state of the task")
    queued: datetime = Field(..., description="The time the task was queued")
    start: datetime | None = Field(None, description="The start time of the task")
    end: datetime | None = Field(None, description="The end time of the task")
    params: dict[str, Any] = Field(..., description="The parameters of the task run")


def get_db_plugin(task_manager: TaskManagerDep) -> TaskDbPlugin:
    """Retrieve the registered [TaskDbPlugin][fluid.scheduler.db.TaskDbPlugin].

    Can be used as a FastAPI dependency in route handlers, or called directly
    from within a task by passing `context.task_manager`.
    """
    return task_manager.state.task_db_plugin


def with_task_history_router(
    app: Annotated[
        FastAPI,
        Doc("FastAPI app instance."),
    ],
    prefix: str = "/task-history",
) -> FastAPI:
    """Add task history endpoints to a FastAPI app."""
    app.include_router(router, prefix=prefix)
    return app


router = APIRouter()


TaskDbPluginDep = Annotated[TaskDbPlugin, Depends(get_db_plugin)]


class TaskRunHistoryPage(BaseModel):
    """A paginated response containing a list of task run history records.

    Returned by [TaskDbPlugin.get_history][fluid.scheduler.db.TaskDbPlugin.get_history]
    and the `GET /task-history` endpoint.
    """

    data: list[TaskRunHistory] = Field(..., description="The task run history records")
    cursor: str = Field(..., description="Pagination cursor to fetch the next page")


class HistoryQuery(BaseModel):
    """Query parameters for fetching task run history."""

    task: Annotated[
        str | None,
        Query(description="Filter by task name"),
    ] = None
    start: Annotated[
        datetime | None,
        Query(description="Filter runs queued at or after this time"),
    ] = None
    end: Annotated[
        datetime | None,
        Query(description="Filter runs queued at or before this time"),
    ] = None
    state: Annotated[
        TaskState | None,
        Query(description="Filter by task state"),
    ] = None
    limit: Annotated[
        int | None,
        Query(description="Maximum number of results to return", ge=1),
    ] = None
    cursor: Annotated[
        str,
        Query(description="Pagination cursor from a previous response"),
    ] = ""

    _filter_map: ClassVar[dict[str, str]] = {
        "task": "name",
        "start": "queued:ge",
        "end": "queued:le",
    }

    def filters(self) -> dict:
        return {
            self._filter_map.get(k, k): v
            for k, v in self.model_dump(
                exclude_none=True, exclude={"limit", "cursor"}
            ).items()
        }


@router.get(
    "",
    response_model=TaskRunHistoryPage,
    summary="Task run history",
)
async def get_history(
    db_plugin: TaskDbPluginDep,
    q: Annotated[HistoryQuery, Depends()],
) -> TaskRunHistoryPage:
    return await db_plugin.get_history(q)


@router.get(
    "/{run_id}",
    response_model=TaskRunHistory,
    summary="Get a task run",
)
async def get_run(db_plugin: TaskDbPluginDep, run_id: str) -> TaskRunHistory:
    try:
        return await db_plugin.get_run(run_id)
    except NoResultFound:
        raise HTTPException(status_code=404, detail="Task run not found") from None


def _row_to_task_run(row: Any) -> TaskRunHistory:
    return TaskRunHistory(
        id=row.id,
        task=row.name,
        priority=row.priority,
        state=row.state,
        queued=row.queued,
        start=row.start,
        end=row.end,
        params=row.params,
    )
