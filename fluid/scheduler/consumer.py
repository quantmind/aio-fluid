from __future__ import annotations

import asyncio
import logging
from collections import deque
from contextlib import AsyncExitStack
from functools import partial
from types import ModuleType
from typing import Any, Awaitable, Callable, Self

from inflection import underscore
from starlette.datastructures import State
from typing_extensions import Annotated, Doc

from fluid.utils import log
from fluid.utils.dispatcher import AsyncDispatcher, Dispatcher, Event
from fluid.utils.worker import AsyncConsumer, WorkerFunction, Workers

from .broker import TaskBroker, TaskRegistry
from .errors import TaskAbortedError, TaskRunError, UnknownTaskError
from .models import (
    Task,
    TaskManagerConfig,
    TaskPriority,
    TaskRun,
    TaskRunWaiter,
    TaskState,
)

AsyncHandler = Callable[[TaskRun], Awaitable[None]]

logger = log.get_logger(__name__)


class TaskDispatcher(Dispatcher[TaskRun]):
    """The task dispatcher is responsible for dispatching task run messages"""

    def event_type(self, message: TaskRun) -> str:
        return message.state


class AsyncTaskDispatcher(AsyncDispatcher[TaskRun]):

    def event_type(self, message: TaskRun) -> str:
        return message.state


class TaskManager:
    """The task manager is the main class for managing tasks"""

    def __init__(
        self,
        *,
        deps: Any = None,
        config: TaskManagerConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self.deps: Annotated[
            Any,
            Doc(
                """
                Dependencies for the task manager.

                Production applications requires global dependencies to be
                available to all tasks. This can be achieved by setting
                the `deps` attribute of the task manager to an object
                with the required dependencies.

                Each task can cast the dependencies to the required type.
                """
            ),
        ] = (
            deps if deps is not None else State()
        )
        self.config: Annotated[
            TaskManagerConfig, Doc("""Task manager configuration""")
        ] = config or TaskManagerConfig(**kwargs)
        self.dispatcher: Annotated[
            TaskDispatcher,
            Doc(
                """
                A dispatcher of [TaskRun][fluid.scheduler.TaskRun] events.

                Application can register handlers to listen for events
                happening during the lifecycle of a task run.
                """
            ),
        ] = TaskDispatcher()
        self.broker = TaskBroker.from_url(self.config.broker_url)
        self._async_contexts: list[Any] = []
        self._stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        for cm in self._async_contexts:
            await self._stack.enter_async_context(cm)
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        try:
            await self._stack.__aexit__(*exc_info)
        finally:
            await self.broker.close()

    async def on_startup(self) -> None:
        await self.__aenter__()

    async def on_shutdown(self) -> None:
        await self.__aexit__(None, None, None)

    def add_async_context_manager(self, cm: Any) -> None:
        """Add an async context manager to the task manager

        These context managers are entered when the task manager starts
        """
        self._async_contexts.append(cm)

    @property
    def registry(self) -> TaskRegistry:
        """The task registry"""
        return self.broker.registry

    @property
    def type(self) -> str:
        """The type of the task manager"""
        return underscore(self.__class__.__name__)

    def register_task(self, task: Annotated[Task, Doc("Task to register")]) -> None:
        """Register a task with the task manager"""
        self.broker.register_task(task)

    async def execute(
        self,
        task: Annotated[
            str | Task,
            Doc(
                "The task or task name,"
                " if a task name it must be registered with the task manager."
            ),
        ],
        *,
        run_id: Annotated[
            str,
            Doc("Unique ID for the task run. If not provided a new UUID is generated."),
        ] = "",
        priority: Annotated[
            TaskPriority | None, Doc("Override the default task priority if provided")
        ] = None,
        **params: Annotated[
            Any,
            Doc(
                "The optional parameters for the task run. "
                "They must match the task params model"
            ),
        ],
    ) -> TaskRun:
        """Execute a task and wait for it to finish"""
        task_run = self.create_task_run(
            task,
            run_id=run_id,
            priority=priority,
            **params,
        )
        await task_run.execute()
        return task_run

    def execute_sync(
        self,
        task: Annotated[
            str | Task,
            Doc(
                "The task or task name,"
                " if a task name it must be registered with the task manager."
            ),
        ],
        *,
        run_id: Annotated[
            str,
            Doc("Unique ID for the task run. If not provided a new UUID is generated."),
        ] = "",
        priority: Annotated[
            TaskPriority | None, Doc("Override the default task priority if provided")
        ] = None,
        **params: Annotated[
            Any,
            Doc(
                "The optional parameters for the task run. "
                "They must match the task params model"
            ),
        ],
    ) -> TaskRun:
        """Execute a task synchronously

        This method is a blocking method that should be used in a synchronous
        context.
        """
        return asyncio.run(
            self._execute_and_exit(
                task,
                run_id=run_id,
                priority=priority,
                **params,
            )
        )

    async def queue(
        self,
        task: Annotated[
            str | Task,
            Doc(
                "The task or task name,"
                " if a task name it must be registered with the task manager."
            ),
        ],
        *,
        run_id: Annotated[
            str,
            Doc("Unique ID for the task run. If not provided a new UUID is generated."),
        ] = "",
        priority: Annotated[
            TaskPriority | None, Doc("Override the default task priority if provided")
        ] = None,
        **params: Annotated[
            Any,
            Doc(
                "The optional parameters for the task run. "
                "They must match the task params model"
            ),
        ],
    ) -> TaskRun:
        """Queue a task for execution

        This methods fires two events:

        - `init`: when the task run is created
        - `queued`: after the task is queued

        It returns the [TaskRun][fluid.scheduler.TaskRun] object
        """
        task_run = self.create_task_run(
            task,
            run_id=run_id,
            priority=priority,
            **params,
        )
        self.dispatcher.dispatch(task_run)
        task_run.set_state(TaskState.queued)
        await self.broker.queue_task(task_run)
        return task_run

    def create_task_run(
        self,
        task: Annotated[
            str | Task,
            Doc(
                "The task or task name,"
                " if a task name it must be registered with the task manager."
            ),
        ],
        *,
        run_id: Annotated[
            str,
            Doc("Unique ID for the task run. If not provided a new UUID is generated."),
        ] = "",
        priority: Annotated[
            TaskPriority | None, Doc("Override the default task priority if provided")
        ] = None,
        **params: Annotated[
            Any,
            Doc(
                "The optional parameters for the task run. "
                "They must match the task params model"
            ),
        ],
    ) -> TaskRun:
        """Create a [TaskRun][fluid.scheduler.TaskRun] in `init` state"""
        task = self.broker.task_from_registry(task)
        run_id = run_id or self.broker.new_uuid()
        return TaskRun(
            id=run_id,
            task=task,
            priority=priority or task.priority,
            params=task.params_model(**params),
            task_manager=self,
        )

    def register_from_module(
        self,
        module: Annotated[
            ModuleType,
            Doc(
                "Python module with tasks implementations "
                "- can contain any object, only instances of Task are registered"
            ),
        ],
    ) -> None:
        """Register tasks from a python module"""
        for name in dir(module):
            if name.startswith("_"):
                continue
            if isinstance(obj := getattr(module, name), Task):
                self.register_task(obj)

    def register_from_dict(
        self,
        data: Annotated[
            dict[str, Any],
            Doc(
                "Python dictionary with tasks implementations "
                "- can contain any object, only instances of Task are registered"
            ),
        ],
    ) -> None:
        """Register tasks from a python dictionary"""
        for name, obj in data.items():
            if name.startswith("_"):
                continue
            if isinstance(obj, Task):
                self.register_task(obj)

    def register_async_handler(self, event: str, handler: AsyncHandler) -> None:
        """Register an async handler for a given event

        This method is a no op for a TaskManager that is not a worker
        """

    def unregister_async_handler(self, event: Event | str) -> AsyncHandler | None:
        """Unregister an async handler for a given event

        This method is a no op for a TaskManager that is not a worker
        """

    async def _execute_and_exit(self, task: Task | str, **params: Any) -> TaskRun:
        async with self:
            return await self.execute(task, **params)


class TaskConsumer(TaskManager, Workers):
    """The Task Consumer is a Task Manager responsible for consuming tasks
    from a task queue
    """

    def __init__(self, **config: Any) -> None:
        super().__init__(**config)
        Workers.__init__(self)
        self._async_dispatcher_worker = AsyncConsumer(AsyncTaskDispatcher())
        self._task_to_queue: deque[str | Task] = deque()
        self._queue_tasks_worker = WorkerFunction(
            self._queue_task, name="queue-task-worker"
        )
        self.add_workers(self._queue_tasks_worker)
        self.add_workers(self._async_dispatcher_worker)
        for i in range(self.config.max_concurrent_tasks):
            worker_name = f"task-worker-{i+1}"
            self.add_workers(
                WorkerFunction(
                    partial(self._consume_tasks, worker_name), name=worker_name
                )
            )

    def sync_queue(self, task: str | Task) -> None:
        """Queue a task synchronously"""
        self._task_to_queue.appendleft(task)

    async def queue_and_wait(
        self, task: str | Task, *, timeout: int | None = None, **params: Any
    ) -> TaskRun:
        """Queue a task and wait for it to finish"""
        with TaskRunWaiter(self) as waiter:
            task_run = await self.queue(task, **params)
            return await waiter.wait(task_run, timeout=timeout)

    def register_async_handler(self, event: Event | str, handler: AsyncHandler) -> None:
        event = Event.from_string_or_event(event)
        self.dispatcher.register_handler(
            f"{event.type}.async_dispatch",
            self._async_dispatcher_worker.send,
        )
        self._async_dispatcher_worker.dispatcher.register_handler(event, handler)

    def unregister_async_handler(self, event: Event | str) -> AsyncHandler | None:
        return self._async_dispatcher_worker.dispatcher.unregister_handler(event)

    # Internals

    # process tasks from the internal queue
    async def _queue_task(self) -> None:
        try:
            task = self._task_to_queue.pop()
        except IndexError:
            await asyncio.sleep(self.config.sleep)
        else:
            await self.queue(task)
            await asyncio.sleep(0)

    async def _consume_tasks(self, worker_name: str) -> None:
        if not self.config.consume_tasks:
            await asyncio.sleep(self.config.sleep)
            return
        try:
            task_run = await self.broker.get_task_run(self)
            if task_run is None:
                await asyncio.sleep(self.config.sleep)
                return
        except UnknownTaskError as exc:
            logger.error(
                "%s unknown task %s - it looks like it is not "
                "registered with this consumer",
                worker_name,
                exc,
            )
            return
        task_name = task_run.name
        task_info = await self.broker.get_tasks_info(task_name)
        if not task_info[0].enabled:
            task_run.set_state(TaskState.aborted)
        else:
            async with self.broker.lock(f"tasks:{task_name}"):
                if task_run.task.max_concurrency > 0:
                    current_runs = await self.broker.current_task_runs(task_name)
                    if current_runs >= task_run.task.max_concurrency:
                        task_run.set_state(TaskState.rate_limited)
                if task_run.state is not TaskState.rate_limited:
                    await self.broker.add_task_run(task_run)
        # run the task
        if not task_run.is_done:
            try:
                params = task_run.params.model_dump_json()
            except Exception:
                task_run.logger.exception("%s - start - params exception", task_run.id)
            else:
                task_run.logger.info("%s - %s - start", task_run.id, params)
            try:
                await task_run.execute()
            except TaskRunError:
                # no logging as this was a controlled exception
                pass
            except TaskAbortedError as exc:
                task_run.logger.info("%s - %s - aborted - %s", task_run.id, params, exc)
            except asyncio.TimeoutError:
                task_run.logger.error("task run %s - %s - timeout", task_run.id, params)
            except Exception:
                task_run.logger.exception("critical exception while executing")
            await self.broker.remove_task_run(task_run)
        duration_ms = task_run.duration_ms
        if duration_ms is not None:
            await self.broker.update_task(
                task_run.task,
                dict(
                    last_run_end=task_run.end,
                    last_run_duration=task_run.duration,
                    last_run_state=task_run.state,
                ),
            )
            task_run.logger.log(
                logging.WARNING if task_run.is_failure else logging.INFO,
                "end - %s - milliseconds - %s",
                task_run.state,
                duration_ms,
            )


# required by pydantic to avoid `Class not fully defined` error
TaskRun.model_rebuild()
