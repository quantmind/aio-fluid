import asyncio
import os
from datetime import timedelta
from typing import Optional

from fluid.node import WorkerApplication
from fluid.scheduler import TaskContext, TaskManager, every, task
from fluid.scheduler.cpubound import cpu_task


@task
async def dummy(context: TaskContext) -> float:
    sleep = context.params.get("sleep", 0.1)
    await asyncio.sleep(sleep)
    if context.params.get("error"):
        raise RuntimeError("just an error")
    return sleep


@task(schedule=every(timedelta(seconds=1)))
async def scheduled(context: TaskContext) -> str:
    await asyncio.sleep(0.1)
    return "OK"


@cpu_task
async def cpu_bound(context: TaskContext) -> int:
    await asyncio.sleep(0.1)
    redis = context.task_manager.broker.redis.cli
    await redis.setex(context.run_id, os.getpid(), 10)


def add_task_manager(app: WorkerApplication, manager: TaskManager) -> TaskManager:
    manager.register_task(dummy)
    manager.register_task(scheduled)
    manager.register_task(cpu_bound)
    app.on_startup.append(manager.start_app)
    app.on_shutdown.append(manager.close_app)
    app[manager.type] = manager
    return manager


def task_application(task_manager: Optional[TaskManager] = None) -> WorkerApplication:
    app = WorkerApplication()
    add_task_manager(app, task_manager or TaskManager())
    return app
