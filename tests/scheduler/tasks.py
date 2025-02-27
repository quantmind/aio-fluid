import asyncio
from dataclasses import dataclass
from datetime import datetime

from fluid.scheduler import TaskInfo
from fluid.utils.http_client import HttpxClient


@dataclass
class TaskClient(HttpxClient):
    url: str = "http://test_api"

    async def get_tasks(self) -> list[TaskInfo]:
        data = await self.get(f"{self.url}/tasks")
        return [TaskInfo(**task) for task in data]

    async def get_task(self, task_name: str) -> TaskInfo:
        data = await self.get(f"{self.url}/tasks/{task_name}")
        return TaskInfo(**data)

    async def wait_for_task(
        self,
        task_name: str,
        last_run_end: datetime | None = None,
        timeout: float = 1.0,
    ) -> TaskInfo:
        sleep = min(timeout / 10.0, 0.1)
        async with asyncio.timeout(timeout):
            while True:
                task = await self.get_task(task_name)
                if task.last_run_end != last_run_end:
                    return task
                await asyncio.sleep(sleep)
