import uvicorn
from fluid.scheduler import TaskScheduler
from fluid.tasks_api import task_manager_routes
from fastapi import FastAPI
from . import tasks


def main():
    task_manager = TaskScheduler()
    task_manager.register_from_module(tasks)
    app = FastAPI()
    task_manager_routes(app, task_manager)
    uvicorn.run(app)


if __name__ == "__main__":
    main()
