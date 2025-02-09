import uvicorn

from examples.tasks import task_scheduler
from fluid.scheduler.endpoints import setup_fastapi
from fluid.utils import log

if __name__ == "__main__":
    log.config()
    app = setup_fastapi(task_scheduler())
    uvicorn.run(app)
