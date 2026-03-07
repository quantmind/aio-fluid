import uvicorn

from examples.tasks import task_app
from fluid.utils import log

if __name__ == "__main__":
    log.config()
    uvicorn.run(task_app())
