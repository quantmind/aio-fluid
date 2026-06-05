import dotenv
import uvicorn

from fluid.utils import log

dotenv.load_dotenv()

if __name__ == "__main__":
    from examples.tasks import task_app

    log.config()
    uvicorn.run(task_app())
