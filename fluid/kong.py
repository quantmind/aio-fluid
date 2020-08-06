from aiohttp.web import Application
from kong.client import Kong


def setup(app: Application) -> None:
    # kong client
    app["kong"] = Kong(request_kwargs=dict(ssl=False))
    app.on_shutdown.append(close_kong)


async def close_kong(app: Application) -> None:
    await app["kong"].close()
