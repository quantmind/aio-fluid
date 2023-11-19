import aiohttp_cors
from aiohttp.web import Application


def setup(app: Application):
    # Configure default CORS settings.
    cors = aiohttp_cors.setup(
        app,
        defaults={
            "*": aiohttp_cors.ResourceOptions(
                allow_credentials=True,
                allow_methods="*",
                expose_headers="*",
                allow_headers="*",
            )
        },
    )

    # Configure CORS on all routes.
    for route in list(app.router.routes()):
        try:
            cors.add(route, webview=True)
        except ValueError:
            pass
