import asyncio
import json

import pytest

from fluid.tools_aiohttp.http import HttpResponseError

from .app import AppClient


async def test_status(restcli: AppClient) -> None:
    data = await restcli.get("/status")
    assert data["ok"] is True


async def test_404(restcli: AppClient) -> None:
    with pytest.raises(HttpResponseError) as e:
        await restcli.get("/xyz")
    assert e.value.status == 404
    assert e.value.data["response_status"] == 404
    assert str(e.value)


async def test_ws(restcli: AppClient) -> None:
    waiter = asyncio.Future()

    def on_text_message(text):
        waiter.set_result(json.loads(text))

    ws = await restcli.stream.connect_and_listen(
        "/stream", on_text_message=on_text_message
    )
    assert ws.ws_url == "/stream"
    assert ws.ws_component == restcli.stream
    await ws.write_json(dict(id="abc", method="echo", payload={"message": "Hi"}))
    data = await waiter
    assert data["response"] == {"message": "Hi"}
