from fluid.utils.http_client import AioHttpClient


async def test_aiohttp_client():
    async with AioHttpClient() as client:
        response = await client.get("https://httpbin.org/get", callback=True)
        if response.status_code >= 502:
            return
        assert response.status_code == 200
        data = await response.json()
        assert data["url"] == "https://httpbin.org/get"
        assert response.headers["Content-Type"] == "application/json"
        assert response.method == "GET"
        assert response.url == "https://httpbin.org/get"
