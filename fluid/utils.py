import asyncio
import uuid
from time import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Union, cast

import async_timeout
from inflection import underscore
from multidict import MultiDict
from openapi.spec.server import server_urls


def as_uuid(uid: Any) -> Optional[str]:
    if uid:
        if hasattr(uid, "hex"):
            uid = uid.hex
        try:
            return uuid.UUID(uid).hex
        except ValueError:
            return None


def add_api_url(
    request, data: Union[List, Dict], path: Optional[str] = None, key: str = "id"
) -> Union[List, Dict]:
    """Add api_url field to data"""
    datas = data
    path = path if path is not None else request.path
    if not isinstance(data, list):
        datas = [cast(dict, data)]
    paths = (f"{path}/{d[key]}" for d in datas if key in d)
    for d, url in zip(datas, server_urls(request, paths)):
        d["api_url"] = url
    return data


def milliseconds() -> int:
    return int(time() * 1000)


def microseconds() -> int:
    return int(time() * 1000000)


def merge_dict(d1: Dict, d2: Dict) -> Dict:
    d1.update(d2)
    return d1


async def close_task(task: asyncio.Future, callback: Callable[[], None] = None) -> None:
    """Close an asyncio task cleanly"""
    if task and not task.done():
        try:
            task.cancel()
            await task
        except asyncio.CancelledError:
            pass
    if callback:
        await callback()
    return None


def dot_name(name: str) -> str:
    return ".".join(underscore(name).split("_"))


def query_dict(**data) -> MultiDict:
    return MultiDict(_query_dict(data))


def with_ops(data: Dict, key: str = "") -> Dict:
    data = {":".join(name.split("__")): value for name, value in data.items()}
    return MultiDict(multi(data)) if key == "params" else data


def multi(data: Dict) -> Iterator:
    for key, value in data.items():
        if isinstance(value, Sequence) and not isinstance(value, str):
            for inner_value in value:
                yield key, inner_value
        else:
            yield key, value


async def wait_for(assertion: Callable[[], bool], timeout=1) -> None:
    async with async_timeout.timeout(timeout):
        while True:
            if assertion():
                return
            await asyncio.sleep(0)
    assertion()


def _query_dict(data: Dict):
    for key, value in data.items():
        if isinstance(value, Sequence) and not isinstance(value, str):
            for d in value:
                yield key, d
        else:
            yield key, value
