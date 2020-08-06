from typing import Any, Coroutine, Dict, List, Union

from aiohttp.web import Response

String = Union[bytes, str]


def to_bytes(s: String) -> bytes:
    return s.encode("utf-8") if isinstance(s, str) else s


def to_string(s: String) -> str:
    return s.decode("utf-8") if isinstance(s, bytes) else s


AsyncResponse = Coroutine[Any, Any, Response]

StrCoroutine = Coroutine[Any, Any, str]

ListCoroutine = Coroutine[Any, Any, List]

DictCoroutine = Coroutine[Any, Any, Dict]

AnyCoroutine = Coroutine[Any, Any, Any]

JsonType = Union[bool, str, float, int, Dict, List]

StrDict = Dict[str, Any]
