import uuid
from typing import Any

TRUE_VALUES = frozenset(("yes", "true", "t", "1"))

String = bytes | str


def to_bytes(s: String) -> bytes:
    return s.encode("utf-8") if isinstance(s, str) else s


def to_string(s: String) -> str:
    return s.decode("utf-8") if isinstance(s, bytes) else s


def to_bool(v: str | bool | int | None) -> bool:
    return str(v).lower() in TRUE_VALUES if v else False


def create_uid() -> str:
    return uuid.uuid4().hex


def as_uuid(uid: Any) -> str | None:
    if uid:
        if hasattr(uid, "hex"):
            uid = uid.hex
        try:
            return uuid.UUID(uid).hex
        except ValueError:
            return None
    return None


def trim_docstring(docstring: str) -> str:
    """Uniformly trims leading/trailing whitespace from doc-strings"""
    if not docstring or not docstring.strip():
        return ""
    # Convert tabs to spaces and split into lines
    lines = docstring.expandtabs().splitlines()
    indent = min(len(line) - len(line.lstrip()) for line in lines if line.lstrip())
    trimmed = [lines[0].lstrip()] + [line[indent:].rstrip() for line in lines[1:]]
    return "\n".join(trimmed).strip()
