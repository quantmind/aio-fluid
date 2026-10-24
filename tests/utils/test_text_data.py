import click
import pytest
from pydantic import SecretStr

from fluid.utils.data import compact_dict, reveal_secrets
from fluid.utils.lazy import LazyGroup
from fluid.utils.text import to_bool, to_bytes, to_string

pytestmark = pytest.mark.asyncio(loop_scope="module")


async def test_to_bytes_and_string() -> None:
    assert to_bytes("ciao") == b"ciao"
    assert to_bytes(b"ciao") == b"ciao"
    assert to_string(b"ciao") == "ciao"
    assert to_string("ciao") == "ciao"


async def test_to_bool() -> None:
    assert to_bool(None) is False
    assert to_bool("") is False
    assert to_bool(0) is False
    assert to_bool(False) is False
    assert to_bool(True) is True
    assert to_bool(1) is True
    assert to_bool("Yes") is True
    assert to_bool("t") is True
    assert to_bool("no") is False


async def test_reveal_secrets() -> None:
    value = {
        "password": SecretStr("s3cret"),
        "nested": {"tokens": [SecretStr("a"), ("b", SecretStr("c"))]},
        "plain": 1,
    }
    assert reveal_secrets(value) == {
        "password": "s3cret",
        "nested": {"tokens": ["a", ["b", "c"]]},
        "plain": 1,
    }


async def test_compact_dict() -> None:
    assert compact_dict({"a": None, "b": False, "c": [None]}, d=0) == {
        "b": False,
        "c": [None],
        "d": 0,
    }


async def test_lazy_group_non_command() -> None:
    group = LazyGroup(name="cli", lazy_subcommands={"bad": "fluid.utils.text:to_bool"})
    ctx = click.Context(group)
    assert group.list_commands(ctx) == ["bad"]
    with pytest.raises(ValueError, match="non-command object"):
        group.get_command(ctx, "bad")
