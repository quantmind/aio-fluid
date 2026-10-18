import json
from datetime import datetime
from decimal import Decimal
from enum import IntEnum
from typing import Any, Literal, NewType

import click
from click.testing import CliRunner
from pydantic import BaseModel, EmailStr, Field, SecretStr

from fluid.scheduler.cli_params import JSON, ModelOptions

UserId = NewType("UserId", str)


class Level(IntEnum):
    LOW = 1
    HIGH = 2


class Inner(BaseModel):
    x: int = 0


class Params(BaseModel):
    email: EmailStr | None = None
    when: datetime | None = None
    amount: Decimal = Decimal("1.5")
    secret: SecretStr | None = None
    count: int = Field(default=3, description="How many")
    ratio: float = 0.5
    flag: bool = False
    level: Level = Level.LOW
    mode: Literal["a", "b"] = "a"
    tags: list[str] = Field(default_factory=list)
    inner: Inner | None = None


class Required(BaseModel):
    name: str


class Collections(BaseModel):
    labels: set[str] = Field(default_factory=set)
    sizes: tuple[int, ...] = ()
    extra: dict[str, int] = Field(default_factory=dict)
    items: list[Inner] = Field(default_factory=list)
    key: int | str | None = None
    either: Inner | int | None = None
    pair: tuple[int, str] | None = None
    anything: Any = None
    user: UserId | None = None
    default_inner: Inner = Inner(x=2)


class Aliased(BaseModel):
    user_name: str = Field(default="", alias="userName")


class Reserved(BaseModel):
    run_id: str = ""
    log: bool = False


class RequiredReserved(BaseModel):
    run_id: str


def command_for(model: type[BaseModel]) -> click.Command:
    """A command printing the params it would pass to the model as JSON"""
    options = ModelOptions(model)

    @options
    @click.command()
    @click.option("--params", type=JSON)
    def cmd(params: dict | None, **extra: Any) -> None:
        ctx = click.get_current_context()
        kwargs = options.collect(ctx, extra)
        kwargs.update(params or {})
        options.check_required(ctx, kwargs)
        click.echo(model(**kwargs).model_dump_json())
        click.echo(json.dumps(sorted(kwargs)))

    return cmd


def invoke(model: type[BaseModel], *args: str) -> click.testing.Result:
    return CliRunner().invoke(command_for(model), list(args))


async def test_plain_strings():
    result = invoke(
        Params,
        "--email",
        "luca@quantmind.com",
        "--when",
        "2026-01-01T10:00:00",
        "--amount",
        "2.25",
        "--secret",
        "hush",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.splitlines()[0])
    assert data["email"] == "luca@quantmind.com"
    assert data["when"] == "2026-01-01T10:00:00"
    assert data["amount"] == "2.25"
    assert data["secret"] == "**********"


async def test_invalid_email():
    result = invoke(Params, "--email", "not-an-email")
    assert result.exit_code == 1
    assert "email" in str(result.exception)


async def test_numbers():
    result = invoke(Params, "--count", "7", "--ratio", "0.25")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.splitlines()[0])
    assert data["count"] == 7
    assert data["ratio"] == 0.25
    result = invoke(Params, "--count", "abc")
    assert result.exit_code == 2
    assert "Invalid value for '--count'" in result.output


async def test_only_given_options_are_collected():
    result = invoke(Params)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[1]) == []
    result = invoke(Params, "--no-flag")
    assert json.loads(result.output.splitlines()[1]) == ["flag"]


async def test_flag():
    result = invoke(Params, "--flag")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[0])["flag"] is True


async def test_choices():
    result = invoke(Params, "--level", "2", "--mode", "b")
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.splitlines()[0])
    assert data["level"] == Level.HIGH
    assert data["mode"] == "b"
    result = invoke(Params, "--mode", "c")
    assert result.exit_code == 2


async def test_list_by_repeating():
    result = invoke(Params, "--tags", "a", "--tags", "b")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[0])["tags"] == ["a", "b"]


async def test_nested_model_as_json():
    result = invoke(Params, "--inner", '{"x": 4}')
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[0])["inner"] == {"x": 4}
    result = invoke(Params, "--inner", "{x: 4}")
    assert result.exit_code == 2
    assert "is not valid JSON" in result.output


async def test_params_override_options():
    result = invoke(Params, "--count", "7", "--params", '{"count": 9}')
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[0])["count"] == 9


async def test_required():
    result = invoke(Required)
    assert result.exit_code == 2
    assert "Missing option '--name'" in result.output
    result = invoke(Required, "--params", '{"name": "foo"}')
    assert result.exit_code == 0, result.output
    result = invoke(Required, "--name", "foo")
    assert result.exit_code == 0, result.output


async def test_help_shows_defaults():
    result = invoke(Params, "--help")
    assert result.exit_code == 0
    assert "How many" in result.output
    assert "[default: 3]" in result.output
    assert "--flag / --no-flag" in result.output


async def test_set_and_tuple_by_repeating():
    result = invoke(
        Collections, "--labels", "a", "--labels", "a", "--sizes", "1", "--sizes", "2"
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.splitlines()[0])
    assert data["labels"] == ["a"]
    assert data["sizes"] == [1, 2]
    result = invoke(Collections, "--sizes", "x")
    assert result.exit_code == 2


async def test_dict_and_list_of_models_as_json():
    result = invoke(
        Collections, "--extra", '{"a": 1}', "--items", '[{"x": 1}, {"x": 2}]'
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.splitlines()[0])
    assert data["extra"] == {"a": 1}
    assert data["items"] == [{"x": 1}, {"x": 2}]


async def test_union_of_plain_values():
    result = invoke(Collections, "--key", "hello")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[0])["key"] == "hello"
    result = invoke(Collections, "--key", "5")
    assert result.exit_code == 0, result.output


async def test_alias():
    result = invoke(Aliased, "--user-name", "luca")
    assert result.exit_code == 0, result.output
    assert json.loads(result.output.splitlines()[0])["user_name"] == "luca"
    assert json.loads(result.output.splitlines()[1]) == ["userName"]


async def test_reserved_names_only_in_params():
    options = ModelOptions(Reserved)
    assert options.options == []
    result = invoke(Reserved, "--params", '{"run_id": "abc", "log": true}')
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.splitlines()[0])
    assert data == {"run_id": "abc", "log": True}


async def test_structured_unions_and_tuples_as_json():
    result = invoke(
        Collections,
        "--either",
        '{"x": 3}',
        "--pair",
        '[1, "a"]',
        "--anything",
        "hi",
        "--user",
        "u1",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output.splitlines()[0])
    assert data["either"] == {"x": 3}
    assert data["pair"] == [1, "a"]
    assert data["anything"] == "hi"
    assert data["user"] == "u1"


async def test_model_default_in_help():
    result = invoke(Collections, "--help")
    assert result.exit_code == 0
    assert '[default: {"x":2}]' in result.output


async def test_required_reserved_name():
    result = invoke(RequiredReserved)
    assert result.exit_code == 2
    assert "Missing parameter 'run_id', pass it with --params" in result.output
    result = invoke(RequiredReserved, "--params", '{"run_id": "abc"}')
    assert result.exit_code == 0, result.output
