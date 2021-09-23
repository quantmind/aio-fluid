from fluid import kernel
from tests.scripts import long_line


async def test_env_variable() -> None:
    result = kernel.CollectBytes()
    await kernel.run("env", result_callback=result, env=dict(KERNEL_TEST_ME="yes"))
    assert result.data == b"KERNEL_TEST_ME=yes\n"


async def test_long_line() -> None:
    result = kernel.CollectBytes()
    code = await kernel.run(
        "python",
        "-m",
        "tests.scripts.long_line",
        result_callback=result,
        stream_output=True,
        stream_error=True,
    )
    assert code == 0
    assert result.data
    lines = [text for text in result.data.decode("utf-8").split("\n") if text]
    assert len(lines) == 1
    assert len(lines[0]) == long_line.length


async def test_long_lines() -> None:
    result = kernel.CollectBytes()
    code = await kernel.run(
        "python",
        "-m",
        "tests.scripts.long_line",
        "3",
        result_callback=result,
        stream_output=True,
        stream_error=True,
    )
    assert code == 0
    assert result.data
    lines = [text for text in result.data.decode("utf-8").split("\n") if text]
    assert len(lines) == 3
    for text in lines:
        assert len(text) == long_line.length
