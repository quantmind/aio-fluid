from fluid import kernel


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
