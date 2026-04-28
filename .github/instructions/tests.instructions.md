# Test Instructions

## Async

All test functions must be `async def`, even when they don't use `await`.
This is required by `pytestmark = pytest.mark.asyncio(loop_scope="module")`.
