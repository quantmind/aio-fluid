import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.asyncio(loop_scope="module")

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "task_queue_stats.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("task_queue_stats", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # register before exec so dataclass field resolution can find the module
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


stats = _load()


async def test_humanize() -> None:
    assert stats.humanize(48_929_015) == "48.9M"
    assert stats.humanize(2_400_000) == "2.4M"
    assert stats.humanize(117_240) == "117k"
    assert stats.humanize(898) == "898"
    assert stats.humanize(None) == "n/a"


async def test_render_table_contains_rows() -> None:
    rows = [(library, 1000) for library in stats.LIBRARIES]
    table = stats.render_table(rows, today="2026-08-01")
    assert "Downloads / mo" in table
    assert "Last refreshed 2026-08-01" in table
    # every library shows up as a linked cell
    for library in stats.LIBRARIES:
        assert f"[{library.name}](https://pypi.org/project/{library.package}/)" in table


async def test_splice_replaces_only_between_markers() -> None:
    original = f"head\n{stats.START}\nOLD BODY\n{stats.END}\ntail"
    updated = stats.splice(original, "NEW BODY")
    assert "NEW BODY" in updated
    assert "OLD BODY" not in updated
    assert updated.startswith("head")
    assert updated.endswith("tail")
    assert stats.START in updated and stats.END in updated


async def test_splice_requires_markers() -> None:
    with pytest.raises(SystemExit):
        stats.splice("no markers here", "NEW BODY")
