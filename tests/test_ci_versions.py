import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

pytestmark = pytest.mark.asyncio(loop_scope="module")

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "ci_versions.py"


def _load() -> ModuleType:
    spec = importlib.util.spec_from_file_location("ci_versions", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # register before exec so dataclass field resolution can find the module
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


ci = _load()

WORKFLOW = """\
jobs:
  build:
    steps:
      - uses: actions/checkout@v6
        with:
          fetch-depth: 0
      - name: install uv
        uses: astral-sh/setup-uv@v10.3.1
        with:
          version: "0.13.5"
      - name: other
        uses: some/action@v1
        with:
          version: "1.0.0"
"""


async def test_action_ref_prefers_major_tag() -> None:
    tags = ["v6", "v6.0.0", "v7", "v7.0.0", "v7.0.1", "v8.0.0-beta.1"]
    assert ci.action_ref(tags) == "v7"


async def test_action_ref_full_tag_without_major() -> None:
    tags = ["v7", "v7.1.0", "v10.3.1", "v10.3.2"]
    assert ci.action_ref(tags) == "v10.3.2"


async def test_action_ref_compares_numerically() -> None:
    assert ci.action_ref(["v9.0.0", "v10.0.0"]) == "v10.0.0"


async def test_action_ref_without_releases() -> None:
    assert ci.action_ref(["main", "latest"]) is None


async def test_latest_uv_release() -> None:
    assert ci.latest_release(["0.9.9", "0.13.5", "0.13.6", "0.14.0rc1"]) == "0.13.6"


async def test_repo_of_sub_path_action() -> None:
    assert ci.repo_of("github/codeql-action/init") == "github/codeql-action"


async def test_scan_and_apply(tmp_path: Path) -> None:
    path = tmp_path / "build.yml"
    path.write_text(WORKFLOW)
    refs = {
        "actions/checkout": "v7",
        "astral-sh/setup-uv": "v10.3.2",
        "some/action": "v1",
    }
    changes = ci.scan([path], refs, "0.13.6")
    assert [(c.what, c.old, c.new) for c in changes] == [
        ("actions/checkout", "v6", "v7"),
        ("astral-sh/setup-uv", "v10.3.1", "v10.3.2"),
        ("uv", "0.13.5", "0.13.6"),
    ]
    ci.apply(changes)
    text = path.read_text()
    assert "actions/checkout@v7" in text
    assert "astral-sh/setup-uv@v10.3.2" in text
    assert 'version: "0.13.6"' in text
    # a version input of another action is left alone
    assert 'version: "1.0.0"' in text
    assert ci.scan([path], refs, "0.13.6") == []
