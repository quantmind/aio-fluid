"""Update the GitHub Actions and the uv version used by the CI workflows.

Every ``uses: owner/repo@ref`` in ``.github/workflows/*.yml`` is moved to the
latest release of the action, referenced by its major tag (``@v7``). Some
actions only publish full tags (astral-sh/setup-uv from v8 on); those are
referenced by the latest full tag instead (``@v10.3.2``). Commit SHA pins are
never used.

The uv version installed by ``astral-sh/setup-uv`` (the ``version:`` input) is
moved to the latest uv release.

Tags are read with ``git ls-remote``, so no GitHub token is needed.

Run it with::

    make ci-versions                              # or:
    uv run python scripts/ci_versions.py          # report what would change
    uv run python scripts/ci_versions.py --write  # apply the changes
    uv run python scripts/ci_versions.py --check  # fail if anything is outdated
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS = ROOT / ".github" / "workflows"
UV_REPO = "astral-sh/uv"
SETUP_UV = "astral-sh/setup-uv"
USES = re.compile(r"^(?P<prefix>\s*-?\s*uses:\s*)(?P<action>[\w.-]+/[\w./-]+)@(?P<ref>\S+)")
UV_VERSION = re.compile(r'^(?P<prefix>\s*version:\s*)"(?P<version>[^"]+)"')
RELEASE = re.compile(r"^v?(\d+)\.(\d+)\.(\d+)$")
MAJOR = re.compile(r"^v(\d+)$")


@dataclass
class Change:
    path: Path
    line: int
    what: str
    old: str
    new: str


def repo_of(action: str) -> str:
    """The repository of an action, ``owner/repo/sub/path`` -> ``owner/repo``"""
    return "/".join(action.split("/")[:2])


def list_tags(repo: str) -> list[str]:
    output = subprocess.run(
        ["git", "ls-remote", "--tags", f"https://github.com/{repo}.git"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    tags = set()
    for line in output.splitlines():
        ref = line.split()[1].removeprefix("refs/tags/").removesuffix("^{}")
        tags.add(ref)
    return sorted(tags)


def latest_release(tags: list[str]) -> str | None:
    """The highest ``X.Y.Z`` tag, with or without a ``v``, pre-releases excluded"""
    releases = [(tuple(map(int, m.groups())), t) for t in tags if (m := RELEASE.match(t))]
    return max(releases)[1] if releases else None


def action_ref(tags: list[str]) -> str | None:
    """The ref to use: the major tag of the latest release when the action
    publishes one, the latest full tag otherwise"""
    latest = latest_release(tags)
    if latest is None:
        return None
    major = f"v{RELEASE.match(latest).group(1)}"  # type: ignore[union-attr]
    return major if major in tags else latest


def scan(paths: list[Path], refs: dict[str, str | None], uv: str | None) -> list[Change]:
    """Changes needed to move the workflows to the given refs and uv version"""
    changes = []
    for path in paths:
        in_setup_uv = False
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if m := USES.match(line):
                action, ref = m["action"], m["ref"]
                in_setup_uv = repo_of(action) == SETUP_UV
                new = refs.get(repo_of(action))
                if new and new != ref:
                    changes.append(Change(path, number, action, ref, new))
            elif in_setup_uv and uv and (m := UV_VERSION.match(line)):
                if m["version"] != uv:
                    changes.append(Change(path, number, "uv", m["version"], uv))
            elif line.strip().startswith("- "):
                in_setup_uv = False
    return changes


def apply(changes: list[Change]) -> None:
    by_path: dict[Path, list[Change]] = {}
    for change in changes:
        by_path.setdefault(change.path, []).append(change)
    for path, path_changes in by_path.items():
        lines = path.read_text().splitlines(keepends=True)
        for change in path_changes:
            line = lines[change.line - 1]
            if change.what == "uv":
                lines[change.line - 1] = line.replace(
                    f'"{change.old}"', f'"{change.new}"', 1
                )
            else:
                lines[change.line - 1] = line.replace(
                    f"{change.what}@{change.old}", f"{change.what}@{change.new}", 1
                )
        path.write_text("".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--write", action="store_true", help="apply the changes")
    mode.add_argument(
        "--check", action="store_true", help="exit 1 when anything is outdated"
    )
    args = parser.parse_args()

    paths = sorted(WORKFLOWS.glob("*.y*ml"))
    repos = {
        repo_of(m["action"])
        for path in paths
        for line in path.read_text().splitlines()
        if (m := USES.match(line))
    }
    refs = {repo: action_ref(list_tags(repo)) for repo in sorted(repos)}
    uv = latest_release(list_tags(UV_REPO))
    changes = scan(paths, refs, uv)

    if not changes:
        print("workflows are up to date")
        return 0
    for c in changes:
        print(f"{c.path.relative_to(ROOT)}:{c.line}  {c.what}  {c.old} -> {c.new}")
    if args.write:
        apply(changes)
        print(f"updated {len(changes)} references")
    return 1 if args.check else 0


if __name__ == "__main__":
    sys.exit(main())
