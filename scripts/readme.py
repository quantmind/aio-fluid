"""Generate ``readme.md`` from ``docs/index.md``.

The home page of the documentation doubles as the project readme, which is also
the long description published on PyPI. The page pulls its code examples in
with pymdownx `snippets <https://facelessuser.github.io/pymdown-extensions/extensions/snippets/>`_
markers (``--8<-- "path"``), so the examples stay executable files that the test
suite can import. Those markers are resolved by mkdocs at build time and mean
nothing to GitHub or PyPI, so this script expands them while copying the page.

Run it with::

    make readme                       # or:
    uv run python scripts/readme.py
    uv run python scripts/readme.py --check   # fail if readme.md is stale
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SOURCE = ROOT / "docs" / "index.md"
TARGET = ROOT / "readme.md"
SNIPPET = re.compile(r'^\s*--8<--\s+"(?P<path>[^"]+)"\s*$')


def expand(text: str) -> str:
    """Replace every snippet marker with the content of the file it points to."""
    lines = []
    for line in text.splitlines():
        if match := SNIPPET.match(line):
            snippet = (ROOT / match.group("path")).resolve()
            lines.append(snippet.read_text().rstrip("\n"))
        else:
            lines.append(line)
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="do not write, exit non-zero if readme.md is out of date",
    )
    args = parser.parse_args()
    content = expand(SOURCE.read_text())
    if args.check:
        current = TARGET.read_text() if TARGET.exists() else ""
        if current != content:
            print(f"{TARGET.name} is out of date, run `make readme`", file=sys.stderr)
            return 1
        return 0
    TARGET.write_text(content)
    return 0


if __name__ == "__main__":
    sys.exit(main())
