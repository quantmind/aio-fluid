"""Refresh the Python task-queue download table in ``docs/comparison.md``.

The comparison page carries a small table of PyPI download counts so readers can
see, with real data, how ``aio-fluid`` sits in the task-queue landscape. Those
numbers go stale quickly, so this script regenerates them from the public
`pypistats.org <https://pypistats.org>`_ API and rewrites the table in place
(between the ``STATS`` marker comments).

Run it with::

    make stats                            # or:
    uv run python scripts/task_queue_stats.py
    uv run python scripts/task_queue_stats.py --json   # print data, page untouched

The set of libraries and their qualitative columns (async-native, how CPU work
is handled) are curated here; only the download numbers are fetched.
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

API = "https://pypistats.org/api/packages/{package}/recent"
DOCS_PAGE = Path(__file__).resolve().parent.parent / "docs" / "comparison.md"
START = "<!-- STATS:START -->"
END = "<!-- STATS:END -->"


@dataclass(frozen=True)
class Library:
    """A task-queue library and how it handles the two axes that matter here."""

    package: str
    name: str
    async_support: str  # "yes" / "partial" / "no"
    cpu_off_loop: str  # how CPU-bound work is kept off the event loop
    note: str


# Curated landscape. Download counts are fetched; everything else is editorial.
LIBRARIES: tuple[Library, ...] = (
    Library(
        "celery",
        "Celery",
        "partial",
        "separate worker fleet",
        "The incumbent: biggest ecosystem, broker-agnostic.",
    ),
    Library(
        "apscheduler",
        "APScheduler",
        "yes",
        "n/a",
        "A *scheduler*, not a distributed queue, listed for scale.",
    ),
    Library(
        "rq",
        "RQ",
        "no",
        "separate worker",
        "Simple, Redis-only; the common 'lite Celery'.",
    ),
    Library(
        "dramatiq",
        "Dramatiq",
        "no",
        "separate worker",
        "Ergonomic Celery alternative.",
    ),
    Library(
        "huey",
        "Huey",
        "no",
        "separate worker",
        "Lightweight, very few dependencies.",
    ),
    Library(
        "arq",
        "arq",
        "yes",
        "no",
        "Async-native, Redis; assumes tasks never block the loop.",
    ),
    Library(
        "taskiq",
        "taskiq",
        "yes",
        "no",
        "Async-native, pluggable brokers; typed parameters.",
    ),
    Library(
        "saq",
        "SAQ",
        "yes",
        "no",
        "Async-native, Redis; small and fast.",
    ),
    Library(
        "procrastinate",
        "Procrastinate",
        "yes",
        "no",
        "Async-native, Postgres-backed.",
    ),
    Library(
        "aio-fluid",
        "aio-fluid",
        "yes",
        "subprocess / k8s Job",
        "This library: CPU-bound work is a first-class task type.",
    ),
)

_SUPPORT = {"yes": "✅", "partial": "partial", "no": "no"}


def fetch_last_month(package: str, *, timeout: float = 30.0) -> int | None:
    """Return the last-30-days download count for ``package`` (``None`` on error)."""
    request = urllib.request.Request(
        API.format(package=package),
        headers={"User-Agent": "aio-fluid-stats (+https://fluid.quantmind.com)"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
        return int(payload["data"]["last_month"])
    except (urllib.error.URLError, TimeoutError, KeyError, ValueError) as exc:
        print(f"warning: could not fetch {package}: {exc}", file=sys.stderr)
        return None


def humanize(count: int | None) -> str:
    """Render a download count compactly: ``48.9M``, ``117k``, ``898``, ``n/a``."""
    if count is None:
        return "n/a"
    if count >= 1_000_000:
        return f"{count / 1_000_000:.1f}M"
    if count >= 1_000:
        return f"{count / 1_000:.0f}k"
    return str(count)


def render_table(rows: list[tuple[Library, int | None]], *, today: str) -> str:
    """Render the ranked markdown table (excluding the surrounding markers)."""
    lines = [
        "_Downloads = last 30 days on PyPI, via "
        "[pypistats.org](https://pypistats.org). "
        f"Last refreshed {today}. Counts are inflated by CI, mirrors and Docker "
        "builds, read them as orders of magnitude, not user counts._",
        "",
        "| Library | Downloads / mo | Async-native | CPU work off the loop | Notes |",
        "|---|---:|:---:|:---:|---|",
    ]
    for library, count in rows:
        lines.append(
            f"| [{library.name}](https://pypi.org/project/{library.package}/) "
            f"| {humanize(count)} "
            f"| {_SUPPORT[library.async_support]} "
            f"| {library.cpu_off_loop} "
            f"| {library.note} |"
        )
    return "\n".join(lines)


def splice(content: str, table: str) -> str:
    """Replace the text between the ``STATS`` markers with ``table``."""
    if START not in content or END not in content:
        raise SystemExit(f"markers {START} / {END} not found in {DOCS_PAGE}")
    before = content[: content.index(START) + len(START)]
    after = content[content.index(END) :]
    return f"{before}\n{table}\n{after}"


def collect() -> list[tuple[Library, int | None]]:
    """Fetch every library's downloads and rank them, highest first."""
    rows = [(library, fetch_last_month(library.package)) for library in LIBRARIES]
    rows.sort(key=lambda row: (row[1] is None, -(row[1] or 0)))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json",
        action="store_true",
        help="print the collected data as JSON and leave the docs page untouched",
    )
    args = parser.parse_args()

    rows = collect()

    if args.json:
        print(
            json.dumps(
                {library.package: count for library, count in rows},
                indent=2,
            )
        )
        return

    today = datetime.now(timezone.utc).date().isoformat()
    table = render_table(rows, today=today)
    content = DOCS_PAGE.read_text()
    DOCS_PAGE.write_text(splice(content, table))
    print(f"updated {DOCS_PAGE.relative_to(Path.cwd())} ({today})")


if __name__ == "__main__":
    main()
