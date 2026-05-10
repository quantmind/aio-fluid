# Release Instructions

Releases are driven by `v*` git tags. Pushing a tag triggers
`.github/workflows/release.yml`, which runs the test suite, publishes the
package to PyPI (`make publish`), then extracts the matching `## vX.Y.Z`
section from `docs/release-notes.md` and publishes it as the GitHub Release
body (re-runs update the existing release instead of failing).

## Cutting a release

1. Bump `version` in `pyproject.toml`. That is the single source of truth —
   `fluid.__version__` is read from the installed package metadata, so there
   is nothing else to edit.
2. Add a `## vX.Y.Z` section at the top of `docs/release-notes.md` describing
   the changes. The release workflow fails if this section is missing. The
   section is posted verbatim as the GitHub Release body, so:
   - use absolute URLs (e.g. `https://fluid.quantmind.com/reference/...`) —
     mkdocstrings `[Name][path]` cross-references do not render outside the
     docs site;
   - do not put backticks (code spans) inside link text — write
     `[task decorator](url)`, not `` [`@task`](url) ``.
3. Commit and push to `main`; let the `build` workflow pass.
4. Run `make release` — it reads the version from `pyproject.toml`, asks for
   confirmation, then creates and pushes the `vX.Y.Z` tag. The `release`
   workflow takes it from there.

Do not publish to PyPI manually; let the tagged workflow do it.
