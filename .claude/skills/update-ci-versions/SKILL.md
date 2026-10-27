---
name: update-ci-versions
description: Update the GitHub Actions versions and the pinned uv version in the CI workflows (.github/workflows). Use when asked to bump, refresh or upgrade CI actions, workflow actions or the uv version used in CI.
---

# Update CI Versions

The workflows in `.github/workflows/` reference actions by version tag and pin
the uv version installed by `astral-sh/setup-uv`. `scripts/ci_versions.py`
moves both to their latest releases.

## Conventions

- Actions are referenced by their **major tag** (`actions/checkout@v7`).
- An action that publishes no major tag for its latest release is referenced by
  its latest **full tag** (`astral-sh/setup-uv@v10.3.2`). Never fall back to an
  older major that still has a floating tag.
- Never pin an action by commit SHA.
- The uv version is the `version:` input of every `astral-sh/setup-uv` step,
  and all of them use the same version.

## Steps

1. Check the branch with `git branch --show-current`.
2. Report what is outdated:
   ```bash
   uv run python scripts/ci_versions.py
   ```
3. For every action whose **major** version changes, read its `action.yml` at
   the new tag and check that every input the workflows pass still exists,
   and look for changed defaults (caching, node runtime):
   ```bash
   curl -fsSL https://raw.githubusercontent.com/<owner>/<repo>/<tag>/action.yml
   ```
   Report any removed or renamed input to the developer before applying.
4. Apply the changes:
   ```bash
   make ci-versions
   ```
5. When the uv version changed, check the lockfile is still accepted by the
   new uv, since CI runs with `UV_LOCKED=1` and fails if uv wants to rewrite it:
   ```bash
   uvx --from uv==<new-version> uv lock --check
   ```
   If it fails, tell the developer: the lock needs regenerating with the new uv
   (`uv lock`), which is their call.
6. Validate the workflow files:
   ```bash
   uv run --no-project --with pyyaml python -c "import yaml,glob; [yaml.safe_load(open(f)) for f in glob.glob('.github/workflows/*.yml')]"
   ```
7. Summarise the changes (action, old ref, new ref, uv version) and stop.
   Do not commit unless the developer asks.

## Notes

- The release workflow sets `enable-cache: false` on setup-uv so release builds
  never restore a cache; keep it when upgrading.
- `uv run python scripts/ci_versions.py --check` exits 1 when anything is
  outdated, for use in automation.
