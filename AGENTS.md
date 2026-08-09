# AGENTS.md

Guidance for AI coding agents. The same conventions apply to human contributors.

## Using aio-fluid in another project

If you are writing code that uses the library rather than changing it, read the
documentation first, it is published in agent friendly form:

- <https://fluid.quantmind.com/llms.txt>, an index of every page
- <https://fluid.quantmind.com/llms-full.txt>, the whole documentation in one file
- <https://fluid.quantmind.com/recipes/>, the canonical patterns and the common mistakes

The package ships `py.typed`, so a type checker resolves every signature.

## Working in this repository

Set up and check your work with the make targets, run `make help` for the full
list:

```bash
make install   # install everything with uv
make lint      # isort, black, ruff, mypy over fluid, tests and examples
make test      # pytest with coverage, needs redis and postgres
make docs      # build the documentation in strict mode
```

Layout:

- `fluid/` the package, `fluid/scheduler/` is the task queue
- `tests/` the test suite
- `examples/` runnable examples, `examples/docs/` holds the code embedded in the docs
- `docs/` the mkdocs site, `docs/reference/` is generated from docstrings by mkdocstrings

## Conventions

Read the instruction files before making changes, they are the authority:

- [.github/copilot-instructions.md](.github/copilot-instructions.md), overview
- [.github/instructions/code.instructions.md](.github/instructions/code.instructions.md), imports, docstrings, prose style
- [.github/instructions/tests.instructions.md](.github/instructions/tests.instructions.md), test conventions
- [.github/instructions/makefile.instructions.md](.github/instructions/makefile.instructions.md), makefile conventions
- [.github/instructions/release.instructions.md](.github/instructions/release.instructions.md), how releases are cut

The points most often missed:

- All imports go at the top of the file, never inside a function.
- Test functions are always `async def`, even when they do not await.
- Never use em dashes or en dashes as punctuation in prose, code comments or
  commit messages. Use a comma, a colon, parentheses, or two sentences.
- Python examples longer than a few lines belong in `examples/docs/` and are
  embedded in the docs with a snippet include, not pasted into the markdown.
- Cross-reference public classes and functions in the docs with the mkdocstrings
  syntax, `[TaskManager][fluid.scheduler.TaskManager]`. The docs build is strict
  and fails on an unresolved reference, so only link to something rendered under
  `docs/reference/`.

## Before you finish

- `make lint` and `make test` pass.
- New functionality has tests, mocking as little as possible.
- `make docs` passes if you touched `docs/`, `examples/docs/` or any docstring.
