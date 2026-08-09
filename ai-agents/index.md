# Use with AI agents

Coding agents write a lot of the code that uses this library, so the documentation is published in a form they can consume directly, following the [llms.txt](https://llmstxt.org/) convention. This page is about pointing your agent at it.

## What is published

| Resource                                                   | Size            | When to use it                                                                  |
| ---------------------------------------------------------- | --------------- | ------------------------------------------------------------------------------- |
| [recipes](https://fluid.quantmind.com/recipes/index.md)    | ~1.5k tokens    | Always. The canonical patterns and the mistakes that are easy to make           |
| [llms.txt](https://fluid.quantmind.com/llms.txt)           | ~1k tokens      | The index. Every page with a one-line description, to pick the one that matters |
| any page, with `index.md` appended to its URL              | 2k to 8k tokens | The specific topic at hand                                                      |
| [llms-full.txt](https://fluid.quantmind.com/llms-full.txt) | ~94k tokens     | A broad sweep, or one-shot ingestion into a vector store                        |

The package also ships `py.typed`, so a type checker resolves every signature, which is the cheapest context an agent can have.

## Pointing your agent at it

Put this in the instructions file your agent reads, `AGENTS.md`, `CLAUDE.md`, `.cursor/rules` or the equivalent, and adjust the last section to your project:

```markdown
## Background tasks (aio-fluid)

This project uses [aio-fluid](https://fluid.quantmind.com/) for its task queue.

Before writing or changing task code, read
<https://fluid.quantmind.com/recipes/index.md>. It is short and covers the
canonical patterns and the mistakes that are easy to make.

For more detail:
- <https://fluid.quantmind.com/llms.txt> indexes every page, fetch the one you need
- any page is available as markdown by appending `index.md` to its URL, for example
  <https://fluid.quantmind.com/tutorials/task_managers/index.md>
- <https://fluid.quantmind.com/llms-full.txt> is the entire documentation in one
  file, use it only when you need a broad sweep

In this project:
- tasks live in `myapp/tasks/`, registered in `myapp/app.py`
- the entry point is `myapp/__main__.py`, a `TaskManagerCLI`, which is required
  because we have `cpu_bound` tasks
- the API process runs a plain `TaskManager` (it only queues), the worker
  deployment runs the `TaskScheduler`
```

That last section is the part that pays off. The documentation cannot know which manager your processes run or where your tasks live, and those are exactly the things an agent will otherwise guess wrong.

## Prefer the recipes page to the full dump

Feeding `llms-full.txt` into every session is the tempting mistake. It is a sizeable fraction of a context window spent on content that is mostly unrelated to the task at hand, and it crowds out your own code. The cheaper path is the [recipes](https://fluid.quantmind.com/recipes/index.md) page for the patterns, then [llms.txt](https://fluid.quantmind.com/llms.txt) to fetch the single page that covers whatever came up.

## Agents without web access

When the agent cannot fetch URLs, vendor a copy into your repository and point the instructions at the local path:

```bash
curl -o docs/vendor/aio-fluid-recipes.md https://fluid.quantmind.com/recipes/index.md
```

Refresh it when you upgrade the library.

## Contributing to aio-fluid

The above is for applications that use the library. For an agent working on this repository itself, the conventions are in [AGENTS.md](https://github.com/quantmind/aio-fluid/blob/main/AGENTS.md).
