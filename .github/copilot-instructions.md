---
applyTo: "**"
---

# AIO-Fluid

AIO-Fluid is an asynchronous task scheduler and runner built on top of Python's asyncio. It provides a flexible API for defining tasks, scheduling them with cron-like syntax, and running them concurrently with support for task dependencies and priorities.

## Working with this codebase

Before starting any task, read all files under `docs/` to understand the project structure, API, and tutorials.

When adding code make sure to follow the existing code style and patterns. Run `make lint` to ensure your code passes linting and `make test` to run tests.

When adding new features, make sure to add tests for them and update the documentation accordingly.

## Documentation

Located in the `docs/` directory, the documentation is built with mkdocs and can be served locally with:

```bash
mkdocs serve
```

* [Reference](docs/reference/) that describes the API of the library. Add here any details about the API that you think are important for users to know.
* [Tutorials](docs/tutorials/) that provide step-by-step guides on how to use the library for common use cases. Add here any details about the tutorials that you think are important for users to know.

When adding new python examples in the documentation, make sure to add them in the `docs_src/` directory and not directly in `docs/` if the code is longer than 5~6 lines.
This is to make sure the code is properly formatted and has valid syntax.

## Tests

Tests are located in the `tests/` directory and run with:

```bash
make test
```

When adding or modifying Python code:
- Always run `make lint` after to ensure the code passes linting
- Add tests for new functionality
- Mock as little as possible
