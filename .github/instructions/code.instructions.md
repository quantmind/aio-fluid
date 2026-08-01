# Code Style Instructions

## Imports

- All imports must be at the top of the file, never inside functions or methods.

## Docstrings and documentation

- Do not add mkdocstrings cross-reference links (`[Name][dotted.path]`) in
  docstrings unless the target is rendered on a page under `docs/reference/`.
  The docs build runs in strict mode and fails on unresolved references.
- After changing docstrings or anything under `docs/`, run `make docs` to
  verify the strict build passes.

## Prose style

- Never use em dashes (`—`), en dashes (`–`) as sentence punctuation, or `--`
  in prose. This applies to all Markdown, docstrings, the README, release
  notes, code comments, and commit/PR text. Use a comma, colon, semicolon,
  or parentheses, or split the sentence into two. Ordinary hyphens in
  compound words (`CPU-bound`, `async-native`) are correct and stay.
