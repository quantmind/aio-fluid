.PHONY: help
help:
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

.PHONY: clean
clean:			## remove python cache files
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*.pyc' -delete
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage

.PHONY: docs
docs:			## build documentation
	cp docs/index.md readme.md
	@uv run mkdocs build

.PHONY: docs-publish
docs-publish:		## publish the book to github pages
	uv run mkdocs gh-deploy

.PHONY: docs-serve
docs-serve:		## serve documentation
	@uv run mkdocs serve --livereload -w fluid -w docs -w docs_src

.PHONY: example
example:		## run task scheduler example
	@APP_NAME=examples uv run python -m examples.main serve

.PHONY: install
install: 		## install all packages via uv
	@./.dev/install

.PHONY: lint
lint: 			## run linters
	uv run ./.dev/lint fix

.PHONY: lint-test
lint-test:		## run test linters
	uv run ./.dev/lint

.PHONY: outdated
outdated:		## Show outdated packages
	uv tree --outdated

.PHONY: publish
publish:		## release to pypi and github tag
	@uv build && uv publish --token $(PYPI_TOKEN)

.PHONY: readme
readme:			## generate readme.md
	cp docs/index.md readme.md

.PHONY: test
test:			## test with coverage
	@uv run \
		pytest -x --log-cli-level error \
		-m "not flaky" \
		--cov --cov-report xml --cov-report html

.PHONY: test-version
test-version:		## check version compatibility
	@./.dev/test-version

.PHONY: upgrade
upgrade: 		## upgrade all packages via uv
	uv lock --upgrade
