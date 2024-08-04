
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

.PHONY: install
install: 		## install all packages via poetry
	@./.dev/install

.PHONY: lint
lint: 			## run linters
	poetry run ./.dev/lint fix

.PHONY: lint-test
lint-test:		## run test linters
	poetry run ./.dev/lint

.PHONY: test
test:			## test with coverage
	@poetry run \
		pytest -x --log-cli-level error \
		-m "not flaky" \
		--cov --cov-report xml --cov-report html

.PHONY: test-version
test-version:		## check version compatibility
	@./dev/test-version


.PHONY: publish
publish:		## release to pypi and github tag
	@poetry publish --build -u lsbardel -p $(PYPI_PASSWORD)

.PHONY: outdated
outdated:		## Show outdated packages
	poetry show -o -a


.PHONY: example
example:		## run task scheduler example
	@APP_NAME=examples poetry run python -m examples.main serve


.PHONY: docs
docs:			## build documentation
	@poetry run mkdocs build

.PHONY: docs-publish
docs-publish:		## publish the book to github pages
	git checkout gh-pages
    git pull origin gh-pages
    git checkout -
	poetry run mkdocs gh-deploy

.PHONY: docs-serve
docs-serve:		## serve documentation
	@poetry run mkdocs serve


.PHONY: readme
readme:			## generate readme.md
	cp docs/index.md readme.md
