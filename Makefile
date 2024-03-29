.PHONY: help clean install lint mypy test test-lint publish

help:
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/##//'

clean:			## remove python cache files
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*.pyc' -delete
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .mypy_cache
	rm -rf .coverage


install: 		## install dev packages via poetry
	@./.dev/install


lint: 			## run linters
	poetry run ./.dev/lint fix


test:			## test with coverage
	@poetry run \
		pytest -x --log-cli-level error \
		-m "not flaky" \
		--cov --cov-report xml --cov-report html


test-lint:		## run test linters
	poetry run ./dev/lint


test-version:		## check version compatibility
	@./dev/test-version


publish:		## release to pypi and github tag
	@poetry publish --build -u lsbardel -p $(PYPI_PASSWORD)


outdated:		## Show outdated packages
	poetry show -o -a



.PHONY: example
example:		## run task scheduler example
	@poetry run python -m examples.all_features
