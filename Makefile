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


install: 		## install packages in virtualenv
	@./dev/install


lint: 			## run linters
	poetry run ./dev/lint


mypy:			## run mypy
	@poetry run mypy fluid


test:			## test with coverage
	@poetry run \
		pytest -x --log-cli-level error \
		-m "not flaky" \
		--cov --cov-report xml --cov-report html


test-lint:		## run linters
	poetry run ./dev/lint --check


test-version:		## check version compatibility
	@./dev/test-version


publish:		## release to pypi and github tag
	@poetry publish --build -u lsbardel -p $(PYPI_PASSWORD)


outdated:		## Show outdated packages
	poetry show -o
