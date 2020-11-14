# Minimal makefile for Sphinx documentation
#

.PHONY: help clean docs

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


version:		## display software version
	@python setup.py --version


install: 		## install packages in virtualenv
	@./dev/install


lint: 			## run linters
	isort .
	./dev/run-black
	flake8


mypy:			## run mypy
	@mypy metablock


test:			## test with coverage
	@pytest -x --log-cli-level error --cov --cov-report xml --cov-report html


test-lint:		## run linters
	isort . --check
	./dev/run-black --check
	flake8


test-version:		## validate version
	@agilekit git validate --yes-no


bundle:			## build python 3.8 bundle
	@python setup.py sdist bdist_wheel


release-github:		## new tag in github
	@agilekit --config dev/agile.json git release --yes


release-pypi:		## release to pypi and github tag
	@twine upload dist/* --username lsbardel --password $(PYPI_PASSWORD)
