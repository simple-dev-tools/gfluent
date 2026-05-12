VERSION = $(shell grep -o "[0-9]\.[0-9]\.[0-9]" gfluent/__init__.py)
PACKAGE_NAME = gfluent

.PHONY: help sync clean clean-build clean-pyc clean-test test test-int coverage docs servedocs release dist install

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

BROWSER := uv run python -c "$$BROWSER_PYSCRIPT"

help:
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_\-]*: *.*## *" | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

sync: ## install/sync deps with uv (creates .venv)
	uv sync

clean: clean-build clean-pyc clean-test ## remove all build, test, coverage and Python artifacts

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -not -path '*/.*' -name '*.egg-info' -exec rm -fr {} +
	find . -not -path '*/.*' -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

test: ## run unit tests only
	uv run pytest -m "not integtest" $(PYTEST_OPTION)

test-int: ## run integration tests
	uv run pytest -m integtest $(PYTEST_OPTION)

coverage: ## check code coverage quickly with the default Python
	uv run coverage run --source gfluent -m pytest -m "not integtest"
	uv run coverage report -m
	uv run coverage html
	$(BROWSER) htmlcov/index.html

docs: ## generate Sphinx HTML documentation, including API docs
	rm -f docs/gfluent.rst
	rm -f docs/modules.rst
	uv run sphinx-apidoc -o docs/ gfluent
	$(MAKE) -C docs clean
	$(MAKE) -C docs html
	$(BROWSER) docs/_build/html/index.html

servedocs: docs ## compile the docs watching for changes
	uv run watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: dist ## package and upload a release
	uv run twine upload dist/*

dist: clean ## build source and wheel package via uv
	uv build
	ls -l dist

install: ## install the package into the uv-managed venv
	uv sync
