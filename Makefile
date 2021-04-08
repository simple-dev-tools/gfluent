PYTEST_OPTION = --log-cli-format="%(asctime)s %(levelname)s %(message)s" --log-cli-date-format="%Y-%m-%d %H:%M:%S" --log-cli-level="INFO"


.PHONY: help clean docs


help:
	@cat $(MAKEFILE_LIST) | grep -e "^[a-zA-Z_\-]*: *.*## *" | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

test-ut: ## run unit test
	pytest $(PYTEST_OPTION) tests/test_gcs.py
	pytest $(PYTEST_OPTION) tests/test_bq.py

test-sit: ## run sit test
	pytest $(PYTEST_OPTION) tests/test_gcs_integration.py
	pytest $(PYTEST_OPTION) tests/test_bq_integration.py

docs: ## generate API document
	make -C docs html


clean: ## delete build temp files
	rm -rf .pytest_cache
	find . | grep -E "(__pycache__|\.pyc|\.pyo$\)" | xargs rm -rf
	find . | grep -E "(\.ipynb_checkpoints$\)" | xargs rm -rf
