PYTEST = py.test
PYTEST_ARGS = -vvv -rfEsxX --cov=harvester --cov-report=term-missing --pep8 $(PYTEST_EXTRA_ARGS)
PYTEST_EXTRA_ARGS = ./tests

.PHONY: all test

all:
	@echo "Available targets"
	@echo
	@echo "test - Run tests using py.test"

test:
	$(PYTEST) $(PYTEST_ARGS)
