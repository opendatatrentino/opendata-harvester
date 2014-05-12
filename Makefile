PYTEST = py.test
PYTEST_ARGS = -vvv -rfEsxX --cov=harvester --cov-report=term-missing --pep8 $(PYTEST_EXTRA_ARGS)
PYTEST_EXTRA_ARGS = ./tests

.PHONY: all test tests check

all:
	@echo "Available targets"
	@echo
	@echo "check - Run tests using py.test"

check:
	$(PYTEST) $(PYTEST_ARGS)

test: check

tests: check
