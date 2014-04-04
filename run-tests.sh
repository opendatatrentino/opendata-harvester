#!/bin/bash
exec py.test -vvv -rfEsxX --cov=harvester --cov-report=term-missing --pep8 "$@"
