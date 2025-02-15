REPO_OWNER=nats-io
PROJECT_NAME=nats.py

SOURCE_CODE=nats
TEST_CODE=tests

.PHONY: help clean deps format lint test ci watch

help:
	@cat $(MAKEFILE_LIST) | \
	grep -E '^[a-zA-Z_-]+:.*?##' | \
	sed "s/local-//" | \
	sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean:
	find . -name "*.py[co]" -delete

deps:
	pip install pipenv --upgrade
	pipenv install --dev

format:
	yapf -i --recursive $(SOURCE_CODE) $(TEST_CODE)

lint:
	yapf --recursive --diff $(SOURCE_CODE) $(TEST_CODE)
	mypy $(SOURCE_CODE)
	flake8 $(SOURCE_CODE)

test:
	pytest

ci: deps
	pipenv run flake8 --ignore="W391, W503, W504" ./nats/js/
	pipenv run pytest -x -vv -s --continue-on-collection-errors

watch:
	while true; do pipenv run pytest -v -s -x; sleep 10; done
