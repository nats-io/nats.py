REPO_OWNER=nats-io
PROJECT_NAME=nats.py
SOURCE_CODE=nats/src/nats


help:
	@cat $(MAKEFILE_LIST) | \
	grep -E '^[a-zA-Z_-]+:.*?##' | \
	sed "s/local-//" | \
	sort | \
	awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'


clean:
	find . -name "*.py[co]" -delete
	find . -name "__pycache__" -type d -delete


deps:
	uv sync


format:
	uv run ruff format $(SOURCE_CODE) nats/tests


test:
	uv run ruff format --check $(SOURCE_CODE) nats/tests
	uv run ruff check $(SOURCE_CODE)
	uv run codespell
	uv run ty check
	uv run pytest


ci: deps
	uv run ruff check $(SOURCE_CODE)
	uv run pytest -x -vv -s --continue-on-collection-errors

watch:
	while true; do uv run pytest -v -s -x; sleep 10; done
