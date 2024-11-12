REPO_OWNER=nats-io
PROJECT_NAME=nats.py
SOURCE_CODE=nats
TEST_CODE=tests


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
	yapf -i --recursive $(SOURCE_CODE)
	yapf -i --recursive $(TEST_CODE)


lint:
	yapf --recursive --diff $(SOURCE_CODE)
	yapf --recursive --diff $(TEST_CODE)
	mypy
	flake8 ./nats/js/


test:
	pytest


ci: deps
	# pipenv run yapf --recursive --diff $(SOURCE_CODE)
	# pipenv run yapf --recursive --diff tests
	# pipenv run mypy
	pipenv run flake8 --ignore="W391, W503, W504" ./nats/js/
	pipenv run pytest -x -vv -s --continue-on-collection-errors

watch:
	while true; do pipenv run pytest -v -s -x; sleep 10; done
