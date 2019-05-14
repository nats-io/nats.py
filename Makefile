REPO_OWNER=nats-io
PROJECT_NAME=nats.py
SOURCE_CODE=nats


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
	yapf -i --recursive tests


test:
	yapf --recursive --diff $(SOURCE_CODE)
	yapf --recursive --diff tests
	coverage erase
	py.test
	coverage report


ci: deps
	pipenv run yapf --recursive --diff $(SOURCE_CODE)
	pipenv run yapf --recursive --diff tests
	pipenv run coverage erase
	pipenv run py.test
	pipenv run coverage report
