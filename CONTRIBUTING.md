# Contributing to NATS.py

Thank you for your interest in contributing to NATS.py!

## Development Setup

This is a [uv workspace](https://docs.astral.sh/uv/concepts/workspaces/) containing multiple packages.

### Prerequisites

1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)
2. [Install nats-server](https://docs.nats.io/running-a-nats-service/introduction/installation) and ensure it's in your PATH: `nats-server -v`

### Installation

```bash
# Clone the repository
git clone https://github.com/nats-io/nats.py
cd nats.py

# Install dependencies
uv sync
```

## Running Tests

```bash
# Run all tests
uv run pytest

# Run tests for specific package
uv run pytest nats/tests
uv run pytest nats-server/tests

# Run tests in parallel
uv run pytest -n auto

# Run with coverage
uv run pytest --cov
```

## Code Quality

### Type Checking

```bash
uv run mypy nats/src
```

### Formatting

```bash
# Format code
uv run yapf -i -r nats/src nats-server/src

# Check formatting
uv run yapf -d -r nats/src nats-server/src
```

### Linting

```bash
# Run ruff
uv run ruff check nats/src nats-server/src

# Run flake8 (for nats-py)
uv run flake8 nats/src/nats/js/

# Run isort
uv run isort nats/src nats-server/src
```

## Updating Documentation

To update the docs, first checkout the `docs` branch under a local copy of the `nats.py` repo:

```sh
git clone https://github.com/nats-io/nats.py
cd nats.py
git clone https://github.com/nats-io/nats.py --branch docs --single-branch docs
cd docs
uv venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows
uv pip install sphinx sphinx_autodoc_typehints myst_parser furo pygments 
make html
# preview the changes:
make serve
```

If you are happy with the changes, make a PR on the docs branch:
```
make publish
git add docs
```

## Pull Request Guidelines

1. Fork the repository and create your branch from `main`
2. Make sure all tests pass
3. Update documentation if needed
4. Follow the existing code style
5. Write clear commit messages
6. Create a pull request with a clear description of the changes

## License

By contributing to NATS.py, you agree that your contributions will be licensed under the Apache License 2.0.
