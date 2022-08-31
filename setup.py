from setuptools import setup

# Metadata goes in pyproject.toml.
# These are here for GitHub's dependency graph.
setup(
    name="nats-py",
    extras_require={
        'nkeys': ['nkeys']
    }
)
