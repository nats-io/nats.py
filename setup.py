from setuptools import setup

# Metadata goes in pyproject.toml.
# These are here for GitHub's dependency graph and help with setuptools support in some environments.
setup(
    name="nats-py",
    version="2.10.0",
    license="Apache 2 License",
    extras_require={
        "nkeys": ["nkeys"],
        "aiohttp": ["aiohttp"],
        "fast_parse": ["fast-mail-parser"],
    },
    packages=["nats", "nats.aio", "nats.micro", "nats.protocol", "nats.js"],
    package_data={"nats": ["py.typed"]},
    zip_safe=True,
)
