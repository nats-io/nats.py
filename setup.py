from setuptools import setup
from nats.aio.client import __version__

setup(
  name='asyncio-nats-client',
  version=__version__,
  description='NATS client for Python Asyncio',
  long_description='Asyncio based Python client for NATS, a lightweight, high-performance cloud native messaging system',
  url='https://github.com/nats-io/asyncio-nats',
  author='Waldemar Quevedo',
  author_email='wally@apcera.com',
  license='MIT License',
  packages=['nats', 'nats.aio', 'nats.protocol'],
  zip_safe=True,
)
