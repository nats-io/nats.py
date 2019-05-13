from setuptools import setup
from nats.aio.client import __version__

EXTRAS = {
    'nkeys': ['nkeys'],
}

setup(
    name='asyncio-nats-client',
    version=__version__,
    description='NATS client for Python Asyncio',
    long_description='Asyncio based Python client for NATS, a lightweight, high-performance cloud native messaging system',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
        ],
    url='https://github.com/nats-io/nats.py',
    author='Waldemar Quevedo',
    author_email='wally@synadia.com',
    license='Apache 2 License',
    packages=['nats', 'nats.aio', 'nats.protocol'],
    zip_safe=True,
    extras_require=EXTRAS
)
