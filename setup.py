from setuptools import setup

from nats.aio.client import __version__

EXTRAS = {
    'nkeys': ['nkeys'],
}

setup(
    name='nats-py',
    version=__version__,
    description='NATS client for Python',
    long_description='Python client for NATS, a lightweight, high-performance cloud native messaging system',
    classifiers=[
        'Intended Audience :: Developers',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10'
        ],
    url='https://github.com/nats-io/nats.py',
    author='Waldemar Quevedo',
    author_email='wally@synadia.com',
    license='Apache 2 License',
    packages=['nats', 'nats.aio', 'nats.protocol', 'nats.js'],
    package_data={"nats": ["py.typed"]},
    zip_safe=True,
    extras_require=EXTRAS
)
