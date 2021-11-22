.. title:: nats.py - Python3 client for NATS

nats.py 
===================================

Python3 client for the `NATS messaging system <https://nats.io>`_.

Getting Started
-----------------------------------

::

    pip install nats-py

You can also optionally install `nkeys <https://github.com/nats-io/nkeys.py>`_
in order to use the `NATS v2.0 decentralized auth features using JWTs <https://docs.nats.io/developing-with-nats/tutorials/jwt>`_.
   
::

    pip install nats-py[nkeys]

NATS Hello World in Python 🐍

::

    import asyncio
    import nats

    async def main():
	# Connect to NATS!
	nc = await nats.connect("demo.nats.io")

	# Receive messages on 'foo'
	sub = await nc.subscribe("foo")

	# Publish a message to 'foo'
	await nc.publish("foo", b'Hello from Python!')

	# Process a message
	msg = await sub.next_msg()
	print("Received:", msg)

	# Close NATS connection
	await nc.close()

    if __name__ == '__main__':
	asyncio.run(main())


.. toctree::
   :maxdepth: 5
   :caption: Documentation

   modules
   releases


Site
------------------------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


License
==================

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the `LICENSE <https://github.com/nats-io/nats.py/blob/main/LICENSE>`_ file.
