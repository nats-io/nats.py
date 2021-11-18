.. title:: nats.py - Python3 client for NATS

nats.py 
===================================

Python3 client for the `NATS messaging system <https://nats.io>`_.

Getting Started
-----------------------------------

::

    pip install nats-py

You can also optionally install `nkeys <https://github.com/nats-io/nkeys.py>`_
in order to use the new NATS v2.0 decentralized auth features.
   
::

    pip install nats-py[nkeys]

::

   import nats

   # Connect to NATS!
   nc = nats.connect("demo.nats.io")

   # Receive messages on 'foo'
   sub = await nc.subscribe("foo")

   # Publish a message to 'foo'
   await nc.publish("foo", b'Hello from Python! 🐍🐍🐍')

   # Process a message
   msg = await sub.next_msg()
   print("Received:", msg)

   # Close NATS connection
   await nc.close()



.. toctree::
   :maxdepth: 4
   :caption: Documentation

   modules
   guides
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
