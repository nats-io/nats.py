.. title:: nats.py - Python3 client for NATS

NATS.py 🐍
===================================

Python3 client for the `NATS messaging system <https://nats.io>`_.

.. toctree::
   :maxdepth: 2
   :caption: Documentation

   releases
   modules


Getting Started
-----------------------------------

::

    pip install nats-py


Hello World
-----------------------------------

::

   import nats

   nc = nats.connect("demo.nats.io")
   

Nkeys and User Credentials (NATS v2)
-------------------------------------

You can also optionally install `nkeys <https://github.com/nats-io/nkeys.py>`_
in order to use the new NATS v2.0 decentralized auth features.
   
::

    pip install nats-py[nkeys]

Usage:

::

    await nats.connect("tls://connect.ngs.global:4222", user_credentials="/path/to/secret.creds")



Site
------------------------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


License
==================

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the `LICENSE <https://github.com/nats-io/nats.py/blob/main/LICENSE>`_ file.
