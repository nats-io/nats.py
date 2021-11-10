Nkeys and User Credentials (NATS v2)
-------------------------------------

You can also optionally install `nkeys <https://github.com/nats-io/nkeys.py>`_
in order to use the new NATS v2.0 decentralized auth features.
   
::

    pip install nats-py[nkeys]

Usage:

::

    await nats.connect("tls://connect.ngs.global:4222", user_credentials="/path/to/secret.creds")

