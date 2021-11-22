Modules
===================================

.. autofunction:: nats.connect

Asyncio Client
-----------------------------------

Asyncio based client for NATS.

.. autofunction:: nats.aio.client.Client.connect
.. autofunction:: nats.aio.client.Client.publish
.. autofunction:: nats.aio.client.Client.subscribe
.. autofunction:: nats.aio.client.Client.flush
.. autofunction:: nats.aio.client.Client.close
.. autofunction:: nats.aio.client.Client.drain
.. autofunction:: nats.aio.client.Client.new_inbox
                 
Subscription
************************************

.. autoclass:: nats.aio.subscription.Subscription
    :members:

Msg
************************************

.. autoclass:: nats.aio.msg.Msg
    :members:

Connection Properties
************************************

.. autoproperty:: nats.aio.client.Client.servers
.. autoproperty:: nats.aio.client.Client.discovered_servers
.. autoproperty:: nats.aio.client.Client.max_payload
.. autoproperty:: nats.aio.client.Client.connected_url
.. autoproperty:: nats.aio.client.Client.client_id
.. autoproperty:: nats.aio.client.Client.pending_data_size

Connection State
************************************

.. autoproperty:: nats.aio.client.Client.is_connected
.. autoproperty:: nats.aio.client.Client.is_closed
.. autoproperty:: nats.aio.client.Client.is_connecting
.. autoproperty:: nats.aio.client.Client.is_reconnecting
.. autoproperty:: nats.aio.client.Client.is_draining
.. autoproperty:: nats.aio.client.Client.is_draining_pubs
.. autoproperty:: nats.aio.client.Client.last_error

JetStream
-----------------------------------

.. autofunction:: nats.aio.client.Client.jetstream

.. autofunction:: nats.js.client.JetStream.publish

.. autofunction:: nats.js.client.JetStream.subscribe

.. autofunction:: nats.js.client.JetStream.pull_subscribe

Manager
***********************************

.. automodule:: nats.js.manager
    :members:

KeyValue
***********************************

.. autoclass:: nats.js.kv.KeyValue
    :members:

API
***********************************

.. automodule:: nats.js.api
    :members:


NUID
-----------------------------------

.. automodule:: nats.nuid
    :members:


Errors
-----------------------------------

.. automodule:: nats.errors
    :members:

.. automodule:: nats.js.errors
    :members:

Deprecated Errors
************************************

Catching the following errors will be removed eventually, please use the recommended alternative error instead.

.. automodule:: nats.aio.errors
    :members:
