from nats.jetstream.context import Context
from nats.aio.client import Client

def new(client: Client) -> Context:
    return Context(client)
