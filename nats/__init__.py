# Copyright 2015 Apcera Inc. All rights reserved.

import asyncio
from .aio.client import Client as NATS

@asyncio.coroutine
def connect(**options):
    nc = NATS()
    yield from nc.connect(**options)
    return nc
