import asyncio
import json
import logging
import signal
import aiohttp

from aiohttp import web
from datetime import datetime
from nats.aio.client import Client as NATS

__version___ = "0.1.0"

class Component():

    def __init__(self,
                 name="aiohttp-nats-example",
                 uri="nats://127.0.0.1:4222",
                 loop=asyncio.get_event_loop(),
                 logger=logging.getLogger(),
                 nats_options={},
                 ):
        # Default NATS Options
        self.name = name
        self.version = __version___
        self.nc = NATS()
        self.loop = loop
        self.nats_uri = uri
        default_nats_options = {
            "name": self.name,
            "io_loop": self.loop,
            "servers": [self.nats_uri],
            
            # NATS handlers
            "error_cb": self.on_error,
            "closed_cb": self.on_close,
            "reconnected_cb": self.on_reconnect,
            "disconnected_cb": self.on_disconnect,
            }
        self.nats_options = {**default_nats_options, **nats_options}
        
        logger.setLevel(logging.DEBUG)
        self.logger = logger

    async def handle_work(self, request):
        self.logger.debug("Received request: {}".format(request))
        try:
            data = await request.json()
            self.logger.debug("Payload: {}".format(data))
        except ValueError:
            # Bad Request
            web.web_response.Response.status = 400
            return web.web_response.Response.status

        # Work handler notifies of events via NATS
        self.logger.debug("Received request: {}".format(request))

        try:
            await self.nc.publish("events", json.dumps(data).encode())
        except Exception as e:
            self.logger.error("Error: {}".format(e))

        return web.Response(text='{"status": "success"}')

    async def on_error(self, e):
        self.logger.warning("Error: {}".format(e))

    async def on_reconnect(self):
        self.logger.warning("Reconnected to NATS at nats://{}".format(self.nc.connected_url.netloc))

    async def on_disconnect(self):
        self.logger.warning("Disconnected from NATS")

    async def on_close(self):
        self.logger.warning("Closed connection to NATS")

    async def signal_handler(self):
        if self.nc.is_connected:
            await self.nc.close()
            self.loop.stop()

    async def start(self):
        self.logger.info("Starting {name} v{version}".format(name=self.name, version=self.version))

        # Setup NATS client
        self.logger.info("Connecting to NATS server at '{}'".format(self.nats_uri))

        await self.nc.connect(**self.nats_options)
        self.logger.info("Connected to NATS")

        # Signal handler
        for sig in ('SIGINT', 'SIGTERM'):
            self.loop.add_signal_handler(
                getattr(signal, sig),
                lambda: asyncio.ensure_future(self.signal_handler()))

        # Server
        app = web.Application()
        runner = web.AppRunner(app)

        # Routes
        app.router.add_post('/work', self.handle_work)

        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)

        self.logger.info("Server listening at '0.0.0.0:8080'")
        await site.start()

class Requestor():
    def __init__(self,
                 uri="http://127.0.0.1:8080",
                 payload={"hello":"world"},
                 max_requests=100,
                 ):
        self.uri = uri
        self.payload = payload
        self.max_requests = max_requests

    async def send_requests(self):
        # Start aiohttp connection sending requests
        for i in range(0, self.max_requests):
            response = await self.send_request(self.payload)

    async def send_request(self, payload):
        async with aiohttp.ClientSession() as session:
            async with session.post(self.uri, json=payload) as response:
                result = await response.text()
                return result

if __name__ == '__main__':
    logging.basicConfig(format='[%(process)s] %(asctime)s.%(msecs)03d [%(levelname)7s] %(message)s', datefmt='%Y/%m/%d %I:%M:%S')
    loop = asyncio.get_event_loop()

    # Start component with defaults
    component = Component(loop=loop)

    # Can customize NATS connection via nats_options
    # component = Component(loop=loop, nats_options={"servers":["nats://127.0.0.1:4223"]})
    loop.run_until_complete(component.start())

    futures = []
    for i in range(0, 10):
        requestor = Requestor(uri="http://127.0.0.1:8080/work", payload={"n": i})
        future = loop.create_task(requestor.send_requests())
        futures.append(future)

    loop.run_until_complete(asyncio.wait(futures))
    loop.run_forever()
