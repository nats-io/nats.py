import asyncio
import json
import logging
import signal
import aiohttp
import platform

from aiohttp import web
from nats import NATS

__version___ = "0.2.0"

# It is very likely that the demo server will see traffic from clients other than yours.
# To avoid this, start your own locally and modify the example to use it.
# nats_server = "nats://127.0.0.1:4222"
nats_server = "nats://demo.nats.io:4222"


class Component():
    def __init__(
        self,
        name="aiohttp-nats-example",
        uri=nats_server,
        loop=asyncio.get_event_loop(),
        logger=logging.getLogger(),
        nats_options={},
        notify_subject="events",
    ):
        # Default NATS Options
        self.name = name
        self.version = __version___
        self.nc = NATS()
        self.loop = loop
        self.nats_uri = uri
        self.notify_subject = notify_subject
        logger.setLevel(logging.DEBUG)
        self.logger = logger
        default_nats_options = {
            "name": self.name,
            "servers": [self.nats_uri],
            # NATS handlers
            "error_cb": self.on_error,
            "closed_cb": self.on_close,
            "reconnected_cb": self.on_reconnect,
            "disconnected_cb": self.on_disconnect,
        }
        self.nats_options = {**default_nats_options, **nats_options}

    async def handle_work(self, request):
        self.logger.debug(f"Received request: {request}")
        try:
            data = await request.json()
            self.logger.debug(f"Payload: {data}")
        except ValueError:
            # Bad Request
            web.web_response.Response.status = 400
            return web.web_response.Response.status

        # Work handler notifies of events via NATS
        self.logger.debug(f"Received request: {request}")

        try:
            await self.nc.publish(
                self.notify_subject,
                json.dumps(data).encode()
            )
        except Exception as e:
            self.logger.error(f"Error: {e}")

        return web.Response(text='{"status": "success"}')

    async def on_error(self, e):
        self.logger.warning(f"Error: {e}")

    async def on_reconnect(self):
        self.logger.warning(
            f"Reconnected to NATS at nats://{self.nc.connected_url.netloc}"
        )

    async def on_disconnect(self):
        self.logger.warning("Disconnected from NATS")

    async def on_close(self):
        self.logger.warning("Closed connection to NATS")

    async def signal_handler(self):
        if self.nc.is_connected:
            await self.nc.close()
            self.loop.stop()

    async def start(self):
        self.logger.info(f"Starting {self.name} v{self.version}")

        # Setup NATS client
        self.logger.info(f"Connecting to NATS server at '{self.nats_uri}'")

        await self.nc.connect(**self.nats_options)
        self.logger.info("Connected to NATS")

        # Signal handler
        if platform.system() == "Linux":
            for sig in ('SIGINT', 'SIGTERM'):
                self.loop.add_signal_handler(
                    getattr(signal, sig),
                    lambda: asyncio.ensure_future(self.signal_handler())
                )

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
    def __init__(
        self,
        uri="http://127.0.0.1:8080",
        payload={"hello": "world"},
        max_requests=100,
        logger=logging.getLogger("requestor"),
    ):
        self.uri = uri
        self.payload = payload
        self.max_requests = max_requests
        logger.setLevel(logging.DEBUG)
        self.logger = logger

    async def send_requests(self):
        # Start aiohttp connection sending requests
        async with aiohttp.ClientSession() as session:
            for i in range(0, self.max_requests):
                payload = self.payload
                payload["seq"] = i
                response = await self.send_request(session, payload)
                self.logger.debug(f"Response: {response}")
                await asyncio.sleep(0.2)

    async def send_request(self, session, payload):
        async with session.post(self.uri, json=payload) as response:
            result = await response.text()
            return result


class Subscriber():
    def __init__(
        self,
        name="nats-subscriber",
        uri=nats_server,
        logger=logging.getLogger("subscriber"),
        nats_options={},
        notify_subject="events",
    ):
        # Default NATS Options
        self.name = name
        self.version = __version___
        self.nc = NATS()
        self.nats_uri = uri
        self.notify_subject = notify_subject
        logger.setLevel(logging.DEBUG)
        self.logger = logger
        default_nats_options = {
            "name": self.name,
            "servers": [self.nats_uri],
        }
        self.nats_options = {**default_nats_options, **nats_options}

    async def events_handler(self, msg):
        self.logger.info(f"NATS Event: {msg.data.decode()}")

    async def start(self):
        await self.nc.connect(**self.nats_options)
        await self.nc.subscribe(self.notify_subject, cb=self.events_handler)


if __name__ == '__main__':
    logging.basicConfig(
        format=
        '[%(process)s] %(asctime)s.%(msecs)03d - %(name)14s - [%(levelname)7s] - %(message)s',
        datefmt='%Y/%m/%d %I:%M:%S'
    )
    loop = asyncio.get_event_loop()

    # Start component with defaults
    component = Component(loop=loop)

    # Can customize NATS connection via nats_options
    # component = Component(loop=loop, nats_options={"servers":["nats://127.0.0.1:4223"]})
    loop.run_until_complete(component.start())

    subscriber = Subscriber()
    loop.run_until_complete(subscriber.start())

    futures = []
    for i in range(0, 10):
        requestor = Requestor(
            uri="http://127.0.0.1:8080/work",
            payload={"client_id": i},
        )
        future = loop.create_task(requestor.send_requests())
        futures.append(future)

    loop.run_until_complete(asyncio.wait(futures))
    loop.run_forever()
