import asyncio
import contextlib
import signal

import nats
import nats.micro


async def echo(req) -> None:
    """Echo the request data back to the client."""
    await req.respond(req.data)


async def main():
    # Define an event to signal when to quit
    quit_event = asyncio.Event()

    # Attach signal handler to the event loop
    loop = asyncio.get_event_loop()
    for sig in (signal.Signals.SIGINT, signal.Signals.SIGTERM):
        loop.add_signal_handler(sig, lambda *_: quit_event.set())

    # Create an async exit stack
    async with contextlib.AsyncExitStack() as stack:
        # Connect to NATS
        nc = await stack.enter_async_context(await nats.connect())

        # Add the service
        service = await stack.enter_async_context(
            await
            nats.micro.add_service(nc, name="demo_service", version="0.0.1")
        )

        group = service.add_group(name="demo")
        # Add an endpoint to the service
        await group.add_endpoint(
            name="echo",
            handler=echo,
        )
        # Wait for the quit event
        await quit_event.wait()


if __name__ == "__main__":
    asyncio.run(main())
