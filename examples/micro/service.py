import asyncio
import contextlib
import signal

from nats import Client, micro

async def echo(req) -> None:
    """Echo the request data back to the client."""
    await req.respond(req.data())


async def main():
    # Define an event to signal when to quit
    quit_event = asyncio.Event()
    # Attach signal handler to the event loop
    loop = asyncio.get_event_loop()
    for sig in (signal.Signals.SIGINT, signal.Signals.SIGTERM):
        loop.add_signal_handler(sig, lambda *_: quit_event.set())
    # Create an async exit stack
    async with contextlib.AsyncExitStack() as stack:
        # Create a NATS client
        nc = Client()
        # Connect to NATS
        await nc.connect("nats://localhost:4222")
        # Push the client.close() method into the stack to be called on exit
        stack.push_async_callback(nc.close)
        # Push a new micro service into the stack to be stopped on exit
        # The service will be stopped and drain its subscriptions before
        # closing the connection.
        service = await stack.enter_async_context(
            await micro.add_service(
                nc,
                name="demo-service",
                version="1.0.0",
                description="Demo service",
            )
        )
        group = service.add_group("demo")
        # Add an endpoint to the service
        await group.add_endpoint(
            name="echo",
            handler=echo,
        )
        # Wait for the quit event
        await quit_event.wait()


if __name__ == "__main__":
    asyncio.run(main())
