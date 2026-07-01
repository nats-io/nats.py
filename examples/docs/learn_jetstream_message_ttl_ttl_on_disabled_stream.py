import asyncio

import nats
from nats.js.api import StreamConfig
from nats.js.errors import APIError


async def main():
    # JetStream requires a server with JetStream enabled (demo.nats.io does not).
    nc = await nats.connect("nats://localhost:4222")

    # Get a JetStream context, which also manages streams.
    js = nc.jetstream()

    # Create a stream that does NOT allow per-message TTLs (allow_msg_ttl
    # defaults to off), so the server rejects any Nats-TTL header.
    await js.add_stream(
        StreamConfig(
            name="ORDERS_NO_TTL",
            subjects=["no-ttl.>"],
        )
    )

    # NATS-DOC-START
    # Publishing with a Nats-TTL header fails when the stream doesn't allow
    # per-message TTLs. The server rejects it with err_code 10166 and stores
    # nothing.
    try:
        await js.publish(
            "no-ttl.msg",
            b'{"id": "A-1002"}',
            headers={"Nats-TTL": "60s"},
        )
    except APIError as e:
        # "per-message TTL is disabled" — the fix is to create or update the
        # stream with allow_msg_ttl=True.
        print(f"publish rejected: {e.description} (err_code {e.err_code})")
    # NATS-DOC-END

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
