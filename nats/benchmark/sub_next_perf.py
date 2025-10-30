import argparse
import asyncio
import sys
import time

import nats

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

DEFAULT_NUM_MSGS = 100000
DEFAULT_MSG_SIZE = 16
DEFAULT_TIMEOUT = 10.0
DEFAULT_SUBJECT = "test"
HASH_MODULO = 1000


def show_usage():
    message = """
Usage: sub_next_perf [options]

options:
    -n COUNT                         Messages to consume (default: 100000)
    -S SUBJECT                       Subject to subscribe to (default: test)
    -t TIMEOUT                       Timeout for next_msg calls (default: 1.0, use 0 to wait forever)
    --servers SERVERS                NATS server URLs (default: nats://127.0.0.1:4222)
    """
    print(message)


def show_usage_and_die():
    show_usage()
    sys.exit(1)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--count", default=DEFAULT_NUM_MSGS, type=int)
    parser.add_argument("-S", "--subject", default=DEFAULT_SUBJECT)
    parser.add_argument("-t", "--timeout", default=DEFAULT_TIMEOUT, type=float)
    parser.add_argument("--servers", default=[], action="append")
    args = parser.parse_args()

    servers = args.servers
    if len(args.servers) < 1:
        servers = ["nats://127.0.0.1:4222"]

    # Connect to NATS
    try:
        nc = await nats.connect(servers, allow_reconnect=False)
    except Exception as e:
        sys.stderr.write(f"ERROR: Failed to connect: {e}\n")
        show_usage_and_die()

    print(f"Connected to NATS server: {servers}")
    print(f"Subscribing to subject: {args.subject}")
    print(f"Expecting {args.count} messages with {args.timeout}s timeout per next_msg()")
    print("Waiting for messages...")
    print()

    # Subscribe without callback to use next_msg()
    sub = await nc.subscribe(args.subject)

    received = 0
    timeouts = 0
    errors = 0
    start_time = time.time()
    first_msg_time = None

    print("Progress: ", end="", flush=True)

    # Consume messages using next_msg()
    for i in range(args.count):
        try:
            await sub.next_msg(timeout=args.timeout)
            received += 1

            # Record when first message arrives for accurate timing
            if received == 1:
                first_msg_time = time.time()

            # Show progress
            if received % HASH_MODULO == 0:
                print("#", end="", flush=True)

        except nats.errors.TimeoutError:
            timeouts += 1
            if timeouts % HASH_MODULO == 0:
                print("T", end="", flush=True)
        except Exception as e:
            errors += 1
            if errors == 1:
                sys.stderr.write(f"\nFirst error: {e}\n")
            if errors % HASH_MODULO == 0:
                print("E", end="", flush=True)

    total_time = time.time() - start_time

    # Calculate timing based on actual message flow
    if first_msg_time and received > 0:
        msg_processing_time = time.time() - first_msg_time
        msgs_per_sec = received / msg_processing_time
    else:
        msg_processing_time = total_time
        msgs_per_sec = received / total_time if total_time > 0 else 0

    print("\n\nBenchmark Results:")
    print("=================")
    print(f"Total time: {total_time:.2f} seconds")
    print(f"Message processing time: {msg_processing_time:.2f} seconds")
    print(f"Messages received: {received}/{args.count}")
    print(f"Timeouts: {timeouts}")
    print(f"Errors: {errors}")

    if received > 0:
        print(f"Messages per second: {msgs_per_sec:.2f}")
        print(f"Average time per next_msg(): {msg_processing_time / received * 1000:.3f} ms")

    if received < args.count:
        print(f"Warning: Only received {received} out of {args.count} expected messages")
        print("Make sure to publish messages to the same subject before or during this benchmark")
        print(f"Example: nats bench pub {args.subject} --msgs {args.count} --size {DEFAULT_MSG_SIZE}")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
