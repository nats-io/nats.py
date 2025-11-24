import argparse
import asyncio
import os
import sys
import time

import nats

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

DEFAULT_NUM_FETCHES = 10
DEFAULT_TIMEOUT = 30
DEFAULT_BUCKET = ""
DEFAULT_OBJECT = ""


class ProgressFileWrapper:
    """
    A file wrapper that shows download progress as data is written.
    """

    def __init__(self, file_obj, total_size: int, object_name: str):
        self.file = file_obj
        self.total_size = total_size
        self.object_name = object_name
        self.bytes_written = 0
        self.last_progress = -1
        self.start_time = time.time()

    def write(self, data):
        """Write data to file and update progress."""
        result = self.file.write(data)
        self.bytes_written += len(data)
        self._update_progress()
        return result

    def _update_progress(self):
        """Update progress display."""
        if self.total_size <= 0:
            return

        progress = int((self.bytes_written / self.total_size) * 100)

        # Only update every 5% to avoid too much output
        if progress >= self.last_progress + 5:
            elapsed = time.time() - self.start_time
            if elapsed > 0:
                speed_mbps = (self.bytes_written / (1024 * 1024)) / elapsed
                mb_written = self.bytes_written / (1024 * 1024)
                mb_total = self.total_size / (1024 * 1024)

                # Clear the current line and show progress
                print(
                    f"\r {self.object_name}: {progress:3d}% ({mb_written:.1f}/{mb_total:.1f} MB) @ {speed_mbps:.1f} MB/s",
                    end="",
                    flush=True,
                )
                self.last_progress = progress

    def __getattr__(self, name):
        """Delegate other attributes to the wrapped file."""
        return getattr(self.file, name)


def show_usage():
    message = """
Usage: obj_fetch_perf [options]

options:
    -n COUNT                         Number of fetches to perform (default: 10)
    -b BUCKET                        Object store bucket name
    -o OBJECT                        Object name to fetch
    -t TIMEOUT                       Timeout per fetch in seconds (default: 30)
    -f FILE                          Write to file (streaming mode, memory efficient)
    --overwrite                      Overwrite output file if it exists
    --servers SERVERS                NATS server URLs (default: nats://demo.nats.io:4222)
    """
    print(message)


def show_usage_and_die():
    show_usage()
    sys.exit(1)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--count", default=DEFAULT_NUM_FETCHES, type=int)
    parser.add_argument("-b", "--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("-o", "--object", default=DEFAULT_OBJECT)
    parser.add_argument("-t", "--timeout", default=DEFAULT_TIMEOUT, type=int)
    parser.add_argument("-f", "--file", help="Write to file (streaming mode)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite output file if it exists")
    parser.add_argument("--servers", default=[], action="append")
    args = parser.parse_args()

    servers = args.servers
    if len(args.servers) < 1:
        servers = ["nats://demo.nats.io:4222"]

    print(f"Connecting to NATS servers: {servers}")

    # Connect to NATS with JetStream
    try:
        nc = await nats.connect(servers, pending_size=1024 * 1024)
        js = nc.jetstream()
    except Exception as e:
        sys.stderr.write(f"ERROR: Failed to connect to NATS: {e}\n")
        show_usage_and_die()

    # Get object store
    try:
        obs = await js.object_store(bucket=args.bucket)
        print(f"Connected to object store bucket: {args.bucket}")
    except Exception as e:
        sys.stderr.write(f"ERROR: Failed to access object store bucket '{args.bucket}': {e}\n")
        await nc.close()
        sys.exit(1)

    # Get object info first to verify it exists and show stats
    try:
        info = await obs.get_info(args.object)
        size_mb = info.size / (1024 * 1024)
        print(f"Object: {args.object}")
        print(f"Size: {info.size} bytes ({size_mb:.2f} MB)")
        print(f"Chunks: {info.chunks}")
        print(f"Description: {info.description}")
        print()
    except Exception as e:
        sys.stderr.write(f"ERROR: Failed to get object info for '{args.object}': {e}\n")
        await nc.close()
        sys.exit(1)

    # Handle file output setup
    if args.file:
        if os.path.exists(args.file) and not args.overwrite:
            sys.stderr.write(f"ERROR: File '{args.file}' already exists. Use --overwrite to replace it.\n")
            await nc.close()
            sys.exit(1)

        # For multiple fetches with file output, append a counter
        if args.count > 1:
            base, ext = os.path.splitext(args.file)
            print(f"Multiple fetches with file output - files will be named: {base}_1{ext}, {base}_2{ext}, etc.")
        else:
            print(f"Streaming output to file: {args.file}")
        print()

    # Start the benchmark
    print(f"Starting benchmark: fetching '{args.object}' {args.count} times")
    if args.file:
        print("Progress (streaming to file):")
    else:
        print("Progress: ", end="", flush=True)

    start = time.time()
    total_bytes = 0
    successful_fetches = 0
    failed_fetches = 0

    for i in range(args.count):
        try:
            # Determine output file for this fetch
            current_file = None
            if args.file:
                if args.count > 1:
                    base, ext = os.path.splitext(args.file)
                    current_file = f"{base}_{i + 1}{ext}"
                else:
                    current_file = args.file

            # Fetch the object
            if current_file:
                # Stream to file with progress tracking
                with open(current_file, "wb") as f:
                    # Wrap the file with progress tracker
                    progress_wrapper = ProgressFileWrapper(f, info.size, args.object)
                    result = await asyncio.wait_for(
                        obs.get(args.object, writeinto=progress_wrapper), timeout=args.timeout
                    )
                    # Get file size for stats
                    fetch_bytes = os.path.getsize(current_file)
                    # Ensure we show 100% completion
                    if progress_wrapper.bytes_written > 0:
                        print(
                            f"\r  📥 {args.object}: 100% ({fetch_bytes / (1024 * 1024):.1f}/{info.size / (1024 * 1024):.1f} MB) ✓"
                        )
            else:
                # Load into memory
                result = await asyncio.wait_for(obs.get(args.object), timeout=args.timeout)
                fetch_bytes = len(result.data)

            total_bytes += fetch_bytes
            successful_fetches += 1

            # Show simple progress for in-memory mode
            if not current_file:
                print("#", end="", flush=True)

        except asyncio.TimeoutError:
            failed_fetches += 1
            if args.file:
                print(f"\r  ❌ {args.object}: Timeout after {args.timeout}s")
            else:
                print("T", end="", flush=True)  # T for timeout
        except Exception as e:
            failed_fetches += 1
            if args.file:
                print(f"\r  ❌ {args.object}: Error - {str(e)[:50]}")
            else:
                print("E", end="", flush=True)  # E for error
            if i == 0:  # Show first error for debugging
                sys.stderr.write(f"\nFirst fetch error: {e}\n")

        # Small pause between fetches
        await asyncio.sleep(0.01)

    elapsed = time.time() - start

    print("\n\nBenchmark Results:")
    print("=================")
    if args.file:
        print("Mode: Streaming to file(s) (memory efficient)")
    else:
        print("Mode: In-memory loading")
    print(f"Total time: {elapsed:.2f} seconds")
    print(f"Successful fetches: {successful_fetches}/{args.count}")
    print(f"Failed fetches: {failed_fetches}")

    if successful_fetches > 0:
        avg_time = elapsed / successful_fetches
        mbytes_per_sec = (total_bytes / elapsed) / (1024 * 1024)
        fetches_per_sec = successful_fetches / elapsed

        print(f"Average fetch time: {avg_time:.3f} seconds")
        print(f"Fetches per second: {fetches_per_sec:.2f}")
        print(f"Throughput: {mbytes_per_sec:.2f} MB/sec")
        print(f"Total data transferred: {total_bytes / (1024 * 1024):.2f} MB")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
