# NATS Client

A Python client for the NATS messaging system.

## Features

- Support for publish/subscribe
- Support for request/reply
- Support for queue groups
- Support for multi-value message headers

## Installation

```bash
pip install nats-client
```

## Usage

```python
import asyncio
from nats.client import connect

async def main():
    client = await connect("nats://localhost:4222")

    # Subscribe
    async with await client.subscribe("foo") as subscription:
        # Publish
        await client.publish("foo", "Hello World!")

        # Receive message
        message = await subscription.next()
        print(f"Received: {message.data}")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## 🚀 Performance

This client implementation delivers significant performance improvements over the nats.aio client, particularly for high-frequency, small message workloads.

Do note tho, it is not as feature complete at this point in time.

| Message Size | nats.py (python3) | nats.py (pypy3) | experimental-nats.py (python3) | experimental-nats (pypy3) | Performance Gain |
|--------------|-------------------|-----------------|--------------------------------|---------------------------|------------------|
| 1B           | 127,411          | 153,009         | 1,522,673                      | **5,376,113**             | **35.1x** 🚀     |
| 2B           | 136,485          | 148,981         | 1,544,513                      | **5,396,347**             | **36.2x** 🚀     |
| 4B           | 131,630          | 149,297         | 1,548,191                      | **5,356,600**             | **35.9x** 🚀     |
| 8B           | 138,229          | 141,117         | 1,530,825                      | **5,307,400**             | **37.6x** 🚀     |
| 16B          | 140,874          | 149,826         | 1,539,244                      | **5,211,168**             | **34.8x** 🚀     |
| 32B          | 141,427          | 146,670         | 1,515,068                      | **5,115,238**             | **34.9x** 🚀     |
| 64B          | 145,257          | 153,542         | 1,505,724                      | **5,339,967**             | **34.8x** 🚀     |
| 128B         | 163,181          | 164,723         | 1,479,100                      | **4,923,321**             | **29.9x** 🔥     |
| 256B         | 145,824          | 161,017         | 1,452,996                      | **4,130,165**             | **25.7x** 🔥     |
| 512B         | 243,641          | 277,321         | 1,297,250                      | **3,430,092**             | **12.4x** ⚡     |
| 1K           | 738,895          | 802,283         | 1,253,102                      | **2,374,747**             | **3.0x** ⚡      |
| 2K           | 696,945          | 736,925         | 1,060,123                      | **1,381,177**             | **1.9x** ✨      |
| 4K           | 577,335          | 625,935         | 798,797                        | **814,393**               | **1.3x** ✨      |
| 8K           | 414,077          | 463,383         | 532,429                        | 450,211                   | 0.97x           |
| 16K          | 266,104          | 309,680         | 345,651                        | 228,815                   | 0.74x           |
| 32K          | 102,460          | 128,852         | 166,028                        | 125,662                   | 0.98x           |
| 64K          | 55,208           | 63,563          | 74,359                         | 56,804                    | 0.89x           |

### Key Performance Insights

**🎯 Sweet Spot: Small to Medium Messages**
- **35-37x faster** for tiny messages (1B-64B)
- **25-30x faster** for small messages (128B-256B)
- **12x faster** for medium messages (512B)

### Benchmark Environment

- **CPU**: Apple M3 Max
- **Memory**: 36 GB
- **Python**: 3.x
- **PyPy**: 3.x

> **Note**: Benchmarks may vary based on your specific hardware, network conditions, and NATS server configuration. We recommend running your own benchmarks for production workloads.
