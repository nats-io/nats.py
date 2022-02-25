# Copyright 2016-2021 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from typing import List, Union

from .aio.client import Client as NATS


async def connect(
    servers: Union[str, List[str]] = ["nats://localhost:4222"],
    **options
) -> NATS:
    """
    :param servers: List of servers to connect.
    :param options: NATS connect options.

    ::

        import asyncio
        import nats

        async def main():
            # Connect to NATS Server.
            nc = await nats.connect('demo.nats.io')
            await nc.publish('foo', b'Hello World!')
            await nc.flush()
            await nc.close()

        if __name__ == '__main__':
            asyncio.run(main())

    """
    nc = NATS()
    await nc.connect(servers, **options)
    return nc
