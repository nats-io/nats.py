# Copyright 2021-2022 The NATS Authors
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

from dataclasses import replace
from nats.aio.client import Client
from typing import Optional

from .service import Service, ServiceConfig
from .request import Request, Handler


async def add_service(
    nc: Client, config: Optional[ServiceConfig] = None, **kwargs
) -> Service:
    """Add a service."""
    if config:
        config = replace(config, **kwargs)
    else:
        config = ServiceConfig(**kwargs)

    service = Service(nc, config)
    await service.start()

    return service


__all__ = ["add_service"]
