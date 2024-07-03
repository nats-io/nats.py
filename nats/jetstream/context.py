# Copyright 2016-2024 The NATS Authors
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

from typing import Any, Type, TypeVar

from .api import Client
from .publish import Publisher
from .stream import StreamManager


class Context(
        Publisher,
        StreamManager,
        # StreamConsumerManager,
        # KeyValueManager,
        # ObjectStoreManager
):
    """
   	Provides a context for interacting with JetStream.
	The capabilities of JetStream include:

	- Publishing messages to a stream using `Publisher`.
	- Managing streams using `StreamManager`.
	- Managing consumers using `StreamConsumerManager`.
	- Managing key value stores using `KeyValueManager`.
	- Managing object stores using `ObjectStoreManager`.
	"""

    def __init__(self, connection: Any, timeout: float = 2):
        client = Client(
            connection,
            timeout=timeout,
        )

        Publisher.__init__(self, client)
        StreamManager.__init__(self, client)
