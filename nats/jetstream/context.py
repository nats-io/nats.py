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

from typing import Type, TypeVar

from nats.aio.client import Client
from nats.errors import NoRespondersError
from nats.jetstream.api import *
from nats.jetstream.errors import *


class AccountInfo:
   pass

class Context:
    def __init__(self, client: Client, api_prefix: str):
        self.client = client
        self.prefix = DEFAULT_PREFIX

    async def account_info(self) -> AccountInfo:
        """
        Fetches account information from the server, containing details
		about the account associated with this JetStream connection.

		If account is not enabled for JetStream, JetStreamNotEnabledForAccountError is raised.
		If the server does not have JetStream enabled, JetStreamNotEnabledError is raised.
		"""
        info_subject = subject(API_ACCOUNT_INFO, self.prefix)
        try:
            account_info = await request_json(self.client, info_subject, b"INFO", AccountInfo)
            return account_info
        except Error as error:
            if error.error_code == 503:
                raise JetStreamNotEnabledError()

            if error.error_code == 0:
                raise JetStreamNotEnabledForAccountError()

            raise error
        except NoRespondersError:
            raise JetStreamNotEnabledError()
