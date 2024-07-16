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

import re

DEFAULT_QUEUE_GROUP = "q"
"""Queue Group name used across all services."""

DEFAULT_PREFIX = "$SRV"
"""The root of all control subjects."""

ERROR_HEADER = "Nats-Service-Error"
ERROR_CODE_HEADER = "Nats-Service-Error-Code"

INFO_RESPONSE_TYPE = "io.nats.micro.v1.info_response"
PING_RESPONSE_TYPE = "io.nats.micro.v1.ping_response"
STATS_RESPONSE_TYPE = "io.nats.micro.v1.stats_response"

SEMVER_REGEX = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
)
NAME_REGEX = re.compile(r"^[A-Za-z0-9\-_]+$")
SUBJECT_REGEX = re.compile(r"^[^ >]*[>]?$")
