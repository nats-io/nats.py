# Copyright 2021 The NATS Authors
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

import json
from nats.js import api
from nats.errors import NoRespondersError
from nats.js.errors import ServiceUnavailableError, APIError
from email.parser import BytesParser
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from nats import NATS


class JetStreamManager:
    """
    JetStreamManager exposes management APIs for JetStream.
    """

    def __init__(
        self,
        conn: "NATS",
        prefix: str = api.DefaultPrefix,
        timeout: float = 5,
    ) -> None:
        self._prefix = prefix
        self._nc = conn
        self._timeout = timeout
        self._hdr_parser = BytesParser()

    async def account_info(self) -> api.AccountInfo:
        info = await self._api_request(
            f"{self._prefix}.INFO", b'', timeout=self._timeout
        )
        return api.AccountInfo.loads(**info)

    async def find_stream_name_by_subject(self, subject: str) -> str:
        """
        Find the stream to which a subject belongs in an account.
        """

        req_sub = f"{self._prefix}.STREAM.NAMES"
        req_data = json.dumps({"subject": subject})
        info = await self._api_request(
            req_sub, req_data.encode(), timeout=self._timeout
        )
        return info['streams'][0]

    async def stream_info(self, name: str) -> api.StreamInfo:
        """
        Get the latest StreamInfo by stream name.
        """
        resp = await self._api_request(
            f"{self._prefix}.STREAM.INFO.{name}", timeout=self._timeout
        )
        return api.StreamInfo.loads(**resp)

    async def add_stream(
        self, config: api.StreamConfig = None, **params
    ) -> api.StreamInfo:
        """
        add_stream creates a stream.
        """
        if config:
            # Merge config and kwargs
            # In case of key collision, explicit key args (`params`) override config
            params = {**asdict(config), **params}
        merged_config = api.StreamConfig.loads(**params)
        if merged_config.name is None:
            raise ValueError("nats: stream name is required")

        data = merged_config.asjson()
        resp = await self._api_request(
            f"{self._prefix}.STREAM.CREATE.{merged_config.name}",
            data.encode(),
            timeout=self._timeout
        )
        return api.StreamInfo.loads(**resp)

    async def delete_stream(self, name: str) -> bool:
        """
        Delete a stream by name.
        """
        resp = await self._api_request(
            f"{self._prefix}.STREAM.DELETE.{name}", timeout=self._timeout
        )
        return resp['success']

    async def consumer_info(
        self, stream: str, consumer: str, timeout: Optional[float] = None
    ):
        # TODO: Validate the stream and consumer names.
        if timeout is None:
            timeout = self._timeout
        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.INFO.{stream}.{consumer}",
            b'',
            timeout=timeout
        )
        return api.ConsumerInfo.loads(**resp)

    async def add_consumer(
        self,
        stream: str,
        config: Optional[api.ConsumerConfig] = None,
        durable_name: Optional[str] = None,
        description: Optional[str] = None,
        deliver_subject: Optional[str] = None,
        deliver_group: Optional[str] = None,
        deliver_policy: api.DeliverPolicy = api.DeliverPolicy.last,
        opt_start_seq: Optional[int] = None,
        opt_start_time: Optional[int] = None,
        ack_policy: api.AckPolicy = api.AckPolicy.explicit,
        ack_wait: Optional[int] = None,
        max_deliver: Optional[int] = None,
        filter_subject: Optional[str] = None,
        replay_policy: api.ReplayPolicy = api.ReplayPolicy.instant,
        sample_freq: Optional[str] = None,
        rate_limit_bps: Optional[int] = None,
        max_waiting: Optional[int] = None,
        max_ack_pending: Optional[int] = None,
        flow_control: Optional[bool] = None,
        idle_heartbeat: Optional[int] = None,
        headers_only: Optional[bool] = None,
        timeout=None,
    ) -> api.ConsumerInfo:
        if not timeout:
            timeout = self._timeout

        # TODO: Convert from seconds into nanoseconds.

        if config is None:
            config_dict = {
                "durable_name": durable_name,
                "description": description,
                "deliver_subject": deliver_subject,
                "deliver_group": deliver_group,
                "deliver_policy": deliver_policy,
                "opt_start_seq": opt_start_seq,
                "opt_start_time": opt_start_time,
                "ack_policy": ack_policy,
                "ack_wait": ack_wait,
                "max_deliver": max_deliver,
                "filter_subject": filter_subject,
                "replay_policy": replay_policy,
                "max_waiting": max_waiting,
                "max_ack_pending": max_ack_pending,
                "flow_control": flow_control,
                "idle_heartbeat": idle_heartbeat,
                "headers_only": headers_only,
            }
        elif isinstance(config, api.ConsumerConfig):
            # Try to transform the config.
            durable_name = config.durable_name
            config_dict = asdict(config)

        # Cleanup empty values.
        # FIXME: Make this part of Base
        for k, v in dict(config_dict).items():
            if v is None:
                del config_dict[k]
        req = {"stream_name": stream, "config": config_dict}
        req_data = json.dumps(req).encode()

        resp = None
        if durable_name is not None:
            resp = await self._api_request(
                f"{self._prefix}.CONSUMER.DURABLE.CREATE.{stream}.{durable_name}",
                req_data,
                timeout=timeout
            )
        else:
            resp = await self._api_request(
                f"{self._prefix}.CONSUMER.CREATE.{stream}",
                req_data,
                timeout=timeout
            )
        return api.ConsumerInfo.loads(**resp)

    async def delete_consumer(self, stream: str, consumer: str) -> bool:
        resp = await self._api_request(
            f"{self._prefix}.CONSUMER.DELETE.{stream}.{consumer}",
            b'',
            timeout=self._timeout
        )
        return resp['success']

    async def _api_request(
        self,
        req_subject: str,
        req: bytes = b'',
        timeout: float = 5,
    ) -> Dict[str, Any]:
        try:
            msg = await self._nc.request(req_subject, req, timeout=timeout)
            resp = json.loads(msg.data)
        except NoRespondersError:
            raise ServiceUnavailableError

        # Check for API errors.
        if 'error' in resp:
            raise APIError.from_error(resp['error'])

        return resp
