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

import datetime
from dataclasses import dataclass, field


class Msg:
    """
    Msg represents a message delivered by NATS.
    """
    __slots__ = (
        'subject', 'reply', 'data', 'sid', '_client', 'headers', '_metadata'
    )

    class Ack:
        Ack = b"+ACK"
        Nak = b"-NAK"
        Progress = b"+WPI"
        Term = b"+TERM"

        # Reply metadata...
        Prefix0 = '$JS'
        Prefix1 = 'ACK'
        Domain = 2
        AccHash = 3
        Stream = 4
        Consumer = 5
        NumDelivered = 6
        StreamSeq = 7
        ConsumerSeq = 8
        Timestamp = 9
        NumPending = 10

        # Subject without domain:
        # $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
        #
        V1TokenCount = 9

        # Subject with domain:
        # $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.<a token with a random value>
        #
        V2TokenCount = 12

    def __init__(
        self,
        subject: str = '',
        reply: str = '',
        data: str = b'',
        sid: int = 0,
        client=None,
        headers: dict = None
    ):
        self.subject = subject
        self.reply = reply
        self.data = data
        self.sid = sid
        self._client = client
        self.headers = headers
        self._metadata = None

    @property
    def header(self):
        """
        header returns the headers from a message.
        """
        return self.headers

    async def respond(self, data: bytes):
        """
        respond replies to the inbox of the message if there is one.
        """
        if not self.reply:
            raise Error('no reply subject available')
        if not self._client:
            raise Error('client not set')

        await self._client.publish(self.reply, data, headers=self.headers)

    async def ack(self):
        """
        ack acknowledges a message delivered by JetStream.
        """
        if self.reply is None or self.reply == '':
            raise NotJSMessageError
        await self._client.publish(self.reply)

    async def ack_sync(self, timeout: float = 1.0):
        """
        ack_sync waits for the acknowledgement to be processed by the server.
        """
        if self.reply is None or self.reply == '':
            raise NotJSMessageError

        resp = await self._client.request(self.reply, timeout=timeout)
        return resp

    async def nak(self):
        """
        ack negatively acknowledges a message delivered by JetStream.
        """
        if self.reply is None or self.reply == '':
            raise NotJSMessageError
        await self._client.publish(self.reply, Msg.Ack.Nak)

    async def in_progress(self):
        """
        ack acknowledges a message delivered by JetStream is still being worked on.
        """
        if self.reply is None or self.reply == '':
            raise NotJSMessageError
        await self._client.publish(self.reply, Msg.Ack.Progress)

    async def term(self):
        """
        ack terminates a message delivered by JetStream and disables redeliveries.
        """
        if self.reply is None or self.reply == '':
            raise NotJSMessageError
        await self._client.publish(self.reply, Msg.Ack.Term)

    @property
    def metadata(self):
        msg = self
        # Memoize the parsed metadata.
        metadata = msg._metadata
        if metadata is not None:
            return metadata

        # TODO: Support V2TokenCount style with domains.
        tokens = Msg.Metadata._get_metadata_fields(msg.reply)
        t = datetime.datetime.fromtimestamp(int(tokens[7]) / 1_000_000_000.0)
        metadata = Msg.Metadata(
            sequence=Msg.Metadata.SequencePair(
                stream=int(tokens[5]), consumer=int(tokens[6])
            ),
            num_delivered=int(tokens[4]),
            num_pending=int(tokens[8]),
            timestamp=t,
            stream=tokens[2],
            consumer=tokens[3],
        )
        msg._metadata = metadata
        return metadata

    def _get_metadata_fields(self, reply):
        return Msg.Metadata._get_metadata_fields(reply)

    def __repr__(self):
        return f"<{self.__class__.__name__}: subject='{self.subject}' reply='{self.reply}' data='{self.data[:10].decode()}...'>"

    class Metadata:
        """
        Metadata is the metadata from a JetStream message.
        """
        __slots__ = (
            'num_delivered', 'num_pending', 'timestamp', 'stream', 'consumer',
            'sequence'
        )

        @dataclass
        class SequencePair:
            """
            SequencePair represents a pair of consumer and stream sequence.
            """
            consumer: int
            stream: int

        def __init__(
            self,
            sequence=None,
            num_pending: int = None,
            num_delivered: int = None,
            timestamp: str = None,
            stream: str = None,
            consumer: str = None,
        ):
            self.sequence = sequence
            self.num_pending = num_pending
            self.num_delivered = num_delivered
            self.timestamp = timestamp
            self.stream = stream
            self.consumer = consumer

        def _get_metadata_fields(reply):
            if reply is None or reply == '':
                raise NotJSMessageError
            tokens = reply.split('.')
            if len(tokens) != Msg.Ack.V1TokenCount or \
                   tokens[0] != Msg.Ack.Prefix0 or \
                   tokens[1] != Msg.Ack.Prefix1:
                raise NotJSMessageError
            return tokens

        def __repr__(self):
            return f"<{self.__class__.__name__}: stream='{self.stream}' consumer='{self.consumer}' sequence=({self.sequence.stream}, {self.sequence.consumer}) timestamp={self.timestamp}>"
