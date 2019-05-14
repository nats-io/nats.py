# Copyright 2016-2018 The NATS Authors
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

from random import Random, SystemRandom
from sys import maxsize as MaxInt

DIGITS = b'0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'
BASE = 62
PREFIX_LENGTH = 12
SEQ_LENGTH = 10
TOTAL_LENGTH = PREFIX_LENGTH + SEQ_LENGTH
MAX_SEQ = BASE**10
MIN_INC = 33
MAX_INC = 333
INC = MAX_INC - MIN_INC


class NUID(object):
    """
    NUID is an implementation of the approach for fast generation of
    unique identifiers used for inboxes in NATS.
    """

    def __init__(self):
        self._srand = SystemRandom()
        self._prand = Random(self._srand.randint(0, MaxInt))
        self._seq = self._prand.randint(0, MAX_SEQ)
        self._inc = MIN_INC + self._prand.randint(0, INC)
        self._prefix = b''
        self.randomize_prefix()

    def next(self):
        self._seq += self._inc
        if self._seq >= MAX_SEQ:
            self.randomize_prefix()
            self.reset_sequential()
        l = self._seq
        prefix = self._prefix[:]

        def _next():
            nonlocal l
            a = DIGITS[int(l) % BASE]
            l /= BASE
            return a

        suffix = bytearray(_next() for i in range(SEQ_LENGTH))
        prefix.extend(suffix)
        return prefix

    def randomize_prefix(self):
        random_bytes = (
            self._srand.getrandbits(8) for i in range(PREFIX_LENGTH)
        )
        self._prefix = bytearray(DIGITS[c % BASE] for c in random_bytes)

    def reset_sequential(self):
        self._seq = self._prand.randint(0, MAX_SEQ)
        self._inc = MIN_INC + self._prand.randint(0, INC)
