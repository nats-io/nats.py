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

import random

# Use the system PRNG if possible
try:
    random = random.SystemRandom()
except NotImplementedError:
    import warnings
    warnings.warn(
        'A secure pseudo-random number generator is not available '
        'on your system. Falling back to Mersenne Twister.'
    )

INBOX_PREFIX = "_INBOX."


def hex_rand(n):
    """
    Generates a hexadecimal string with `n` random bits.
    """
    return "%x" % random.getrandbits(n)


def new_inbox():
    """
    Generates a unique _INBOX subject which can be used
    for publishing and receiving events.
    """
    return ''.join([
        INBOX_PREFIX,
        hex_rand(0x10),
        hex_rand(0x10),
        hex_rand(0x10),
        hex_rand(0x10),
        hex_rand(0x24)
    ])
