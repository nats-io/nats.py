# Copyright 2015-2017 Apcera Inc. All rights reserved.

import random


# Use the system PRNG if possible
try:
    random = random.SystemRandom()
except NotImplementedError:
    import warnings
    warnings.warn('A secure pseudo-random number generator is not available '
                  'on your system. Falling back to Mersenne Twister.')


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
    return ''.join([INBOX_PREFIX,
                    hex_rand(0x10),
                    hex_rand(0x10),
                    hex_rand(0x10),
                    hex_rand(0x10),
                    hex_rand(0x24)])
