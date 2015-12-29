# Copyright 2015 Apcera Inc. All rights reserved.

import random

INBOX_PREFIX = "_INBOX."

def hex_rand(n):
    """
    Generates a hexadecimal string with `n` random bits.
    """
    return "%x" % random.SystemRandom().getrandbits(n)

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
