import cProfile as prof

from nats.aio.utils import *
from nats.aio.nuid import NUID


def gen_inboxes(n):
    for i in range(0, n):
        new_inbox()


INBOX_PREFIX = bytearray(b'_INBOX.')


def gen_inboxes_nuid(n):
    nuid = NUID()
    for i in range(0, n):
        inbox = INBOX_PREFIX[:]
        inbox.extend(nuid.next())


if __name__ == '__main__':
    benchs = [
        "gen_inboxes(1000)",
        "gen_inboxes_nuid(1000)",
        "gen_inboxes(100000)",
        "gen_inboxes_nuid(100000)",
        "gen_inboxes(1000000)",
        "gen_inboxes_nuid(1000000)",
    ]
    for bench in benchs:
        print(f"=== {bench}")
        prof.run(bench)
