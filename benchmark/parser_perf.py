import asyncio
import cProfile as prof

from nats.protocol.parser import *


class DummyNatsClient:

    def __init__(self):
        self._subs = {}
        self._pongs = []
        self._pings_outstanding = 0
        self._pongs_received = 0
        self._server_info = {"max_payload": 1048576, "auth_required": False }
        self.stats = {
            'in_msgs':    0,
            'out_msgs':   0,
            'in_bytes':   0,
            'out_bytes':  0,
            'reconnects': 0,
            'errors_received': 0
            }

    async def _send_command(self, cmd):
        pass

    async def _process_pong(self):
        pass

    async def _process_ping(self):
        pass

    async def _process_msg(self, sid, subject, reply, data):
        self.stats['in_msgs']  += 1
        self.stats['in_bytes'] += len(data)

    async def _process_err(self, err=None):
        pass

def generate_msg(subject, nbytes, reply=""):
    msg = []
    protocol_line = "MSG {subject} 1 {reply} {nbytes}\r\n".format(
        subject=subject, reply=reply, nbytes=nbytes).encode()
    msg.append(protocol_line)
    msg.append(b'A' * nbytes)
    msg.append(b'r\n')
    return b''.join(msg)

def parse_msgs(max_msgs=1, nbytes=1):
    buf = b''.join([generate_msg("foo", nbytes) for i in range(0, max_msgs)])
    print("--- buffer size: {}".format(len(buf)))
    loop = asyncio.get_event_loop()
    ps = Parser(DummyNatsClient())
    loop.run_until_complete(ps.parse(buf))
    print("--- stats: ", ps.nc.stats)

if __name__ == '__main__':

    benchs = [
        "parse_msgs(max_msgs=10000,   nbytes=1)",
        "parse_msgs(max_msgs=100000,  nbytes=1)",
        "parse_msgs(max_msgs=1000000, nbytes=1)",
        "parse_msgs(max_msgs=10000,   nbytes=64)",
        "parse_msgs(max_msgs=100000,  nbytes=64)",
        "parse_msgs(max_msgs=1000000, nbytes=64)",
        "parse_msgs(max_msgs=10000,   nbytes=256)",
        "parse_msgs(max_msgs=100000,  nbytes=256)",
        "parse_msgs(max_msgs=10000,   nbytes=1024)",
        "parse_msgs(max_msgs=100000,  nbytes=1024)",
        "parse_msgs(max_msgs=10000,   nbytes=8192)",
        "parse_msgs(max_msgs=100000,  nbytes=8192)",
        "parse_msgs(max_msgs=10000,   nbytes=16384)",
        "parse_msgs(max_msgs=100000,  nbytes=16384)",
        ]

    for bench in benchs:
        print(f"=== {bench}")
        prof.run(bench)
