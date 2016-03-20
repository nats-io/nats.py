import cProfile as prof
from nats.protocol.parser import *

class DummyNatsClient:

    def __init__(self):
        self._subs = {}
        self._pongs = []
        self._pings_outstanding = 0
        self._pongs_received = 0
        self._server_info = {"max_payload": 1048576, "auth_required": False }

    def _send_command(self, cmd):
        pass

    def _process_pong(self):
        pass

    def _process_ping(self):
        pass

    def _process_msg(self, sid, subject, reply, payload):
        pass

    def _process_err(self, err=None):
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
    ps = Parser(DummyNatsClient())
    ps.parse(buf)

if __name__ == '__main__':

    benchs = [
        "parse_msgs(max_msgs=100,     nbytes=1)",
        "parse_msgs(max_msgs=1000,    nbytes=1)",
        "parse_msgs(max_msgs=10000,   nbytes=1)",
        "parse_msgs(max_msgs=20000,   nbytes=1)",
        "parse_msgs(max_msgs=100,     nbytes=10)",
        "parse_msgs(max_msgs=1000,    nbytes=10)",
        "parse_msgs(max_msgs=10000,   nbytes=10)",
        "parse_msgs(max_msgs=20000,   nbytes=10)",
        "parse_msgs(max_msgs=100,     nbytes=100)",
        "parse_msgs(max_msgs=1000,    nbytes=100)",
        "parse_msgs(max_msgs=10000,   nbytes=100)",
        "parse_msgs(max_msgs=20000,   nbytes=100)",
        "parse_msgs(max_msgs=100,     nbytes=1000)",
        "parse_msgs(max_msgs=1000,    nbytes=1000)",
        "parse_msgs(max_msgs=100,     nbytes=10000)",
        "parse_msgs(max_msgs=1000,    nbytes=10000)",
        "parse_msgs(max_msgs=10,      nbytes=100000)",
        "parse_msgs(max_msgs=100,     nbytes=100000)",
        ]

    for bench in benchs:
        print("=== {0}".format(bench))
        prof.run(bench)
