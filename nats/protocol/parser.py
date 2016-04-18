# Copyright 2015-2016 Apcera Inc. All rights reserved.

"""
NATS network protocol parser.
"""

import re
import asyncio

MSG_RE  = re.compile(b'\AMSG\s+([^\s]+)\s+([^\s]+)\s+(([^\s]+)[^\S\r\n]+)?(\d+)\r\n')
OK_RE   = re.compile(b'\A\+OK\s*\r\n')
ERR_RE  = re.compile(b'\A-ERR\s+(\'.+\')?\r\n')
PING_RE = re.compile(b'\APING\s*\r\n')
PONG_RE = re.compile(b'\APONG\s*\r\n')
INFO_RE = re.compile(b'\AINFO\s+([^\r\n]+)\r\n')

INFO_OP     = b'INFO'
CONNECT_OP  = b'CONNECT'
PUB_OP      = b'PUB'
MSG_OP      = b'MSG'
SUB_OP      = b'SUB'
UNSUB_OP    = b'UNSUB'
PING_OP     = b'PING'
PONG_OP     = b'PONG'
OK_OP       = b'+OK'
ERR_OP      = b'-ERR'
MSG_END     = b'\n'
_CRLF_      = b'\r\n'
_SPC_       = b' '

OK          = OK_OP + _CRLF_
PING        = PING_OP + _CRLF_
PONG        = PONG_OP + _CRLF_
CRLF_SIZE   = len(_CRLF_)
OK_SIZE     = len(OK)
PING_SIZE   = len(PING)
PONG_SIZE   = len(PONG)
MSG_OP_SIZE = len(MSG_OP)
ERR_OP_SIZE = len(ERR_OP)

# States
AWAITING_CONTROL_LINE  = 1
AWAITING_MSG_PAYLOAD   = 2
MAX_CONTROL_LINE_SIZE  = 1024

class Parser(object):

    def __init__(self, nc=None):
        self.nc = nc
        self.reset()

    def __repr__(self):
        return "<nats protocol parser state={0}>".format(self.state)

    def reset(self):
        self.buf = bytearray()
        self.state = AWAITING_CONTROL_LINE
        self.needed = 0
        self.msg_arg = {}

    @asyncio.coroutine
    def parse(self, data=b''):
        """
        Parses the wire protocol from NATS for the client
        and dispatches the subscription callbacks.
        """
        self.buf.extend(data)
        while self.buf:
            if self.state == AWAITING_CONTROL_LINE:
                msg = MSG_RE.match(self.buf)
                if msg:
                    try:
                        subject, sid, _, reply, needed_bytes = msg.groups()
                        self.msg_arg["subject"] = subject
                        self.msg_arg["sid"] = int(sid)
                        if reply:
                            self.msg_arg["reply"] = reply
                        else:
                            self.msg_arg["reply"] = b''
                        self.needed = int(needed_bytes)
                        del self.buf[:msg.end()]
                        self.state = AWAITING_MSG_PAYLOAD
                        continue
                    except:
                        raise ErrProtocol("nats: malformed MSG")

                ok = OK_RE.match(self.buf)
                if ok:
                    # Do nothing and just skip.
                    del self.buf[:ok.end()]
                    continue

                err = ERR_RE.match(self.buf)
                if err:
                    err_msg = err.groups()
                    yield from self.nc._process_err(err_msg)
                    del self.buf[:err.end()]
                    continue

                ping = PING_RE.match(self.buf)
                if ping:
                    del self.buf[:ping.end()]
                    yield from self.nc._process_ping()
                    continue

                pong = PONG_RE.match(self.buf)
                if pong:
                    del self.buf[:pong.end()]
                    yield from self.nc._process_pong()
                    continue

                # If nothing matched at this point, then probably
                # a split buffer and need to gather more bytes,
                # otherwise it would mean that there is an issue
                # and we're getting malformed control lines.
                if len(self.buf) < MAX_CONTROL_LINE_SIZE and _CRLF_ not in self.buf:
                    break
                else:
                    raise ErrProtocol("nats: unknown protocol")

            elif self.state == AWAITING_MSG_PAYLOAD:
                if len(self.buf) >= self.needed+CRLF_SIZE:
                    subject = self.msg_arg["subject"]
                    sid     = self.msg_arg["sid"]
                    reply   = self.msg_arg["reply"]

                    # Consume msg payload from buffer and set next parser state.
                    payload = bytes(self.buf[:self.needed])
                    del self.buf[:self.needed+CRLF_SIZE]
                    self.state = AWAITING_CONTROL_LINE
                    yield from self.nc._process_msg(sid, subject, reply, payload)
                else:
                    # Wait until we have enough bytes in buffer.
                    break

class ErrProtocol(Exception):
    def __str__(self):
        return "nats: Protocol Error"
