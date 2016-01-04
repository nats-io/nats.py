# Copyright 2015 Apcera Inc. All rights reserved.

"""
NATS network protocol parser.
"""

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
AWAITING_CONTROL_LINE   = 1
AWAITING_MSG_ARG        = 2
AWAITING_MSG_PAYLOAD    = 3
AWAITING_MINUS_ERR_ARG  = 4
MAX_CONTROL_LINE_SIZE   = 1024

class Parser(object):

    def __init__(self, nc=None):
        self.nc = nc
        self.reset()

    def __repr__(self):
        return "<nats protocol parser state={0} buflen={1} needed={2}>".format(
            self.state, len(self.scratch), self.needed)

    def reset(self):
        self.buf = b''
        self.state = AWAITING_CONTROL_LINE
        self.needed = 0
        self.msg_arg = {}

    def parse(self, data=''):
        """
        Parses the wire protocol from NATS for the client
        and dispatches the subscription callbacks.
        """
        self.buf = b''.join([self.buf, data])
        while self.buf:
            if self.state == AWAITING_CONTROL_LINE:
                scratch = self.buf[:MAX_CONTROL_LINE_SIZE]

                # MSG
                if scratch.startswith(MSG_OP):
                    self.buf = self.buf[MSG_OP_SIZE:]
                    self.state = AWAITING_MSG_ARG

                # OK
                elif scratch.startswith(OK):
                    self.buf = self.buf[OK_SIZE:]
                    self.state = AWAITING_CONTROL_LINE

                # -ERR
                elif scratch.startswith(ERR_OP):
                    self.buf = self.buf[ERR_OP_SIZE:]
                    self.state = AWAITING_MINUS_ERR_ARG

                # PONG
                elif scratch.startswith(PONG):
                    self.buf = self.buf[PONG_SIZE:]
                    self.state = AWAITING_CONTROL_LINE
                    self.nc._process_pong()

                # PING
                elif scratch.startswith(PING):
                    self.buf = self.buf[PING_SIZE:]
                    self.state = AWAITING_CONTROL_LINE
                    self.nc._process_ping()
                else:
                    break

            # -ERR 'error'
            elif self.state == AWAITING_MINUS_ERR_ARG:
                scratch = self.buf[:MAX_CONTROL_LINE_SIZE]

                # Skip until we have the full control line.
                if _CRLF_ not in scratch:
                    break

                i = scratch.find(_CRLF_)
                line = scratch[:i]
                _, err = line.split(_SPC_, 1)

                # Consume buffer and set next state before handling err.
                self.buf = self.buf[i+CRLF_SIZE:]
                self.state = AWAITING_CONTROL_LINE
                self.nc._process_err(err)

            elif self.state == AWAITING_MSG_ARG:
                scratch = self.buf[:MAX_CONTROL_LINE_SIZE]

                # Skip until we have the full control line.
                if _CRLF_ not in scratch:
                    break

                i = scratch.find(_CRLF_)
                line = scratch[:i]
                args = line.split(_SPC_)

                # Check in case of using a queue.
                args_size = len(args)
                if args_size == 5:
                    self.msg_arg["subject"] = args[1]
                    self.msg_arg["sid"] = int(args[2])
                    self.msg_arg["reply"] = args[3]
                    self.needed = int(args[4])
                elif args_size == 4:
                    self.msg_arg["subject"] = args[1]
                    self.msg_arg["sid"] = int(args[2])
                    self.msg_arg["reply"] = b''
                    self.needed = int(args[3])
                else:
                    raise ErrProtocol("nats: Wrong number of arguments in MSG")

                # Consume buffer and set next state.
                self.buf = self.buf[i+CRLF_SIZE:]
                self.state = AWAITING_MSG_PAYLOAD

            elif self.state == AWAITING_MSG_PAYLOAD:
                if len(self.buf) < self.needed+CRLF_SIZE:
                    break

                # Protocol wise, a MSG should end with a break
                # so signal protocol error if next char is not.
                msg_op_payload = self.buf[:self.needed+CRLF_SIZE]
                if msg_op_payload.endswith(MSG_END):
                    # Set next stage already before dispatching to callback.
                    self.buf = self.buf[self.needed+CRLF_SIZE:]
                    self.state = AWAITING_CONTROL_LINE

                    subject = self.msg_arg["subject"]
                    sid     = self.msg_arg["sid"]
                    reply   = self.msg_arg["reply"]
                    payload = msg_op_payload[:self.needed]
                    self.nc._process_msg(sid, subject, reply, payload)
                else:
                    raise ErrProtocol("nats: Wrong termination sequence for MSG")

class ErrProtocol(Exception):
    def __str__(self):
        return "nats: Protocol Error"
