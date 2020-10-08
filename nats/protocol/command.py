PUB_OP = 'PUB'
SUB_OP = 'SUB'
UNSUB_OP = 'UNSUB'
_CRLF_ = '\r\n'


def pub_cmd(subject, reply, payload):
    return f'{PUB_OP} {subject} {reply} {len(payload)}{_CRLF_}'.encode(
    ) + payload + _CRLF_.encode()


def sub_cmd(subject, queue, sid):
    return f'{SUB_OP} {subject} {queue} {sid}{_CRLF_}'.encode()


def unsub_cmd(sid, limit):
    limit_s = '' if limit == 0 else f'{limit}'
    return f'{UNSUB_OP} {sid} {limit_s}{_CRLF_}'.encode()
