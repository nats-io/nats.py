PUB_OP = 'PUB'
HPUB_OP = 'HPUB'
SUB_OP = 'SUB'
UNSUB_OP = 'UNSUB'
_CRLF_ = '\r\n'


def pub_cmd(subject: str, reply: str, payload: bytes) -> bytes:
    return f'{PUB_OP} {subject} {reply} {len(payload)}{_CRLF_}'.encode(
    ) + payload + _CRLF_.encode()


def hpub_cmd(subject: str, reply: str, hdr: bytes, payload: bytes) -> bytes:
    hdr_len = len(hdr)
    total_size = len(payload) + hdr_len
    return f'{HPUB_OP} {subject} {reply} {hdr_len} {total_size}{_CRLF_}'.encode(
    ) + hdr + payload + _CRLF_.encode()


def sub_cmd(subject: str, queue: str, sid: int) -> bytes:
    return f'{SUB_OP} {subject} {queue} {sid}{_CRLF_}'.encode()


def unsub_cmd(sid: int, limit: int) -> bytes:
    limit_s = '' if limit == 0 else f'{limit}'
    return f'{UNSUB_OP} {sid} {limit_s}{_CRLF_}'.encode()
