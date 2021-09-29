from .constants import _CRLF_, _SPC_, HPUB_OP, PUB_OP, SUB_OP, UNSUB_OP


def pub_cmd(subject: str, reply: str, payload: bytes) -> bytes:
    return PUB_OP + _SPC_ + f'{subject} {reply} {len(payload)}'.encode(
    ) + _CRLF_ + payload + _CRLF_


def hpub_cmd(subject: str, reply: str, hdr: bytes, payload: bytes) -> bytes:
    hdr_len = len(hdr)
    total_size = len(payload) + hdr_len
    return HPUB_OP + _SPC_ + f'{subject} {reply} {hdr_len} {total_size}'.encode(
    ) + _CRLF_ + hdr + payload + _CRLF_


def sub_cmd(subject: str, queue: str, sid: int) -> bytes:
    return SUB_OP + _SPC_ + f'{subject} {queue} {sid}'.encode() + _CRLF_


def unsub_cmd(sid: int, limit: int) -> bytes:
    limit_s = '' if limit == 0 else f'{limit}'
    return UNSUB_OP + _SPC_ + f'{sid} {limit_s}'.encode() + _CRLF_
