from typing import List, Union

from nkeys import KeyPair, from_seed

Key = Union[str, bytes, KeyPair]


def from_public(public_key):
    raise NotImplementedError()


def parse_key(v: Union[str, bytes]) -> KeyPair:
    if isinstance(v, bytes):
        v = v.decode('utf-8')
    if v[0] == "S":
        return from_seed(v.encode('utf-8'))
    return from_public(v)


def check_key(
    v: Key, type: Union[str, List[str]] = "", seed: bool = False
) -> KeyPair:
    if isinstance(v, (str, bytes)):
        kp: KeyPair = parse_key(v)
    else:
        kp = v

    k = kp.public_key.decode()
    types = []

    if isinstance(type, list):
        types.extend(type)
    elif type != "":
        types.append(type)

    if len(types) > 0 and k[0] not in types:
        raise ValueError(f"unexpected type {k[0]} - wanted {types}")

    if seed:
        kp.private_key  # may raise if not seed

    return kp
