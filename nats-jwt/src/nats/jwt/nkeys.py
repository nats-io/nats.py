import os
from enum import Enum, IntEnum
from typing import Union

import nats
from nats.jwt.constants import CurveKeyLen, Prefix
from nkeys import KeyPair, decode_seed, encode_seed, from_seed


def create_pair(prefix: Prefix) -> KeyPair:
    length = CurveKeyLen if prefix == Prefix.Curve else 32
    raw_seed = os.urandom(length)
    seed_str = encode_seed(raw_seed, prefix.value)

    if prefix == Prefix.Curve:
        raise NotImplementedError()
        # return CurveKP(raw_seed)
    else:
        return from_seed(seed_str)


def createOperator() -> KeyPair:
    ''' Creates a KeyPair with an operator prefix
    '''
    return create_pair(Prefix.Operator)


def createAccount() -> KeyPair:
    ''' Creates a KeyPair with an account prefix
    '''
    return create_pair(Prefix.Account)


def createUser() -> KeyPair:
    ''' Creates a KeyPair with a user prefix
    '''
    return create_pair(Prefix.User)


def createCluster() -> KeyPair:
    return create_pair(Prefix.Cluster)


def createServer() -> KeyPair:
    return create_pair(Prefix.Server)
