from typing import List, Optional, Union

from nats.jwt.accounts.models import Account
from nats.jwt.flatten_model import FlatteningModel
from nats.jwt.operator.models import Operator
from nats.jwt.types import Types
from nats.jwt.users.models import User


class Claims(FlatteningModel):
    # Claims Data
    exp: Optional[int]  # Expires   int64  `json:"exp,omitempty"`
    jti: Optional[str]  # ID        string `json:"jti,omitempty"`
    iat: Optional[int]  # IssuedAt  int64  `json:"iat,omitempty"`
    iss: Optional[str]  # Issuer    string `json:"iss,omitempty"`
    name: Optional[str]  # Name      string `json:"name,omitempty"`
    nbf: Optional[int]  # NotBefore int64  `json:"nbf,omitempty"`
    sub: Optional[str]  # Subject   string `json:"sub,omitempty"`

    # Nats Data
    nats: Optional[Union[User, Account, Operator]]
    issuer_account: Optional[Union[str, bytes]]

    def __init__(
        self,
        exp: Optional[int] = None,
        jti: Optional[str] = None,
        iat: Optional[int] = None,
        iss: Optional[str] = None,
        name: Optional[str] = None,
        nbf: Optional[int] = None,
        sub: Optional[str] = None,
        nats: Optional[Union[User, Account, Operator]] = None,
        issuer_account: Optional[Union[str, bytes]] = None,
    ):
        self.exp: Optional[int] = exp
        self.jti: Optional[str] = jti
        self.iat: Optional[int] = iat
        self.iss: Optional[str] = iss
        self.name: Optional[str] = name
        self.nbf: Optional[int] = nbf
        self.sub: Optional[str] = sub

        self.nats = nats
        self.issuer_account = issuer_account

    class Meta:
        unflatten_fields = [('nats', 'nats')]


class UserClaims(Claims):
    pass


class AccountClaims(Claims):
    pass


class OperatorClaims(Claims):
    pass
