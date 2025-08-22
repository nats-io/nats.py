import asyncio

import pytest

nkeys_installed = None
try:
    import nkeys
    nkeys_installed = True
except ModuleNotFoundError:
    nkeys_installed = False

from nats.contrib.accounts.limits import (
    AccountLimits,
    JetStreamLimits,
    NatsLimits,
    OperatorLimits,
)
from nats.contrib.accounts.models import Account
from nats.contrib.claims.generic import GenericFields
from nats.contrib.claims.models import Claims
from nats.contrib.types import Permission, Permissions, Types
from nats.contrib.users.limits import UserPermissionsLimits
from nats.contrib.users.models import User
from tests.utils import (
    NkeysServerTestCase,
    SingleServerTestCase,
    TrustedServerTestCase,
    async_test,
    get_config_file,
)


class ClaimsTest(SingleServerTestCase):

    def test_account_claims(self):

        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        from nats.contrib.nkeys import from_seed

        operatorKP = from_seed(
            b'SOALU7LPGJK2BDF7IHD7UZT6ZM23UMKYLGJLNN35QJSUI5BNR4DJRFH4R4'
        )
        accountKP = from_seed(
            b'SAALXUEDN2QR5KZDDSH5S4RIWAZDM7CVDG5HNJI2HS5LBVYFTLAQCOXZAU'
        )
        userKP = from_seed(
            b'SUALJTG5JNRQCQKFE652DV4XID522ALOHJNQVHKKDJNVGWHCLHOEXEROEM'
        )

        account_claims = Claims(
            name="my-account",
            jti="PBFES33GGIFZM6UGC7NY5ARHRBFVFU4UD7FS2WNLZH3KPGWFVEFQ",
            iat=1678973945,
            iss=operatorKP.public_key.decode(),
            sub=accountKP.public_key.decode(),
            nats=Account(
                limits=OperatorLimits(
                    nats_limits=NatsLimits(data=-1, payload=-1, subs=-1),
                    account_limits=AccountLimits(
                        exports=-1,
                        imports=-1,
                        wildcards=True,
                        conn=-1,
                        leaf=-1
                    ),
                    jetstream_limits=JetStreamLimits(
                        disk_storage=-1, mem_storage=-1
                    )
                ),
                default_permissions=Permissions(),
                generic_fields=GenericFields(version=2, type=Types.Account)
            )
        )

        self.assertEqual(
            account_claims.to_dict(), {
                "iat": 1678973945,
                "name": "my-account",
                "iss":
                    "OCCKR76QCKV4R224WP6ZISXWLXLJZWDF22TRZFQM2I6KUPEDQ3OVCJ6N",
                "jti": "PBFES33GGIFZM6UGC7NY5ARHRBFVFU4UD7FS2WNLZH3KPGWFVEFQ",
                "sub":
                    "AB5D6N64ZGUTCGETBW3HSORLTJH5UCCB5CKPZFWCF6UV3KI5BCTRPFDC",
                "nats": {
                    "default_permissions": {
                        "pub": {},
                        "sub": {}
                    },
                    "limits": {
                        "conn": -1,
                        "data": -1,
                        "disk_storage": -1,
                        "wildcards": True,
                        "exports": -1,
                        "imports": -1,
                        "leaf": -1,
                        "mem_storage": -1,
                        "payload": -1,
                        "subs": -1,
                    },
                    "type": Types.Account,
                    "version": 2
                },
            }
        )

    def test_user_claims(self):

        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        from nats.contrib.nkeys import from_seed

        operatorKP = from_seed(
            b'SOALU7LPGJK2BDF7IHD7UZT6ZM23UMKYLGJLNN35QJSUI5BNR4DJRFH4R4'
        )
        accountKP = from_seed(
            b'SAALXUEDN2QR5KZDDSH5S4RIWAZDM7CVDG5HNJI2HS5LBVYFTLAQCOXZAU'
        )
        userKP = from_seed(
            b'SUALJTG5JNRQCQKFE652DV4XID522ALOHJNQVHKKDJNVGWHCLHOEXEROEM'
        )

        user_claims = Claims(
            name="my-user",
            jti="OJZUDMVGC5WOWNEVFJOEBQG7CGNVRSEZUPM7KTRLE26TX7OPH3TQ",
            iat=1678973945,
            iss=accountKP.public_key.decode(),
            sub=userKP.public_key.decode(),
            nats=User(
                permissions_limits=UserPermissionsLimits(
                    limits=NatsLimits(data=1073741824, payload=-1, subs=-1),
                    permissions=Permissions(
                        pub=Permission(allow=["foo.>", "bar.>"], deny=None),
                        sub=Permission(allow=["_INBOX.>"], deny=None),
                    )
                ),
                generic_fields=GenericFields(version=2, type=Types.User)
            )
        )

        self.assertEqual(
            user_claims.to_dict(), {
                "iat": 1678973945,
                "iss":
                    "AB5D6N64ZGUTCGETBW3HSORLTJH5UCCB5CKPZFWCF6UV3KI5BCTRPFDC",
                "sub":
                    "UATWOEX5T5LGWJ54SC7H762G5PKSSFPVTJX67IUFENTVPYHA4DPDJUZU",
                "jti": "OJZUDMVGC5WOWNEVFJOEBQG7CGNVRSEZUPM7KTRLE26TX7OPH3TQ",
                "name": "my-user",
                "nats": {
                    "data": 1073741824,
                    "payload": -1,
                    "pub": {
                        "allow": ["foo.>", "bar.>"]
                    },
                    "sub": {
                        "allow": ["_INBOX.>"]
                    },
                    "subs": -1,
                    "type": Types.User,
                    "version": 2
                },
            }
        )
