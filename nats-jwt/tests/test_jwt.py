import asyncio

import pytest

nkeys_installed = None
try:
    import nkeys
    nkeys_installed = True
except ModuleNotFoundError:
    nkeys_installed = False

import base64
import json

from nats.jwt.jwt import Algorithms
from nats.jwt.utils import bytes_serializer
from tests.utils import SingleServerTestCase, async_test


def encode_account_claim(account_claims, operator_key_pair, options):

    header = json.dumps({
        "typ": "JWT",
        "alg": Algorithms.v2
    },
                        separators=(',', ':')).encode()
    jwt_header = base64.urlsafe_b64encode(header).decode().rstrip("=")

    payload = {
        "jti": options["jti"],
        "iat": options["iat"],
        "iss": options["iss"],
        "name": account_claims["name"],
        "sub": account_claims["sub"],
        "nats": {
            "limits": {
                "subs": account_claims["nats"]["limits"]["subs"],
                "data": account_claims["nats"]["limits"]["data"],
                "payload": account_claims["nats"]["limits"]["payload"],
                "imports": account_claims["nats"]["limits"]["imports"],
                "exports": account_claims["nats"]["limits"]["exports"],
                "wildcards": account_claims["nats"]["limits"]["wildcards"],
                "conn": account_claims["nats"]["limits"]["conn"],
                "leaf": account_claims["nats"]["limits"]["leaf"],
                "mem_storage": account_claims["nats"]["limits"]["mem_storage"],
                "disk_storage":
                    account_claims["nats"]["limits"]["disk_storage"],
            },
            "default_permissions": {
                "pub": account_claims["nats"]["default_permissions"]["pub"],
                "sub": account_claims["nats"]["default_permissions"]["sub"]
            },
            "type": options["type"],
            "version": options["version"]
        }
    }

    payload = json.dumps(
        payload, default=bytes_serializer, separators=(',', ':')
    )
    jwt_payload = base64.urlsafe_b64encode(payload.encode()
                                           ).decode().rstrip("=")

    jwt_header_payload = f"{jwt_header}.{jwt_payload}"

    jwt_signature = base64.urlsafe_b64encode(
        operator_key_pair.sign(jwt_header_payload.encode())
    ).decode().rstrip("=")

    jwt_token = f"{jwt_header}.{jwt_payload}.{jwt_signature}"
    return jwt_token


def encode_user_claim(user_claims, account_key_pair, options):

    header = json.dumps({
        "typ": "JWT",
        "alg": Algorithms.v2
    },
                        separators=(',', ':')).encode()
    jwt_header = base64.urlsafe_b64encode(header).decode().rstrip("=")

    payload = {
        "jti": options["jti"],
        "iat": options["iat"],
        "iss": options["iss"],
        "name": user_claims["name"],
        "sub": user_claims["sub"],
        "nats": {
            "pub": {
                "allow": user_claims["nats"]["pub"]["allow"],
            },
            "sub": {
                "allow": user_claims["nats"]["sub"]["allow"]
            },
            "subs": user_claims["nats"]["subs"],
            "data": user_claims["nats"]["data"],
            "payload": user_claims["nats"]["payload"],
            "type": options["type"],
            "version": options["version"]
        }
    }

    payload = json.dumps(
        payload, default=bytes_serializer, separators=(',', ':')
    )
    jwt_payload = base64.urlsafe_b64encode(
        payload.encode().replace(
            b">", b"\u003e"
        )  # Unicode as per example in https://natsbyexample.com/examples/auth/nkeys-jwts/go
    ).decode().rstrip("=")

    jwt_header_payload = f"{jwt_header}.{jwt_payload}"

    jwt_signature = base64.urlsafe_b64encode(
        account_key_pair.sign(jwt_header_payload.encode())
    ).decode().rstrip("=")

    jwt_token = f"{jwt_header}.{jwt_payload}.{jwt_signature}"

    return jwt_token


class JWTTest(SingleServerTestCase):

    def test_account_claim_jwt(self):
        # NOTE: Test replicated from https://natsbyexample.com/examples/auth/nkeys-jwts/go

        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        from nats.jwt.nkeys import from_seed

        operatorKP = from_seed(
            b'SOALU7LPGJK2BDF7IHD7UZT6ZM23UMKYLGJLNN35QJSUI5BNR4DJRFH4R4'
        )
        accountKP = from_seed(
            b'SAALXUEDN2QR5KZDDSH5S4RIWAZDM7CVDG5HNJI2HS5LBVYFTLAQCOXZAU'
        )
        userKP = from_seed(
            b'SUALJTG5JNRQCQKFE652DV4XID522ALOHJNQVHKKDJNVGWHCLHOEXEROEM'
        )

        account_claim = {
            "name": "my-account",
            "nats": {
                "default_permissions": {
                    "pub": {},
                    "sub": {}
                },
                "limits": {
                    "conn": -1,
                    "data": -1,
                    "disk_storage": -1,
                    "exports": -1,
                    "imports": -1,
                    "leaf": -1,
                    "mem_storage": -1,
                    "payload": -1,
                    "subs": -1,
                    "wildcards": True
                }
            },
            "sub": accountKP.public_key.decode()
        }
        opts = {
            "jti": "PBFES33GGIFZM6UGC7NY5ARHRBFVFU4UD7FS2WNLZH3KPGWFVEFQ",
            "iat": 1678973945,
            "iss": operatorKP.public_key.decode(),
            "type": "account",
            "version": 2
        }
        account_claim_jwt_token = encode_account_claim(
            account_claim, operatorKP, options=opts
        )

        self.assertEqual(
            account_claim_jwt_token,
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJQQkZFUzMzR0dJRlpNNlVHQzdOWTVBUkhSQkZWRlU0VUQ3RlMyV05MWkgzS1BHV0ZWRUZRIiwiaWF0IjoxNjc4OTczOTQ1LCJpc3MiOiJPQ0NLUjc2UUNLVjRSMjI0V1A2WklTWFdMWExKWldERjIyVFJaRlFNMkk2S1VQRURRM09WQ0o2TiIsIm5hbWUiOiJteS1hY2NvdW50Iiwic3ViIjoiQUI1RDZONjRaR1VUQ0dFVEJXM0hTT1JMVEpINVVDQ0I1Q0tQWkZXQ0Y2VVYzS0k1QkNUUlBGREMiLCJuYXRzIjp7ImxpbWl0cyI6eyJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJpbXBvcnRzIjotMSwiZXhwb3J0cyI6LTEsIndpbGRjYXJkcyI6dHJ1ZSwiY29ubiI6LTEsImxlYWYiOi0xLCJtZW1fc3RvcmFnZSI6LTEsImRpc2tfc3RvcmFnZSI6LTF9LCJkZWZhdWx0X3Blcm1pc3Npb25zIjp7InB1YiI6e30sInN1YiI6e319LCJ0eXBlIjoiYWNjb3VudCIsInZlcnNpb24iOjJ9fQ.4-kUapoPK_9A9L_CfJRBEe1XukgVBGaSU3J5tBFbajF3G5660BORa2CRUnN6x0dv-jUgui5EIQeANDB5kh_wDw"
        )

    def test_user_claim_jwt(self):
        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        from nats.jwt.nkeys import from_seed

        operatorKP = from_seed(
            b'SOALU7LPGJK2BDF7IHD7UZT6ZM23UMKYLGJLNN35QJSUI5BNR4DJRFH4R4'
        )
        accountKP = from_seed(
            b'SAALXUEDN2QR5KZDDSH5S4RIWAZDM7CVDG5HNJI2HS5LBVYFTLAQCOXZAU'
        )
        userKP = from_seed(
            b'SUALJTG5JNRQCQKFE652DV4XID522ALOHJNQVHKKDJNVGWHCLHOEXEROEM'
        )

        user_claim = {
            "name": "my-user",
            "sub": userKP.public_key.decode(),
            "nats": {
                "pub": {
                    "allow": ["foo.>", "bar.>"]
                },
                "sub": {
                    "allow": ["_INBOX.>"]
                },
                "subs": -1,
                "data": 1073741824,
                "payload": -1,
            }
        }
        opts = {
            "jti": "OJZUDMVGC5WOWNEVFJOEBQG7CGNVRSEZUPM7KTRLE26TX7OPH3TQ",
            "iat": 1678973945,
            "iss": accountKP.public_key.decode(),
            "type": "user",
            "version": 2
        }
        user_claim_jwt_token = encode_user_claim(
            user_claim, accountKP, options=opts
        )

        self.assertEqual(
            user_claim_jwt_token,
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJPSlpVRE1WR0M1V09XTkVWRkpPRUJRRzdDR05WUlNFWlVQTTdLVFJMRTI2VFg3T1BIM1RRIiwiaWF0IjoxNjc4OTczOTQ1LCJpc3MiOiJBQjVENk42NFpHVVRDR0VUQlczSFNPUkxUSkg1VUNDQjVDS1BaRldDRjZVVjNLSTVCQ1RSUEZEQyIsIm5hbWUiOiJteS11c2VyIiwic3ViIjoiVUFUV09FWDVUNUxHV0o1NFNDN0g3NjJHNVBLU1NGUFZUSlg2N0lVRkVOVFZQWUhBNERQREpVWlUiLCJuYXRzIjp7InB1YiI6eyJhbGxvdyI6WyJmb28uXHUwMDNlIiwiYmFyLlx1MDAzZSJdfSwic3ViIjp7ImFsbG93IjpbIl9JTkJPWC5cdTAwM2UiXX0sInN1YnMiOi0xLCJkYXRhIjoxMDczNzQxODI0LCJwYXlsb2FkIjotMSwidHlwZSI6InVzZXIiLCJ2ZXJzaW9uIjoyfX0.PYT1aJJXiJd9Jb5b1m03jBs64GJzjKRtLOH4hoKJ8v8MKk13nhzGFtKcIKn2vg00uYBYlOkWPgzJ6hYuKr0ECA"
        )
