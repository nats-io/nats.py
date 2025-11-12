import re
from enum import Enum


class Algorithms(str, Enum):
    v1 = "ed25519"
    v2 = "ed25519-nkey"


nkeys_installed = None
try:
    import nkeys
    nkeys_installed = True
except ModuleNotFoundError:
    nkeys_installed = False

if nkeys_installed:

    def fmt_creds(token: str, kp: nkeys.KeyPair) -> bytes:
        seed = kp.seed.decode()
        return f'''-----BEGIN NATS USER JWT-----
{token}
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
{seed}
------END USER NKEY SEED------
'''.encode()

else:

    def fmt_creds(*args, **kwargs) -> bytes:
        raise Exception("Nkeys is not installed")


# def parse_creds(creds: bytes):
#     text = creds.decode()
#     pattern = re.compile(r"\s*(?:[-]{3,}[^\n]*[-]{3,}\n)(.+?)(?:\n\s*[-]{3,}[^\n]*[-]{3,}\n)", re.DOTALL)
#     matches = pattern.findall(text)
#     if len(matches) != 2:
#         raise ValueError("bad credentials")
#     jwt = matches[0].strip()
#     key = matches[1].strip()
#     uc = decode(jwt)
#     aid = uc["nats"].get("issuer_account", uc["iss"])
#     return {"key": key, "jwt": jwt, "uc": uc, "aid": aid}
