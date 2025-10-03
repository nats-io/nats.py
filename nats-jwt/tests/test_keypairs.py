import asyncio

import pytest

nkeys_installed = None
try:
    import nkeys
    nkeys_installed = True
except ModuleNotFoundError:
    nkeys_installed = False

from tests.utils import (
    NkeysServerTestCase,
    TrustedServerTestCase,
    async_test,
    get_config_file,
)


class NkeysTest(NkeysServerTestCase):

    def test_create_nkeys(self):

        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        from nats.jwt.nkeys import (
            createAccount,
            createOperator,
            createUser,
        )

        operatorKP = createOperator()
        operatorPub = operatorKP.public_key
        operatorPriv = operatorKP.private_key
        operatorSeed = operatorKP.seed

        self.assertEqual(type(operatorPub), bytes)
        self.assertEqual(type(operatorPriv), bytes)
        self.assertEqual(type(operatorSeed), bytes)

        self.assertTrue(operatorPub.startswith(b'O'))
        self.assertTrue(operatorPriv.startswith(b'P'))
        self.assertTrue(operatorSeed.startswith(b'S'))

        accountKP = createAccount()
        accountPub = accountKP.public_key
        accountPriv = accountKP.private_key
        accountSeed = accountKP.seed

        self.assertEqual(type(accountPub), bytes)
        self.assertEqual(type(accountPriv), bytes)
        self.assertEqual(type(accountSeed), bytes)

        self.assertTrue(accountPub.startswith(b'A'))
        self.assertTrue(accountPriv.startswith(b'P'))
        self.assertTrue(accountSeed.startswith(b'S'))

        userKP = createUser()
        userPub = userKP.public_key
        userPriv = userKP.private_key
        userSeed = userKP.seed

        self.assertEqual(type(userPub), bytes)
        self.assertEqual(type(userPriv), bytes)
        self.assertEqual(type(userSeed), bytes)

        self.assertTrue(userPub.startswith(b'U'))
        self.assertTrue(userPriv.startswith(b'P'))
        self.assertTrue(userSeed.startswith(b'S'))

    def test_nkeys_from_seed(self):
        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        from nats.jwt.nkeys import from_seed

        operatorKP = from_seed(
            b'SOALU7LPGJK2BDF7IHD7UZT6ZM23UMKYLGJLNN35QJSUI5BNR4DJRFH4R4'
        )
        self.assertEqual(
            operatorKP.public_key,
            b"OCCKR76QCKV4R224WP6ZISXWLXLJZWDF22TRZFQM2I6KUPEDQ3OVCJ6N"
        )

        accountKP = from_seed(
            b'SAALXUEDN2QR5KZDDSH5S4RIWAZDM7CVDG5HNJI2HS5LBVYFTLAQCOXZAU'
        )
        self.assertEqual(
            accountKP.public_key,
            b"AB5D6N64ZGUTCGETBW3HSORLTJH5UCCB5CKPZFWCF6UV3KI5BCTRPFDC"
        )

        userKP = from_seed(
            b'SUALJTG5JNRQCQKFE652DV4XID522ALOHJNQVHKKDJNVGWHCLHOEXEROEM'
        )
        self.assertEqual(
            userKP.public_key,
            b"UATWOEX5T5LGWJ54SC7H762G5PKSSFPVTJX67IUFENTVPYHA4DPDJUZU"
        )
