import pytest

nkeys_installed = None
try:
    import nkeys
    nkeys_installed = True
except ModuleNotFoundError:
    nkeys_installed = False


def test_create_nkeys():

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

    assert type(operatorPub) == bytes
    assert type(operatorPriv) == bytes
    assert type(operatorSeed) == bytes

    assert operatorPub.startswith(b'O')
    assert operatorPriv.startswith(b'P')
    assert operatorSeed.startswith(b'S')

    accountKP = createAccount()
    accountPub = accountKP.public_key
    accountPriv = accountKP.private_key
    accountSeed = accountKP.seed

    assert type(accountPub) == bytes
    assert type(accountPriv) == bytes
    assert type(accountSeed) == bytes

    assert accountPub.startswith(b'A')
    assert accountPriv.startswith(b'P')
    assert accountSeed.startswith(b'S')

    userKP = createUser()
    userPub = userKP.public_key
    userPriv = userKP.private_key
    userSeed = userKP.seed

    assert type(userPub) == bytes
    assert type(userPriv) == bytes
    assert type(userSeed) == bytes

    assert userPub.startswith(b'U')
    assert userPriv.startswith(b'P')
    assert userSeed.startswith(b'S')


def test_nkeys_from_seed():
    if not nkeys_installed:
        pytest.skip("nkeys not installed")

    from nats.jwt.nkeys import from_seed

    operatorKP = from_seed(
        b'SOALU7LPGJK2BDF7IHD7UZT6ZM23UMKYLGJLNN35QJSUI5BNR4DJRFH4R4'
    )
    assert operatorKP.public_key == b"OCCKR76QCKV4R224WP6ZISXWLXLJZWDF22TRZFQM2I6KUPEDQ3OVCJ6N"

    accountKP = from_seed(
        b'SAALXUEDN2QR5KZDDSH5S4RIWAZDM7CVDG5HNJI2HS5LBVYFTLAQCOXZAU'
    )
    assert accountKP.public_key == b"AB5D6N64ZGUTCGETBW3HSORLTJH5UCCB5CKPZFWCF6UV3KI5BCTRPFDC"

    userKP = from_seed(
        b'SUALJTG5JNRQCQKFE652DV4XID522ALOHJNQVHKKDJNVGWHCLHOEXEROEM'
    )
    assert userKP.public_key == b"UATWOEX5T5LGWJ54SC7H762G5PKSSFPVTJX67IUFENTVPYHA4DPDJUZU"
