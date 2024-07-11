import base64
import sys
from typing import Optional

if sys.version_info < (3, 9):
    from functools import lru_cache

    cache = lru_cache(maxsize=None)
else:
    from functools import cache

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey


@cache
def private_key_from_string(
    private_key_string: str, passphrase: Optional[str] = None
) -> RSAPrivateKey:

    if passphrase:
        encoded_passphrase = passphrase.encode()
    else:
        encoded_passphrase = None

    if private_key_string.startswith("-"):
        return serialization.load_pem_private_key(
            data=bytes(private_key_string, "utf-8"),
            password=encoded_passphrase,
            backend=default_backend(),
        )
    return serialization.load_der_private_key(
        data=base64.b64decode(private_key_string),
        password=encoded_passphrase,
        backend=default_backend(),
    )


@cache
def private_key_from_file(
    private_key_path: str, passphrase: Optional[str] = None
) -> RSAPrivateKey:

    if passphrase:
        encoded_passphrase = passphrase.encode()
    else:
        encoded_passphrase = None

    with open(private_key_path, "rb") as file:
        private_key_bytes = file.read()

    return serialization.load_pem_private_key(
        data=private_key_bytes,
        password=encoded_passphrase,
        backend=default_backend(),
    )
