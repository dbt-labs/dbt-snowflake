import os
import tempfile
from typing import Generator

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import pytest

from dbt.adapters.snowflake.auth import private_key_from_file, private_key_from_string


PASSPHRASE = "password1234"


def serialize(private_key: rsa.RSAPrivateKey) -> bytes:
    return private_key.private_bytes(
        serialization.Encoding.DER,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


@pytest.fixture(scope="session")
def private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture(scope="session")
def private_key_string(private_key) -> str:
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(PASSPHRASE.encode()),
    )
    return private_key_bytes.decode("utf-8")


@pytest.fixture(scope="session")
def private_key_file(private_key) -> Generator[str, None, None]:
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(PASSPHRASE.encode()),
    )
    file = tempfile.NamedTemporaryFile()
    file.write(private_key_bytes)
    file.seek(0)
    yield file.name
    file.close()


def test_private_key_from_string_pem(private_key_string, private_key):
    assert isinstance(private_key_string, str)
    calculated_private_key = private_key_from_string(private_key_string, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)


def test_private_key_from_file(private_key_file, private_key):
    assert os.path.exists(private_key_file)
    calculated_private_key = private_key_from_file(private_key_file, PASSPHRASE)
    assert serialize(calculated_private_key) == serialize(private_key)
