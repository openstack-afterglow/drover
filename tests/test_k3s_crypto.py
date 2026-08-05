from __future__ import annotations

import base64
import os
from types import SimpleNamespace

import pytest
from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from drover import crypto

_KEY = bytes.fromhex("0123456789abcdef" * 4)


@pytest.fixture
def fixed_key(monkeypatch):
    monkeypatch.setattr(crypto, "_get_key", lambda: _KEY)


@pytest.mark.parametrize(
    ("encrypt", "decrypt", "value"),
    [
        (crypto.encrypt_kubeconfig, crypto.decrypt_kubeconfig, "apiVersion: v1\nkind: Config\n"),
        (crypto.encrypt_node_token, crypto.decrypt_node_token, "K10secret::server:value"),
        (crypto.encrypt_manager_password, crypto.decrypt_manager_password, "manager-secret"),
    ],
)
def test_drover_crypto_round_trip(encrypt, decrypt, value, fixed_key):
    ciphertext = encrypt(value)
    assert ciphertext.startswith("v3:")
    assert decrypt(ciphertext) == value


def test_crypto_domains_are_isolated(fixed_key):
    ciphertext = crypto.encrypt_kubeconfig("secret")
    with pytest.raises(InvalidTag):
        crypto.decrypt_node_token(ciphertext)


def test_legacy_ciphertext_remains_decryptable(fixed_key):
    nonce = os.urandom(12)
    encrypted = AESGCM(_KEY).encrypt(nonce, b"legacy", None)
    ciphertext = base64.b64encode(nonce + encrypted).decode()
    assert crypto.decrypt_kubeconfig(ciphertext) == "legacy"


def test_invalid_key_length_fails_closed(monkeypatch):
    monkeypatch.setattr(crypto, "get_settings", lambda: SimpleNamespace(drover_kubeconfig_encryption_key="ab"))
    with pytest.raises(ValueError, match="64 hex characters"):
        crypto._get_key()


def test_non_hex_key_fails_closed(monkeypatch):
    monkeypatch.setattr(crypto, "get_settings", lambda: SimpleNamespace(drover_kubeconfig_encryption_key="g" * 64))
    with pytest.raises(ValueError, match="hexadecimal"):
        crypto._get_key()
