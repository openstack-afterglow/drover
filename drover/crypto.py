"""Drover-owned AES-256-GCM encryption domains."""

from __future__ import annotations

from afterglow_crypto import aesgcm

from drover.config import get_settings

_DOMAIN_KUBECONFIG = b"kubeconfig"
_DOMAIN_NODE_TOKEN = b"node_token"
_DOMAIN_MANAGER_PASSWORD = b"manager_password"


def _get_key() -> bytes:
    hex_key = get_settings().drover_kubeconfig_encryption_key.strip()
    if len(hex_key) != 64:
        raise ValueError(
            "drover_kubeconfig_encryption_key must be 64 hex characters (32 bytes). "
            "Generate with: openssl rand -hex 32"
        )
    try:
        return bytes.fromhex(hex_key)
    except ValueError as exc:
        raise ValueError("drover_kubeconfig_encryption_key must be hexadecimal") from exc


def derive_encryption_subkey(domain: bytes) -> bytes:
    return aesgcm.derive_encryption_subkey(_get_key(), domain)


def encrypt_kubeconfig(plaintext: str) -> str:
    return aesgcm.encrypt(_get_key(), _DOMAIN_KUBECONFIG, plaintext)


def decrypt_kubeconfig(ciphertext: str) -> str:
    return aesgcm.decrypt(_get_key(), _DOMAIN_KUBECONFIG, ciphertext)


def encrypt_node_token(plaintext: str) -> str:
    return aesgcm.encrypt(_get_key(), _DOMAIN_NODE_TOKEN, plaintext)


def decrypt_node_token(ciphertext: str) -> str:
    return aesgcm.decrypt(_get_key(), _DOMAIN_NODE_TOKEN, ciphertext)


def encrypt_manager_password(plaintext: str) -> str:
    return aesgcm.encrypt(_get_key(), _DOMAIN_MANAGER_PASSWORD, plaintext)


def decrypt_manager_password(ciphertext: str) -> str:
    return aesgcm.decrypt(_get_key(), _DOMAIN_MANAGER_PASSWORD, ciphertext)
