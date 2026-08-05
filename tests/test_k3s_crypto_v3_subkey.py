from __future__ import annotations

from afterglow_crypto import aesgcm

from drover import crypto

_KEY = bytes.fromhex("0123456789abcdef" * 4)


def test_owned_domains_derive_distinct_subkeys():
    domains = (
        crypto._DOMAIN_KUBECONFIG,
        crypto._DOMAIN_NODE_TOKEN,
        crypto._DOMAIN_MANAGER_PASSWORD,
    )
    subkeys = {aesgcm.derive_encryption_subkey(_KEY, domain) for domain in domains}
    assert len(subkeys) == len(domains)


def test_ciphertext_uses_random_nonce(monkeypatch):
    monkeypatch.setattr(crypto, "_get_key", lambda: _KEY)
    first = crypto.encrypt_kubeconfig("same")
    second = crypto.encrypt_kubeconfig("same")
    assert first != second
