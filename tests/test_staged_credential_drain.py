"""Staged credential verification must not retain discarded HTTP pools."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from keystoneauth1 import exceptions as ks_exc

from drover.services import cluster_authority
from drover.services.execution import AuthorityRevoked, ExecutionAuthorityUnavailable


def test_staged_credential_verification_closes_authenticated_session(monkeypatch):
    session = SimpleNamespace(session=SimpleNamespace(close=Mock()))
    authenticate = Mock(return_value=session)
    monkeypatch.setattr(cluster_authority, "_authenticate_sync", authenticate)
    snapshot = object()
    cluster_authority._verify_staged_credential_sync(snapshot, "synthetic-secret")
    authenticate.assert_called_once_with(snapshot, "synthetic-secret")
    session.session.close.assert_called_once_with()


@pytest.mark.parametrize("error,expected", [
    (ks_exc.Unauthorized, AuthorityRevoked),
    (ks_exc.Forbidden, AuthorityRevoked),
    (ks_exc.NotFound, AuthorityRevoked),
    (ks_exc.ClientException, ExecutionAuthorityUnavailable),
])
def test_staged_authentication_failure_closes_session(monkeypatch, error, expected):
    session = SimpleNamespace(session=SimpleNamespace(close=Mock()))
    plugin = SimpleNamespace(get_access=Mock(side_effect=error()))
    monkeypatch.setattr(cluster_authority, "get_settings", lambda: SimpleNamespace(
        os_auth_url="https://keystone.invalid/v3", ssl_verify=True,
    ))
    monkeypatch.setattr(cluster_authority.v3, "ApplicationCredential", Mock(return_value=plugin))
    monkeypatch.setattr(cluster_authority.ks_session, "Session", Mock(return_value=session))
    snapshot = SimpleNamespace(app_credential_id="synthetic-credential")
    with pytest.raises(expected):
        cluster_authority._verify_staged_credential_sync(snapshot, "synthetic-secret")
    session.session.close.assert_called_once_with()


def test_staged_token_scope_mismatch_closes_session(monkeypatch):
    session = SimpleNamespace(session=SimpleNamespace(close=Mock()))
    access = SimpleNamespace(trust_scoped=True, _application_credential={})
    plugin = SimpleNamespace(get_access=Mock(return_value=access))
    monkeypatch.setattr(cluster_authority, "get_settings", lambda: SimpleNamespace(
        os_auth_url="https://keystone.invalid/v3", ssl_verify=True,
    ))
    monkeypatch.setattr(cluster_authority.v3, "ApplicationCredential", Mock(return_value=plugin))
    monkeypatch.setattr(cluster_authority.ks_session, "Session", Mock(return_value=session))
    snapshot = SimpleNamespace(app_credential_id="synthetic-credential")
    with pytest.raises(AuthorityRevoked, match="does not match"):
        cluster_authority._verify_staged_credential_sync(snapshot, "synthetic-secret")
    session.session.close.assert_called_once_with()
