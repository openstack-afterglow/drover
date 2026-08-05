"""Drover request-scoped OpenStack authorization contracts."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from drover.auth import get_os_conn

pytestmark = pytest.mark.asyncio


_TOKEN_INFO = {
    "token": "caller-scoped-token",
    "project_id": "project-1",
    "user_id": "user-1",
}


async def test_get_os_conn_uses_caller_token_and_closes_connection():
    conn = MagicMock()
    generator = get_os_conn(_TOKEN_INFO)
    with patch("openstack.connect", return_value=conn) as connect:
        yielded = await anext(generator)
        assert yielded is conn
        await generator.aclose()

    kwargs = connect.call_args.kwargs
    assert kwargs["auth_type"] == "token"
    assert kwargs["token"] == "caller-scoped-token"
    assert kwargs["project_id"] == "project-1"
    assert "username" not in kwargs
    assert "password" not in kwargs
    assert conn._afterglow_token == "caller-scoped-token"
    assert conn._afterglow_project_id == "project-1"
    assert conn._afterglow_user_id == "user-1"
    conn.close.assert_called_once_with()


async def test_get_os_conn_fails_closed_when_scoped_connection_fails():
    generator = get_os_conn(_TOKEN_INFO)
    with patch("openstack.connect", side_effect=RuntimeError("Keystone unavailable")):
        with pytest.raises(HTTPException) as exc_info:
            await anext(generator)

    assert exc_info.value.status_code == 401
