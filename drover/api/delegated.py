"""HTTP adapters for requester-owned execution authority admission."""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator

from fastapi import HTTPException

from drover.services import cluster_authority, delegation


@contextlib.asynccontextmanager
async def admitted_operation(
    token_info: dict, *, project_id: str, cluster_id: str, action: str
) -> AsyncIterator[delegation.AdmittedDelegation]:
    """Admit a bounded requester trust for one job; unused trusts are deleted with the caller token."""
    try:
        async with delegation.admission(
            token_info, project_id=project_id, cluster_id=cluster_id, action=action
        ) as admitted:
            yield admitted
    except delegation.DelegationDenied as exc:
        raise HTTPException(status_code=403, detail=f"Delegated execution denied: {exc}") from exc
    except delegation.DelegationUnavailable as exc:
        raise HTTPException(status_code=503, detail="Keystone delegation is temporarily unavailable") from exc


def require_cluster_project(token_info: dict, cluster: dict) -> str:
    """Delegation is created from the caller's token, so it must be scoped to the cluster's project."""
    project_id = cluster.get("project_id") or ""
    if token_info.get("project_id") != project_id:
        raise HTTPException(
            status_code=403,
            detail="Mutations require a token scoped to the cluster's project; no cross-project service authority exists",
        )
    return project_id


async def issue_credentials(conn, token_info: dict, *, project_id: str, cluster_id: str, generation: int,
                            purposes: list[str]) -> list[cluster_authority.IssuedCredential]:
    """Create the caller's restricted resource credentials or translate the failure to HTTP."""
    from openstack import exceptions as os_exc

    try:
        return await cluster_authority.issue(
            conn, token_info, project_id=project_id, cluster_id=cluster_id, generation=generation, purposes=purposes
        )
    except (delegation.DelegationDenied, PermissionError, os_exc.ForbiddenException) as exc:
        raise HTTPException(status_code=403, detail="Resource credential issuance denied for the requester") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="Keystone application credential issuance is unavailable") from exc
