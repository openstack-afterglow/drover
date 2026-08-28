"""Drover Operations & Events API router."""

from __future__ import annotations

import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from drover.auth import get_token_info
from drover.models.orm import DroverOperation, DroverOperationEvent
from drover.models.schemas import DroverOperationEventInfo, DroverOperationInfo
from drover.policy import authorize
from drover.services import operations

router = APIRouter()
_logger = logging.getLogger(__name__)


def _format_dt(dt: object) -> str | None:
    if isinstance(dt, datetime):
        return dt.isoformat()
    if dt is not None:
        return str(dt)
    return None


def _operation_to_info(op: DroverOperation) -> DroverOperationInfo:
    return DroverOperationInfo(
        id=op.id,
        project_id=op.project_id,
        cluster_id=op.cluster_id,
        kind=op.kind,
        status=op.status,
        request_id=op.request_id,
        idempotency_key=op.idempotency_key,
        request_hash=op.request_hash,
        error=op.error,
        created_at=_format_dt(op.created_at),
        started_at=_format_dt(op.started_at),
        finished_at=_format_dt(op.finished_at),
    )


def _event_to_info(ev: DroverOperationEvent) -> DroverOperationEventInfo:
    return DroverOperationEventInfo(
        id=ev.id,
        operation_id=ev.operation_id,
        sequence=ev.sequence,
        phase=ev.phase,
        message=ev.message,
        payload_json=ev.payload_json,
        created_at=_format_dt(ev.created_at),
    )


@router.get("/{operation_id}", response_model=DroverOperationInfo)
async def get_operation(
    operation_id: str,
    token_info: dict = Depends(get_token_info),
):
    """Retrieve an operation by ID. Enforces project isolation (returns 404 for cross-tenant)."""
    op = await operations.get_operation(None, operation_id)
    if op is None or not authorize("drover:operations:get", {"project_id": op.project_id}, token_info, do_raise=False):
        raise HTTPException(status_code=404, detail="Operation not found")
    return _operation_to_info(op)


@router.get("/{operation_id}/events", response_model=list[DroverOperationEventInfo])
async def list_operation_events(
    operation_id: str,
    since_sequence: int = Query(default=0, ge=0),
    token_info: dict = Depends(get_token_info),
):
    """Retrieve sequenced events for an operation. Enforces project isolation (returns 404 for cross-tenant)."""
    op = await operations.get_operation(None, operation_id)
    if op is None or not authorize("drover:operations:get", {"project_id": op.project_id}, token_info, do_raise=False):
        raise HTTPException(status_code=404, detail="Operation not found")

    events = await operations.get_operation_events(None, operation_id, since_sequence=since_sequence)
    return [_event_to_info(ev) for ev in events]
