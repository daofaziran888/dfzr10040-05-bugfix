"""统一资源接口：查询、导出、登记和动作按模块 key 分派到领域服务。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    ClaimPayload,
    EntryPayload,
    HoldTerminalPayload,
    PageResult,
    ReplayReceiptPayload,
    ResolveTerminalPayload,
    StagedRowsPayload,
)
from app.services import service_for
from app.services.backfill_task import BackfillTaskService
from app.services.replay_queue import ReplayQueueService

router = APIRouter(prefix="/api", tags=["业务资源"])


def require_service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def _backfill_service() -> BackfillTaskService:
    service = service_for("backfill_task")
    assert isinstance(service, BackfillTaskService)
    return service


def _replay_service() -> ReplayQueueService:
    service = service_for("replay_queue")
    assert isinstance(service, ReplayQueueService)
    return service


@router.get("/{module}", response_model=PageResult[dict])
def list_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, total = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{module}/export")
def export_entries(
    module: str,
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    service = require_service(module)
    filters = {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in service.LIST_FIELDS and value.strip()
    }
    items, _ = service.list_entries(
        filters=filters, keyword=keyword, status=status, page=1, size=1_000_000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *service.LIST_FIELDS, "status"])
    for item in items:
        values = [item.get("id"), *(item.get(field, "") for field in service.LIST_FIELDS),
                  item.get("status", "")]
        writer.writerow([
            "'" + str(value) if str(value or "").startswith(("=", "+", "-", "@")) else value
            for value in values
        ])
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{module}.csv"'},
    )


@router.get("/backfill_task/next-range", response_model=dict)
def backfill_next_range() -> dict[str, Any]:
    """下一次可领的数据范围：由生效终态回执与已落地证据共同决定。"""
    return _backfill_service().next_claimable_range()


@router.get("/backfill_task/segments/{segment_code}/detail", response_model=dict)
def backfill_segment_detail(segment_code: str) -> dict[str, Any]:
    """回填详情：终态、两份回执与片段里已经生效的行。"""
    return _backfill_service().segment_detail(segment_code)


@router.get("/backfill_task/segments/{segment_code}/wide-table", response_model=dict)
def backfill_segment_wide_table(segment_code: str) -> dict[str, Any]:
    """读取宽表里该片段的行明细，用于核对行数。"""
    return _backfill_service().wide_table_detail(segment_code)


@router.post("/backfill_task/claims", response_model=dict)
def backfill_claim(payload: ClaimPayload) -> dict[str, Any]:
    """领取一次后续工作，只给出范围，不写宽表。"""
    return _backfill_service().claim_next(payload.worker)


@router.get("/backfill_task/claims/{claim_code}", response_model=dict)
def backfill_claim_detail(claim_code: str) -> dict[str, Any]:
    record = _backfill_service().claim_detail(claim_code)
    if record is None:
        raise HTTPException(status_code=404, detail=f"领取单 {claim_code} 不存在")
    return record


@router.post("/backfill_task/claims/{claim_code}/execute", response_model=dict)
def backfill_claim_execute(claim_code: str) -> dict[str, Any]:
    """按领取实际发起补采；宽表按 (片段, 行标识) 幂等交付。"""
    return _backfill_service().execute_claim(claim_code)


@router.post("/backfill_task/receipts", response_model=dict)
def backfill_submit_receipt(payload: ReplayReceiptPayload) -> dict[str, Any]:
    """接收重放终态回执；同一尝试只认一个终态，冲突回执保留证据。"""
    return _backfill_service().submit_receipt(
        segment_code=payload.segment_code,
        attempt_no=payload.attempt_no,
        outcome=payload.outcome,
        error_fragment=payload.error_fragment,
        row_keys=payload.row_keys,
        write_credential=payload.write_credential,
    )


@router.post("/backfill_task/staged-rows", response_model=dict)
def backfill_stage_rows(payload: StagedRowsPayload) -> dict[str, Any]:
    """登记回填端暂存行与写入凭据，先于终态回执落定。"""
    return _backfill_service().stage_rows(
        segment_code=payload.segment_code,
        attempt_no=payload.attempt_no,
        row_keys=payload.row_keys,
        write_credential=payload.write_credential,
    )


@router.post("/backfill_task/hold-terminal", response_model=dict)
def backfill_hold_terminal(payload: HoldTerminalPayload) -> dict[str, Any]:
    """暂扣某次尝试的终态确认，等待写入证据。"""
    return _backfill_service().hold_terminal(
        segment_code=payload.segment_code,
        attempt_no=payload.attempt_no,
        reason=payload.reason,
    )


@router.post("/backfill_task/resolve-terminal", response_model=dict)
def backfill_resolve_terminal(payload: ResolveTerminalPayload) -> dict[str, Any]:
    """写入证据齐全时落定唯一终态。"""
    return _backfill_service().resolve_terminal(
        segment_code=payload.segment_code,
        attempt_no=payload.attempt_no,
    )


@router.get("/replay_queue/receipts", response_model=list[dict])
def replay_receipts(segment_code: str | None = Query(default=None)) -> list[dict[str, Any]]:
    return _replay_service().terminal_receipts(segment_code)


@router.get("/replay_queue/held", response_model=list[dict])
def replay_held() -> list[dict[str, Any]]:
    return _replay_service().held_terminals()


@router.post("/replay_queue/{entry_id}/confirm-terminal", response_model=ActionResult)
def replay_confirm_terminal(entry_id: int) -> ActionResult:
    service = _replay_service()
    entry, message = service.confirm_terminal(entry_id)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.get("/{module}/{entry_id}", response_model=dict)
def get_entry(module: str, entry_id: int) -> dict[str, Any]:
    service = require_service(module)
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"{service.ENTITY} {entry_id} 不存在或已归档")
    return entry


@router.post("/{module}", response_model=ActionResult)
def create_entry(module: str, payload: EntryPayload) -> ActionResult:
    service = require_service(module)
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message=f"{service.ENTITY}已登记", entry=entry)


@router.post("/{module}/{entry_id}/actions", response_model=ActionResult)
def run_action(module: str, entry_id: int, payload: ActionPayload) -> ActionResult:
    service = require_service(module)
    entry, message = service.run_action(entry_id, payload.action.strip())
    return ActionResult(ok=entry is not None, message=message, entry=entry)
