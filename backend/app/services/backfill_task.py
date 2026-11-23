"""数据回填领域服务：按重放终态回执核对片段、交付宽表并给出下次领取范围。"""
from __future__ import annotations

from typing import Any

from app.backfill_replay import coordinator
from app.services.base import BaseModuleService


class BackfillTaskService(BaseModuleService):
    MODULE = "backfill_task"
    ENTITY = "回填任务"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["任务编号", "目标宽表", "回填区间", "并行分区", "依赖检查", "断点位置", "已处理行", "剩余行数"]
    REQUIRED_FIELDS = ["任务编号", "目标宽表", "回填区间"]
    STATUS_ORDER = ["待回填", "回填中", "已完成", "回填失败"]
    ACTION_RULES = {"启动回填": "回填中", "暂停回填": "已完成", "标记失败": "回填失败"}
    NEGATIVE_ACTIONS = []

    # ---- 详情与范围 ----

    def segment_detail(self, segment_code: str) -> dict[str, Any]:
        """回填详情：终态、两份回执（含原出错片段与尝试编号）与实际写入。"""
        return coordinator.backfill_detail(segment_code)

    def next_claimable_range(self) -> dict[str, Any]:
        return coordinator.next_claimable_range()

    def wide_table_detail(self, segment_code: str) -> dict[str, Any]:
        rows = coordinator.wide_table_detail(segment_code)
        return {
            "segment_code": segment_code,
            "row_count": len(rows),
            "rows": rows,
            "distinct_row_count": len({row["行标识"] for row in rows}),
        }

    # ---- 领取与执行 ----

    def claim_next(self, worker: str = "值班") -> dict[str, Any]:
        claim = coordinator.claim_next(worker)
        return self._claim_record(claim)

    def claim_detail(self, claim_code: str) -> dict[str, Any] | None:
        claim = coordinator.get_claim(claim_code)
        return self._claim_record(claim) if claim else None

    def execute_claim(self, claim_code: str) -> dict[str, Any]:
        return coordinator.execute_claim(claim_code)

    # ---- 回执 ----

    def submit_receipt(self, *, segment_code: str, attempt_no: int, outcome: str,
                       error_fragment: str = "", row_keys: list[str] | None = None,
                       write_credential: str = "") -> dict[str, Any]:
        receipt = coordinator.submit_receipt(
            segment_code=segment_code,
            attempt_no=attempt_no,
            outcome=outcome,
            error_fragment=error_fragment,
            row_keys=row_keys,
            write_credential=write_credential,
        )
        return {
            "receipt": receipt.to_record(),
            "terminal": coordinator.ledger.terminal_outcome(segment_code, attempt_no),
            "next_claim_range": coordinator.next_claimable_range(),
        }

    def stage_rows(self, *, segment_code: str, attempt_no: int,
                   row_keys: list[str], write_credential: str) -> dict[str, Any]:
        staged = coordinator.stage_write(segment_code, attempt_no, row_keys, write_credential)
        return {
            "staged": [row.to_record() for row in staged],
            "evidence": coordinator.write_evidence_complete(segment_code, attempt_no),
        }

    def hold_terminal(self, *, segment_code: str, attempt_no: int,
                      reason: str = "两份回执冲突，等待写入证据") -> dict[str, Any]:
        coordinator.hold_terminal(segment_code, attempt_no, reason)
        return {
            "held": True,
            "terminal": coordinator.ledger.terminal_outcome(segment_code, attempt_no),
            "next_claim_range": coordinator.next_claimable_range(),
        }

    def resolve_terminal(self, *, segment_code: str, attempt_no: int) -> dict[str, Any]:
        result = coordinator.resolve_held_terminal(segment_code, attempt_no)
        result["next_claim_range"] = coordinator.next_claimable_range()
        return result

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["replay_backfill"] = coordinator.snapshot()
        return snapshot

    @staticmethod
    def _claim_record(claim: Any) -> dict[str, Any]:
        return {
            "claim_code": claim.claim_code,
            "worker": claim.worker,
            "created_at": claim.created_at,
            "status": claim.status,
            "segments": claim.segments,
            "total_rows": claim.total_rows,
        }
