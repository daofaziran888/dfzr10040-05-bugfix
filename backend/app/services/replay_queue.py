"""重放队列领域服务：终态回执驱动的状态流转与证据查询。

状态流转只在这里改：终态（已恢复 / 永久跳过）一旦落定，
迟到回执不能再改写；同一尝试只认先到的终态。
"""
from __future__ import annotations

from typing import Any

from app.backfill_replay import coordinator
from app.recovery import OUTCOME_EXHAUSTED, OUTCOME_SKIPPED, OUTCOME_SUCCESS
from app.services.base import BaseModuleService


class ReplayQueueService(BaseModuleService):
    MODULE = "replay_queue"
    ENTITY = "重放记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "融合作业", "失败原因", "重试次数", "退避策略", "最大重试", "最后重试", "队列状态"]
    REQUIRED_FIELDS = ["记录编号", "融合作业", "失败原因"]
    STATUS_ORDER = ["待重放", "重放中", "已恢复", "永久跳过"]
    ACTION_RULES = {"加入队列": "重放中", "手动重放": "已恢复", "永久跳过": "永久跳过"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        segment_code = str(entry.get("片段编号") or entry.get("记录编号") or entry_id)
        attempt_no = int(entry.get("尝试编号") or 1)
        entry.setdefault("片段编号", segment_code)
        entry.setdefault("尝试编号", attempt_no)

        if entry.get("status") in ("已恢复", "永久跳过"):
            return entry, (
                f"{self.ENTITY}已是终态「{entry.get('status')}」，"
                f"动作「{action}」不会改写终态"
            )

        # 「已恢复 / 永久跳过」是终态动作：要么由已有终态回执确认，
        # 要么把这次人工处理也登记成一份回执，保证队列与回填两端口径一致。
        if action in ("手动重放", "永久跳过"):
            if coordinator.ledger.is_held(segment_code, attempt_no):
                return entry, (
                    f"片段 {segment_code} 尝试 {attempt_no} 的终态确认暂扣中，"
                    f"动作「{action}」暂不能落定终态"
                )
            allowed = {
                "手动重放": (OUTCOME_SUCCESS,),
                "永久跳过": (OUTCOME_SKIPPED, OUTCOME_EXHAUSTED),
            }[action]
            receipt = coordinator.ledger.terminal_for(segment_code, attempt_no)
            if receipt is None:
                receipt = coordinator.submit_receipt(
                    segment_code=segment_code,
                    attempt_no=attempt_no,
                    outcome=(
                        OUTCOME_SUCCESS if action == "手动重放" else OUTCOME_SKIPPED
                    ),
                    error_fragment=str(
                        entry.get("失败原因") or f"{self.ENTITY}人工{action}"
                    ),
                    row_keys=[
                        row.row_key
                        for row in coordinator.staged_rows(segment_code, attempt_no)
                    ],
                )
            if receipt.outcome not in allowed:
                return entry, (
                    f"动作「{action}」与已生效回执 {receipt.receipt_code} 的终态"
                    f"「{receipt.outcome}」不一致，拒绝改写"
                )
            entry["终态"] = receipt.outcome

        entry["status"] = target
        entry["pending"] = target != self.STATUS_ORDER[-1]
        entry["abnormal"] = action in self.NEGATIVE_ACTIONS
        return entry, f"{self.ENTITY}已{action}"

    def terminal_receipts(self, segment_code: str | None = None) -> list[dict[str, Any]]:
        return [receipt.to_record() for receipt in coordinator.receipts(segment_code)]

    def confirm_terminal(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """按终态回执确认终态，冲突回执只保留证据、不改写结果。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        segment_code = str(entry.get("片段编号") or entry.get("记录编号") or entry_id)
        attempt_no = int(entry.get("尝试编号") or 1)
        receipt = coordinator.ledger.terminal_for(segment_code, attempt_no)
        if receipt is None:
            return entry, f"{self.ENTITY}尚无生效终态回执（片段 {segment_code} 尝试 {attempt_no}）"
        entry["status"] = "已恢复" if receipt.outcome == OUTCOME_SUCCESS else "永久跳过"
        entry["pending"] = entry["status"] != self.STATUS_ORDER[-1]
        entry["终态"] = receipt.outcome
        return entry, f"{self.ENTITY}按回执 {receipt.receipt_code} 确认终态「{receipt.outcome}」"

    def held_terminals(self) -> list[dict[str, Any]]:
        return coordinator.ledger.held_attempts()

    def exhausted_outcome(self) -> str:
        return OUTCOME_EXHAUSTED

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["terminal_ledger"] = coordinator.ledger.snapshot()
        return snapshot
