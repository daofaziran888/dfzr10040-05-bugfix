"""恢复与重放：断点续传、中断重放与增量确认机制。

与重放队列联动，融合作业失败后在队列中记录，
按退避策略重试，支持断点恢复。
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# 重放终态：一次尝试只允许落定其中一个。
OUTCOME_SUCCESS = "成功"
OUTCOME_EXHAUSTED = "耗尽"
OUTCOME_SKIPPED = "永久跳过"
REPLAY_OUTCOMES = (OUTCOME_SUCCESS, OUTCOME_EXHAUSTED, OUTCOME_SKIPPED)


@dataclass
class ReplayReceipt:
    """重放终态回执：一次尝试、一个片段、一份原始出错证据。

    回执一旦写入就不可变；同一尝试的冲突回执只做证据保留，
    ``applied`` 为 False，不会改写已落定的终态。
    """
    receipt_id: int
    receipt_code: str
    segment_code: str
    attempt_no: int
    outcome: str
    error_fragment: str
    row_keys: list[str] = field(default_factory=list)
    write_credential: str = ""
    received_at: str = ""
    applied: bool = False
    duplicate_of: int | None = None
    reject_reason: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "回执编号": self.receipt_code,
            "片段编号": self.segment_code,
            "尝试编号": self.attempt_no,
            "终态": self.outcome,
            "原出错片段": self.error_fragment,
            "行标识": list(self.row_keys),
            "写入凭据": self.write_credential,
            "到达时间": self.received_at,
            "已生效": self.applied,
            "重复于": self.duplicate_of,
            "未生效原因": self.reject_reason,
        }


class ReplayTerminalLedger:
    """重放终态账本：同一片段同一尝试只确认一个终态。

    - 先到且生效的回执确认终态，后到的冲突回执原样保留（含原出错片段与尝试编号）；
    - 同一份回执重复投递只登记一次，其余记为重复证据，不再产生生效行；
    - 可以「暂扣」某个尝试的终态确认：暂扣期间所有回执只作证据，
      等写入证据齐全后再由协调器落定唯一的终态；
    - 成功终态里同一片段同一行标识只记一份有效回填行。
    """

    def __init__(self) -> None:
        self._receipts: list[ReplayReceipt] = []
        self._terminals: dict[tuple[str, int], ReplayReceipt] = {}
        self._holds: dict[tuple[str, int], str] = {}
        self._suspended: dict[tuple[str, int], ReplayReceipt] = {}
        self._next_id: int = 1

    def _new_receipt(self, **values: Any) -> ReplayReceipt:
        receipt = ReplayReceipt(
            receipt_id=self._next_id,
            receipt_code=f"RCPT-{self._next_id:04d}",
            received_at=_now_iso(),
            **values,
        )
        self._next_id += 1
        self._receipts.append(receipt)
        return receipt

    def receive_receipt(
        self,
        *,
        segment_code: str,
        attempt_no: int,
        outcome: str,
        error_fragment: str = "",
        row_keys: list[str] | None = None,
        write_credential: str = "",
    ) -> ReplayReceipt:
        if outcome not in REPLAY_OUTCOMES:
            raise ValueError(f"未知重放终态：{outcome}")

        rows = list(row_keys or [])
        signature = (
            segment_code, int(attempt_no), outcome,
            tuple(sorted(rows)), write_credential,
        )
        for prior in self._receipts:
            if getattr(prior, "_signature", None) == signature:
                return self._new_receipt(
                    segment_code=segment_code,
                    attempt_no=int(attempt_no),
                    outcome=outcome,
                    error_fragment=error_fragment,
                    row_keys=rows,
                    write_credential=write_credential,
                    applied=False,
                    duplicate_of=prior.receipt_id,
                    reject_reason="重复回执，已有同一份证据，不再重复生效",
                )

        key = (segment_code, int(attempt_no))
        settled = self._terminals.get(key)
        held = key in self._holds
        receipt = self._new_receipt(
            segment_code=segment_code,
            attempt_no=int(attempt_no),
            outcome=outcome,
            error_fragment=error_fragment,
            row_keys=rows,
            write_credential=write_credential,
            applied=settled is None and not held,
            reject_reason=(
                "" if settled is None and not held
                else (
                    f"终态确认暂扣中：{self._holds[key]}，回执先作证据"
                    if held
                    else f"同一尝试已确认终态「{settled.outcome}」，本回执只作证据保留"
                )
            ),
        )
        receipt._signature = signature  # type: ignore[attr-defined]
        if settled is None and not held:
            self._terminals[key] = receipt
        return receipt

    # ---- 暂扣与落定 ----

    def hold_terminal(self, segment_code: str, attempt_no: int,
                      reason: str = "两份回执冲突，等待写入证据") -> None:
        """暂扣终态确认：已确认的终态一并挂起，后续由证据裁决。"""
        key = (segment_code, int(attempt_no))
        self._holds[key] = reason
        settled = self._terminals.pop(key, None)
        if settled is not None:
            settled.applied = False
            settled.reject_reason = f"终态确认已暂扣：{reason}"
            self._suspended[key] = settled

    def is_held(self, segment_code: str, attempt_no: int) -> bool:
        return (segment_code, int(attempt_no)) in self._holds

    def held_attempts(self) -> list[dict[str, Any]]:
        return [
            {"segment_code": seg, "attempt_no": attempt, "reason": reason}
            for (seg, attempt), reason in self._holds.items()
        ]

    def settle_terminal(self, segment_code: str, attempt_no: int,
                        outcome: str, *, receipt: ReplayReceipt | None = None) -> ReplayReceipt | None:
        """落定唯一终态；暂扣状态随之解除，其它同尝试回执转为证据。"""
        key = (segment_code, int(attempt_no))
        chosen = receipt
        if chosen is None:
            candidates = [
                r for r in self._receipts
                if r.segment_code == segment_code
                and r.attempt_no == int(attempt_no)
                and r.outcome == outcome
            ]
            chosen = candidates[-1] if candidates else None
        if chosen is None:
            return None
        self._holds.pop(key, None)
        self._suspended.pop(key, None)
        for other in self._receipts:
            if other is chosen or other.duplicate_of is not None:
                continue
            if other.segment_code == segment_code and other.attempt_no == int(attempt_no):
                other.applied = False
                other.reject_reason = (
                    f"同一尝试已确认终态「{outcome}」，本回执只作证据保留"
                )
        chosen.applied = True
        chosen.reject_reason = ""
        self._terminals[key] = chosen
        return chosen

    # ---- 查询 ----

    def terminal_for(self, segment_code: str, attempt_no: int) -> ReplayReceipt | None:
        return self._terminals.get((segment_code, int(attempt_no)))

    def terminal_outcome(self, segment_code: str, attempt_no: int) -> str:
        terminal = self.terminal_for(segment_code, attempt_no)
        return terminal.outcome if terminal else ""

    def receipts_for(self, segment_code: str | None = None) -> list[ReplayReceipt]:
        if segment_code is None:
            return list(self._receipts)
        return [r for r in self._receipts if r.segment_code == segment_code]

    def attempt_rows(self, segment_code: str) -> list[int]:
        return sorted({r.attempt_no for r in self.receipts_for(segment_code)})

    def effective_rows(self, segment_code: str | None = None) -> list[dict[str, Any]]:
        """成功终态里真正生效的回填行，同一片段同一行只保留一份。"""
        seen: dict[tuple[str, str], dict[str, Any]] = {}
        for receipt in self._receipts:
            if not receipt.applied or receipt.outcome != OUTCOME_SUCCESS:
                continue
            if segment_code is not None and receipt.segment_code != segment_code:
                continue
            for row_key in receipt.row_keys:
                seen.setdefault(
                    (receipt.segment_code, str(row_key)),
                    {
                        "segment_code": receipt.segment_code,
                        "attempt_no": receipt.attempt_no,
                        "row_key": str(row_key),
                        "receipt_code": receipt.receipt_code,
                        "write_credential": receipt.write_credential,
                    },
                )
        return list(seen.values())

    def effective_row_count(self, segment_code: str) -> int:
        return len(self.effective_rows(segment_code))

    def confirmed_segments(self) -> list[str]:
        return sorted({code for code, _ in self._terminals})

    def conflicting_attempts(self) -> list[dict[str, Any]]:
        return [
            {
                "segment_code": r.segment_code,
                "attempt_no": r.attempt_no,
                "outcome": r.outcome,
                "duplicate_of": r.duplicate_of,
                "reject_reason": r.reject_reason,
            }
            for r in self._receipts
            if not r.applied
        ]

    def snapshot(self) -> dict[str, Any]:
        settled = list(self._terminals.values())
        return {
            "module": "replay_terminal_ledger",
            "receipts": len(self._receipts),
            "terminals": len(settled),
            "applied": sum(1 for r in self._receipts if r.applied),
            "retained_conflicts": sum(1 for r in self._receipts if not r.applied),
            "held": len(self._holds),
            "by_outcome": {
                outcome: sum(1 for r in settled if r.outcome == outcome)
                for outcome in REPLAY_OUTCOMES
            },
            "effective_rows": len(self.effective_rows()),
        }


@dataclass
class ReplayEntry:
    """重放队列条目：记录失败融合片段与重试状态。"""
    entry_id: int
    record_code: str
    fusion_job: str
    failure_reason: str
    retry_count: int = 0
    backoff_strategy: str = "exponential"
    max_retries: int = 5
    last_retry: str = ""
    queue_status: str = "待重放"
    checkpoint_data: dict[str, Any] = field(default_factory=dict)
    segment_code: str = ""
    attempt_no: int = 1
    terminal_outcome: str = ""

    def can_retry(self) -> bool:
        return self.retry_count < self.max_retries

    def next_backoff_seconds(self) -> int:
        if self.backoff_strategy == "exponential":
            return min(2 ** (self.retry_count + 1), 300)
        elif self.backoff_strategy == "linear":
            return min((self.retry_count + 1) * 10, 300)
        return 60


class ReplayQueue:
    """重放队列管理器：管理融合失败数据的自动重放。

    终态（已恢复 / 永久跳过）一旦落定就不可被迟到回执改写；
    重试次数耗尽先落定「永久跳过+耗尽」，之后的成功回执只作证据保留。
    """

    def __init__(self) -> None:
        self._entries: dict[int, ReplayEntry] = {}
        self._next_id: int = 1

    def enqueue(self, fusion_job_code: str, failure_reason: str,
                checkpoint_data: dict[str, Any] | None = None,
                backoff_strategy: str = "exponential",
                max_retries: int = 5,
                segment_code: str = "",
                attempt_no: int = 1) -> ReplayEntry:
        sid = self._next_id
        self._next_id += 1
        entry = ReplayEntry(
            entry_id=sid,
            record_code=f"REPL-{sid:04d}",
            fusion_job=fusion_job_code,
            failure_reason=failure_reason,
            retry_count=0,
            backoff_strategy=backoff_strategy,
            max_retries=max_retries,
            last_retry="",
            queue_status="待重放",
            checkpoint_data=checkpoint_data or {},
            segment_code=segment_code or f"REPL-{sid:04d}",
            attempt_no=max(1, int(attempt_no)),
        )
        self._entries[sid] = entry
        return entry

    def dequeue(self, entry_id: int) -> ReplayEntry | None:
        return self._entries.pop(entry_id, None)

    def get_entry(self, entry_id: int) -> ReplayEntry | None:
        return self._entries.get(entry_id)

    @staticmethod
    def _is_terminal(entry: ReplayEntry) -> bool:
        return entry.queue_status in ("已恢复", "永久跳过")

    def mark_retrying(self, entry_id: int) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        if self._is_terminal(entry):
            return entry
        entry.queue_status = "重放中"
        entry.last_retry = _now_iso()
        return entry

    def mark_success(self, entry_id: int) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        if self._is_terminal(entry):
            # 迟到成功不得覆盖已落定的终态，原结果保持可追溯。
            return entry
        entry.queue_status = "已恢复"
        entry.terminal_outcome = OUTCOME_SUCCESS
        return entry

    def mark_permanent_skip(self, entry_id: int,
                            outcome: str = OUTCOME_SKIPPED) -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        if self._is_terminal(entry):
            return entry
        entry.queue_status = "永久跳过"
        entry.terminal_outcome = outcome
        return entry

    def increment_retry(self, entry_id: int,
                        new_failure_reason: str = "") -> ReplayEntry | None:
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        if self._is_terminal(entry):
            return entry
        entry.retry_count += 1
        if new_failure_reason:
            entry.failure_reason = new_failure_reason
        entry.last_retry = _now_iso()
        if not entry.can_retry():
            entry.queue_status = "永久跳过"
            entry.terminal_outcome = OUTCOME_EXHAUSTED
        else:
            entry.queue_status = "待重放"
        return entry

    def confirm_from_ledger(self, entry_id: int,
                            ledger: "ReplayTerminalLedger",
                            force: bool = False) -> ReplayEntry | None:
        """用重放终态回执确认队列条目的终态，返回确认后的条目。

        ``force`` 只给“暂扣后按写入证据改判”用：普通迟到回执永远不能
        改写终态，只有协调器显式落定了新终态时才允许两端对齐。
        """
        entry = self._entries.get(entry_id)
        if entry is None:
            return None
        outcome = ledger.terminal_outcome(entry.segment_code, entry.attempt_no)
        if not outcome:
            return entry
        status = "已恢复" if outcome == OUTCOME_SUCCESS else "永久跳过"
        if self._is_terminal(entry) and not force:
            return entry
        entry.queue_status = status
        entry.terminal_outcome = outcome
        entry.pending = status != "永久跳过"
        return entry

    @property
    def pending_entries(self) -> list[ReplayEntry]:
        return [e for e in self._entries.values() if e.queue_status == "待重放"]

    @property
    def all_entries(self) -> list[ReplayEntry]:
        return list(self._entries.values())

    def domain_snapshot(self) -> dict[str, Any]:
        entries = self.all_entries
        by_status = {
            status: sum(1 for e in entries if e.queue_status == status)
            for status in ["待重放", "重放中", "已恢复", "永久跳过"]
        }
        return {
            "module": "replay_queue",
            "entity": "重放记录",
            "count": len(entries),
            "by_status": by_status,
            "pending": by_status.get("待重放", 0),
            "permanently_skipped": by_status.get("永久跳过", 0),
        }


class CheckpointManager:
    """检查点管理器：维护回填任务的断点位置与恢复元数据。"""

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict[str, Any]] = {}
        self._recovery_log: list[dict[str, Any]] = []

    def save_checkpoint(self, task_code: str, position: str,
                        metadata: dict[str, Any] | None = None) -> dict[str, Any]:
        checkpoint = {
            "task_code": task_code,
            "position": position,
            "saved_at": _now_iso(),
            "metadata": metadata or {},
        }
        self._checkpoints[task_code] = checkpoint
        return dict(checkpoint)

    def load_checkpoint(self, task_code: str) -> dict[str, Any] | None:
        checkpoint = self._checkpoints.get(task_code)
        if checkpoint is None:
            return None
        return dict(checkpoint)

    def clear_checkpoint(self, task_code: str) -> bool:
        if task_code in self._checkpoints:
            self._recovery_log.append({
                "task_code": task_code,
                "action": "cleared",
                "at": _now_iso(),
                "former_position": self._checkpoints[task_code].get("position"),
            })
            del self._checkpoints[task_code]
            return True
        return False

    def record_recovery(self, task_code: str, action: str,
                        result: dict[str, Any] | None = None) -> None:
        self._recovery_log.append({
            "task_code": task_code,
            "action": action,
            "at": _now_iso(),
            "result": result or {},
        })

    def get_active_checkpoints(self) -> list[dict[str, Any]]:
        return [
            {"task_code": tc, "position": cp["position"], "saved_at": cp["saved_at"]}
            for tc, cp in self._checkpoints.items()
        ]

    def get_log(self) -> list[dict[str, Any]]:
        return self._recovery_log[-100:]

    def replay_from_checkpoint(self, task_code: str,
                               replay_fn: Any,
                               total_items: int,
                               batch_size: int = 100) -> dict[str, Any]:
        """从检查点重放处理：调用 replay_fn(batch_items, start_index) 逐批恢复。"""
        checkpoint = self.load_checkpoint(task_code)
        start_idx = 0
        if checkpoint:
            try:
                start_idx = int(checkpoint.get("position", 0))
            except (ValueError, TypeError):
                start_idx = 0

        self.record_recovery(task_code, "replay_started",
                             {"start_idx": start_idx, "total": total_items})

        processed = start_idx
        current = start_idx
        errors: list[dict[str, Any]] = []

        while current < total_items:
            end_idx = min(current + batch_size, total_items)
            batch = list(range(current, end_idx))
            try:
                result = replay_fn(batch, current)
                if isinstance(result, dict) and not result.get("ok", True):
                    errors.append({"segment": f"{current}-{end_idx}",
                                   "error": result.get("message", "unknown")})
                    break
                processed = end_idx
                self.save_checkpoint(task_code, str(processed))
                current = end_idx
            except Exception as e:
                errors.append({"segment": f"{current}-{end_idx}", "error": str(e)})
                break

        complete = processed >= total_items
        if complete:
            self.clear_checkpoint(task_code)

        self.record_recovery(task_code,
                             "replay_completed" if complete else "replay_failed",
                             {"processed": processed, "total": total_items, "errors": len(errors)})

        return {
            "ok": len(errors) == 0,
            "task_code": task_code,
            "processed": processed,
            "total": total_items,
            "complete": complete,
            "errors": errors,
        }


# 全局单例
checkpoint_manager = CheckpointManager()
replay_queue = ReplayQueue()
replay_terminal_ledger = ReplayTerminalLedger()