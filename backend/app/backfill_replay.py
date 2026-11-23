"""重放终态回执驱动的回填协调。

回填端与重放端各自会给出证据：
- 回填端：暂存行 + 写入凭据（证明这一批行已经写下去）；
- 重放端：终态回执（证明这个片段这次尝试只落定一个结果）。

协调器把两者对上，算出“下一次可领的数据范围”，并按
(片段, 行标识) 做宽表交付幂等：任何一端先确认，另一端都不会再交付一次。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.recovery import (
    OUTCOME_EXHAUSTED,
    OUTCOME_SKIPPED,
    OUTCOME_SUCCESS,
    ReplayQueue,
    ReplayReceipt,
    ReplayTerminalLedger,
)
from app.recovery import replay_queue as _global_replay_queue


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StagedRow:
    """回填端暂存行：带着写入凭据等重放终态来核对。"""
    segment_code: str
    attempt_no: int
    row_key: str
    payload: dict[str, Any] = field(default_factory=dict)
    write_credential: str = ""
    staged_at: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "片段编号": self.segment_code,
            "尝试编号": self.attempt_no,
            "行标识": self.row_key,
            "写入凭据": self.write_credential,
            "暂存时间": self.staged_at,
        }


@dataclass
class WideRow:
    """宽表行：同一片段同一行标识只允许存在一份。"""
    segment_code: str
    attempt_no: int
    row_key: str
    payload: dict[str, Any] = field(default_factory=dict)
    write_credential: str = ""
    confirmed: bool = False
    delivered_by: str = ""
    written_at: str = ""

    def to_record(self) -> dict[str, Any]:
        return {
            "片段编号": self.segment_code,
            "尝试编号": self.attempt_no,
            "行标识": self.row_key,
            "写入凭据": self.write_credential,
            "已确认": self.confirmed,
            "交付来源": self.delivered_by,
            "写入时间": self.written_at,
        }


@dataclass
class Claim:
    """一次领取：给出可领范围，执行时才真正补采/回填。"""
    claim_id: int
    claim_code: str
    worker: str
    created_at: str
    segments: list[dict[str, Any]] = field(default_factory=list)
    status: str = "待执行"
    executed_at: str = ""
    result: dict[str, Any] = field(default_factory=dict)

    @property
    def row_keys(self) -> list[str]:
        return [key for seg in self.segments for key in seg["row_keys"]]

    @property
    def total_rows(self) -> int:
        return len(self.row_keys)


class ReplayBackfillCoordinator:
    """重放与回填的协调器：终态、暂存凭据、领取范围与宽表交付。"""

    def __init__(self,
                 ledger: ReplayTerminalLedger | None = None,
                 replay_queue: ReplayQueue | None = None) -> None:
        self.ledger = ledger or ReplayTerminalLedger()
        self.replay_queue = replay_queue
        self._staging: dict[tuple[str, int, str], StagedRow] = {}
        self._wide_rows: dict[tuple[str, str], WideRow] = {}
        self._claims: list[Claim] = []
        self._next_claim_id = 1
        self._queue_bindings: dict[tuple[str, int], int] = {}

    # ---- 回填端：暂存行与写入凭据 ----

    def stage_write(self, segment_code: str, attempt_no: int,
                    rows: list[dict[str, Any]] | list[str],
                    write_credential: str) -> list[StagedRow]:
        """登记回填端已写下的暂存行；写入凭据先于终态回执落定。"""
        staged: list[StagedRow] = []
        for row in rows:
            if isinstance(row, str):
                row_key, payload = row, {}
            else:
                row_key = str(row.get("row_key") or row.get("id") or "")
                payload = dict(row.get("payload") or row)
                payload.pop("row_key", None)
            if not row_key:
                continue
            key = (segment_code, int(attempt_no), row_key)
            existing = self._staging.get(key)
            if existing is None:
                existing = StagedRow(
                    segment_code=segment_code,
                    attempt_no=int(attempt_no),
                    row_key=row_key,
                    payload=payload,
                    write_credential=write_credential,
                    staged_at=_now_iso(),
                )
                self._staging[key] = existing
            elif write_credential and not existing.write_credential:
                existing.write_credential = write_credential
            staged.append(existing)
        return staged

    def staged_rows(self, segment_code: str | None = None,
                    attempt_no: int | None = None) -> list[StagedRow]:
        rows = list(self._staging.values())
        if segment_code is not None:
            rows = [r for r in rows if r.segment_code == segment_code]
        if attempt_no is not None:
            rows = [r for r in rows if r.attempt_no == int(attempt_no)]
        return rows

    # ---- 重放端：终态回执 ----

    def bind_queue_entry(self, entry_id: int, segment_code: str,
                         attempt_no: int) -> None:
        self._queue_bindings[(segment_code, int(attempt_no))] = entry_id

    def submit_receipt(self, *, segment_code: str, attempt_no: int,
                       outcome: str, error_fragment: str = "",
                       row_keys: list[str] | None = None,
                       write_credential: str = "") -> ReplayReceipt:
        """提交终态回执；同一次尝试只让先到的那份生效。"""
        receipt = self.ledger.receive_receipt(
            segment_code=segment_code,
            attempt_no=int(attempt_no),
            outcome=outcome,
            error_fragment=error_fragment,
            row_keys=row_keys,
            write_credential=write_credential,
        )
        # 用回执里的行标识补齐暂存证据：终态回执本身也证明这些行属于该片段。
        if receipt.applied and receipt.outcome == OUTCOME_SUCCESS and receipt.row_keys:
            self.stage_write(segment_code, int(attempt_no),
                             receipt.row_keys, receipt.write_credential)
        if self.replay_queue is not None:
            entry_id = self._queue_bindings.get((segment_code, int(attempt_no)))
            if entry_id is not None:
                self.replay_queue.confirm_from_ledger(entry_id, self.ledger)
        return receipt

    def receipts(self, segment_code: str | None = None) -> list[ReplayReceipt]:
        return self.ledger.receipts_for(segment_code)

    # ---- 暂扣与写入证据 ----

    def hold_terminal(self, segment_code: str, attempt_no: int,
                      reason: str = "两份回执冲突，等待写入证据") -> None:
        self.ledger.hold_terminal(segment_code, attempt_no, reason)

    def row_landed(self, segment_code: str, row_key: str) -> bool:
        """写入证据：该行是否已在宽表落地，或已有成功终态回执确认。"""
        if (segment_code, row_key) in self._wide_rows:
            return True
        return any(
            row["row_key"] == row_key
            for row in self.ledger.effective_rows(segment_code)
        )

    def write_evidence_complete(self, segment_code: str,
                                attempt_no: int) -> dict[str, Any]:
        """核对回填端暂存行是否全部有写入证据。"""
        staged = self.staged_rows(segment_code, attempt_no)
        with_credential = [row for row in staged if row.write_credential]
        missing = [
            row.row_key for row in with_credential
            if not self.row_landed(segment_code, row.row_key)
        ]
        return {
            "segment_code": segment_code,
            "attempt_no": int(attempt_no),
            "staged_rows": len(staged),
            "with_credential": len(with_credential),
            "missing_rows": missing,
            "complete": bool(with_credential) and not missing,
        }

    def resolve_held_terminal(self, segment_code: str,
                              attempt_no: int) -> dict[str, Any]:
        """写入证据足以证明该尝试的行全部落地时，落定成功终态。"""
        evidence = self.write_evidence_complete(segment_code, attempt_no)
        if not evidence["complete"]:
            return {
                "ok": False,
                "resolved": False,
                "evidence": evidence,
                "message": "写入证据不足，终态继续暂扣",
            }
        success = next(
            (
                receipt for receipt in self.ledger.receipts_for(segment_code)
                if receipt.attempt_no == int(attempt_no)
                and receipt.outcome == OUTCOME_SUCCESS
                and receipt.duplicate_of is None
            ),
            None,
        )
        settled = self.ledger.settle_terminal(
            segment_code, int(attempt_no), OUTCOME_SUCCESS, receipt=success,
        )
        if settled is not None and self.replay_queue is not None:
            entry_id = self._queue_bindings.get((segment_code, int(attempt_no)))
            if entry_id is not None:
                # 暂扣期间按写入证据改判：这是显式落定，允许两端对齐。
                self.replay_queue.confirm_from_ledger(entry_id, self.ledger, force=True)
        return {
            "ok": True,
            "resolved": True,
            "terminal": settled.outcome if settled else "",
            "evidence": evidence,
            "message": "写入证据齐全，已确认成功终态",
        }

    # ---- 领取范围 ----

    def _present_row_keys(self, segment_code: str) -> set[str]:
        return {
            row_key for (seg, row_key) in self._wide_rows if seg == segment_code
        }

    def _segments(self) -> list[str]:
        codes = {seg for seg, _, _ in self._staging}
        codes.update(self.ledger.confirmed_segments())
        return sorted(codes)

    def _range_for_segment(self, segment_code: str) -> dict[str, Any]:
        attempt_nos = sorted(
            {attempt for seg, attempt, _ in self._staging if seg == segment_code}
            | set(self.ledger.attempt_rows(segment_code))
        )
        attempt_no = attempt_nos[-1] if attempt_nos else 1
        terminal = self.ledger.terminal_for(segment_code, attempt_no)
        present = self._present_row_keys(segment_code)

        if terminal is not None and terminal.outcome in (OUTCOME_EXHAUSTED, OUTCOME_SKIPPED):
            return {
                "segment_code": segment_code,
                "attempt_no": attempt_no,
                "mode": "不回填",
                "row_keys": [],
                "skipped_rows": sorted(present),
                "receivable_rows": 0,
                "reason": f"终态「{terminal.outcome}」，该片段不再交付新行",
                "terminal": terminal.outcome,
            }

        if terminal is not None and terminal.outcome == OUTCOME_SUCCESS:
            candidates = [row["row_key"] for row in self.ledger.effective_rows(segment_code)]
            mode = "回填"
            reason = "按成功终态核对已生效行"
        else:
            candidates = [r.row_key for r in self.staged_rows(segment_code, attempt_no)]
            mode = "补采"
            reason = "终态未确认，按暂存写入凭据补采"

        candidates = list(dict.fromkeys(candidates))
        pending = [key for key in candidates if key not in present]
        settled = [key for key in candidates if key in present]
        if not pending and settled:
            reason = f"{reason}；行已全部落地，无需重复交付"
        return {
            "segment_code": segment_code,
            "attempt_no": attempt_no,
            "mode": mode,
            "row_keys": pending,
            "skipped_rows": settled,
            "receivable_rows": len(pending),
            "reason": reason,
            "terminal": terminal.outcome if terminal else "",
        }

    def next_claimable_range(self) -> dict[str, Any]:
        """下一次可领的数据范围：只由生效的终态回执与已落地证据决定。"""
        segments = [self._range_for_segment(code) for code in self._segments()]
        segments = [seg for seg in segments if seg["receivable_rows"] or seg["skipped_rows"]]
        return {
            "segments": segments,
            "total_rows": sum(seg["receivable_rows"] for seg in segments),
            "processed_segments": [seg["segment_code"] for seg in segments],
        }

    def claim_next(self, worker: str = "值班") -> Claim:
        plan = self.next_claimable_range()
        claim = Claim(
            claim_id=self._next_claim_id,
            claim_code=f"CLAM-{self._next_claim_id:04d}",
            worker=worker,
            created_at=_now_iso(),
            segments=plan["segments"],
        )
        self._next_claim_id += 1
        self._claims.append(claim)
        return claim

    def get_claim(self, claim_code: str) -> Claim | None:
        for claim in self._claims:
            if claim.claim_code == claim_code:
                return claim
        return None

    def execute_claim(self, claim_code: str) -> dict[str, Any]:
        """执行领取：按范围补采/回填，并对宽表做幂等写入。"""
        claim = self.get_claim(claim_code)
        if claim is None:
            return {"ok": False, "message": f"领取单 {claim_code} 不存在"}
        if claim.status == "已完成":
            return {
                "ok": True,
                "claim_code": claim.claim_code,
                "already_executed": True,
                "inserted": 0,
                "confirmed_existing": 0,
                "skipped_duplicates": 0,
                "segments": claim.result.get("segments", []),
                "wide_table": self.wide_table_snapshot(),
            }

        inserted = 0
        confirmed = 0
        duplicates = 0
        segments: list[dict[str, Any]] = []
        for segment in claim.segments:
            seg_code = segment["segment_code"]
            attempt_no = int(segment["attempt_no"])
            seg_result = {
                "segment_code": seg_code,
                "attempt_no": attempt_no,
                "mode": segment["mode"],
                "requested": list(segment["row_keys"]),
                "inserted": [],
                "confirmed_existing": [],
                "skipped_duplicates": [],
                "processed": True,
            }
            if segment["mode"] == "不回填":
                seg_result["processed"] = False
                seg_result["reason"] = segment["reason"]
                segments.append(seg_result)
                continue

            for row_key in segment["row_keys"]:
                staged = self._staging.get((seg_code, attempt_no, row_key))
                wide_key = (seg_code, row_key)
                if wide_key in self._wide_rows:
                    existing = self._wide_rows[wide_key]
                    if not existing.confirmed:
                        existing.confirmed = True
                        confirmed += 1
                        seg_result["confirmed_existing"].append(row_key)
                    else:
                        duplicates += 1
                        seg_result["skipped_duplicates"].append(row_key)
                    continue
                self._wide_rows[wide_key] = WideRow(
                    segment_code=seg_code,
                    attempt_no=attempt_no,
                    row_key=row_key,
                    payload=dict(staged.payload) if staged else {},
                    write_credential=(
                        staged.write_credential if staged else
                        (self.ledger.terminal_for(seg_code, attempt_no).write_credential
                         if self.ledger.terminal_for(seg_code, attempt_no) else "")
                    ),
                    confirmed=True,
                    delivered_by=claim.claim_code,
                    written_at=_now_iso(),
                )
                inserted += 1
                seg_result["inserted"].append(row_key)
            segments.append(seg_result)

        claim.status = "已完成"
        claim.executed_at = _now_iso()
        claim.result = {
            "inserted": inserted,
            "confirmed_existing": confirmed,
            "skipped_duplicates": duplicates,
            "segments": segments,
            "wide_table": self.wide_table_snapshot(),
        }
        return {
            "ok": True,
            "claim_code": claim.claim_code,
            "worker": claim.worker,
            "inserted": inserted,
            "confirmed_existing": confirmed,
            "skipped_duplicates": duplicates,
            "segments": segments,
            "wide_table": self.wide_table_snapshot(),
        }

    # ---- 宽表读取 ----

    def wide_table_rows(self, segment_code: str | None = None,
                        row_key: str | None = None) -> list[WideRow]:
        rows = list(self._wide_rows.values())
        if segment_code is not None:
            rows = [r for r in rows if r.segment_code == segment_code]
        if row_key is not None:
            rows = [r for r in rows if r.row_key == row_key]
        return rows

    def wide_table_count(self, segment_code: str) -> int:
        return len(self.wide_table_rows(segment_code))

    def wide_table_detail(self, segment_code: str) -> list[dict[str, Any]]:
        return [row.to_record() for row in self.wide_table_rows(segment_code)]

    def wide_table_snapshot(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for row in self._wide_rows.values():
            counts[row.segment_code] = counts.get(row.segment_code, 0) + 1
        return counts

    # ---- 回填详情 ----

    def backfill_detail(self, segment_code: str) -> dict[str, Any]:
        """回填详情：终态、原始出错片段、尝试编号与实际写入行。"""
        receipts = [
            {
                **receipt.to_record(),
                "结果": receipt.outcome,
            }
            for receipt in self.ledger.receipts_for(segment_code)
        ]
        terminal = self.ledger.terminal_for(segment_code, self._latest_attempt(segment_code))
        effective = self.ledger.effective_rows(segment_code)
        written = [
            row.to_record() for row in self.wide_table_rows(segment_code)
        ]
        return {
            "segment_code": segment_code,
            "terminal": terminal.outcome if terminal else "",
            "attempts": self.ledger.attempt_rows(segment_code),
            "receipts": receipts,
            "effective_rows": effective,
            "effective_row_count": len(effective),
            "written_rows": written,
            "written_row_count": len(written),
            "next_claim_range": self._range_for_segment(segment_code),
        }

    def _latest_attempt(self, segment_code: str) -> int:
        attempts = self.ledger.attempt_rows(segment_code)
        if attempts:
            return attempts[-1]
        staged = self.staged_rows(segment_code)
        return staged[-1].attempt_no if staged else 1

    def snapshot(self) -> dict[str, Any]:
        return {
            "staged_rows": len(self._staging),
            "wide_rows": len(self._wide_rows),
            "claims": len(self._claims),
            "ledger": self.ledger.snapshot(),
            "queue": self.replay_queue.domain_snapshot() if self.replay_queue else {},
        }


# 全局单例：重放队列、终态账本与回填协调共用一份状态。
coordinator = ReplayBackfillCoordinator(replay_queue=_global_replay_queue)
