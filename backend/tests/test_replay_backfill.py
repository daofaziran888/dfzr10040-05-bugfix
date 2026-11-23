"""重放终态回执与回填交付测试。

覆盖题面要求的完整流转：
- 永久跳过标记不被迟到成功覆盖，回填详情与队列结果一致；
- 同一尝试只确认一个终态，冲突回执保留原出错片段与尝试编号；
- 成功终态里同一片段同一行只记一份有效回填行；
- 领取范围由终态回执决定，宽表按 (片段, 行标识) 幂等交付。
"""
from __future__ import annotations

import pytest

from app.backfill_replay import ReplayBackfillCoordinator, coordinator
from app.recovery import (
    OUTCOME_EXHAUSTED,
    OUTCOME_SKIPPED,
    OUTCOME_SUCCESS,
    ReplayQueue,
    ReplayTerminalLedger,
)


@pytest.fixture
def rig() -> tuple[ReplayBackfillCoordinator, ReplayQueue]:
    queue = ReplayQueue()
    return ReplayBackfillCoordinator(replay_queue=queue), queue


def _segment(coordinator: ReplayBackfillCoordinator, code: str) -> dict:
    return next(
        seg for seg in coordinator.next_claimable_range()["segments"]
        if seg["segment_code"] == code
    )


class TestReplayTerminalFlow:
    """重放队列终态不被迟到回执改写。"""

    def test_permanent_skip_survives_late_success(self) -> None:
        rq = ReplayQueue()
        entry = rq.enqueue("FUSI-SKIP", "网络超时", max_retries=1)
        rq.mark_retrying(entry.entry_id)
        rq.increment_retry(entry.entry_id, "仍然失败")
        assert entry.queue_status == "永久跳过"
        assert entry.terminal_outcome == OUTCOME_EXHAUSTED

        rq.mark_success(entry.entry_id)
        assert entry.queue_status == "永久跳过"
        assert entry.terminal_outcome == OUTCOME_EXHAUSTED

    def test_terminal_entry_ignores_retry_and_skip(self) -> None:
        rq = ReplayQueue()
        entry = rq.enqueue("FUSI-DONE", "失败")
        rq.mark_success(entry.entry_id)
        rq.increment_retry(entry.entry_id, "迟到失败")
        assert entry.queue_status == "已恢复"
        assert entry.terminal_outcome == OUTCOME_SUCCESS

    def test_success_receipt_never_overwritten(self) -> None:
        ledger = ReplayTerminalLedger()
        first = ledger.receive_receipt(
            segment_code="S", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="原错误", row_keys=["r1"],
        )
        late = ledger.receive_receipt(
            segment_code="S", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="原错误", row_keys=["r1"],
        )
        assert first.applied is True
        assert late.applied is False
        assert ledger.terminal_outcome("S", 1) == OUTCOME_SUCCESS
        assert late.reject_reason


class TestTerminalReceipts:
    """同一尝试只确认一个终态，两份回执都留证据。"""

    def test_same_attempt_keeps_both_receipts(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, queue = rig
        entry = queue.enqueue("FUSI-K", "超时", segment_code="K", attempt_no=1)
        coordinator.bind_queue_entry(entry.entry_id, "K", 1)
        coordinator.stage_write("K", 1, ["k1", "k2"], "WC-K")

        exhausted = coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="K 原出错片段", row_keys=["k1", "k2"],
            write_credential="WC-K",
        )
        success = coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="K 原出错片段", row_keys=["k1", "k2"],
            write_credential="WC-K",
        )

        receipts = coordinator.receipts("K")
        assert len(receipts) == 2
        assert exhausted.applied is True
        assert success.applied is False
        assert {r.attempt_no for r in receipts} == {1}
        assert {r.error_fragment for r in receipts} == {"K 原出错片段"}
        assert coordinator.ledger.terminal_outcome("K", 1) == OUTCOME_EXHAUSTED
        assert entry.queue_status == "永久跳过"
        assert entry.terminal_outcome == OUTCOME_EXHAUSTED

    def test_arrival_order_swap_keeps_single_terminal(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, queue = rig
        entry = queue.enqueue("FUSI-K2", "超时", segment_code="K2", attempt_no=1)
        coordinator.bind_queue_entry(entry.entry_id, "K2", 1)
        coordinator.stage_write("K2", 1, ["k1"], "WC-K2")

        coordinator.submit_receipt(
            segment_code="K2", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="err-k2", row_keys=["k1"], write_credential="WC-K2",
        )
        coordinator.submit_receipt(
            segment_code="K2", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="err-k2", row_keys=["k1"], write_credential="WC-K2",
        )
        assert coordinator.ledger.terminal_outcome("K2", 1) == OUTCOME_SUCCESS
        assert entry.queue_status == "已恢复"
        # 成功终态只记一次有效回填行
        assert coordinator.ledger.effective_row_count("K2") == 1

    def test_duplicate_success_receipts_do_not_duplicate_rows(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, queue = rig
        queue.enqueue("FUSI-L", "失败", segment_code="L", attempt_no=1)
        base = coordinator.submit_receipt(
            segment_code="L", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="L-err", row_keys=["l1", "l2"], write_credential="WC-L",
        )
        assert base.applied is True
        for _ in range(3):
            duplicate = coordinator.submit_receipt(
                segment_code="L", attempt_no=1, outcome=OUTCOME_SUCCESS,
                error_fragment="L-err", row_keys=["l1", "l2"], write_credential="WC-L",
            )
            assert duplicate.applied is False
            assert duplicate.duplicate_of == base.receipt_id

        assert len(coordinator.receipts("L")) == 4
        assert coordinator.ledger.effective_row_count("L") == 2

    def test_unknown_outcome_rejected(self) -> None:
        ledger = ReplayTerminalLedger()
        with pytest.raises(ValueError):
            ledger.receive_receipt(segment_code="X", attempt_no=1, outcome="未知")


class TestReplayQueueServiceActions:
    """重放队列模块动作只能按终态回执落定，且终态不可改写。"""

    def test_terminal_action_requires_receipt(self) -> None:
        from app.services.replay_queue import ReplayQueueService

        service = ReplayQueueService()
        entry, _ = service.create_entry(
            {"记录编号": "REPL-7001", "融合作业": "FUSI-7001", "失败原因": "超时"},
        )
        _, message = service.run_action(entry["id"], "永久跳过")
        # 人工终态动作也会登记成回执，保证回填端能查到同一个终态。
        assert entry["status"] == "永久跳过"
        assert entry["终态"] == OUTCOME_SKIPPED
        assert "已永久跳过" in message
        receipt = coordinator.ledger.terminal_for("REPL-7001", 1)
        assert receipt is not None
        assert receipt.outcome == OUTCOME_SKIPPED
        assert receipt.error_fragment == "超时"

    def test_held_terminal_blocks_queue_action(self) -> None:
        from app.backfill_replay import coordinator
        from app.services.replay_queue import ReplayQueueService

        service = ReplayQueueService()
        entry, _ = service.create_entry(
            {"记录编号": "REPL-7004", "融合作业": "FUSI-7004", "失败原因": "超时"},
        )
        coordinator.submit_receipt(
            segment_code="REPL-7004", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="原错误",
        )
        coordinator.hold_terminal("REPL-7004", 1)
        _, message = service.run_action(entry["id"], "永久跳过")
        assert "暂扣" in message
        assert entry["status"] == "待重放"
        assert "终态" not in entry

    def test_exhausted_receipt_blocks_late_manual_replay(self) -> None:
        from app.backfill_replay import coordinator
        from app.services.replay_queue import ReplayQueueService

        service = ReplayQueueService()
        entry, _ = service.create_entry(
            {"记录编号": "REPL-7002", "融合作业": "FUSI-7002", "失败原因": "超时"},
        )
        coordinator.submit_receipt(
            segment_code="REPL-7002", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="原错误",
        )
        _, message = service.run_action(entry["id"], "手动重放")
        assert "不一致" in message
        assert entry["status"] == "待重放"

        _, message = service.run_action(entry["id"], "永久跳过")
        assert entry["status"] == "永久跳过"
        assert entry["终态"] == OUTCOME_EXHAUSTED

        _, message = service.run_action(entry["id"], "手动重放")
        assert "不会改写终态" in message
        assert entry["status"] == "永久跳过"
        assert entry["终态"] == OUTCOME_EXHAUSTED

    def test_success_receipt_allows_manual_replay(self) -> None:
        from app.backfill_replay import coordinator
        from app.services.replay_queue import ReplayQueueService

        service = ReplayQueueService()
        entry, _ = service.create_entry(
            {"记录编号": "REPL-7003", "融合作业": "FUSI-7003", "失败原因": "超时"},
        )
        coordinator.submit_receipt(
            segment_code="REPL-7003", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="原错误", row_keys=["r1"],
        )
        service.run_action(entry["id"], "手动重放")
        assert entry["status"] == "已恢复"
        assert entry["终态"] == OUTCOME_SUCCESS


class TestClaimRangeAndDelivery:
    """领取范围按终态回执核算，宽表交付幂等。"""

    def test_range_follows_terminal_receipt(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, _ = rig
        coordinator.stage_write("L", 1, ["l1", "l2"], "WC-L")
        assert _segment(coordinator, "L")["mode"] == "补采"

        coordinator.submit_receipt(
            segment_code="L", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="L-err", row_keys=["l1", "l2"], write_credential="WC-L",
        )
        segment = _segment(coordinator, "L")
        assert segment["mode"] == "回填"
        assert segment["terminal"] == OUTCOME_SUCCESS
        assert segment["row_keys"] == ["l1", "l2"]

    def test_exhausted_segment_not_receivable(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, _ = rig
        coordinator.stage_write("K", 1, ["k1"], "WC-K")
        coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="K-err", row_keys=["k1"], write_credential="WC-K",
        )
        plan = coordinator.next_claimable_range()
        assert "K" not in plan["processed_segments"]
        assert plan["total_rows"] == 0
        detail_range = coordinator.backfill_detail("K")["next_claim_range"]
        assert detail_range["mode"] == "不回填"
        assert detail_range["receivable_rows"] == 0

    def test_claim_executes_only_given_range(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, _ = rig
        coordinator.stage_write("L", 1, ["l1", "l2"], "WC-L")
        coordinator.submit_receipt(
            segment_code="L", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="L-err", row_keys=["l1", "l2"], write_credential="WC-L",
        )
        claim = coordinator.claim_next("值班")
        result = coordinator.execute_claim(claim.claim_code)
        assert result["ok"] is True
        assert result["inserted"] == 2
        assert coordinator.wide_table_count("L") == 2

        again = coordinator.execute_claim(claim.claim_code)
        assert again["already_executed"] is True
        assert again["inserted"] == 0
        assert coordinator.wide_table_count("L") == 2


class TestHeldTerminalWithWriteEvidence:
    """暂扣终态后用写入证据落定，两端结果对齐。"""

    def _setup(self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue]):
        coordinator, queue = rig
        entry = queue.enqueue("FUSI-K", "超时", segment_code="K", attempt_no=1)
        coordinator.bind_queue_entry(entry.entry_id, "K", 1)
        coordinator.stage_write("K", 1, ["k1", "k2"], "WC-K")
        coordinator.stage_write("L", 1, ["l1", "l2"], "WC-L")
        coordinator.submit_receipt(
            segment_code="L", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="L-err", row_keys=["l1", "l2"], write_credential="WC-L",
        )
        return coordinator, entry

    def test_hold_blocks_terminal_and_keeps_receipts(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, entry = self._setup(rig)
        coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="K-err", row_keys=["k1", "k2"], write_credential="WC-K",
        )
        coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="K-err", row_keys=["k1", "k2"], write_credential="WC-K",
        )
        coordinator.hold_terminal("K", 1, "两份回执冲突")

        assert coordinator.ledger.terminal_outcome("K", 1) == ""
        assert coordinator.ledger.is_held("K", 1) is True
        assert len(coordinator.receipts("K")) == 2
        detail = coordinator.backfill_detail("K")
        assert detail["terminal"] == ""
        assert len(detail["receipts"]) == 2

    def test_claim_backfills_by_write_credential_then_resolve(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, entry = self._setup(rig)
        coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="K-err", row_keys=["k1", "k2"], write_credential="WC-K",
        )
        coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="K-err", row_keys=["k1", "k2"], write_credential="WC-K",
        )
        coordinator.hold_terminal("K", 1, "两份回执冲突")

        claim = coordinator.claim_next("值班")
        modes = {seg["segment_code"]: seg["mode"] for seg in claim.segments}
        assert modes["K"] == "补采"
        assert modes["L"] == "回填"

        executed = coordinator.execute_claim(claim.claim_code)
        assert executed["ok"] is True
        assert coordinator.wide_table_count("K") == 2
        assert coordinator.wide_table_count("L") == 2

        evidence = coordinator.write_evidence_complete("K", 1)
        assert evidence["complete"] is True
        resolved = coordinator.resolve_held_terminal("K", 1)
        assert resolved["resolved"] is True
        assert resolved["terminal"] == OUTCOME_SUCCESS
        assert coordinator.ledger.terminal_outcome("K", 1) == OUTCOME_SUCCESS
        # 两端终态对齐
        assert entry.queue_status == "已恢复"
        assert entry.terminal_outcome == OUTCOME_SUCCESS

        detail = coordinator.backfill_detail("K")
        assert detail["terminal"] == OUTCOME_SUCCESS
        assert detail["effective_row_count"] == 2
        assert detail["written_row_count"] == 2

    def test_resolve_requires_complete_write_evidence(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, _ = rig
        coordinator.stage_write("K", 1, ["k1", "k2"], "WC-K")
        coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="K-err", row_keys=["k1"], write_credential="WC-K",
        )
        coordinator.hold_terminal("K", 1)
        result = coordinator.resolve_held_terminal("K", 1)
        assert result["resolved"] is False
        assert "k2" in result["evidence"]["missing_rows"]
        assert coordinator.ledger.is_held("K", 1) is True

    def test_second_claim_and_duplicate_receipt_do_not_redeliver(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, entry = self._setup(rig)
        coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_EXHAUSTED,
            error_fragment="K-err", row_keys=["k1", "k2"], write_credential="WC-K",
        )
        coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="K-err", row_keys=["k1", "k2"], write_credential="WC-K",
        )
        coordinator.hold_terminal("K", 1)

        first = coordinator.claim_next("值班")
        coordinator.execute_claim(first.claim_code)
        coordinator.resolve_held_terminal("K", 1)

        second = coordinator.claim_next("值班")
        assert second.total_rows == 0
        assert {seg["segment_code"] for seg in second.segments} == {"K", "L"}

        duplicate = coordinator.submit_receipt(
            segment_code="K", attempt_no=1, outcome=OUTCOME_SUCCESS,
            error_fragment="K-err", row_keys=["k1", "k2"], write_credential="WC-K",
        )
        assert duplicate.applied is False

        executed = coordinator.execute_claim(second.claim_code)
        assert executed["ok"] is True
        assert executed["inserted"] == 0
        # L 仍可办理，K 不会因两端各自确认而再次交付
        assert coordinator.wide_table_count("L") == 2
        assert coordinator.wide_table_count("K") == 2
        assert coordinator.ledger.effective_row_count("K") == 2

    def test_wide_table_detail_has_at_most_one_row_per_key(
        self, rig: tuple[ReplayBackfillCoordinator, ReplayQueue],
    ) -> None:
        coordinator, _ = self._setup(rig)
        for _ in range(2):
            coordinator.submit_receipt(
                segment_code="K", attempt_no=1, outcome=OUTCOME_SUCCESS,
                error_fragment="K-err", row_keys=["k1", "k2"], write_credential="WC-K",
            )
        claim = coordinator.claim_next("值班")
        coordinator.execute_claim(claim.claim_code)
        again = coordinator.claim_next("值班")
        coordinator.execute_claim(again.claim_code)

        rows = coordinator.wide_table_detail("K")
        keys = [row["行标识"] for row in rows]
        assert len(keys) == len(set(keys)) == 2
