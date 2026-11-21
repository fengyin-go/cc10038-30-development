"""可续做采集回执（中断补交）业务行为测试。

覆盖题设主线：三行输入在第二行后中断并断线重连，上游重发已收行并夹带一条同编号
但正文变化的更正。核对：
1. 恢复窗口（已确认行 / 缺失行 / 待核更正行）；
2. 可供解析的日志范围（被更正取代的旧记录不参与解析）；
3. 最终计入标准化明细的行号与行身份（已确认行重发不换新身份）。
"""
from __future__ import annotations

import json
import threading

import pytest

from app.store import store


def _prepare_source(pipeline, source_id: str = "resume-src") -> None:
    pipeline.register_source(source_id, "http", "default")
    pipeline.validate_source(source_id)


class TestOpenResumableReceipt:
    """回执建立与恢复入口测试。"""

    def test_open_creates_receipt_and_batch(self, pipeline):
        _prepare_source(pipeline)
        result = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        assert result["ok"] is True
        receipt = result["receipt"]
        assert receipt["回执编号"].startswith("RCPT-")
        assert receipt["期望行总数"] == "3"
        # 恢复窗口一开始就是全部期望行，且全部缺失。
        window = result["resume_window"]
        assert window["missing_lines"] == [1, 2, 3]
        assert window["confirmed_lines"] == []
        assert window["is_complete"] is False
        # 预占的批次处于采集中。
        batch = next(
            b for b in store.rows("ingest_batch")
            if b["批次编号"] == result["batch_no"]
        )
        assert batch["status"] == "采集中"
        assert batch["resumable"] is True

    def test_open_pre_registers_missing_line_slots(self, pipeline):
        _prepare_source(pipeline)
        result = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        lines = store.rows("receipt_line")
        slots = [ln for ln in lines if ln["回执编号"] == result["receipt_no"]]
        assert sorted(int(ln["原始行号"]) for ln in slots) == [1, 2, 3]
        assert all(ln["确认状态"] == "缺失" for ln in slots)

    def test_open_rejects_unvalidated_source(self, pipeline):
        pipeline.register_source("unvalidated", "http", "default")
        result = pipeline.open_resumable_receipt("unvalidated", "2026-11-21", 3)
        assert result["ok"] is False
        assert "未通过验证" in result["message"]

    def test_open_rejects_line_no_out_of_range(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        result = pipeline.submit_line(opened["receipt_no"], 4, "x|y|z")
        assert result["ok"] is False
        assert "超出回执期望范围" in result["message"]


class TestInterruptAndResumeWindow:
    """中断登记与恢复窗口核对。"""

    def test_interrupt_after_second_line_freezes_window(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")

        interrupt = pipeline.mark_interrupt(receipt_no, "second line done, link down")
        assert interrupt["ok"] is True
        window = interrupt["resume_window"]
        # 恢复窗口：前两行已确认，第三行缺失，无待核更正。
        assert window["confirmed_lines"] == [1, 2]
        assert window["missing_lines"] == [3]
        assert window["pending_correction_lines"] == []
        assert window["confirmed_count"] == 2
        assert window["missing_count"] == 1
        assert window["is_complete"] is False

        receipt = pipeline.get_receipt(receipt_no)
        assert receipt["status"] == "待补齐"
        assert receipt["resume_window"]["missing_lines"] == [3]

    def test_resume_then_final_line_completes_window(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        pipeline.mark_interrupt(receipt_no)

        # 恢复后补第三行。
        third = pipeline.submit_line(receipt_no, 3, "t3|ERROR|line-3")
        assert third["classification"] == "新行"
        assert third["resume_window"]["missing_lines"] == []
        assert third["resume_window"]["is_complete"] is True


class TestRetransmitClassification:
    """接收与补交并发时区分原样重试、待核更正和新行。"""

    def test_identical_retransmit_is_retry_without_new_identity(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        first_raw_id = int(
            next(ln for ln in store.rows("receipt_line")
                 if ln["回执编号"] == receipt_no and ln["原始行号"] == "1")["关联原始记录ID"]
        )

        # 断线重连，上游把已收的第一行原样再发一次。
        retry = pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        assert retry["classification"] == "原样重试"
        # 已确认行不能单靠重发获得新的身份：raw_record_id 保持不变。
        assert retry["raw_record_id"] == first_raw_id
        line = next(ln for ln in store.rows("receipt_line")
                    if ln["回执编号"] == receipt_no and ln["原始行号"] == "1")
        assert line["重试次数"] == "1"
        assert line["关联原始记录ID"] == str(first_raw_id)
        # raw_records 没有因为重发新增记录。
        batch_no = opened["batch_no"]
        same_line_raw = [
            r for r in store.rows("raw_records")
            if r["批次编号"] == batch_no and r["原始行号"] == "1"
        ]
        assert len(same_line_raw) == 1

    def test_changed_retransmit_is_pending_correction(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        pipeline.mark_interrupt(receipt_no)

        # 恢复窗口里夹着一条同编号但正文变化的更正（第二行）。
        correction = pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2-CORRECTED")
        assert correction["classification"] == "待核更正"
        window = correction["resume_window"]
        assert window["pending_correction_lines"] == [2]
        assert window["confirmed_lines"] == [1]
        assert window["missing_lines"] == [3]
        assert window["is_complete"] is False

        line = next(ln for ln in store.rows("receipt_line")
                    if ln["回执编号"] == receipt_no and ln["原始行号"] == "2")
        # 原确认正文保留，待核正文单独挂起，两者都可回查。
        assert line["当前正文"] == "t2|WARN|line-2"
        assert line["待核正文"] == "t2|WARN|line-2-CORRECTED"
        assert line["当前正文摘要"] != line["待核摘要"]

    def test_original_retransmit_while_correction_pending_is_still_retry(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2-CORRECTED")
        # 上游又把原始版本重发了一遍。
        again = pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        assert again["classification"] == "原样重试"
        line = next(ln for ln in store.rows("receipt_line")
                    if ln["回执编号"] == receipt_no and ln["原始行号"] == "2")
        assert line["确认状态"] == "更正待核"
        assert line["待核正文"] == "t2|WARN|line-2-CORRECTED"

    def test_three_way_classification_in_one_resume_batch(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        pipeline.mark_interrupt(receipt_no)

        # 恢复批次同时包含：原样重试（行1）、待核更正（行2）、首次新行（行3）。
        r1 = pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        r2 = pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2-CORRECTED")
        r3 = pipeline.submit_line(receipt_no, 3, "t3|ERROR|line-3")
        assert r1["classification"] == "原样重试"
        assert r2["classification"] == "待核更正"
        assert r3["classification"] == "新行"

    def test_second_correction_replaces_pending_candidate_with_trace(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 1)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-2-CANDIDATE-A")
        # 又来一条不同的更正，替换掉挂起的候选 A。
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-2-CANDIDATE-B")

        detail = pipeline.get_receipt(receipt_no)
        line1 = next(ln for ln in detail["lines"] if ln["line_no"] == 1)
        assert line1["confirm_status"] == "更正待核"
        assert line1["pending_correction_body"] == "t1|INFO|line-2-CANDIDATE-B"
        # 被替换的候选 A 仍在版本历史里可回查，原文也在。
        bodies = [v["body"] for v in line1["version_history"]]
        assert "t1|INFO|line-1" in bodies
        assert "t1|INFO|line-2-CANDIDATE-A" in bodies
        assert "t1|INFO|line-2-CANDIDATE-B" not in bodies  # 当前候选未裁决，不进历史


class TestCorrectionResolution:
    """更正裁决与原文/更正可回查。"""

    def test_accept_correction_keeps_old_version_and_switches_active(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        batch_no = opened["batch_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        old_raw_id = int(
            next(ln for ln in store.rows("receipt_line")
                 if ln["回执编号"] == receipt_no and ln["原始行号"] == "2")["关联原始记录ID"]
        )
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2-CORRECTED")
        pipeline.submit_line(receipt_no, 3, "t3|ERROR|line-3")

        resolved = pipeline.resolve_correction(receipt_no, 2, "accept")
        assert resolved["ok"] is True
        new_raw_id = resolved["active_raw_record_id"]
        # 更正产生新版本 raw_records，身份行号不变但记录 ID 更新。
        assert new_raw_id != old_raw_id

        old_record = store.find("raw_records", old_raw_id)
        new_record = store.find("raw_records", new_raw_id)
        assert old_record["line_scope"] == "superseded"
        assert old_record["superseded_by"] == str(new_raw_id)
        assert new_record["line_scope"] == "active"
        assert new_record["supersedes_raw_id"] == str(old_raw_id)
        assert new_record["原始正文"] == "t2|WARN|line-2-CORRECTED"
        # 行身份（line_identity）保持不变，只是正文版本递增。
        assert new_record["line_identity"] == old_record["line_identity"]
        assert new_record["body_version"] == "2"

        detail = pipeline.get_receipt(receipt_no)
        line2 = next(ln for ln in detail["lines"] if ln["line_no"] == 2)
        assert line2["body"] == "t2|WARN|line-2-CORRECTED"
        assert line2["body_version"] == "2"
        # 原文和更正内容都在版本历史里可回查。
        bodies = [v["body"] for v in line2["version_history"]]
        assert "t2|WARN|line-2" in bodies
        assert "t2|WARN|line-2-CORRECTED" in bodies

    def test_reject_correction_keeps_original_body(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 2)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        active_before = int(
            next(ln for ln in store.rows("receipt_line")
                 if ln["回执编号"] == receipt_no and ln["原始行号"] == "2")["关联原始记录ID"]
        )
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2-CORRECTED")

        resolved = pipeline.resolve_correction(receipt_no, 2, "reject")
        assert resolved["ok"] is True
        # 驳回不换身份、不新建 raw_records。
        assert resolved["active_raw_record_id"] == active_before
        detail = pipeline.get_receipt(receipt_no)
        line2 = next(ln for ln in detail["lines"] if ln["line_no"] == 2)
        assert line2["body"] == "t2|WARN|line-2"
        # 被驳回的候选正文仍留痕可回查。
        bodies = [v["body"] for v in line2["version_history"]]
        assert "t2|WARN|line-2-CORRECTED" in bodies

    def test_resolve_without_pending_correction_fails(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 2)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        result = pipeline.resolve_correction(receipt_no, 1, "accept")
        assert result["ok"] is False
        assert "无待核更正" in result["message"]


class TestCompletenessGate:
    """未确认齐备的批次不能冒充完整输入。"""

    def test_complete_rejects_missing_lines(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        result = pipeline.complete_receipt(receipt_no)
        assert result["ok"] is False
        assert "不能关闭为完整输入" in result["message"]
        assert result["resume_window"]["missing_lines"] == [2, 3]

    def test_complete_rejects_pending_correction(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 2)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2-CORRECTED")
        result = pipeline.complete_receipt(receipt_no)
        assert result["ok"] is False
        assert result["resume_window"]["pending_correction_lines"] == [2]

    def test_parse_blocked_until_receipt_complete(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        batch_no = opened["batch_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        # 回执路径直接拒绝。
        blocked = pipeline.parse_resumable_batch(receipt_no)
        assert blocked["ok"] is False
        assert "输入不完整" in blocked["message"]
        # 通用解析路径同样拒绝，防止绕过回执冒充完整输入。
        blocked_generic = pipeline.parse_batch(batch_no)
        assert blocked_generic["ok"] is False
        assert "未确认齐备" in blocked_generic["message"]


class TestResumableParseScope:
    """可解析日志范围与最终计入明细的行号核对（题设主线全链路）。"""

    def test_full_interrupt_retransmit_correction_flow(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 3)
        receipt_no = opened["receipt_no"]
        batch_no = opened["batch_no"]

        # 第一阶段：第二行写完后断线。
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        interrupt = pipeline.mark_interrupt(receipt_no)
        assert interrupt["resume_window"]["confirmed_lines"] == [1, 2]
        assert interrupt["resume_window"]["missing_lines"] == [3]

        # 第二阶段：重发已收行（原样）+ 同编号更正 + 新行。
        retry = pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        correction = pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2-CORRECTED")
        new_line = pipeline.submit_line(receipt_no, 3, "t3|ERROR|line-3")
        assert retry["classification"] == "原样重试"
        assert correction["classification"] == "待核更正"
        assert new_line["classification"] == "新行"

        # 裁决：采纳第二行更正。
        pipeline.resolve_correction(receipt_no, 2, "accept")
        completed = pipeline.complete_receipt(receipt_no)
        assert completed["ok"] is True

        # 可供解析的日志：3 条 active；被取代的旧第二行 raw_records 被排除。
        active_raw = [
            r for r in store.rows("raw_records")
            if r["批次编号"] == batch_no and r["line_scope"] != "superseded"
        ]
        superseded_raw = [
            r for r in store.rows("raw_records")
            if r["批次编号"] == batch_no and r["line_scope"] == "superseded"
        ]
        assert len(active_raw) == 3
        assert len(superseded_raw) == 1
        assert superseded_raw[0]["原始正文"] == "t2|WARN|line-2"
        active_line2 = next(r for r in active_raw if r["原始行号"] == "2")
        assert active_line2["原始正文"] == "t2|WARN|line-2-CORRECTED"

        parse_result = pipeline.parse_resumable_batch(receipt_no)
        assert parse_result["ok"] is True
        assert parse_result["success"] == 3
        # 最终计入明细的行号为 1、2、3，且读取范围覆盖整个回执。
        assert parse_result["parsed_line_nos"] == [1, 2, 3]
        scope = parse_result["parsed_scope"]
        assert scope["line_range"] == [1, 3]
        assert scope["excluded_superseded_raw_ids"] == [superseded_raw[0]["id"]]

        # 标准化明细逐条关联 raw_records、行身份与实际解析的正文摘要。
        entities = [
            p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no
        ]
        assert len(entities) == 3
        by_line = {int(p["source_line_no"]): p for p in entities}
        assert sorted(by_line) == [1, 2, 3]
        line2_entity = by_line[2]
        line2_receipt = next(
            ln for ln in store.rows("receipt_line")
            if ln["回执编号"] == receipt_no and ln["原始行号"] == "2"
        )
        # 明细读取的正文范围与回执当前确认版本一致。
        assert line2_entity["source_body_digest"] == line2_receipt["parsed_body_digest"]
        assert line2_entity["原始记录ID"] == line2_receipt["关联原始记录ID"]
        assert line2_entity["line_identity"] == f"{receipt_no}#L2"
        assert line2_entity["source_body_version"] == "2"
        # 行1 经过原样重试，明细仍指向它最初的 raw_records 身份。
        line1_receipt = next(
            ln for ln in store.rows("receipt_line")
            if ln["回执编号"] == receipt_no and ln["原始行号"] == "1"
        )
        assert by_line[1]["原始记录ID"] == line1_receipt["关联原始记录ID"]
        assert line1_receipt["重试次数"] == "1"

        # 批次记录总数按有效记录计，不把被取代的旧记录算进去。
        batch = next(b for b in store.rows("ingest_batch") if b["批次编号"] == batch_no)
        assert batch["记录总数"] == "3"
        assert batch["status"] == "已完成"

    def test_rejected_correction_parses_original_body(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 2)
        receipt_no = opened["receipt_no"]
        batch_no = opened["batch_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2-CORRECTED")
        pipeline.resolve_correction(receipt_no, 2, "reject")
        pipeline.complete_receipt(receipt_no)

        parse_result = pipeline.parse_resumable_batch(receipt_no)
        assert parse_result["parsed_line_nos"] == [1, 2]
        entities = [
            p for p in store.rows("parsed_entity") if p.get("来源批次") == batch_no
        ]
        line2 = next(p for p in entities if p["source_line_no"] == "2")
        fields = json.loads(line2["标准化字段集"])
        assert fields["message"] == "line-2"
        # 驳回更正不产生 superseded 记录。
        assert parse_result["parsed_scope"]["excluded_superseded_raw_ids"] == []

    def test_parse_atomically_closes_receipt_and_moves_batch_to_pending(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 2)
        receipt_no = opened["receipt_no"]
        batch_no = opened["batch_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
        pipeline.submit_line(receipt_no, 2, "t2|WARN|line-2")

        # 未显式 complete 也允许解析，但解析必须原子关闭回执、推进批次状态。
        result = pipeline.parse_resumable_batch(receipt_no)
        assert result["ok"] is True
        receipt = pipeline.get_receipt(receipt_no)
        assert receipt["status"] == "已完成"
        batch = next(b for b in store.rows("ingest_batch") if b["批次编号"] == batch_no)
        assert batch["input_complete"] is True
        assert batch["status"] == "已完成"


class TestConcurrentSubmit:
    """接收与补交并发：分类与行身份必须一致。"""

    def test_concurrent_retry_and_new_lines_keep_single_identity(self, pipeline):
        _prepare_source(pipeline)
        opened = pipeline.open_resumable_receipt("resume-src", "2026-11-21", 4)
        receipt_no = opened["receipt_no"]
        pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")

        errors: list[str] = []

        def retry_line_one() -> None:
            try:
                for _ in range(5):
                    result = pipeline.submit_line(receipt_no, 1, "t1|INFO|line-1")
                    if result["classification"] != "原样重试":
                        errors.append("重发被错误分类")
            except Exception as exc:  # pragma: no cover - 并发保护失败时暴露
                errors.append(str(exc))

        def fill_other_lines() -> None:
            try:
                for line_no, body in [
                    (2, "t2|WARN|line-2"),
                    (3, "t3|ERROR|line-3"),
                    (4, "t4|INFO|line-4"),
                ]:
                    result = pipeline.submit_line(receipt_no, line_no, body)
                    if result["classification"] != "新行":
                        errors.append(f"行 {line_no} 被错误分类为 {result['classification']}")
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))

        t1 = threading.Thread(target=retry_line_one)
        t2 = threading.Thread(target=fill_other_lines)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert errors == []
        line1 = next(
            ln for ln in store.rows("receipt_line")
            if ln["回执编号"] == receipt_no and ln["原始行号"] == "1"
        )
        assert line1["重试次数"] == "5"
        # 并发重发没有给行1制造第二个 raw_records 身份。
        batch_no = opened["batch_no"]
        line1_raw = [
            r for r in store.rows("raw_records")
            if r["批次编号"] == batch_no and r["原始行号"] == "1"
        ]
        assert len(line1_raw) == 1
        window = pipeline.get_receipt(receipt_no)["resume_window"]
        assert window["is_complete"] is True
        assert window["confirmed_lines"] == [1, 2, 3, 4]
