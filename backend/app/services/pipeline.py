"""跨模块数据采集流水线：编排 source_registry → ingest_batch → raw_records
→ field_parser → parsed_entity → dedup_engine → aggregation_view → data_export
的完整数据链路，包含版本追踪、失败恢复、批次去重和水位管理。
"""
from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from typing import Any

from app.store import store


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def _stable_digest(data: dict[str, Any]) -> str:
    serialized = json.dumps(data, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode()).hexdigest()[:16]


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


class IngestPipeline:
    """采集流水线统一编排，管理从数据源到导出的完整数据链路。"""

    # 接收与补交可能并发：同一回执的行写入需要串行化，保证分类与身份一致。
    _receipt_lock = threading.RLock()

    # ---------- 阶段0：数据源登记与版本 ----------
    def register_source(self, source_id: str, protocol: str, region: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                return {"ok": False, "message": f"数据源 {source_id} 已登记", "entry": s}
        entry = {
            "id": _next_id("source_registry"),
            "来源标识": source_id,
            "接入协议": protocol,
            "连接凭证": f"token-{_stable_digest({'source': source_id})}",
            "采集区域": region,
            "状态": source_id,
            "status": "待验证",
            "pending": True,
            "abnormal": False,
            "source_version": 1,
            "registered_at": _now_iso(),
        }
        sources.append(entry)
        return {"ok": True, "message": "数据源已登记", "entry": entry}

    def validate_source(self, source_id: str) -> dict[str, Any]:
        sources = store.rows("source_registry")
        for s in sources:
            if s.get("来源标识") == source_id:
                s["status"] = "已验证"
                s["pending"] = False
                s["abnormal"] = False
                s["source_version"] = s.get("source_version", 1) + 1
                s["validated_at"] = _now_iso()
                return {"ok": True, "message": f"数据源 {source_id} 已验证", "entry": s}
        return {"ok": False, "message": f"数据源 {source_id} 不存在"}

    # ---------- 阶段1：采集批次启动 ----------
    def start_ingest_batch(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """启动采集批次：登记批次并为其生成原始日志记录。"""
        sources = store.rows("source_registry")
        source_entry = None
        for s in sources:
            if s.get("来源标识") == source_id:
                source_entry = s
                break
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        # 检查是否有未完成的同源批次
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                return {"ok": False, "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}", "batch": b}

        batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
        batch_entry = {
            "id": _next_id("ingest_batch"),
            "批次编号": batch_no,
            "所属数据源": source_id,
            "采集时间窗口": time_window,
            "记录总数": str(len(raw_logs) if raw_logs else 0),
            "批次状态": time_window,
            "status": "采集中",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_version": source_entry.get("source_version", 1),
            "started_at": _now_iso(),
        }
        batches.append(batch_entry)

        # 生成原始日志记录
        raw_records_list = store.rows("raw_records")
        created_ids: list[int] = []
        if raw_logs:
            for idx, log in enumerate(raw_logs):
                raw_id = _next_id("raw_records")
                record = {
                    "id": raw_id,
                    "批次编号": batch_no,
                    "原始行号": str(idx + 1),
                    "采集时间戳": _now_iso(),
                    "原始正文": log.get("body", str(log)),
                    "解析状态": "未解析",
                    "status": "待解析",
                    "pending": True,
                    "abnormal": False,
                    "batch_version": 1,
                    "source_id": source_id,
                    "ingested_at": _now_iso(),
                }
                raw_records_list.append(record)
                created_ids.append(raw_id)

        batch_entry["记录总数"] = str(len(created_ids))
        batch_entry["raw_record_ids"] = json.dumps(created_ids)
        return {
            "ok": True,
            "message": f"采集批次 {batch_no} 已启动",
            "batch": batch_entry,
            "record_count": len(created_ids),
        }

    # ---------- 阶段1.5：可续做采集回执（中断补交） ----------
    def _body_digest(self, body: str) -> str:
        return hashlib.sha256(str(body).encode()).hexdigest()[:12]

    def _find_receipt(self, receipt_no: str) -> dict[str, Any] | None:
        for r in store.rows("ingest_receipt"):
            if r.get("回执编号") == receipt_no:
                return r
        return None

    def _receipt_lines(self, receipt_no: str) -> list[dict[str, Any]]:
        return [
            line for line in store.rows("receipt_line")
            if line.get("回执编号") == receipt_no
        ]

    def _find_line(self, receipt_no: str, line_no: int) -> dict[str, Any] | None:
        for line in self._receipt_lines(receipt_no):
            if int(line.get("原始行号", -1)) == line_no:
                return line
        return None

    def open_resumable_receipt(
        self,
        source_id: str,
        time_window: str,
        expected_total: int,
    ) -> dict[str, Any]:
        """登记可续做采集回执并预占一个"采集中"批次。

        回执记录期望行总数，逐行到达时只写回执行清单；中断后凭回执编号即可定位
        已确认行与缺失位置，继续补交。
        """
        sources = store.rows("source_registry")
        source_entry = next((s for s in sources if s.get("来源标识") == source_id), None)
        if source_entry is None:
            return {"ok": False, "message": f"数据源 {source_id} 未登记"}
        if source_entry.get("status") != "已验证":
            return {"ok": False, "message": f"数据源 {source_id} 未通过验证"}

        with self._receipt_lock:
            for b in store.rows("ingest_batch"):
                if b.get("所属数据源") == source_id and b.get("status") in ("待采集", "采集中"):
                    return {
                        "ok": False,
                        "message": f"数据源 {source_id} 已有进行中的批次 {b.get('批次编号')}",
                        "batch": b,
                    }

            batch_no = f"INGE-{_next_id('ingest_batch'):04d}"
            batch_entry = {
                "id": _next_id("ingest_batch"),
                "批次编号": batch_no,
                "所属数据源": source_id,
                "采集时间窗口": time_window,
                "记录总数": "0",
                "批次状态": time_window,
                "status": "采集中",
                "pending": True,
                "abnormal": False,
                "batch_version": 1,
                "source_version": source_entry.get("source_version", 1),
                "started_at": _now_iso(),
                "resumable": True,
                "raw_record_ids": "[]",
            }
            store.rows("ingest_batch").append(batch_entry)

            receipt_id = _next_id("ingest_receipt")
            receipt_no = f"RCPT-{receipt_id:04d}"
            receipt_entry = {
                "id": receipt_id,
                "回执编号": receipt_no,
                "所属批次": batch_no,
                "数据源": source_id,
                "采集时间窗口": time_window,
                "期望行总数": str(expected_total),
                "已确认行数": "0",
                "缺失行号": json.dumps(list(range(1, expected_total + 1))),
                "回执状态": "采集中",
                "status": "采集中",
                "pending": True,
                "abnormal": False,
                "opened_at": _now_iso(),
                "interrupted_at": "",
                "completed_at": "",
                "resume_window": json.dumps({}),
            }
            store.rows("ingest_receipt").append(receipt_entry)

            # 预登记缺失行：恢复窗口一开始就是全部期望行。
            line_rows = store.rows("receipt_line")
            for line_no in range(1, expected_total + 1):
                line_rows.append({
                    "id": _next_id("receipt_line"),
                    "回执编号": receipt_no,
                    "原始行号": str(line_no),
                    "接收分类": "缺失",
                    "确认状态": "缺失",
                    "status": "缺失",
                    "pending": True,
                    "abnormal": False,
                    "当前正文": "",
                    "当前正文摘要": "",
                    "正文版本": "0",
                    "历史版本": "[]",
                    "关联原始记录ID": "",
                    "重试次数": "0",
                    "更正次数": "0",
                    "待核正文": "",
                    "待核摘要": "",
                    "最近接收时间": "",
                    "首次接收时间": "",
                })

            return {
                "ok": True,
                "message": f"可续做采集回执 {receipt_no} 已建立，批次 {batch_no}",
                "receipt": receipt_entry,
                "receipt_no": receipt_no,
                "batch_no": batch_no,
                "resume_window": self._resume_window(receipt_entry),
            }

    def _resume_window(self, receipt: dict[str, Any]) -> dict[str, Any]:
        """根据回执行清单计算恢复窗口：已确认行、缺失行、待核更正行。"""
        lines = self._receipt_lines(receipt["回执编号"])
        confirmed = sorted(
            int(ln["原始行号"]) for ln in lines if ln.get("确认状态") == "已确认"
        )
        missing = sorted(
            int(ln["原始行号"]) for ln in lines if ln.get("确认状态") == "缺失"
        )
        pending_correction = sorted(
            int(ln["原始行号"]) for ln in lines if ln.get("确认状态") == "更正待核"
        )
        return {
            "receipt_no": receipt["回执编号"],
            "batch_no": receipt["所属批次"],
            "expected_total": int(receipt.get("期望行总数", 0)),
            "confirmed_lines": confirmed,
            "missing_lines": missing,
            "pending_correction_lines": pending_correction,
            "confirmed_count": len(confirmed),
            "missing_count": len(missing),
            "is_complete": not missing and not pending_correction
            and len(confirmed) == int(receipt.get("期望行总数", 0)),
        }

    def _refresh_receipt_totals(self, receipt: dict[str, Any]) -> dict[str, Any]:
        window = self._resume_window(receipt)
        receipt["已确认行数"] = str(window["confirmed_count"])
        receipt["缺失行号"] = json.dumps(window["missing_lines"])
        receipt["resume_window"] = json.dumps(window, ensure_ascii=False)
        if window["pending_correction_lines"]:
            receipt["回执状态"] = "更正待核"
            receipt["status"] = "更正待核"
            receipt["abnormal"] = True
        elif window["missing_lines"]:
            receipt["回执状态"] = "待补齐"
            receipt["status"] = "待补齐"
            receipt["abnormal"] = False
        else:
            receipt["回执状态"] = "已完成"
            receipt["status"] = "已完成"
            receipt["pending"] = False
            receipt["abnormal"] = False
            receipt["completed_at"] = receipt.get("completed_at") or _now_iso()
        return window

    def _create_raw_record(
        self,
        receipt: dict[str, Any],
        line: dict[str, Any],
        body: str,
        *,
        supersedes_id: int | None = None,
        reason: str = "首次接收",
    ) -> int:
        """为回执行落一条 raw_records；同一行的重试不新建身份，更正才产生新版本记录。"""
        raw_rows = store.rows("raw_records")
        batch_no = receipt["所属批次"]
        line_no = int(line["原始行号"])
        raw_id = _next_id("raw_records")
        record = {
            "id": raw_id,
            "批次编号": batch_no,
            "原始行号": str(line_no),
            "采集时间戳": _now_iso(),
            "原始正文": body,
            "解析状态": "未解析",
            "status": "待解析",
            "pending": True,
            "abnormal": False,
            "batch_version": 1,
            "source_id": receipt["数据源"],
            "ingested_at": _now_iso(),
            "receipt_no": receipt["回执编号"],
            "line_identity": f"{receipt['回执编号']}#L{line_no}",
            "body_version": line.get("正文版本", "1"),
            "receipt_reason": reason,
            "record_active": True,
            "supersedes_raw_id": str(supersedes_id) if supersedes_id else "",
            "line_scope": "active",
        }
        raw_rows.append(record)
        if supersedes_id is not None:
            old = store.find("raw_records", supersedes_id)
            if old is not None:
                old["line_scope"] = "superseded"
                old["record_active"] = False
                old["superseded_by"] = str(raw_id)
        # 批次记录总数始终等于当前有效（未被更正取代）的原始记录数。
        self._sync_batch_record_count(batch_no)
        return raw_id

    def _sync_batch_record_count(self, batch_no: str) -> None:
        active = [
            r for r in store.rows("raw_records")
            if r.get("批次编号") == batch_no and r.get("line_scope") != "superseded"
        ]
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == batch_no:
                b["记录总数"] = str(len(active))
                b["raw_record_ids"] = json.dumps([r["id"] for r in active])
                break

    def submit_line(
        self,
        receipt_no: str,
        line_no: int,
        body: str,
    ) -> dict[str, Any]:
        """向回执提交一行，自动区分三种情形：

        - 原样重试：行已确认且正文摘要一致 → 只累加重试计数，不产生新身份；
        - 待核更正：行已确认但正文变化 → 保留原文，挂起待核正文，不覆盖已确认内容；
        - 首次收到的新行：缺失位收到正文 → 落 raw_records 并确认该行。
        """
        with self._receipt_lock:
            receipt = self._find_receipt(receipt_no)
            if receipt is None:
                return {"ok": False, "message": f"回执 {receipt_no} 不存在"}
            expected = int(receipt.get("期望行总数", 0))
            if line_no < 1 or line_no > expected:
                return {
                    "ok": False,
                    "message": f"行号 {line_no} 超出回执期望范围 1..{expected}",
                }

            line = self._find_line(receipt_no, line_no)
            if line is None:  # 理论不可达：open 时已预登记
                return {"ok": False, "message": f"回执 {receipt_no} 缺少行 {line_no} 的登记位"}

            digest = self._body_digest(body)
            now = _now_iso()
            line["最近接收时间"] = now

            # 情形一：原样重试（含已确认行的重复重发、以及更正挂起期间原文再次到达）
            if digest == line.get("当前正文摘要") and line.get("当前正文摘要"):
                line["接收分类"] = "原样重试"
                line["重试次数"] = str(int(line.get("重试次数", "0")) + 1)
                window = self._refresh_receipt_totals(receipt)
                return {
                    "ok": True,
                    "classification": "原样重试",
                    "message": f"行 {line_no} 正文与已确认版本一致，按重试处理，身份不变",
                    "line_no": line_no,
                    "raw_record_id": int(line["关联原始记录ID"]) if line.get("关联原始记录ID") else None,
                    "resume_window": window,
                }

            # 情形二：待核更正（同编号、正文有变化，且该行已有确认正文）
            if line.get("确认状态") == "已确认" or line.get("当前正文摘要"):
                # 已有待核候选且与本次正文不同：先把被替换的候选留痕，保证可回查。
                if line.get("待核摘要") and line.get("待核摘要") != digest:
                    history = json.loads(line.get("历史版本") or "[]")
                    history.append({
                        "version": int(line.get("正文版本", "1")),
                        "body": line.get("待核正文", ""),
                        "digest": line.get("待核摘要", ""),
                        "raw_record_id": None,
                        "reason": "待核候选被新候选替换",
                        "received_at": line.get("最近接收时间", now),
                    })
                    line["历史版本"] = json.dumps(history, ensure_ascii=False)
                line["接收分类"] = "待核更正"
                line["确认状态"] = "更正待核"
                line["status"] = "更正待核"
                line["abnormal"] = True
                line["待核正文"] = body
                line["待核摘要"] = digest
                line["更正次数"] = str(int(line.get("更正次数", "0")) + 1)
                window = self._refresh_receipt_totals(receipt)
                return {
                    "ok": True,
                    "classification": "待核更正",
                    "message": f"行 {line_no} 正文发生变化，已挂起待核，原确认正文保留可回查",
                    "line_no": line_no,
                    "raw_record_id": int(line["关联原始记录ID"]) if line.get("关联原始记录ID") else None,
                    "resume_window": window,
                }

            # 情形三：首次收到的新行（此前为缺失位）
            raw_id = self._create_raw_record(receipt, line, body, reason="首次接收")
            line["接收分类"] = "新行"
            line["确认状态"] = "已确认"
            line["status"] = "已确认"
            line["pending"] = False
            line["abnormal"] = False
            line["当前正文"] = body
            line["当前正文摘要"] = digest
            line["正文版本"] = "1"
            line["历史版本"] = json.dumps([
                {"version": 1, "body": body, "digest": digest, "raw_record_id": raw_id,
                 "reason": "首次接收", "received_at": now}
            ], ensure_ascii=False)
            line["关联原始记录ID"] = str(raw_id)
            line["首次接收时间"] = now
            window = self._refresh_receipt_totals(receipt)
            return {
                "ok": True,
                "classification": "新行",
                "message": f"行 {line_no} 首次接收并确认，已生成原始日志记录 {raw_id}",
                "line_no": line_no,
                "raw_record_id": raw_id,
                "resume_window": window,
            }

    def mark_interrupt(self, receipt_no: str, reason: str = "连接中断") -> dict[str, Any]:
        """登记一次中断事件并固化当前恢复窗口（已确认/缺失/待核各行号）。"""
        with self._receipt_lock:
            receipt = self._find_receipt(receipt_no)
            if receipt is None:
                return {"ok": False, "message": f"回执 {receipt_no} 不存在"}
            receipt["interrupted_at"] = _now_iso()
            receipt["last_interrupt_reason"] = reason
            window = self._refresh_receipt_totals(receipt)
            return {
                "ok": True,
                "message": f"回执 {receipt_no} 已登记中断，恢复窗口已固化",
                "reason": reason,
                "resume_window": window,
            }

    def resolve_correction(
        self,
        receipt_no: str,
        line_no: int,
        decision: str,
    ) -> dict[str, Any]:
        """裁决待核更正：accept 采用新正文（保留原文版本可回查），reject 维持原文。"""
        with self._receipt_lock:
            receipt = self._find_receipt(receipt_no)
            if receipt is None:
                return {"ok": False, "message": f"回执 {receipt_no} 不存在"}
            line = self._find_line(receipt_no, line_no)
            if line is None:
                return {"ok": False, "message": f"回执 {receipt_no} 无行 {line_no}"}
            if line.get("确认状态") != "更正待核":
                return {
                    "ok": False,
                    "message": f"行 {line_no} 当前无待核更正（状态={line.get('确认状态')}）",
                }

            candidate = line.get("待核正文", "")
            candidate_digest = line.get("待核摘要", "") or self._body_digest(candidate)
            now = _now_iso()
            previous_raw_id = int(line["关联原始记录ID"]) if line.get("关联原始记录ID") else None

            if decision == "accept":
                new_version = int(line.get("正文版本", "1")) + 1
                # 先抬升行的正文版本，再落 raw_records，保证新记录记下的是新版本号。
                line["正文版本"] = str(new_version)
                raw_id = self._create_raw_record(
                    receipt, line, candidate,
                    supersedes_id=previous_raw_id, reason=f"更正采纳v{new_version}",
                )
                history = json.loads(line.get("历史版本") or "[]")
                history.append({
                    "version": new_version,
                    "body": candidate,
                    "digest": candidate_digest,
                    "raw_record_id": raw_id,
                    "reason": "更正采纳",
                    "received_at": now,
                })
                line["当前正文"] = candidate
                line["当前正文摘要"] = candidate_digest
                line["历史版本"] = json.dumps(history, ensure_ascii=False)
                line["关联原始记录ID"] = str(raw_id)
                line["接收分类"] = "更正已采纳"
                decision_msg = "更正已采纳，新版本生效，原文保留在历史版本可回查"
            elif decision == "reject":
                history = json.loads(line.get("历史版本") or "[]")
                history.append({
                    "version": int(line.get("正文版本", "1")),
                    "body": candidate,
                    "digest": candidate_digest,
                    "raw_record_id": None,
                    "reason": "更正驳回",
                    "received_at": now,
                })
                line["历史版本"] = json.dumps(history, ensure_ascii=False)
                line["接收分类"] = "更正已驳回"
                decision_msg = "更正已驳回，维持原确认正文，候选正文留痕可回查"
            else:
                return {"ok": False, "message": "decision 只支持 accept 或 reject"}

            line["确认状态"] = "已确认"
            line["status"] = "已确认"
            line["abnormal"] = False
            line["pending"] = False
            line["待核正文"] = ""
            line["待核摘要"] = ""
            line["最近接收时间"] = now
            window = self._refresh_receipt_totals(receipt)
            return {
                "ok": True,
                "decision": decision,
                "message": f"行 {line_no} {decision_msg}",
                "line_no": line_no,
                "active_raw_record_id": int(line["关联原始记录ID"]) if line.get("关联原始记录ID") else None,
                "resume_window": window,
            }

    def get_receipt(self, receipt_no: str) -> dict[str, Any]:
        """读取回执及其补交清单：逐行正文、缺失位置、更正轨迹与 raw_records 关联。"""
        receipt = self._find_receipt(receipt_no)
        if receipt is None:
            return {"ok": False, "message": f"回执 {receipt_no} 不存在"}
        lines = []
        for line in sorted(self._receipt_lines(receipt_no), key=lambda x: int(x["原始行号"])):
            lines.append({
                "line_no": int(line["原始行号"]),
                "classification": line.get("接收分类"),
                "confirm_status": line.get("确认状态"),
                "body": line.get("当前正文", ""),
                "body_digest": line.get("当前正文摘要", ""),
                "body_version": line.get("正文版本", "0"),
                "pending_correction_body": line.get("待核正文", ""),
                "retry_count": int(line.get("重试次数", "0")),
                "correction_count": int(line.get("更正次数", "0")),
                "raw_record_id": int(line["关联原始记录ID"]) if line.get("关联原始记录ID") else None,
                "first_received_at": line.get("首次接收时间", ""),
                "last_received_at": line.get("最近接收时间", ""),
                "version_history": json.loads(line.get("历史版本") or "[]"),
            })
        window = self._refresh_receipt_totals(receipt)
        return {
            "ok": True,
            "receipt_no": receipt_no,
            "batch_no": receipt["所属批次"],
            "status": receipt["回执状态"],
            "resume_window": window,
            "lines": lines,
        }

    def complete_receipt(self, receipt_no: str) -> dict[str, Any]:
        """关闭回执：未确认齐备（仍有缺失或待核更正）的批次不能冒充完整输入。"""
        with self._receipt_lock:
            receipt = self._find_receipt(receipt_no)
            if receipt is None:
                return {"ok": False, "message": f"回执 {receipt_no} 不存在"}
            window = self._refresh_receipt_totals(receipt)
            if not window["is_complete"]:
                receipt["abnormal"] = True
                return {
                    "ok": False,
                    "message": (
                        f"回执 {receipt_no} 尚未确认齐备：缺失行 {window['missing_lines']}，"
                        f"待核更正行 {window['pending_correction_lines']}，不能关闭为完整输入"
                    ),
                    "resume_window": window,
                }
            receipt["回执状态"] = "已完成"
            receipt["status"] = "已完成"
            receipt["pending"] = False
            receipt["abnormal"] = False
            receipt["completed_at"] = _now_iso()
            for b in store.rows("ingest_batch"):
                if b.get("批次编号") == receipt["所属批次"]:
                    b["status"] = "待解析"
                    b["pending"] = True
                    b["abnormal"] = False
                    b["receipt_no"] = receipt_no
                    b["input_complete"] = True
                    break
            return {
                "ok": True,
                "message": f"回执 {receipt_no} 已确认齐备并关闭，批次可进入解析",
                "resume_window": window,
            }

    def parse_resumable_batch(
        self,
        receipt_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """按回执解析：只读取已确认齐备范围内的有效原始记录正文。

        - 回执未关闭（仍有缺失/待核更正）时拒绝解析，防止不完整输入冒充完整批次；
        - 被更正取代的旧 raw_records（line_scope=superseded）不参与解析；
        - 每条明细回写来源 raw_records 与实际解析读取的正文范围，行号与回执对齐。
        """
        receipt = self._find_receipt(receipt_no)
        if receipt is None:
            return {"ok": False, "message": f"回执 {receipt_no} 不存在"}
        with self._receipt_lock:
            window = self._refresh_receipt_totals(receipt)
            if not window["is_complete"]:
                return {
                    "ok": False,
                    "message": (
                        f"回执 {receipt_no} 输入不完整，拒绝解析：缺失 {window['missing_lines']}，"
                        f"待核更正 {window['pending_correction_lines']}"
                    ),
                    "resume_window": window,
                }
            # 原子关闭回执：与并发提交共用同一把锁，关闭后批次才进入待解析。
            closure = self.complete_receipt(receipt_no)
            if not closure["ok"]:
                return closure
            batch_no = receipt["所属批次"]
        # 实际解析读取的正文范围：当前有效 raw_records 对应的行号区间。
        active_raw = [
            r for r in store.rows("raw_records")
            if r.get("批次编号") == batch_no and r.get("line_scope") != "superseded"
        ]
        active_raw.sort(key=lambda r: int(r.get("原始行号", 0)))
        if not active_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无有效原始日志记录"}

        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            rule = next((p for p in parsers if p.get("规则名称") == parser_rule_name), None)
        if rule is None:
            rule = next((p for p in parsers if p.get("status") == "已发布"), None)
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        schema_ver = next(
            (sv for sv in store.rows("schema_version") if sv.get("status") == "已发布"), None
        )
        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []
        parsed_line_nos: list[int] = []

        for raw in active_raw:
            raw_id = raw["id"]
            try:
                fields = self._parse_body(raw.get("原始正文", ""), rule)
                ent_id = _next_id("parsed_entity")
                line_no = int(raw.get("原始行号", 0))
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                    "receipt_no": receipt_no,
                    "source_line_no": str(line_no),
                    "line_identity": raw.get("line_identity", f"{receipt_no}#L{line_no}"),
                    "source_body_version": raw.get("body_version", "1"),
                    "source_body_digest": self._body_digest(raw.get("原始正文", "")),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
                parsed_line_nos.append(line_no)

                line = self._find_line(receipt_no, line_no)
                if line is not None:
                    line["parsed_entity_id"] = str(ent_id)
                    line["parsed_body_digest"] = entity["source_body_digest"]
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        line_nos_sorted = sorted(parsed_line_nos)
        parsed_scope = {
            "receipt_no": receipt_no,
            "batch_no": batch_no,
            "active_raw_record_ids": [r["id"] for r in active_raw],
            "parsed_line_nos": line_nos_sorted,
            "line_range": [line_nos_sorted[0], line_nos_sorted[-1]] if line_nos_sorted else [],
            "excluded_superseded_raw_ids": [
                r["id"] for r in store.rows("raw_records")
                if r.get("批次编号") == batch_no and r.get("line_scope") == "superseded"
            ],
        }
        for b in store.rows("ingest_batch"):
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["parsed_scope"] = json.dumps(parsed_scope, ensure_ascii=False)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        journal = store.rows("parse_journal")
        journal.append({
            "id": _next_id("parse_journal"),
            "操作类型": "回执续做解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
            "receipt_no": receipt_no,
            "parsed_scope": json.dumps(parsed_scope, ensure_ascii=False),
        })

        return {
            "ok": True,
            "message": f"回执 {receipt_no} 对应批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
            "parsed_line_nos": line_nos_sorted,
            "parsed_scope": parsed_scope,
        }

    # ---------- 阶段2：字段解析 ----------
    def parse_batch(
        self,
        batch_no: str,
        parser_rule_name: str | None = None,
    ) -> dict[str, Any]:
        """解析批次中的原始日志为标准化明细。"""
        raw_records_list = store.rows("raw_records")
        batch_raw = [r for r in raw_records_list if r.get("批次编号") == batch_no]
        if not batch_raw:
            return {"ok": False, "message": f"批次 {batch_no} 无原始日志记录"}

        # 可续做批次必须走回执解析：回执未确认齐备时不能冒充完整输入。
        for receipt in store.rows("ingest_receipt"):
            if receipt.get("所属批次") == batch_no:
                window = self._refresh_receipt_totals(receipt)
                if not window["is_complete"]:
                    return {
                        "ok": False,
                        "message": (
                            f"批次 {batch_no} 关联回执 {receipt['回执编号']} 未确认齐备"
                            f"（缺失 {window['missing_lines']}，待核更正 "
                            f"{window['pending_correction_lines']}），请先完成补交再解析"
                        ),
                        "resume_window": window,
                    }
                # 已被更正取代的旧记录不参与通用解析。
                batch_raw = [r for r in batch_raw if r.get("line_scope") != "superseded"]
                break

        # 查找匹配的解析规则
        parsers = store.rows("field_parser")
        rule = None
        if parser_rule_name:
            for p in parsers:
                if p.get("规则名称") == parser_rule_name:
                    rule = p
                    break
        if rule is None:
            for p in parsers:
                if p.get("status") == "已发布":
                    rule = p
                    break
        if rule is None:
            return {"ok": False, "message": "无可用解析规则，请先发布一条字段解析规则"}

        # 获取当前 schema_version
        schema_versions = store.rows("schema_version")
        schema_ver = None
        for sv in schema_versions:
            if sv.get("status") == "已发布":
                schema_ver = sv
                break

        parsed_entities = store.rows("parsed_entity")
        success_count = 0
        failure_count = 0
        parsed_ids: list[int] = []
        failed_ids: list[int] = []

        for raw in batch_raw:
            raw_id = raw["id"]
            try:
                # 模拟字段解析：将原始正文按规则打散
                body = raw.get("原始正文", "")
                fields = self._parse_body(body, rule)
                ent_id = _next_id("parsed_entity")
                entity = {
                    "id": ent_id,
                    "来源批次": batch_no,
                    "原始记录ID": str(raw_id),
                    "解析规则版本": rule.get("规则名称", "unknown"),
                    "标准化字段集": json.dumps(fields, ensure_ascii=False),
                    "校验状态": "",
                    "status": "待校验",
                    "pending": True,
                    "abnormal": False,
                    "parser_rule": rule.get("规则名称"),
                    "schema_version": schema_ver.get("版本号") if schema_ver else "v1.0",
                    "parsed_at": _now_iso(),
                    "content_digest": _stable_digest(fields),
                }
                parsed_entities.append(entity)
                raw["status"] = "已解析"
                raw["pending"] = False
                raw["解析状态"] = "已解析"
                success_count += 1
                parsed_ids.append(ent_id)
            except Exception:
                raw["status"] = "解析失败"
                raw["解析状态"] = "解析失败"
                raw["abnormal"] = True
                failure_count += 1
                failed_ids.append(raw_id)

        # 更新批次状态
        batches = store.rows("ingest_batch")
        for b in batches:
            if b.get("批次编号") == batch_no:
                b["status"] = "已完成" if failure_count == 0 else "部分失败"
                b["pending"] = failure_count > 0
                b["abnormal"] = failure_count > 0
                b["parsed_ids"] = json.dumps(parsed_ids)
                b["batch_version"] = b.get("batch_version", 1) + 1
                break

        # 记录解析日志
        journal = store.rows("parse_journal")
        journal_entry = {
            "id": _next_id("parse_journal"),
            "操作类型": "批量解析",
            "批次编号": batch_no,
            "执行时间": _now_iso(),
            "影响的记录数": str(success_count),
            "执行结果": "成功" if failure_count == 0 else "部分成功",
            "status": "成功" if failure_count == 0 else "部分成功",
            "pending": failure_count > 0,
            "abnormal": failure_count > 0,
            "success_count": str(success_count),
            "failure_count": str(failure_count),
        }
        journal.append(journal_entry)

        # 失败记录进入重试队列
        if failure_count > 0:
            retry_q = store.rows("retry_queue")
            for fid in failed_ids:
                retry_entry = {
                    "id": _next_id("retry_queue"),
                    "失败记录ID": str(fid),
                    "失败原因": "字段解析失败",
                    "重试次数": "0",
                    "最大重试": "3",
                    "下次重试时间": _now_iso(),
                    "status": "待重试",
                    "pending": True,
                    "abnormal": False,
                    "batch_no": batch_no,
                    "parser_rule": rule.get("规则名称"),
                }
                retry_q.append(retry_entry)

        return {
            "ok": True,
            "message": f"批次 {batch_no} 解析完成",
            "success": success_count,
            "failure": failure_count,
            "parsed_ids": parsed_ids,
            "failed_ids": failed_ids,
        }

    def _parse_body(self, body: str, rule: dict[str, Any]) -> dict[str, Any]:
        """模拟字段解析逻辑。种子数据中的规则可能只有占位文本，此时使用默认字段名。"""
        parts = body.split("|") if "|" in body else [body]
        raw_field_list = (rule.get("输出字段列表", "") or "").strip()
        # 识别真实字段列表：含逗号分隔且每段不超过20字
        if raw_field_list and "," in raw_field_list:
            field_names = [f.strip() for f in raw_field_list.split(",")]
        else:
            field_names = ["time", "level", "message"]
        fields: dict[str, Any] = {}
        for i, fn in enumerate(field_names):
            fn = fn.strip()
            if fn:
                fields[fn] = parts[i].strip() if i < len(parts) else ""
        return fields

    # ---------- 阶段3：去重 ----------
    def run_dedup(
        self,
        dedup_key_fields: list[str],
        window: str,
        retain_strategy: str = "keep_first",
    ) -> dict[str, Any]:
        """对标准化明细执行去重。"""
        dedup_strategies = store.rows("dedup_engine")
        parsed = store.rows("parsed_entity")

        # 构建去重策略
        strategy_entry = {
            "id": _next_id("dedup_engine"),
            "策略名称": f"dedup-{_now_iso()}",
            "去重键字段": ",".join(dedup_key_fields),
            "去重窗口": window,
            "保留策略": retain_strategy,
            "生效数据源": "全部",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
        }
        dedup_strategies.append(strategy_entry)

        # 执行去重
        seen: dict[str, int] = {}
        duplicates = 0
        for rec in parsed:
            if rec.get("status") != "校验通过":
                continue
            fields_json = rec.get("标准化字段集", "{}")
            try:
                fields_data = json.loads(fields_json)
            except (json.JSONDecodeError, TypeError):
                fields_data = {}
            key_parts = [str(fields_data.get(k, "")) for k in dedup_key_fields]
            key = "||".join(key_parts)
            if key in seen:
                if retain_strategy == "keep_first":
                    rec["status"] = "校验失败"
                    rec["abnormal"] = True
                    rec["dedup_id"] = str(seen[key])
                    duplicates += 1
                else:
                    # keep_last：把旧记录标记为重复
                    old = store.find("parsed_entity", seen[key])
                    if old:
                        old["status"] = "校验失败"
                        old["abnormal"] = True
                        old["dedup_id"] = str(rec["id"])
                    rec["status"] = "校验通过"
                    rec["dedup_id"] = ""
                    seen[key] = rec["id"]
            else:
                seen[key] = rec["id"]

        return {
            "ok": True,
            "message": f"去重完成",
            "strategy": strategy_entry["策略名称"],
            "dedup_key_fields": dedup_key_fields,
            "duplicates_found": duplicates,
            "unique_keys": len(seen),
        }

    # ---------- 阶段4：校验 ----------
    def validate_entities(self, batch_no: str | None = None) -> dict[str, Any]:
        """对标准化明细执行质量校验。"""
        parsed = store.rows("parsed_entity")
        quality_rules = store.rows("quality_check")

        active_rule = None
        for r in quality_rules:
            if r.get("status") == "已生效":
                active_rule = r
                break

        target = [p for p in parsed if p.get("status") == "待校验"]
        if batch_no:
            target = [p for p in target if p.get("来源批次") == batch_no]

        passed = 0
        failed = 0
        for entity in target:
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                fields_data = {}

            is_valid = bool(fields_data) and all(
                str(v).strip() for v in fields_data.values()
            )
            if is_valid:
                entity["status"] = "校验通过"
                entity["校验状态"] = "校验通过"
                entity["pending"] = False
                entity["validated_at"] = _now_iso()
                if active_rule:
                    entity["quality_rule"] = active_rule.get("规则名称")
                passed += 1
            else:
                entity["status"] = "校验失败"
                entity["校验状态"] = "校验失败"
                entity["abnormal"] = True
                failed += 1

        return {
            "ok": True,
            "message": f"校验完成",
            "passed": passed,
            "failed": failed,
            "quality_rule": active_rule.get("规则名称") if active_rule else None,
        }

    # ---------- 阶段5：聚合视图刷新 ----------
    def refresh_aggregation(
        self,
        view_name: str,
        dim_field: str,
        metric_field: str,
        agg_func: str = "count",
    ) -> dict[str, Any]:
        """基于标准化明细刷新聚合视图。"""
        parsed = store.rows("parsed_entity")
        results: dict[str, dict[str, Any]] = {}

        for entity in parsed:
            if entity.get("status") != "校验通过":
                continue
            try:
                fields_data = json.loads(entity.get("标准化字段集", "{}"))
            except (json.JSONDecodeError, TypeError):
                continue

            dim_val = str(fields_data.get(dim_field, "unknown"))
            metric_val = fields_data.get(metric_field, "0")

            if dim_val not in results:
                results[dim_val] = {"count": 0, "sum": 0.0, "values": []}

            results[dim_val]["count"] += 1
            try:
                results[dim_val]["sum"] += float(metric_val) if metric_val else 0.0
            except (ValueError, TypeError):
                pass
            results[dim_val]["values"].append(metric_val)

        # 创建/更新聚合视图
        aggs = store.rows("aggregation_view")
        agg_entry = {
            "id": _next_id("aggregation_view"),
            "视图名称": view_name,
            "聚合维度": dim_field,
            "指标定义": f"{agg_func}({metric_field})",
            "数据源范围": "标准化明细",
            "刷新间隔": "manual",
            "status": "已生效",
            "pending": False,
            "abnormal": False,
            "results": results,
            "refreshed_at": _now_iso(),
            "source_records": len(parsed),
        }
        aggs.append(agg_entry)

        return {
            "ok": True,
            "message": f"聚合视图 {view_name} 已刷新",
            "view": agg_entry,
            "dimensions": len(results),
        }

    # ---------- 阶段6：数据导出 ----------
    def build_export(
        self,
        view_name: str,
        export_format: str = "csv",
        filters: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """从聚合视图或标准化明细生成导出数据。"""
        exports = store.rows("data_export")
        lineages = store.rows("data_lineage")

        # 查找目标视图
        aggs = store.rows("aggregation_view")
        view = None
        for a in aggs:
            if a.get("视图名称") == view_name:
                view = a
                break

        results_data = view.get("results", {}) if view else {}

        export_id = _next_id("data_export")
        export_entry = {
            "id": export_id,
            "导出范围": view_name,
            "目标格式": export_format,
            "过滤条件": json.dumps(filters or {}, ensure_ascii=False),
            "导出状态": "",
            "文件路径": f"/exports/{view_name}.{export_format}",
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "results": results_data,
            "record_count": len(results_data),
            "exported_at": _now_iso(),
        }
        exports.append(export_entry)

        # 记录血缘
        lineage_entry = {
            "id": _next_id("data_lineage"),
            "来源批次": view_name,
            "解析规则版本": view.get("视图名称", "unknown") if view else "unknown",
            "标准化版本": view_name,
            "下游消费标识": f"export-{export_id}",
            "记录时间": _now_iso(),
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "export_id": str(export_id),
            "export_format": export_format,
        }
        lineages.append(lineage_entry)

        return {
            "ok": True,
            "message": f"数据导出 {view_name}.{export_format} 完成",
            "export": export_entry,
            "lineage": lineage_entry,
        }

    # ---------- 水位管理 ----------
    def advance_watermark(
        self,
        source_id: str,
        batch_no: str,
        watermark_value: str,
    ) -> dict[str, Any]:
        """推进指定数据源的采集水位，并防止倒退。"""
        watermarks = store.rows("watermark_mgr")
        existing = None
        for w in watermarks:
            if w.get("数据源") == source_id:
                existing = w
                break

        if existing is None:
            entry = {
                "id": _next_id("watermark_mgr"),
                "数据源": source_id,
                "最后成功批次": batch_no,
                "当前水位值": watermark_value,
                "水位类型": "增量时间戳",
                "更新时间": _now_iso(),
                "status": "正常",
                "pending": False,
                "abnormal": False,
                "previous_value": "",
                "is_rollback": False,
            }
            watermarks.append(entry)
            return {"ok": True, "message": f"数据源 {source_id} 水位已初始化", "watermark": entry}

        prev_value = existing.get("当前水位值", "")
        # 防止水位倒退
        if watermark_value < prev_value:
            existing["status"] = "异常回退"
            existing["abnormal"] = True
            existing["previous_value"] = prev_value
            existing["is_rollback"] = True
            existing["更新时间"] = _now_iso()
            return {"ok": False, "message": f"水位倒退被阻止：{watermark_value} < {prev_value}"}

        existing["最后成功批次"] = batch_no
        existing["当前水位值"] = watermark_value
        existing["更新时间"] = _now_iso()
        existing["status"] = "正常" if existing.get("status") == "异常回退" else existing.get("status", "正常")
        existing["previous_value"] = prev_value
        existing["is_rollback"] = False
        existing["abnormal"] = False

        return {"ok": True, "message": f"数据源 {source_id} 水位已推进到 {watermark_value}"}

    # ---------- 回溯重放 ----------
    def replay_batch(
        self,
        history_batch_range: str,
        new_parser_rule: str,
    ) -> dict[str, Any]:
        """用新解析规则版本回溯重放历史批次。"""
        replayers = store.rows("replay_engine")

        replay_id = _next_id("replay_engine")
        replay_entry = {
            "id": replay_id,
            "历史批次范围": history_batch_range,
            "新解析规则版本": new_parser_rule,
            "重放状态": "已创建",
            "产出记录数": "0",
            "执行时间": _now_iso(),
            "status": "重放中",
            "pending": True,
            "abnormal": False,
        }
        replayers.append(replay_entry)

        # 对历史批次的原始日志重新解析
        parsed = store.rows("parsed_entity")
        replay_count = 0
        for entity in parsed:
            if entity.get("status") == "校验通过":
                entity["status"] = "待校验"
                entity["pending"] = True
                entity["解析规则版本"] = new_parser_rule
                entity["replay_id"] = str(replay_id)
                entity["replayed_at"] = _now_iso()
                replay_count += 1

        replay_entry["产出记录数"] = str(replay_count)
        replay_entry["status"] = "已完成"
        replay_entry["pending"] = False
        replay_entry["重放状态"] = "已完成"

        return {
            "ok": True,
            "message": f"历史批次 {history_batch_range} 用规则 {new_parser_rule} 重放完成",
            "replay": replay_entry,
            "replayed_count": replay_count,
        }

    # ---------- Schema 版本演进 ----------
    def evolve_schema(
        self,
        version: str,
        data_source: str,
        field_defs: list[str],
        backward_compatible: bool = True,
    ) -> dict[str, Any]:
        """发布新 Schema 版本并检查向后兼容性。"""
        schemas = store.rows("schema_version")

        # 如果已有同名版本，不能重复发布
        for s in schemas:
            if s.get("版本号") == version:
                return {"ok": False, "message": f"Schema 版本 {version} 已存在"}

        entry = {
            "id": _next_id("schema_version"),
            "版本号": version,
            "适用数据源": data_source,
            "字段定义": json.dumps(field_defs, ensure_ascii=False),
            "向后兼容标记": "是" if backward_compatible else "否",
            "发布说明": f"版本 {version} 发布，兼容={backward_compatible}",
            "status": "已发布",
            "pending": False,
            "abnormal": False,
            "published_at": _now_iso(),
            "field_count": len(field_defs),
        }
        schemas.append(entry)

        return {"ok": True, "message": f"Schema 版本 {version} 已发布", "schema": entry}

    # ---------- 全链路流水线 ----------
    def run_full_pipeline(
        self,
        source_id: str,
        time_window: str,
        raw_logs: list[dict[str, Any]],
        parser_rule: str | None = None,
    ) -> dict[str, Any]:
        """执行完整采集 → 解析 → 去重 → 校验 → 聚合 → 导出的全链路。"""
        steps: dict[str, Any] = {}

        # S0: 登记并验证数据源
        reg = self.register_source(source_id, "http", "default")
        if not reg["ok"]:
            return {"ok": False, "message": reg["message"]}
        self.validate_source(source_id)
        steps["source_registry"] = reg

        # S1: 启动采集批次
        batch_result = self.start_ingest_batch(source_id, time_window, raw_logs)
        if not batch_result["ok"]:
            return {"ok": False, "message": batch_result["message"]}
        steps["ingest_batch"] = batch_result
        batch_no = batch_result["batch"]["批次编号"]

        # S2: 字段解析
        parse_result = self.parse_batch(batch_no, parser_rule)
        steps["field_parser"] = parse_result

        # S3: 校验
        valid_result = self.validate_entities(batch_no)
        steps["validate"] = valid_result

        # S4: 去重
        dedup_result = self.run_dedup(["time", "level"], "batch", "keep_first")
        steps["dedup"] = dedup_result

        # S5: 聚合
        agg_result = self.refresh_aggregation(f"view-{batch_no}", "level", "time", "count")
        steps["aggregation"] = agg_result

        # S6: 导出
        export_result = self.build_export(f"view-{batch_no}", "csv")
        steps["export"] = export_result

        # S7: 水位推进
        wm_result = self.advance_watermark(source_id, batch_no, time_window)
        steps["watermark"] = wm_result

        return {
            "ok": True,
            "message": "全链路流水线执行完成",
            "batch_no": batch_no,
            "steps": steps,
        }

    # ---------- 失败恢复 ----------
    def recover_batch(self, batch_no: str) -> dict[str, Any]:
        """恢复失败的采集批次：重试解析失败记录。"""
        retry_q = store.rows("retry_queue")
        raw_records_list = store.rows("raw_records")

        failed_raw = [r for r in raw_records_list
                      if r.get("批次编号") == batch_no and r.get("status") == "解析失败"]

        recovered = 0
        still_failed = 0
        for raw in failed_raw:
            # 查找对应重试记录
            retry_entry = None
            for r in retry_q:
                if r.get("失败记录ID") == str(raw["id"]):
                    retry_entry = r
                    break
            if retry_entry:
                retry_count = int(retry_entry.get("重试次数", "0"))
                max_retry = int(retry_entry.get("最大重试", "3"))
                if retry_count >= max_retry:
                    retry_entry["status"] = "超过上限"
                    still_failed += 1
                    continue
                retry_entry["重试次数"] = str(retry_count + 1)
                retry_entry["status"] = "重试中"
                retry_entry["下次重试时间"] = _now_iso()

            # 重试解析
            body = raw.get("原始正文", "")
            parts = body.split("|") if "|" in body else [body]
            if len(parts) >= 2:
                raw["status"] = "待解析" if retry_entry else raw["status"]
                raw["解析状态"] = "待解析"
                raw["pending"] = True
                raw["abnormal"] = False
                recovered += 1
            else:
                raw["status"] = "解析失败"
                raw["abnormal"] = True
                still_failed += 1

        return {
            "ok": True,
            "message": f"批次 {batch_no} 恢复完成",
            "recovered": recovered,
            "still_failed": still_failed,
        }


pipeline = IngestPipeline()