"""采集回执领域服务。

回执本身是可续做采集的恢复入口：每条已收正文、缺失位置、重试与更正轨迹都落在
回执行清单（receipt_line）里，由 app.services.pipeline.IngestPipeline 维护，
本服务只提供回执表头的通用查询/登记骨架。
"""
from __future__ import annotations

from app.services.base import BaseModuleService


class IngestReceiptService(BaseModuleService):
    MODULE = "ingest_receipt"
    ENTITY = "采集回执"
    OPERATION_TERM = "补交"
    LIST_FIELDS = [
        "回执编号",
        "所属批次",
        "数据源",
        "期望行总数",
        "已确认行数",
        "缺失行号",
        "回执状态",
    ]
    REQUIRED_FIELDS = ["回执编号", "所属批次", "数据源"]
    STATUS_ORDER = ["采集中", "待补齐", "已完成", "更正待核"]
    ACTION_RULES = {"标记补齐": "待补齐", "确认完结": "已完成"}
    NEGATIVE_ACTIONS = []
