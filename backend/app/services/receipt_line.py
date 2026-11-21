"""回执行清单领域服务：逐行登记接收结果（已确认/缺失/更正待核）、正文版本与
raw_records 关联，是中断补交时定位已接收行的依据。
"""
from __future__ import annotations

from app.services.base import BaseModuleService


class ReceiptLineService(BaseModuleService):
    MODULE = "receipt_line"
    ENTITY = "回执行"
    OPERATION_TERM = "接收"
    LIST_FIELDS = [
        "回执编号",
        "原始行号",
        "接收分类",
        "确认状态",
        "当前正文摘要",
        "关联原始记录ID",
        "最近接收时间",
    ]
    REQUIRED_FIELDS = ["回执编号", "原始行号"]
    # 已确认：行已齐备且可参与解析；缺失：恢复窗口里还没收到；更正待核：同编号出现正文变化
    STATUS_ORDER = ["已确认", "缺失", "更正待核"]
    ACTION_RULES = {"确认更正": "已确认", "驳回更正": "已确认"}
    NEGATIVE_ACTIONS = []
