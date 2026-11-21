"""领域服务注册表：通用资源路由按模块 key 分派到独立业务服务。"""
from __future__ import annotations

from app.services.base import BaseModuleService
from app.services.source_registry import SourceRegistryService
from app.services.ingest_batch import IngestBatchService
from app.services.field_parser import FieldParserService
from app.services.raw_records import RawRecordsService
from app.services.parsed_entity import ParsedEntityService
from app.services.dedup_engine import DedupEngineService
from app.services.watermark_mgr import WatermarkMgrService
from app.services.retry_queue import RetryQueueService
from app.services.format_adapter import FormatAdapterService
from app.services.quality_check import QualityCheckService
from app.services.data_archive import DataArchiveService
from app.services.rate_limiter import RateLimiterService
from app.services.parse_journal import ParseJournalService
from app.services.rpa_workflow import RpaWorkflowService
from app.services.schema_version import SchemaVersionService
from app.services.alert_policy import AlertPolicyService
from app.services.data_export import DataExportService
from app.services.replay_engine import ReplayEngineService
from app.services.aggregation_view import AggregationViewService
from app.services.data_lineage import DataLineageService
from app.services.ingest_receipt import IngestReceiptService
from app.services.receipt_line import ReceiptLineService

SERVICES: dict[str, BaseModuleService] = {
    "source_registry": SourceRegistryService(),
    "ingest_batch": IngestBatchService(),
    "field_parser": FieldParserService(),
    "raw_records": RawRecordsService(),
    "parsed_entity": ParsedEntityService(),
    "dedup_engine": DedupEngineService(),
    "watermark_mgr": WatermarkMgrService(),
    "retry_queue": RetryQueueService(),
    "format_adapter": FormatAdapterService(),
    "quality_check": QualityCheckService(),
    "data_archive": DataArchiveService(),
    "rate_limiter": RateLimiterService(),
    "parse_journal": ParseJournalService(),
    "rpa_workflow": RpaWorkflowService(),
    "schema_version": SchemaVersionService(),
    "alert_policy": AlertPolicyService(),
    "data_export": DataExportService(),
    "replay_engine": ReplayEngineService(),
    "aggregation_view": AggregationViewService(),
    "data_lineage": DataLineageService(),
    "ingest_receipt": IngestReceiptService(),
    "receipt_line": ReceiptLineService(),
}


def service_for(module: str) -> BaseModuleService | None:
    return SERVICES.get(module)
