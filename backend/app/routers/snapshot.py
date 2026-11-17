"""数据快照 时间线接口：返回作业时间序列结构。"""
from __future__ import annotations

import csv
import io
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import (
    ActionPayload,
    ActionResult,
    EntryPayload,
    ShardValidationPayload,
    ShardValidationResult,
)
from app.services.snapshot import SnapshotService

router = APIRouter(prefix="/api/snapshot", tags=["数据快照"])
service = SnapshotService()
LIST_FIELDS = ["快照编号", "数据集名称", "快照时间", "行数快照", "校验哈希", "快照策略", "存储位置", "快照状态"]
STATUSES = ["待执行", "执行中", "已完成", "执行失败"]


def csv_cell(value: Any) -> str:
    text = str(value) if value is not None else ""
    if text and text[0] in "=+-@":
        text = "'" + text
    return '"' + text.replace('"', '""') + '"'


@router.get("")
def list_entries() -> dict[str, Any]:
    items = service.get_timeline()
    return {"items": items}


@router.get("/export")
def export_entries() -> StreamingResponse:
    items = service.get_timeline()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    all_keys = ["id"]
    if items:
        all_keys = list(items[0].keys())
    writer.writerow(all_keys)
    for item in items:
        writer.writerow([csv_cell(item.get(k, "")) for k in all_keys])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": "attachment; filename=snapshot.csv"})


@router.get("/shards")
def list_shards(
    snapshot_code: str | None = Query(default=None, alias="快照编号"),
    version: str | None = Query(default=None, alias="版本"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> dict[str, Any]:
    """读取快照分片校验记录：按（快照编号, 版本, 分片序号）稳定排序后分页。"""
    return service.list_shards(snapshot_code, version, page, size)


@router.get("/{entry_id}")
def get_entry(entry_id: int) -> dict:
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"快照记录 {entry_id} 不存在")
    return entry


@router.post("")
def create_entry(payload: EntryPayload) -> ActionResult:
    entry_or_none, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="快照记录已登记", entry=entry_or_none)


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    entry_or_none, message = service.run_action(entry_id, payload.action)
    if entry_or_none is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry_or_none)


@router.post("/{entry_id}/shards/validate")
def validate_shards(entry_id: int, payload: ShardValidationPayload) -> ShardValidationResult:
    """编排快照分片校验记录并验证消费结果：完成状态与分片校验一起判定。"""
    entry_or_none, message, verification = service.validate_shards(
        entry_id,
        payload.shards,
        version=payload.version,
        repair=payload.repair,
        request_id=payload.request_id,
        total_shards=payload.total_shards,
    )
    if entry_or_none is None:
        return ShardValidationResult(ok=False, message=message, verification=verification)
    return ShardValidationResult(ok=True, message=message, entry=entry_or_none, verification=verification)


@router.post("/{entry_id}/recover")
def recover(entry_id: int, version: str | None = Query(default=None, alias="版本")) -> ActionResult:
    """恢复入口：只打开通过分片校验的可恢复版本；可显式指定回滚到的历史完整版本。"""
    result, message = service.recover(entry_id, version=version)
    if result is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=result)
