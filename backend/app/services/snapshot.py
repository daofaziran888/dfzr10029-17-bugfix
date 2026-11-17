"""数据快照业务规则：分片校验、版本完整性与恢复入口。

快照完成状态不再只看作业动作，而是由分片校验结果与历史完整快照共同判定：

* 只有全部分片校验通过、行数一致，才允许进入「已完成」并成为可恢复版本；
* 任一分片失败 / 无数据 / 缺失时，当前版本只能是「执行失败」，半成品绝不可恢复；
* 失败时保留上一份完整快照版本作为恢复入口，中断恢复的异常结果进入待核对区；
* 分片记录以（快照编号, 快照版本, 分片序号）为唯一键，重复提交只保留一份有效结果。
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Any

from app.store import store

MODULE = "snapshot"
SHARD_MODULE = "snapshot_shard"
VERSION_MODULE = "snapshot_version"
REVIEW_MODULE = "snapshot_review"
REQUIRED_FIELDS = ["快照编号", "数据集名称", "快照时间"]
STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
ACTION_RULES = {"执行快照": "执行中", "暂停快照": "待执行", "标记失败": "执行失败"}
NEGATIVE_ACTIONS = []

SHARD_PASSED = "通过"
SHARD_FAILED = "失败"
SHARD_EMPTY = "无数据"
SHARD_PENDING = "待校验"

# 分片校验结果里参与指纹计算的字段，顺序固定，保证重复提交可判定。
SHARD_FIELDS = ["分片序号", "分片行数", "期望行数", "校验哈希", "期望哈希", "校验状态"]

# 恢复建议的两种入口：回滚到历史完整版本，或发布带修复标记的新版本。
RECOVER_BY_ROLLBACK = "回滚上一份完整快照"
RECOVER_BY_REPAIR = "发布带修复标记的新快照"

EMPTY_STATE_REASON = "空态：尚未提交任何分片校验记录，作业仍在执行中"
NO_DATA_REASON = "无数据：分片校验记录存在但分片行数为 0，无法作为数据快照"
FAILED_SHARD_REASON = "分片失败：存在未通过校验的分片，半成品不可恢复"
UNDECLARED_SHARD_REASON = "未声明期望分片数：无法判定快照是否完整，不允许标记完成"
MANUAL_FAIL_REASON = "人工标记失败：当前版本不可恢复，半成品不进入恢复清单"


def _fingerprint(shard: dict[str, Any], snapshot_code: str, version: str) -> str:
    """分片校验记录的稳定指纹，用于识别重复提交。"""
    payload = {
        "快照编号": snapshot_code,
        "快照版本": version,
        **{field: shard.get(field) for field in SHARD_FIELDS},
    }
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _version_key(version: Any) -> tuple[int, str]:
    """版本号按自然序比较，避免 v10 排在 v2 前面。"""
    text = str(version or "")
    digits = "".join(char for char in text if char.isdigit())
    return (int(digits) if digits else 0, text)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _shard_status(raw: Any) -> str:
    """把外部提交的校验结果归一到三种可判定状态。"""
    text = str(raw or "").strip()
    if text in {SHARD_PASSED, "通过校验", "成功", "pass", "passed"}:
        return SHARD_PASSED
    if text in {SHARD_EMPTY, "空数据", "无记录"}:
        return SHARD_EMPTY
    if text in {SHARD_FAILED, "校验失败", "失败", "fail", "failed"}:
        return SHARD_FAILED
    return SHARD_PENDING


def _shard_verdict(shard: dict[str, Any]) -> str:
    """分片是否真正可用：状态通过、行数达到期望、哈希一致，三者缺一不可。"""
    if _shard_status(shard.get("校验状态")) != SHARD_PASSED:
        return _shard_status(shard.get("校验状态"))
    if _int(shard.get("分片行数")) <= 0:
        return SHARD_EMPTY
    expected_rows = shard.get("期望行数")
    if expected_rows not in (None, "") and _int(expected_rows) != _int(shard.get("分片行数")):
        return SHARD_FAILED
    expected_hash = shard.get("期望哈希")
    if expected_hash not in (None, "") and str(expected_hash) != str(shard.get("校验哈希")):
        return SHARD_FAILED
    return SHARD_PASSED


class SnapshotService:
    def __init__(self) -> None:
        # 请求级幂等表：同一请求号重复提交直接返回首次结果，不再写入。
        self._request_log: dict[tuple[str, str], dict[str, Any]] = {}

    # ------------------------------------------------------------------ 读取
    @staticmethod
    def _row_version(row: dict[str, Any]) -> str:
        """兼容既有数据：早期快照分片记录没有版本字段，按首个版本处理。"""
        return str(row.get("快照版本") or "v1")

    def _shard_rows(self, snapshot_code: str | None = None, version: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(SHARD_MODULE)
        return [
            row for row in rows
            if (snapshot_code is None or row.get("快照编号") == snapshot_code)
            and (version is None or self._row_version(row) == str(version))
        ]

    def _sorted_shards(self, snapshot_code: str | None = None, version: str | None = None) -> list[dict[str, Any]]:
        """按（快照编号, 版本, 分片序号）稳定排序，避免列表错位。"""
        rows = self._shard_rows(snapshot_code, version)
        return sorted(
            rows,
            key=lambda row: (
                str(row.get("快照编号", "")),
                _version_key(self._row_version(row)),
                _int(row.get("分片序号")),
            ),
        )

    def _version_row(self, snapshot_code: str, version: str) -> dict[str, Any] | None:
        for row in store.rows(VERSION_MODULE):
            if row.get("快照编号") == snapshot_code and row.get("版本") == version:
                return row
        return None

    def _complete_versions(self, snapshot_code: str) -> list[dict[str, Any]]:
        rows = [
            row for row in store.rows(VERSION_MODULE)
            if row.get("快照编号") == snapshot_code and row.get("状态") == "完整"
        ]
        return sorted(rows, key=lambda row: (str(row.get("完成时间", "")), _version_key(row.get("版本"))))

    def _current_recoverable(self, snapshot_code: str) -> dict[str, Any] | None:
        for row in store.rows(VERSION_MODULE):
            if row.get("快照编号") == snapshot_code and row.get("当前可恢复"):
                return row
        return None

    def _previous_complete(self, snapshot_code: str, version: str) -> dict[str, Any] | None:
        """上一份完整快照：版本号严格更小，绝不把当前版本自己当成回滚目标。"""
        candidates = [
            row for row in self._complete_versions(snapshot_code)
            if _version_key(row.get("版本")) < _version_key(version)
        ]
        return candidates[-1] if candidates else None

    def shard_summary(self, snapshot_code: str, version: str) -> dict[str, Any]:
        shards = self._sorted_shards(snapshot_code, version)
        passed = sum(1 for shard in shards if _shard_verdict(shard) == SHARD_PASSED)
        failed = sum(1 for shard in shards if _shard_verdict(shard) == SHARD_FAILED)
        empty = sum(1 for shard in shards if _shard_verdict(shard) == SHARD_EMPTY)
        pending = sum(1 for shard in shards if _shard_verdict(shard) == SHARD_PENDING)
        return {
            "记录数": len(shards),
            "通过": passed,
            "失败": failed,
            "无数据": empty,
            "待校验": pending,
        }

    def _diagnose(self, snapshot_code: str, version: str, total: int) -> dict[str, Any]:
        """空态、无数据、分片失败分别给出原因，供页面与待核对区复用。"""
        shards = self._sorted_shards(snapshot_code, version)
        summary = self.shard_summary(snapshot_code, version)
        existing = {_int(shard.get("分片序号")) for shard in shards}
        # 期望分片数只认显式声明的值：用 max(分片序号) 推全会把「缺最后一片」
        # 当成完整快照，正是失败分片穿过完成边界的根因。
        expected = total
        missing = [index for index in range(1, expected + 1) if index not in existing]
        failed_indexes = [
            _int(shard.get("分片序号")) for shard in shards
            if _shard_verdict(shard) == SHARD_FAILED
        ]
        empty_indexes = [
            _int(shard.get("分片序号")) for shard in shards
            if _shard_verdict(shard) == SHARD_EMPTY
        ]

        def result(kind: str, reason: str) -> dict[str, Any]:
            return {
                "原因类型": kind,
                "原因": reason,
                "失败分片": failed_indexes,
                "无数据分片": empty_indexes,
                "缺失分片": missing,
                "可完成": False,
            }

        # 空态：还没有任何分片校验记录，无论是否声明了分片总数，作业都尚未产出结果。
        if not shards:
            return {
                "原因类型": "空态",
                "原因": EMPTY_STATE_REASON,
                "失败分片": [],
                "无数据分片": [],
                "缺失分片": missing,
                "可完成": False,
            }
        # 无数据：有校验记录但分片行数为 0，整份快照没有可用数据。
        if summary["通过"] == 0 and empty_indexes:
            return result("无数据", NO_DATA_REASON)
        if expected <= 0:
            return result("分片失败", UNDECLARED_SHARD_REASON)
        # 分片失败：失败 / 无数据 / 缺失任一存在，或通过数不足期望分片数。
        if failed_indexes or empty_indexes or missing or summary["通过"] < expected:
            return result("分片失败", FAILED_SHARD_REASON)
        return {
            "原因类型": "",
            "原因": "全部分片校验通过",
            "失败分片": [],
            "无数据分片": [],
            "缺失分片": [],
            "可完成": True,
        }

    def _resolve_version(self, entry: dict[str, Any]) -> str:
        return str(entry.get("版本") or "v1")

    def _next_version(self, current: Any) -> str:
        major, _, rest = str(current or "v1").partition(".")
        digits = "".join(char for char in major if char.isdigit())
        prefix = major[: len(major) - len(digits)] if digits else ""
        number = int(digits) if digits else 0
        return f"{prefix}{number + 1}" + (f".{rest}" if rest else "")

    def _sync_recoverable(self, entry: dict[str, Any]) -> None:
        """可恢复标记始终由版本记录推导，避免动作改状态后与校验结论脱钩。"""
        code = str(entry.get("快照编号", ""))
        version = self._resolve_version(entry)
        version_row = self._version_row(code, version)
        shards = self._sorted_shards(code, version)
        complete = (
            version_row is not None
            and version_row.get("状态") == "完整"
            and bool(shards)
            and all(_shard_verdict(shard) == SHARD_PASSED for shard in shards)
            and len(shards) >= (_int(entry.get("分片总数")) or _int(version_row.get("分片总数")))
        )
        if not complete and version_row is not None and version_row.get("状态") == "完整":
            # 版本表声称完整，但分片事实不成立：降级为未完成，半成品不能留在恢复清单里。
            version_row["状态"] = "未完成"
            version_row["当前可恢复"] = False
        if not complete and str(entry.get("status")) == "已完成":
            # 既有数据里「已完成 + 失败分片」的组合必须收敛到失败边界，
            # 否则页面会继续显示完成，恢复时才发现行数不足。
            entry["status"] = "执行失败"
            entry["快照状态"] = "执行失败"
            entry["pending"] = False
            entry["abnormal"] = True
        entry["可恢复"] = complete
        if not complete:
            current = self._current_recoverable(code)
            if current is not None and str(current.get("版本")) == version:
                # 自己已经不完整了，就不能再把自己当成可恢复版本。
                previous = self._previous_complete(code, version)
                current = None if previous is None else previous
            entry["可恢复版本"] = (current or {}).get("版本", "")

    def _recovery_suggestion(
        self,
        entry: dict[str, Any],
        version: str,
        diagnosis: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """恢复建议：当前版本完整就直接用；否则保留上一份完整快照或发布修复版本。"""
        diagnosis = diagnosis or self._diagnose(
            str(entry.get("快照编号", "")), version, _int(entry.get("分片总数"))
        )
        version_row = self._version_row(str(entry.get("快照编号", "")), version)
        if diagnosis["可完成"] and version_row is not None and version_row.get("状态") == "完整":
            return {
                "方式": "当前版本完整",
                "版本": version,
                "行数合计": version_row.get("行数合计"),
                "完成时间": version_row.get("完成时间"),
                "说明": f"当前版本 {version} 全部分片校验通过，可直接恢复",
            }
        previous = self._previous_complete(str(entry.get("快照编号", "")), version)
        if previous is not None:
            return {
                "方式": RECOVER_BY_ROLLBACK,
                "版本": previous.get("版本"),
                "行数合计": previous.get("行数合计"),
                "完成时间": previous.get("完成时间"),
                "说明": f"当前版本 {version} 未通过校验，保留上一份完整快照 {previous.get('版本')} 作为恢复入口",
            }
        return {
            "方式": RECOVER_BY_REPAIR,
            "版本": version,
            "行数合计": 0,
            "完成时间": "",
            "说明": f"当前版本 {version} 之前没有完整快照，需发布带修复标记的新快照后才能恢复",
        }

    def _recoverable_version(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        code = str(entry.get("快照编号", ""))
        current = self._current_recoverable(code)
        if current is not None:
            return current
        # 没有显式标记时回退到历史最后一份完整版本。
        completes = self._complete_versions(code)
        return completes[-1] if completes else None

    def get_timeline(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        today = date.today()
        items: list[dict] = []
        for i, row in enumerate(rows):
            start = (today - timedelta(days=7 - i * 2)).isoformat()
            end = (today + timedelta(days=3 - i)).isoformat()
            code = str(row.get("快照编号", ""))
            version = self._resolve_version(row)
            # 列表读到的可恢复标记必须与分片校验结论一致，避免状态与恢复入口分叉。
            self._sync_recoverable(row)
            summary = self.shard_summary(code, version)
            diagnosis = self._diagnose(code, version, _int(row.get("分片总数")))
            recoverable = self._recoverable_version(row)
            bad_shards = diagnosis["失败分片"] + diagnosis["无数据分片"]
            if diagnosis["原因类型"] == "分片失败":
                # 缺失分片同样是失败原因的一部分（例如最后一片没写完）。
                bad_shards = bad_shards + diagnosis["缺失分片"]
            items.append({
                "id": int(row.get("id", i + 1)),
                "name": str(row.get("快照编号") or row.get("数据集名称") or ""),
                "dataset": str(row.get("数据集名称", "")),
                "version": version,
                "start": start,
                "end": end,
                "status": str(row.get("status", STATUS_ORDER[0])),
                "recoverable": bool(row.get("可恢复")),
                "repairMark": bool(row.get("修复标记")),
                "recoverableVersion": (recoverable or {}).get("版本", ""),
                "failedShards": bad_shards,
                "missingShards": diagnosis["缺失分片"],
                "shardSummary": summary,
                "reasonType": diagnosis["原因类型"],
                "reason": diagnosis["原因"],
                "logs": self._build_logs(row, code, version, diagnosis, recoverable),
            })
        return items

    def _build_logs(
        self,
        row: dict[str, Any],
        code: str,
        version: str,
        diagnosis: dict[str, Any],
        recoverable: dict[str, Any] | None,
    ) -> str:
        status = str(row.get("status", STATUS_ORDER[0]))
        lines = [f"[{row.get('快照时间', '')}] 版本 {version} 作业启动"]
        summary = self.shard_summary(code, version)
        lines.append(f"[校验] 分片记录 {summary['记录数']} 条，通过 {summary['通过']}，失败 {summary['失败']}")
        if diagnosis["失败分片"] or diagnosis["无数据分片"] or diagnosis["缺失分片"]:
            bad = diagnosis["失败分片"] + diagnosis["无数据分片"] + diagnosis["缺失分片"]
            lines.append(f"[校验] 失败分片 {bad}，当前版本不可恢复")
            if recoverable is not None:
                lines.append(f"[恢复] 保留上一份完整快照 {recoverable.get('版本')} 作为恢复入口")
            else:
                lines.append("[恢复] 无历史完整快照，需发布带修复标记的新快照")
        elif diagnosis["可完成"]:
            lines.append("[校验] 全部分片通过，可发布为可恢复版本")
        else:
            lines.append(f"[校验] {diagnosis['原因']}")
        lines.append(f"[{row.get('快照时间', '')}] 作业状态：{status}")
        return "\n".join(lines)

    def list_shards(
        self,
        snapshot_code: str | None = None,
        version: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> dict[str, Any]:
        """分片校验记录分页读取：过滤、稳定排序后再切片，避免跨版本错位。"""
        page = max(_int(page, 1), 1)
        size = max(_int(size, 20), 1)
        rows = self._sorted_shards(snapshot_code, version)
        total = len(rows)
        start = (page - 1) * size
        items = [
            {
                "id": row.get("id"),
                "快照编号": row.get("快照编号"),
                "快照版本": self._row_version(row),
                "分片序号": _int(row.get("分片序号")),
                "分片行数": _int(row.get("分片行数")),
                "期望行数": _int(row.get("期望行数")),
                "校验哈希": row.get("校验哈希"),
                "期望哈希": row.get("期望哈希"),
                "校验状态": _shard_status(row.get("校验状态")),
                "校验结论": _shard_verdict(row),
                "校验时间": row.get("校验时间", ""),
                "来源模块": row.get("来源模块", "数据快照"),
            }
            for row in rows[start:start + size]
        ]
        # 版本下拉需要全量版本清单，不能只依赖当前页里出现过的版本。
        versions = sorted(
            {self._row_version(row) for row in self._shard_rows(snapshot_code)},
            key=_version_key,
        )
        return {"items": items, "total": total, "page": page, "size": size, "versions": versions}

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        code = str(entry.get("快照编号", ""))
        version = self._resolve_version(entry)
        # 详情与列表一样，可恢复标记必须由分片校验结论推导，不能只看登记时的状态。
        self._sync_recoverable(entry)
        diagnosis = self._diagnose(code, version, _int(entry.get("分片总数")))
        recoverable = self._recoverable_version(entry)
        # 详情只挂当前版本的分片记录，历史版本的残留记录不会再串进详情。
        shards = self.list_shards(code, version, page=1, size=max(len(self._shard_rows(code, version)), 1))["items"]
        reviews = [
            dict(row) for row in store.rows(REVIEW_MODULE)
            if row.get("快照编号") == code and row.get("版本") == version
        ]
        detail = dict(entry)
        detail.update({
            "版本": version,
            "分片校验记录": shards,
            "分片校验汇总": self.shard_summary(code, version),
            "失败分片": diagnosis["失败分片"],
            "无数据分片": diagnosis["无数据分片"],
            "缺失分片": diagnosis["缺失分片"],
            "校验原因": diagnosis["原因"],
            "校验原因类型": diagnosis["原因类型"],
            "当前可恢复版本": (
                {
                    "版本": recoverable.get("版本"),
                    "行数合计": recoverable.get("行数合计"),
                    "完成时间": recoverable.get("完成时间"),
                    "修复标记": bool(recoverable.get("修复标记")),
                }
                if recoverable is not None else None
            ),
            "恢复建议": self._recovery_suggestion(entry, version),
            "待核对记录": reviews,
        })
        return detail

    # ------------------------------------------------------------------ 写入
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [f for f in REQUIRED_FIELDS if not str(values.get(f) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(r.get("id", 0)) for r in rows), default=0) + 1}
        entry.update({f: values.get(f) for f in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["分片总数"] = _int(values.get("分片总数"))
        entry["版本"] = str(values.get("版本") or f"v{entry['id']}")
        entry["可恢复"] = False
        entry["修复标记"] = False
        entry["行数快照"] = 0
        entry["校验哈希"] = ""
        entry["快照状态"] = entry["status"]
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"快照记录 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据快照可执行范围"
        target = ACTION_RULES[action]
        code = str(entry.get("快照编号", ""))

        if target == "执行中" and entry.get("status") == "已完成":
            # 对已完成版本重跑：开一个新版本，旧版本继续作为恢复入口，直到新版本校验通过。
            previous_version = self._resolve_version(entry)
            entry["版本"] = self._next_version(previous_version)
            entry["修复标记"] = False
            entry["行数快照"] = 0
            entry["校验哈希"] = ""
            action = "执行快照"
        entry["status"] = target
        entry["pending"] = target not in {STATUS_ORDER[2], STATUS_ORDER[3]}
        entry["abnormal"] = action in NEGATIVE_ACTIONS or target == STATUS_ORDER[3]
        entry["快照状态"] = target

        if target == "执行失败":
            version = self._resolve_version(entry)
            diagnosis = self._diagnose(code, version, _int(entry.get("分片总数")))
            self._apply_failure(entry, code, version, {
                **diagnosis,
                "原因类型": diagnosis["原因类型"] or "分片失败",
                "原因": diagnosis["原因"] if diagnosis["原因类型"] else MANUAL_FAIL_REASON,
            })
        else:
            self._sync_recoverable(entry)
        return entry, f"快照记录已{action}"

    def validate_shards(
        self,
        entry_id: int,
        shards: list[dict[str, Any]],
        version: str | None = None,
        repair: bool = False,
        request_id: str | None = None,
        total_shards: int | None = None,
    ) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
        """编排快照分片校验记录：去重写入、判定完成边界、必要时回滚。

        total_shards 是本次提交声明的期望分片数；缺片必须由调用方声明，
        否则无法区分「最后一片失败」和「快照本来就少一片」。
        返回 (entry, message, verification)。verification 描述本次消费结果。
        """
        entry = store.find(MODULE, entry_id)
        verification: dict[str, Any] = {
            "写入记录": 0,
            "重复跳过": 0,
            "冲突记录": 0,
            "冲突范围": [],
            "完成判定": False,
        }
        if entry is None:
            return None, f"快照记录 {entry_id} 不存在或已归档", verification

        code = str(entry.get("快照编号", ""))
        current_version = self._resolve_version(entry)
        target_version = str(version or current_version)
        total_hint = _int(total_shards)
        if not shards:
            return None, "未提交任何分片校验结果，空态不改变快照状态", verification

        request_key = f"{code}:{request_id}" if request_id else ""
        if request_key and request_key in self._request_log:
            cached = dict(self._request_log[request_key])
            cached["重复提交"] = True
            return entry, "重复提交，已返回首次校验结果，未写入新记录", cached

        is_stale_version = target_version != current_version
        if is_stale_version and self._version_row(code, target_version) is None and not self._shard_rows(code, target_version):
            return None, f"版本 {target_version} 不属于快照 {code}，无法写入分片校验记录", verification

        # 冲突范围只在提交涉及多版本时才算跨版本，避免单版本提交被误报。
        has_other_versions = is_stale_version or any(
            other.get("快照编号") == code and self._row_version(other) != target_version
            for other in store.rows(SHARD_MODULE)
        )

        for raw in shards:
            shard = {field: raw.get(field) for field in SHARD_FIELDS}
            index = _int(shard.get("分片序号"))
            if index <= 0:
                return None, "分片序号必须为正整数", verification
            shard["分片序号"] = index
            shard["快照编号"] = code
            shard["快照版本"] = target_version
            shard["校验状态"] = _shard_verdict(shard)
            shard["来源模块"] = str(raw.get("来源模块") or "数据快照")
            shard["校验时间"] = str(raw.get("校验时间") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            shard["校验指纹"] = _fingerprint(shard, code, target_version)

            existing = next(
                (
                    row for row in store.rows(SHARD_MODULE)
                    if row.get("快照编号") == code
                    and self._row_version(row) == target_version
                    and _int(row.get("分片序号")) == index
                ),
                None,
            )
            if existing is not None:
                stored_fingerprint = existing.get("校验指纹") or _fingerprint(existing, code, target_version)
                if stored_fingerprint == shard["校验指纹"]:
                    verification["重复跳过"] += 1
                    continue
                # 冲突一：同一版本内同一分片结果变化，原地覆盖，只保留一份有效结果。
                existing.update(shard)
                existing["修订次数"] = _int(existing.get("修订次数")) + 1
                existing["冲突范围"] = "同版本"
                verification["冲突记录"] += 1
                if "同版本" not in verification["冲突范围"]:
                    verification["冲突范围"].append("同版本")
                continue

            rows = store.rows(SHARD_MODULE)
            row = {"id": max((_int(r.get("id")) for r in rows), default=0) + 1}
            row.update(shard)
            row["修订次数"] = 0
            # 冲突二：跨版本提交，只影响所属版本，不触碰当前可恢复状态。
            row["冲突范围"] = "跨版本" if has_other_versions else ""
            if has_other_versions:
                verification["冲突记录"] += 1
                if "跨版本" not in verification["冲突范围"]:
                    verification["冲突范围"].append("跨版本")
            verification["写入记录"] += 1
            rows.append(row)

        if repair:
            # 修复标记只在版本真正发布时落库，失败版本不能携带修复标记。
            verification["修复标记"] = True

        total = _int(entry.get("分片总数"))
        if not total and total_hint:
            total = total_hint
            entry["分片总数"] = total

        diagnosis = self._diagnose(code, target_version, total)
        shards_now = self._sorted_shards(code, target_version)
        verification["完成判定"] = bool(diagnosis["可完成"])
        verification["失败分片"] = diagnosis["失败分片"]
        verification["无数据分片"] = diagnosis["无数据分片"]
        verification["缺失分片"] = diagnosis["缺失分片"]

        if diagnosis["可完成"] and not is_stale_version:
            entry["修复标记"] = bool(repair or entry.get("修复标记"))
            self._publish_complete(entry, code, target_version, shards_now)
            verification["可恢复版本"] = target_version
            message = f"全部分片校验通过，快照版本 {target_version} 已完成并可恢复"
        elif diagnosis["可完成"] and is_stale_version:
            self._publish_complete(entry, code, target_version, shards_now, adopt=False, repair=repair)
            verification["可恢复版本"] = (self._current_recoverable(code) or {}).get("版本", "")
            message = f"历史版本 {target_version} 分片校验补全，但当前版本仍为 {current_version}"
        elif is_stale_version:
            # 历史版本的分片失败只影响它自己，不改变当前版本状态和恢复入口。
            self._open_review(code, target_version, diagnosis)
            verification["可恢复版本"] = (self._current_recoverable(code) or {}).get("版本", "")
            message = f"历史版本 {target_version} 分片校验未通过，当前版本 {current_version} 不受影响"
        else:
            self._apply_failure(entry, code, target_version, diagnosis)
            verification["可恢复版本"] = (self._recoverable_version(entry) or {}).get("版本", "")
            message = f"分片校验未通过，版本 {target_version} 保持不可恢复：{diagnosis['原因']}"

        if request_key:
            self._request_log[request_key] = dict(verification)
        return entry, message, verification

    def _publish_complete(
        self,
        entry: dict[str, Any],
        code: str,
        version: str,
        shards: list[dict[str, Any]],
        adopt: bool = True,
        repair: bool = False,
    ) -> None:
        total_rows = sum(_int(shard.get("分片行数")) for shard in shards)
        combined = hashlib.sha256(
            "|".join(str(shard.get("校验哈希", "")) for shard in sorted(shards, key=lambda s: _int(s.get("分片序号")))).encode("utf-8")
        ).hexdigest()

        version_row = self._version_row(code, version)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if version_row is None:
            rows = store.rows(VERSION_MODULE)
            version_row = {"id": max((_int(r.get("id")) for r in rows), default=0) + 1}
            rows.append(version_row)
        version_row.update({
            "快照编号": code,
            "版本": version,
            "状态": "完整",
            "分片总数": len(shards),
            "通过分片数": sum(1 for shard in shards if _shard_verdict(shard) == SHARD_PASSED),
            "行数合计": total_rows,
            "校验哈希": f"sha256:{combined[:12]}",
            "完成时间": now,
            "修复标记": bool(entry.get("修复标记") or repair),
        })

        if adopt:
            for row in store.rows(VERSION_MODULE):
                if row.get("快照编号") == code:
                    row["当前可恢复"] = row.get("版本") == version
            version_row["当前可恢复"] = True
            entry["status"] = "已完成"
            entry["快照状态"] = "已完成"
            entry["pending"] = False
            entry["abnormal"] = False
            entry["可恢复"] = True
            entry["行数快照"] = total_rows
            entry["校验哈希"] = version_row["校验哈希"]

        self._close_reviews(code, version)

    def _apply_failure(
        self,
        entry: dict[str, Any],
        code: str,
        version: str,
        diagnosis: dict[str, Any],
    ) -> None:
        """失败边界：当前版本不可恢复，半成品不进入恢复清单，异常进待核对区。"""
        entry["status"] = "执行失败"
        entry["快照状态"] = "执行失败"
        entry["pending"] = False
        entry["abnormal"] = True
        entry["可恢复"] = False

        # 半成品版本不允许留在完整版本表里，更不能被标记为当前可恢复。
        for row in store.rows(VERSION_MODULE):
            if row.get("快照编号") == code and str(row.get("版本")) == str(version) and row.get("状态") == "完整":
                row["状态"] = "未完成"
                row["当前可恢复"] = False

        previous = self._previous_complete(code, version)
        if previous is not None:
            # 保留上一份完整快照：它仍是对外唯一的恢复入口。
            for row in store.rows(VERSION_MODULE):
                if row.get("快照编号") == code:
                    row["当前可恢复"] = row is previous
            entry["可恢复版本"] = previous.get("版本")
        else:
            for row in store.rows(VERSION_MODULE):
                if row.get("快照编号") == code:
                    row["当前可恢复"] = False
            entry["可恢复版本"] = ""

        self._open_review(code, version, diagnosis)

    def _open_review(self, code: str, version: str, diagnosis: dict[str, Any]) -> None:
        """中断恢复后的异常结果留在待核对区，同一版本只保留一条待核对记录。"""
        rows = store.rows(REVIEW_MODULE)
        existing = next(
            (
                row for row in rows
                if row.get("快照编号") == code
                and row.get("版本") == version
                and row.get("状态") == "待核对"
            ),
            None,
        )
        review = existing
        if review is None:
            review = {"id": max((_int(r.get("id")) for r in rows), default=0) + 1}
            rows.append(review)
        review.update({
            "快照编号": code,
            "版本": version,
            "类型": diagnosis["原因类型"] or "分片失败",
            "原因": diagnosis["原因"],
            "失败分片": diagnosis["失败分片"],
            "无数据分片": diagnosis["无数据分片"],
            "缺失分片": diagnosis["缺失分片"],
            "状态": "待核对",
            "创建时间": review.get("创建时间") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "校验指纹": hashlib.sha256(
                f"{code}|{version}|{diagnosis['原因类型']}|{diagnosis['失败分片']}".encode("utf-8")
            ).hexdigest()[:16],
        })

    def _close_reviews(self, code: str, version: str) -> None:
        for row in store.rows(REVIEW_MODULE):
            if row.get("快照编号") == code and row.get("版本") == version and row.get("状态") == "待核对":
                row["状态"] = "已核对"
                row["核对时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------------------ 恢复
    def recover(self, entry_id: int, version: str | None = None) -> tuple[dict[str, Any] | None, str]:
        """恢复入口：只打开通过校验的可恢复版本，失败分片不会进入恢复清单。

        不指定版本时取当前版本；当前版本失败但保留了上一份完整快照时，
        可以显式按版本号打开那份历史完整快照。
        """
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"快照记录 {entry_id} 不存在或已归档"
        code = str(entry.get("快照编号", ""))
        current_version = self._resolve_version(entry)
        version = str(version or current_version)

        if version != current_version:
            # 恢复历史完整版本：它必须是完整版本表里通过校验的那一份。
            shards = self._sorted_shards(code, version)
            version_row = self._version_row(code, version)
            if version_row is None or version_row.get("状态") != "完整":
                return None, f"版本 {version} 不是完整快照，不能作为恢复入口"
            if not shards or any(_shard_verdict(row) != SHARD_PASSED for row in shards):
                return None, f"版本 {version} 存在未通过校验的分片，恢复入口已关闭"
            if len(shards) < (_int(version_row.get("分片总数")) or len(shards)):
                return None, f"版本 {version} 分片记录不完整，恢复入口已关闭"
            entry["可恢复版本"] = version
            return {
                "快照编号": code,
                "版本": version,
                "分片数": len(shards),
                "行数合计": sum(_int(row.get("分片行数")) for row in shards),
                "修复标记": bool(version_row.get("修复标记")),
                "恢复方式": RECOVER_BY_ROLLBACK,
                "说明": f"当前版本 {current_version} 未通过校验，回滚到上一份完整快照 {version}",
            }, f"已打开上一份完整快照 {code} 版本 {version}"

        if not entry.get("可恢复"):
            diagnosis = self._diagnose(code, version, _int(entry.get("分片总数")))
            suggestion = self._recovery_suggestion(entry, version, diagnosis)
            failed = diagnosis["失败分片"] + diagnosis["无数据分片"] + diagnosis["缺失分片"]
            return None, (
                f"版本 {version} 未通过分片校验，不能作为恢复入口"
                f"（失败分片：{failed or '无'}）；{suggestion['说明']}"
            )

        shards = self._sorted_shards(code, version)
        bad = [row for row in shards if _shard_verdict(row) != SHARD_PASSED]
        if bad:
            return None, f"版本 {version} 仍存在未通过校验的分片，恢复入口已关闭"

        version_row = self._version_row(code, version)
        if version_row is None or version_row.get("状态") != "完整":
            return None, f"版本 {version} 没有完整版本记录，恢复入口已关闭"

        total = _int(entry.get("分片总数")) or _int(version_row.get("分片总数"))
        if len(shards) < total:
            return None, f"版本 {version} 分片记录不完整（{len(shards)}/{total}），恢复入口已关闭"

        return {
            "快照编号": code,
            "版本": version,
            "分片数": len(shards),
            "行数合计": sum(_int(row.get("分片行数")) for row in shards),
            "修复标记": bool(entry.get("修复标记")),
            "恢复方式": RECOVER_BY_REPAIR if entry.get("修复标记") else RECOVER_BY_ROLLBACK,
        }, f"已打开可恢复快照 {code} 版本 {version}"
