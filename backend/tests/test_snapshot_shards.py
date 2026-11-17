"""数据快照：分片校验边界、版本完整性与恢复入口的回归测试。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.services.snapshot import SnapshotService
from app.store import Store, store


def reset_store() -> None:
    """每个用例都从种子数据重新开始，避免用例之间互相污染。"""
    fresh = Store()
    store._tables = fresh._tables  # noqa: SLF001 - 内存仓库没有重置接口，测试内直接换表


def shard(index: int, rows: int = 100, ok: bool = True, total_rows: int = 100) -> dict:
    return {
        "分片序号": index,
        "分片行数": rows,
        "期望行数": total_rows,
        "校验哈希": f"h{index}" if ok else f"bad{index}",
        "期望哈希": f"h{index}",
        "校验状态": "通过" if ok else "失败",
    }


class SnapshotShardBoundaryTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.service = SnapshotService()

    def _new_entry(self, code: str, total: int = 3) -> int:
        entry, missing = self.service.create_entry({
            "快照编号": code,
            "数据集名称": f"{code} 数据集",
            "快照时间": "2026-11-17",
            "版本": "v1",
            "分片总数": total,
        })
        self.assertEqual(missing, [])
        return int(entry["id"])

    def test_failed_shard_never_marks_complete(self) -> None:
        """一个分片失败、其余已写入时，快照不能进入完成状态。"""
        eid = self._new_entry("SNAP-T1")
        self.service.validate_shards(eid, [shard(1), shard(2)], total_shards=3)
        entry, message, verification = self.service.validate_shards(
            eid, [shard(3, ok=False)], total_shards=3
        )
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "执行失败")
        self.assertFalse(entry["可恢复"])
        self.assertFalse(verification["完成判定"])
        self.assertIn(3, verification["失败分片"])
        self.assertIn("不可恢复", message)

    def test_missing_last_shard_blocks_completion(self) -> None:
        """只剩一个分片没写入时，不能因为 max(分片序号) 就当成完整快照。"""
        eid = self._new_entry("SNAP-T2", total=4)
        _, _, verification = self.service.validate_shards(
            eid, [shard(1), shard(2), shard(3)], total_shards=4
        )
        self.assertFalse(verification["完成判定"])
        self.assertEqual(verification["缺失分片"], [4])
        detail = self.service.get_entry(eid)
        self.assertEqual(detail["status"], "执行失败")

    def test_undeclared_shard_total_cannot_complete(self) -> None:
        eid = self._new_entry("SNAP-T3", total=0)
        _, _, verification = self.service.validate_shards(eid, [shard(1), shard(2)])
        self.assertFalse(verification["完成判定"])
        self.assertEqual(self.service.get_entry(eid)["status"], "执行失败")

    def test_row_count_mismatch_counts_as_failed(self) -> None:
        """校验状态写「通过」但行数不够，仍然算失败分片。"""
        eid = self._new_entry("SNAP-T4", total=2)
        _, _, verification = self.service.validate_shards(
            eid, [shard(1), shard(2, rows=60)], total_shards=2
        )
        self.assertFalse(verification["完成判定"])
        self.assertIn(2, verification["失败分片"])

    def test_hash_mismatch_counts_as_failed(self) -> None:
        eid = self._new_entry("SNAP-T5", total=1)
        bad = shard(1)
        bad["校验哈希"] = "tampered"
        _, _, verification = self.service.validate_shards(eid, [bad], total_shards=1)
        self.assertFalse(verification["完成判定"])

    def test_complete_only_when_all_shards_pass(self) -> None:
        eid = self._new_entry("SNAP-T6", total=3)
        for index in (1, 2, 3):
            entry, _, verification = self.service.validate_shards(eid, [shard(index)], total_shards=3)
        self.assertTrue(verification["完成判定"])
        self.assertEqual(entry["status"], "已完成")
        self.assertTrue(entry["可恢复"])
        self.assertEqual(entry["行数快照"], 300)

    def test_empty_state_and_no_data_are_distinguished(self) -> None:
        empty_id = self._new_entry("SNAP-T7", total=2)
        self.assertEqual(self.service.get_entry(empty_id)["校验原因类型"], "空态")

        no_data_id = self._new_entry("SNAP-T8", total=2)
        self.service.validate_shards(
            no_data_id,
            [{"分片序号": 1, "分片行数": 0, "期望行数": 100, "校验状态": "通过"}],
            total_shards=2,
        )
        detail = self.service.get_entry(no_data_id)
        self.assertEqual(detail["校验原因类型"], "无数据")
        self.assertEqual(detail["无数据分片"], [1])

    def test_interrupted_recovery_stays_in_review_zone(self) -> None:
        eid = self._new_entry("SNAP-T9", total=2)
        self.service.validate_shards(eid, [shard(1), shard(2, ok=False)], total_shards=2)
        reviews = [r for r in store.rows("snapshot_review") if r["快照编号"] == "SNAP-T9"]
        self.assertEqual(len(reviews), 1)
        self.assertEqual(reviews[0]["状态"], "待核对")

        self.service.validate_shards(eid, [shard(2)], total_shards=2)
        reviews = [r for r in store.rows("snapshot_review") if r["快照编号"] == "SNAP-T9"]
        self.assertEqual(reviews[0]["状态"], "已核对")


class SnapshotVersionRetentionTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.service = SnapshotService()

    def test_failure_keeps_previous_complete_version(self) -> None:
        """失败边界保留上一份完整快照，且它仍是唯一恢复入口。"""
        detail = self.service.get_entry(2)  # 种子：v2 分片 3 失败、分片 4 缺失
        self.assertEqual(detail["status"], "执行失败")
        self.assertFalse(detail["可恢复"])
        self.assertEqual(detail["当前可恢复版本"]["版本"], "v1")
        self.assertEqual(detail["恢复建议"]["方式"], "回滚上一份完整快照")

        result, message = self.service.recover(2)
        self.assertIsNone(result)
        self.assertIn("不能作为恢复入口", message)

        rolled_back, _ = self.service.recover(2, version="v1")
        self.assertIsNotNone(rolled_back)
        self.assertEqual(rolled_back["版本"], "v1")
        self.assertEqual(rolled_back["行数合计"], 1800)

    def test_failed_shard_not_in_recoverable_snapshot(self) -> None:
        """恢复出来的版本只包含通过校验的分片。"""
        recovered, _ = self.service.recover(2, version="v1")
        shards = self.service.list_shards("SNAP-0002", "v1")["items"]
        self.assertEqual(recovered["分片数"], len(shards))
        self.assertTrue(all(item["校验结论"] == "通过" for item in shards))

    def test_failure_without_history_suggests_repair_marked_release(self) -> None:
        detail = self.service.get_entry(4)
        self.assertEqual(detail["校验原因类型"], "无数据")
        self.assertIsNone(detail["当前可恢复版本"])
        self.assertEqual(detail["恢复建议"]["方式"], "发布带修复标记的新快照")
        result, _ = self.service.recover(4)
        self.assertIsNone(result)

    def test_repair_marked_version_is_new_recovery_entry(self) -> None:
        eid = 4
        entry, message, verification = self.service.validate_shards(
            eid,
            [
                {"分片序号": 1, "分片行数": 100, "期望行数": 100, "校验哈希": "r1", "期望哈希": "r1", "校验状态": "通过"},
                {"分片序号": 2, "分片行数": 100, "期望行数": 100, "校验哈希": "r2", "期望哈希": "r2", "校验状态": "通过"},
            ],
            total_shards=2,
            repair=True,
        )
        self.assertTrue(verification["完成判定"])
        self.assertTrue(entry["修复标记"])
        self.assertTrue(entry["可恢复"])
        recovered, _ = self.service.recover(eid)
        self.assertIsNotNone(recovered)
        self.assertTrue(recovered["修复标记"])

    def test_already_complete_version_still_guarded(self) -> None:
        """种子 SNAP-0001 是完整版本，恢复入口正常开放。"""
        detail = self.service.get_entry(1)
        self.assertTrue(detail["可恢复"])
        result, _ = self.service.recover(1)
        self.assertIsNotNone(result)
        self.assertEqual(result["分片数"], 4)

    def test_rerun_opens_new_version_and_keeps_old_as_fallback(self) -> None:
        entry, _ = self.service.run_action(1, "执行快照")
        self.assertEqual(entry["版本"], "v2")
        self.assertFalse(entry["可恢复"])
        self.assertEqual(self.service.get_entry(1)["当前可恢复版本"]["版本"], "v1")
        fallback, _ = self.service.recover(1, version="v1")
        self.assertIsNotNone(fallback)


class SnapshotShardRecordTest(unittest.TestCase):
    def setUp(self) -> None:
        reset_store()
        self.service = SnapshotService()

    def test_list_ordering_is_stable_across_versions(self) -> None:
        """列表按（快照编号, 版本, 分片序号）排序，不会跨版本串行错位。"""
        payload = self.service.list_shards(page=1, size=200)
        keys = [(row["快照编号"], row["快照版本"], row["分片序号"]) for row in payload["items"]]
        self.assertEqual(keys, sorted(keys, key=lambda item: (item[0], int(item[1][1:]), item[2])))

    def test_list_pagination_does_not_shift(self) -> None:
        first = self.service.list_shards(page=1, size=4)["items"]
        second = self.service.list_shards(page=2, size=4)["items"]
        self.assertEqual(len(first), 4)
        self.assertTrue(all(a["id"] != b["id"] for a in first for b in second))

    def test_detail_excludes_other_version_residue(self) -> None:
        """详情只挂当前版本分片，历史版本残留不会混进当前详情。"""
        detail = self.service.get_entry(2)
        versions = {row["快照版本"] for row in detail["分片校验记录"]}
        self.assertEqual(versions, {"v2"})

    def test_duplicate_submission_writes_single_result(self) -> None:
        eid = self._fresh_entry()
        body = [shard(1)]
        self.service.validate_shards(eid, body, total_shards=2, request_id="req-1")
        _, message, verification = self.service.validate_shards(
            eid, body, total_shards=2, request_id="req-1"
        )
        self.assertTrue(verification.get("重复提交"))
        rows = [r for r in store.rows("snapshot_shard") if r["快照编号"] == "SNAP-DUP"]
        self.assertEqual(len(rows), 1)

    def test_same_shard_idempotent_without_request_id(self) -> None:
        eid = self._fresh_entry()
        self.service.validate_shards(eid, [shard(1)], total_shards=2)
        _, _, verification = self.service.validate_shards(eid, [shard(1)], total_shards=2)
        self.assertEqual(verification["写入记录"], 0)
        self.assertEqual(verification["重复跳过"], 1)
        rows = [r for r in store.rows("snapshot_shard") if r["快照编号"] == "SNAP-DUP"]
        self.assertEqual(len(rows), 1)

    def test_conflict_scopes_are_distinguished(self) -> None:
        """同版本覆盖只保留一份，跨版本写入不动当前状态。"""
        eid = self._fresh_entry()
        self.service.validate_shards(eid, [shard(1)], total_shards=2, request_id="a")
        _, _, same_version = self.service.validate_shards(
            eid, [shard(1, ok=False)], total_shards=2, request_id="b"
        )
        self.assertEqual(same_version["冲突范围"], ["同版本"])

        # 历史版本 v1 已存在分片记录：对它补写属于跨版本冲突，只影响 v1。
        stale = self.service.validate_shards(
            2,
            [{"分片序号": 3, "分片行数": 900, "期望行数": 900, "校验哈希": "sha256:s2v1-3", "期望哈希": "sha256:s2v1-3", "校验状态": "通过"}],
            version="v1",
            request_id="c",
        )[2]
        self.assertIn("跨版本", stale["冲突范围"])
        self.assertEqual(stale["可恢复版本"], "v1")
        self.assertEqual(self.service.get_entry(2)["校验原因类型"], "分片失败")
        self.assertEqual(self.service.get_entry(2)["当前可恢复版本"]["版本"], "v1")

        # 当前版本 v2 的恢复入口没有被历史版本写入改动。
        result, _ = self.service.recover(2)
        self.assertIsNone(result)

    def test_unrelated_version_is_rejected(self) -> None:
        eid = self._fresh_entry()
        entry, message, _ = self.service.validate_shards(eid, [shard(1)], version="v99")
        self.assertIsNone(entry)
        self.assertIn("不属于快照", message)

    def _fresh_entry(self) -> int:
        entry, _ = self.service.create_entry({
            "快照编号": "SNAP-DUP",
            "数据集名称": "去重数据集",
            "快照时间": "2026-11-17",
            "版本": "v1",
            "分片总数": 2,
        })
        return int(entry["id"])


if __name__ == "__main__":
    unittest.main()
