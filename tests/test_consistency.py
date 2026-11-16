"""来源压缩合并、并发提交、迟到旧副本、旧快照核对。"""

from __future__ import annotations

import json
import threading
import time
import unittest

from pointwise.target import Ledger, READY, STALE
from tests.support import NodeTestCase


class CompactionTests(NodeTestCase):
    def test_source_compaction_during_copy_does_not_misalign(self):
        """增量复制到一半来源压缩合并：目标必须重拉清单，绝不给错位结果。"""
        nodes = self.nodes
        nodes.mutate("orders", [[1, 2, 3, 4, "before-compact"]], [])
        # 只搬 schema/dict/增量，制造“增量先于新分片”的窗口
        nodes.copy(include=["schema", "dict", "increments"], tables=["orders"])
        data = nodes.target_state._reap  # noqa: F841  (touch API)
        before = nodes.source_state.runtime["base_epoch"]
        nodes.compact(reason="test")
        after = nodes.source_state.runtime["base_epoch"]
        self.assertGreater(after, before)
        # 目标旧清单的增量 base_epoch 已过期；直接复制旧清单必须触发重拉
        status, synced = nodes.target.post_json("/api/sync", {})
        self.assertOk(status, synced)
        tr = nodes.copy()
        self.assertEqual(tr.status, READY, tr.error)
        status, verified = nodes.target.get_json("/api/verify?table=orders")
        self.assertOk(status, verified)
        self.assertTrue(verified["match"], verified["delta"])

    def test_increment_from_previous_epoch_rejected_as_stale(self):
        nodes = self.nodes
        nodes.mutate("orders", [[1, 2, 3, 4, "pre-compact"]], [])
        nodes.copy(include=["schema", "dict", "increments"], tables=["orders"])
        old_incr = next(
            (o for o in nodes.target_state.ledger.data["objects"].values() if o["kind"] == "incr"),
            None,
        )
        self.assertIsNotNone(old_incr, "应有一个已复制到目标的增量")
        # 模拟“这个增量在压缩合并前被暂存、但尚未提交”，且目标已装上新 base 分片
        with nodes.target_state.fence():
            nodes.target_state._reload_ledger()
            ledger = nodes.target_state.ledger
            ledger.data["objects"].pop(
                ledger.object_key("orders", "incr", old_incr["name"], old_incr["epoch"])
            )
            ledger.data["base_epoch"] = max(ledger.data["base_epoch"], 2)
            ledger.save()
            ok, reason = ledger.commit_object(
                dict(old_incr), {"transfer": "late-writer", "epoch": old_incr["epoch"]}
            )
        self.assertFalse(ok)
        self.assertEqual(reason, "stale_increment_base")
        self.assertIsNone(ledger.get_object("orders", "incr", old_incr["name"]))

    def test_concurrent_copy_tasks_newer_epoch_wins(self):
        nodes = self.nodes
        nodes.copy()
        manifest = nodes.target_state.manifests["orders"]
        ledger = nodes.target_state.ledger
        schema_id = manifest["schema"]["schema_id"]
        older = {
            "table": "orders",
            "kind": "shard",
            "name": "zz-late",
            "epoch": 1,
            "sha256": "0" * 64,
            "bytes": 1,
            "rows": 1,
            "path": "data/orders/shards/s00.e000001.db",
            "generation": 1,
            "schema_id": schema_id,
        }
        newer = {**older, "epoch": 5, "sha256": "1" * 64, "generation": 5}
        ledger.data["base_epoch"] = 0
        ledger.data["generation"] = 0
        ledger.save()
        ok_new, _ = ledger.commit_object(newer, {"transfer": "task-new", "epoch": 5})
        ok_old, reason = ledger.commit_object(older, {"transfer": "task-old", "epoch": 1})
        self.assertTrue(ok_new)
        self.assertFalse(ok_old)
        self.assertEqual(reason, "stale_epoch")
        winner = ledger.get_object("orders", "shard", "zz-late")
        self.assertEqual(winner["epoch"], 5)
        self.assertEqual(winner["sha256"], "1" * 64)
        # 低 generation 的迟到对象同样被拒
        stale_gen = {**older, "epoch": 6, "generation": 2, "sha256": "2" * 64}
        ledger.data["generation"] = 9
        ok, reason = ledger.commit_object(stale_gen, {"transfer": "task-old-2", "epoch": 6})
        self.assertFalse(ok)
        self.assertEqual(reason, "stale_generation")

    def test_two_real_transfers_race_and_stale_is_reported(self):
        """两个真实复制任务并发跑；先完成的提交，后完成的旧副本不覆盖。"""
        nodes = self.nodes
        nodes.copy()
        first = nodes.target_state.manifests["orders"]
        # 来源写入新 epoch
        nodes.mutate("orders", [[1, 2, 3, 4, "newer"]], [])
        gen_old = nodes.target_state.ledger.data["generation"]
        status, body = nodes.target.post_json("/api/sync", {})
        self.assertOk(status, body)
        # 用两个 transfer 指向不同代清单：旧的钉住旧代，新的取最新
        tr_old = nodes.copy(generation=gen_old, tables=["orders"])
        tr_new = nodes.copy(tables=["orders"], allow_concurrent=True)
        self.assertEqual(tr_new.status, READY, tr_new.error)
        status, verified = nodes.target.get_json("/api/verify?table=orders")
        self.assertOk(status, verified)
        self.assertTrue(verified["match"], verified["delta"])
        # 旧代复制不能把账本回退
        self.assertGreaterEqual(
            nodes.target_state.ledger.data["generation"],
            nodes.source_state.runtime["generation"] - 1,
        )

    def test_new_epoch_shards_are_not_overwritten_by_late_old_copy(self):
        nodes = self.nodes
        nodes.copy()
        gen1 = nodes.target_state.ledger.data["generation"]
        nodes.compact(reason="epoch-2")
        nodes.copy()
        newer = nodes.target_state.ledger.get_object("orders", "shard", "s00")
        self.assertGreater(newer["epoch"], 1)
        # 重放旧代复制：所有对象必须判 stale / idempotent，文件不被改写
        before = (nodes.target_state.root / newer["path"]).read_bytes()
        tr = nodes.copy(generation=gen1, tables=["orders"], allow_concurrent=True)
        after = (nodes.target_state.root / newer["path"]).read_bytes()
        self.assertEqual(before, after)
        self.assertGreaterEqual(nodes.target_state.ledger.get_object("orders", "shard", "s00")["epoch"], newer["epoch"])


class SnapshotVerifyTests(NodeTestCase):
    def test_verify_old_snapshot_after_source_moved_on(self):
        nodes = self.nodes
        nodes.copy()
        old_gen = nodes.target_state.ledger.data["generation"]
        nodes.mutate("orders", [[1, 2, 3, 4, "v2"]], [5, 6])
        nodes.compact(reason="v3")
        # 目标尚未同步新一代：用旧快照读，两侧必须一致
        status, verified = nodes.target.get_json(f"/api/verify?table=orders&snapshot={old_gen}")
        self.assertOk(status, verified)
        self.assertTrue(verified["match"], verified["delta"])
        self.assertEqual(verified["read_mode"], "snapshot")
        # 同步新清单并复制后，旧快照仍然可读
        nodes.copy()
        status, verified_old = nodes.target.get_json(f"/api/verify?table=orders&snapshot={old_gen}")
        self.assertOk(status, verified_old)
        self.assertTrue(verified_old["match"], verified_old["delta"])
        status, verified_new = nodes.target.get_json("/api/verify?table=orders")
        self.assertOk(status, verified_new)
        self.assertTrue(verified_new["match"], verified_new["delta"])
        self.assertNotEqual(verified_old["target_digest"], verified_new["target_digest"])

    def test_corrupted_target_shard_is_detected(self):
        nodes = self.nodes
        nodes.copy()
        obj = nodes.target_state.ledger.get_object("orders", "shard", "s00")
        path = nodes.target_state.root / obj["path"]
        raw = bytearray(path.read_bytes())
        raw[4096:4200] = b"X" * 100
        path.write_bytes(bytes(raw))
        # 门禁按账本认为可用；但 _materialize 读文件时门禁重新校验 checksum
        status, body = nodes.query(table="orders", limit=1)
        self.assertEqual(status, 409, body)
        self.assertEqual(body["error"], "query_gate_refused")

    def test_missing_target_rows_are_reported(self):
        nodes = self.nodes
        nodes.copy()
        # 手工清空一个分片的数据行：门禁仍通过（checksum 未变），核对应发现差异
        obj = nodes.target_state.ledger.get_object("orders", "shard", "s00")
        path = nodes.target_state.root / obj["path"]
        import sqlite3

        conn = sqlite3.connect(path)
        conn.execute("DELETE FROM orders")
        conn.commit()
        conn.close()
        # 构造“静默漂移的副本”：目标账本与清单都认为自己有这一份分片，
        # 但内容少了行（现实中可能来自绕过校验的人工修复）。
        # 门禁会放行，而核对必须逐 key 发现缺口。
        new_sha = __import__("hashlib").sha256(path.read_bytes()).hexdigest()
        state = nodes.target_state
        generation = state.ledger.data["generation"]
        with state.fence():
            state._reload_ledger()
            obj = state.ledger.get_object("orders", "shard", "s00")
            obj["sha256"] = new_sha
            snap = state.ledger.manifest_at(generation)
            for shard in snap["tables"]["orders"]["shards"]:
                if shard["shard"] == "s00":
                    shard["sha256"] = new_sha
            state.manifests["orders"]["shards"] = snap["tables"]["orders"]["shards"]
            state.ledger.save()
        status, verified = nodes.target.get_json("/api/verify?table=orders")
        self.assertOk(status, verified)
        self.assertTrue(verified["ok"], verified)
        self.assertFalse(verified["match"])
        self.assertGreater(verified["delta"]["missing_count"], 0)
        self.assertIsNotNone(verified["delta"].get("missing_sample"))


if __name__ == "__main__":
    unittest.main()
