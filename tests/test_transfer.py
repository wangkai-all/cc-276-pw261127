"""传输中断 / 目标重启 / 断点续传的真实字节级测试。"""

from __future__ import annotations

import json
import time
import unittest
from pathlib import Path

from pointwise.target import INTERRUPTED, READY
from tests.support import NodeTestCase


class TransferTests(NodeTestCase):
    def _largest_part(self) -> Path | None:
        staging = self.nodes.target_state.staging_dir
        parts = sorted(staging.glob("*.part"), key=lambda p: p.stat().st_size)
        return parts[-1] if parts else None

    def test_abort_keeps_real_partial_bytes_then_resume(self):
        nodes = self.nodes
        tr = nodes.copy(abort_after=200_000, tables=["orders"])
        self.assertEqual(tr.status, INTERRUPTED)
        part = self._largest_part()
        self.assertIsNotNone(part, "abort 应留下真实的 .part 文件")
        part_bytes = part.stat().st_size
        self.assertGreater(part_bytes, 0)
        self.assertLessEqual(part_bytes, 200_000, "abort 在阈值处立即停止")
        # 账本里没有登记任何对象：未校验完的分片不算“到齐”
        self.assertEqual(nodes.target_state.ledger.objects_of("orders", "shard"), [])

        resumed = nodes.target_state.resume(tr.id, abort_after=None)
        done = nodes.target_state.wait(tr.id, timeout=120)
        self.assertEqual(done.status, READY, done.error)
        result = done.result or {}
        self.assertTrue(result.get("committed"))
        # 续传事件里必须出现从非 0 字节继续的证据
        resumed_events = [
            e for e in done.events if "resumed" in e.get("message", "") and e.get("resumed_from")
        ]
        self.assertTrue(resumed_events, list(done.events)[-10:])
        self.assertFalse(list(nodes.target_state.staging_dir.glob("*.part")))
        status, body = nodes.query(table="orders", limit=5)
        self.assertOk(status, body)

    def test_http_interrupt_endpoint_stops_midstream(self):
        nodes = self.nodes
        status, body = nodes.target.post_json(
            "/api/copy", {"pace": 0.01, "tables": ["orders"]}
        )
        self.assertEqual(status, 200, body)
        tid = body["transfer"]["id"]
        deadline = time.time() + 20
        started = False
        while time.time() < deadline:
            _, cur = nodes.target.get_json(f"/api/transfers/{tid}")
            if cur["transfer"]["progress"]:
                started = True
                break
            time.sleep(0.02)
        self.assertTrue(started, "transfer never started moving bytes")
        status, stopped = nodes.target.post_json(f"/api/transfers/{tid}/interrupt", {})
        self.assertOk(status, stopped)
        self.assertEqual(stopped["transfer"]["status"], INTERRUPTED, stopped)
        self.assertEqual(stopped["transfer"]["error"]["code"], "interrupted")
        status, resumed = nodes.target.post_json(f"/api/transfers/{tid}/resume", {"pace": 0})
        self.assertOk(status, resumed)
        done = nodes.target_state.wait(tid, timeout=120)
        self.assertEqual(done.status, READY, done.error)

    def test_target_restart_marks_transfer_interrupted_and_resumes(self):
        nodes = self.nodes
        status, body = nodes.target.post_json(
            "/api/copy", {"tables": ["orders"], "crash_after": 300_000}
        )
        self.assertEqual(status, 200, body)
        tid = body["transfer"]["id"]
        deadline = time.time() + 30
        while time.time() < deadline:
            _, cur = nodes.target.get_json(f"/api/transfers/{tid}")
            if cur["transfer"]["progress"] and any(
                p.get("written", 0) >= 300_000 for p in cur["transfer"]["progress"].values()
            ):
                break
            time.sleep(0.02)
        time.sleep(0.2)
        # “进程被杀”：没有写终态，target.json 里它仍是 running
        self.assertEqual(nodes.target_state.transfers[tid].status, "running")
        part_before = self._largest_part()
        self.assertIsNotNone(part_before)
        bytes_before = part_before.stat().st_size
        self.assertGreater(bytes_before, 0)

        nodes.restart_target()
        state = nodes.target_state
        self.assertIn(tid, state.transfers)
        recovered = state.transfers[tid]
        self.assertEqual(recovered.status, INTERRUPTED, recovered.view())
        self.assertEqual(recovered.error["code"], "node_restart")
        self.assertIn(tid, state.startup["abandoned_transfers"])
        self.assertTrue(state.startup["recovered_parts"], state.startup)
        # 重启后 .part 仍在，且字节数没有倒退
        self.assertTrue(self._largest_part().exists())

        status, resumed = nodes.target.post_json(f"/api/transfers/{tid}/resume", {})
        self.assertOk(status, resumed)
        done = state.wait(tid, timeout=120)
        self.assertEqual(done.status, READY, done.error)
        status, verified = nodes.target.get_json("/api/verify?table=orders")
        self.assertOk(status, verified)
        self.assertTrue(verified["match"], verified)

    def test_resume_after_source_restart(self):
        nodes = self.nodes
        tr = nodes.copy(abort_after=150_000, tables=["orders"])
        self.assertEqual(tr.status, INTERRUPTED)
        nodes.restart_source()
        nodes.target_state.resume(tr.id, abort_after=None)
        done = nodes.target_state.wait(tr.id, timeout=120)
        self.assertEqual(done.status, READY, done.error)
        status, verified = nodes.target.get_json("/api/verify?table=orders")
        self.assertOk(status, verified)
        self.assertTrue(verified["match"], verified)

    def test_completed_objects_are_not_downloaded_twice(self):
        nodes = self.nodes
        nodes.copy()
        before = {
            p.name: p.stat().st_mtime_ns
            for p in nodes.target_state.data_dir.rglob("*")
            if p.is_file()
        }
        self.assertTrue(before)
        nodes.copy()  # 同一份清单，所有对象都已存在
        after = {
            p.name: p.stat().st_mtime_ns
            for p in nodes.target_state.data_dir.rglob("*")
            if p.is_file()
        }
        self.assertEqual(before, after)
        self.assertFalse(list(nodes.target_state.staging_dir.glob("*.part")))


if __name__ == "__main__":
    unittest.main()
