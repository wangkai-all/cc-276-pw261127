"""门禁测试：结构已到但字典未到、增量基准分片未到、整库 vs 逐表。"""

from __future__ import annotations

import json
import unittest

from tests.support import NodeTestCase


class GateTests(NodeTestCase):
    def test_dictionary_missing_refuses_queries_that_use_it(self):
        nodes = self.nodes
        tr = nodes.copy(include=["schema", "dict"], tables=["customers"])
        self.assertEqual(tr.status, "ready")
        # orders 只到了 schema；字典与分片都缺
        tr = nodes.copy(include=["schema"], tables=["orders"])
        self.assertEqual(tr.status, "ready")

        status, body = nodes.query(table="orders", uses=["dict"], limit=5)
        self.assertEqual(status, 409, body)
        self.assertGateRefused(body, "dictionary")
        self.assertGateRefused(body, "shards")

        # 查询显式声明不依赖字典 → 字典不再是硬依赖，但分片仍然缺
        status, body = nodes.query(table="orders", uses=[], limit=5)
        self.assertEqual(status, 409, body)
        self.assertNotIn("dictionary", [m["kind"] for m in body["missing"]])
        self.assertIn("shards", [m["kind"] for m in body["missing"]])

        gate_status, gate = nodes.target.get_json("/api/gate?table=orders&uses=dict")
        self.assertOk(gate_status, gate)
        self.assertEqual(gate["decision"], "refuse")

    def test_schema_present_dictionary_absent_is_reported_not_served(self):
        nodes = self.nodes
        nodes.copy(include=["schema"], tables=["customers"])
        state = nodes.target_state.overview(scope="per-table")
        customers = next(t for t in state["tables"] if t["table"] == "customers")
        self.assertFalse(customers["ready"])
        kinds = {m["kind"] for m in customers["missing"]}
        self.assertIn("dictionary", kinds)
        status, body = nodes.query(table="customers", limit=1)
        self.assertEqual(status, 409)
        self.assertGateRefused(body, "dictionary")

    def test_increment_without_base_shard_is_not_served(self):
        nodes = self.nodes
        nodes.mutate("orders", [[1, 2, 3, 4, "increment-1"]], [10])
        tr = nodes.copy(include=["schema", "dict", "increments"])
        self.assertEqual(tr.status, "ready")
        status, body = nodes.query(table="orders", limit=5)
        self.assertEqual(status, 409, body)
        self.assertGateRefused(body, "shards")
        # 增量文件到了，但基准分片没到：必须单独报告 increment_base
        self.assertGateRefused(body, "increment_base")
        # 不接受“只有增量”的部分结果
        self.assertNotIn("data", body)

    def test_base_shard_arrives_and_query_becomes_available(self):
        nodes = self.nodes
        nodes.mutate("orders", [[1, 2, 3, 4, "increment-1"]], [10])
        nodes.copy(include=["schema", "dict", "increments"])
        status, _ = nodes.query(table="orders")
        self.assertEqual(status, 409)
        nodes.copy(include=["shards"])
        status, body = nodes.query(table="orders", limit=600)
        self.assertOk(status, body)
        self.assertIn([1, 2, 3, 4, "increment-1"], body["data"])
        self.assertNotIn([10, 11, 3, 370, "payload-000010-" + "x" * 241], body["data"])

    def test_full_scope_waits_for_every_table(self):
        nodes = self.nodes
        nodes.copy(include=["schema", "dict", "shards", "increments"], tables=["customers"])
        overview = nodes.target_state.overview(scope="full")
        self.assertFalse(overview["available"])
        self.assertEqual(overview["tables_ready"], 1)
        per_table = nodes.target_state.overview(scope="per-table")
        self.assertTrue(per_table["available"])
        # 逐表可用时间早于整库可用时间
        self.assertIsNotNone(per_table["first_table_available_at"])
        self.assertIsNone(per_table["full_available_at"])

    def test_unknown_table_is_503(self):
        status, body = self.nodes.query(table="does_not_exist")
        self.assertEqual(status, 503, body)
        self.assertEqual(body["error"], "query_unavailable")


if __name__ == "__main__":
    unittest.main()
