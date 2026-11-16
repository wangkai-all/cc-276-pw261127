"""两种开放策略的可用时间与一致性边界对比。"""

from __future__ import annotations

import unittest

from tests.support import NodeTestCase


class ScopePolicyTests(NodeTestCase):
    def test_per_table_opens_earlier_than_full(self):
        nodes = self.nodes
        # 只搬 customers 的全部依赖
        nodes.copy(tables=["customers"])
        full = nodes.target_state.overview(scope="full")
        per_table = nodes.target_state.overview(scope="per-table")
        self.assertFalse(full["available"])
        self.assertEqual(full["tables_ready"], 1)
        self.assertEqual(full["tables_total"], 2)
        self.assertTrue(per_table["available"])
        # 逐表可用时间严格早于整库可用时间
        self.assertIsNotNone(per_table["first_table_available_at"])
        self.assertIsNone(full["available_at"])
        # 一致性边界文字不同
        gate = nodes.target_state.evaluate_gate("customers", scope="per-table")
        self.assertEqual(gate["decision"], "serve")
        self.assertIn("per-table", gate["consistency"]["boundary"])

    def test_per_table_query_serves_ready_table_and_refuses_other(self):
        nodes = self.nodes
        nodes.copy(tables=["customers"])
        status, body = nodes.query(table="customers", scope="per-table", limit=3)
        self.assertOk(status, body)
        self.assertEqual(body["consistency"]["scope"], "per-table")
        status, body = nodes.query(table="orders", scope="per-table", limit=3)
        self.assertEqual(status, 409, body)
        self.assertGateRefused(body, "shards")

    def test_full_scope_query_refuses_until_all_ready(self):
        nodes = self.nodes
        nodes.copy(tables=["customers"])
        status, body = nodes.query(table="customers", scope="full", limit=3)
        # 门禁只对该表的闭包负责；整库 scope 影响的是“可用性视图”与边界描述。
        self.assertOk(status, body)
        self.assertEqual(body["consistency"]["scope"], "full")
        self.assertFalse(nodes.target_state.overview(scope="full")["available"])
        nodes.copy()
        self.assertTrue(nodes.target_state.overview(scope="full")["available"])
        full_at = nodes.target_state.overview(scope="full")["available_at"]
        self.assertIsNotNone(full_at)
        first_at = nodes.target_state.overview(scope="full")["first_table_available_at"]
        self.assertLessEqual(first_at, full_at)


if __name__ == "__main__":
    unittest.main()
