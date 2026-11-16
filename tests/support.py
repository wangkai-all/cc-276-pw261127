"""测试工具：起两个节点、种数据、断言门禁。

只依赖标准库 ``unittest``（环境里没有 pytest）。
"""

from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from typing import Any

from pointwise.dataset import Dataset, demo_dataset
from pointwise.httpmini import ApiError, HttpClient
from pointwise.source import SourceState, build_source_app
from pointwise.target import TargetState, build_target_app


class NodePair:
    """一个来源 + 一个目标，二者都是真实 HTTP 服务（随机端口）。"""

    def __init__(self, root: Path, dataset: Dataset | None = None) -> None:
        self.root = root
        self.dataset = dataset or demo_dataset(orders_rows=6000, payload_len=2048, shards=2)
        self.source_state = SourceState(root / "source", self.dataset)
        self.source_state.compact(reason="seed")
        self.source_api = build_source_app(self.source_state, log_path=root / "source.log")
        self.source_api.start()
        self.target_state = TargetState(root / "target", self.source_api.base_url)
        self.target_api = build_target_app(self.target_state, log_path=root / "target.log")
        self.target_api.start()
        self.source = HttpClient(self.source_api.base_url)
        self.target = HttpClient(self.target_api.base_url)

    # -- convenience --------------------------------------------------------
    def copy(self, **kwargs: Any):
        if kwargs.pop("restart_target", False):
            self.restart_target()
        return self.target_state.copy_blocking(**kwargs)

    def mutate(self, table: str, upserts: list[list[Any]] | None = None, deletes: list[Any] | None = None):
        status, body = self.source.post_json(
            "/api/mutate",
            {"table": table, "upserts": upserts or [], "deletes": deletes or []},
        )
        assert status == 200, body
        return body

    def compact(self, **body: Any):
        status, data = self.source.post_json("/api/compact", body)
        assert status == 200, data
        return data

    def restart_target(self) -> None:
        self.target_api.stop()
        self.target_state = TargetState(self.target_state.root, self.source_api.base_url)
        self.target_api = build_target_app(self.target_state, log_path=self.root / "target.log")
        self.target_api.start()
        self.target = HttpClient(self.target_api.base_url)

    def restart_source(self) -> None:
        self.source_api.stop()
        self.source_state = SourceState(self.source_state.root, self.dataset)
        self.source_api = build_source_app(self.source_state, log_path=self.root / "source.log")
        self.source_api.start()
        self.source = HttpClient(self.source_api.base_url)
        self.target_state.set_source(self.source_api.base_url)

    def query(self, **body: Any):
        return self.target.post_json("/api/query", body)

    def close(self) -> None:
        self.target_api.stop()
        self.source_api.stop()


class NodeTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp(prefix="pw-test-"))
        self.nodes = NodePair(self.tmp)

    def tearDown(self) -> None:
        self.nodes.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- assertions ---------------------------------------------------------
    def assertOk(self, status: int, body: dict) -> None:
        self.assertEqual(status, 200, json.dumps(body, ensure_ascii=False, indent=1))

    def assertGateRefused(self, body: dict, kind: str | None = None) -> None:
        self.assertFalse(body.get("ok"), body)
        self.assertEqual(body.get("error"), "query_gate_refused", body)
        kinds = [m["kind"] for m in body.get("missing", [])]
        if kind is not None:
            self.assertIn(kind, kinds, body)
        # 拒绝时绝不能带 data —— 避免“看似完整的错误结果”
        self.assertNotIn("data", body)
