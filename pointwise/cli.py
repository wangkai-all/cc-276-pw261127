"""两个本地服务的启动入口。

    python -m pointwise.cli source  --root .pointwise/source --port 18081
    python -m pointwise.cli target  --root .pointwise/target --source http://127.0.0.1:18081 --port 18082
    python -m pointwise.cli demo     # 起两个节点并种入演示数据

两端都只依赖 Python 标准库。
"""

from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path
from .common import write_json_atomic
from .dataset import Dataset, demo_dataset
from .source import SourceState, build_source_app
from .target import TargetState, build_target_app


def load_dataset(path: str | None) -> Dataset:
    if path:
        return Dataset.load(path)
    return demo_dataset()


def seed_source(state: SourceState) -> None:
    """只在来源尚未发布过任何 generation 时初始化。"""
    if state.bundle_generations():
        return
    state.compact(reason="seed")
    table = next(iter(state.dataset.tables.values()))
    pk = table.pk_index
    sample = table.rows[:5]
    if sample:
        state.mutate(table.name, sample, [])


def cmd_source(args: argparse.Namespace) -> int:
    dataset = load_dataset(args.dataset)
    state = SourceState(args.root, dataset)
    if args.seed:
        seed_source(state)
    if not state.bundle_generations():
        state.compact(reason="init")
    api = build_source_app(state, log_path=Path(args.root) / "source.log")
    api.port = args.port
    print(f"source listening on http://{args.host}:{args.port}", flush=True)
    print(json.dumps(state.summary(), ensure_ascii=False)[:400], flush=True)
    api.start(block=True)
    return 0


def cmd_target(args: argparse.Namespace) -> int:
    state = TargetState(args.root, args.source)
    if args.sync:
        print(json.dumps(state.sync_manifests(None), ensure_ascii=False), flush=True)
    api = build_target_app(state, log_path=Path(args.root) / "target.log")
    api.port = args.port
    print(f"target listening on http://{args.host}:{args.port}", flush=True)
    print(f"console: http://{args.host}:{args.port}/", flush=True)
    api.start(block=True)
    return 0


def cmd_demo(args: argparse.Namespace) -> int:
    root = Path(args.root)
    if args.fresh and root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=True)
    dataset = load_dataset(args.dataset)
    src_state = SourceState(root / "source", dataset)
    seed_source(src_state)
    if not src_state.bundle_generations():
        src_state.compact(reason="init")
    src_api = build_source_app(src_state, log_path=root / "source" / "source.log")
    src_api.port = args.source_port
    src_api.start()

    tgt_state = TargetState(root / "target", src_api.base_url)
    tgt_state.sync_manifests(None)
    if args.copy:
        tr = tgt_state.copy_blocking()
        count = len(tr.result.get("committed", [])) if tr.result else 0
        print(f"initial copy: {tr.status} ({count} objects)")
    tgt_api = build_target_app(tgt_state, log_path=root / "target" / "target.log")
    tgt_api.port = args.port
    tgt_api.start()

    write_json_atomic(
        root / "nodes.json",
        {"source": src_api.base_url, "target": tgt_api.base_url},
    )
    print(f"source: {src_api.base_url}")
    print(f"target: {tgt_api.base_url}")
    print(f"console: {tgt_api.base_url}/")
    if args.exit_after:
        time.sleep(args.exit_after)
        tgt_api.stop()
        src_api.stop()
        return 0
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        tgt_api.stop()
        src_api.stop()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="pointwise")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("source", help="run the source node")
    p.add_argument("--root", default=".pointwise/source")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=18081)
    p.add_argument("--dataset")
    p.add_argument("--seed", action="store_true", default=True)
    p.set_defaults(func=cmd_source)

    t = sub.add_parser("target", help="run the target node")
    t.add_argument("--root", default=".pointwise/target")
    t.add_argument("--host", default="127.0.0.1")
    t.add_argument("--port", type=int, default=18082)
    t.add_argument("--source", default="http://127.0.0.1:18081")
    t.add_argument("--sync", action="store_true")
    t.set_defaults(func=cmd_target)

    d = sub.add_parser("demo", help="run both nodes with demo data")
    d.add_argument("--root", default=".pointwise/demo")
    d.add_argument("--source-port", type=int, default=18081)
    d.add_argument("--port", type=int, default=18082)
    d.add_argument("--dataset")
    d.add_argument("--fresh", action="store_true")
    d.add_argument("--copy", action="store_true", default=True)
    d.add_argument("--exit-after", type=float, default=0)
    d.set_defaults(func=cmd_demo)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
