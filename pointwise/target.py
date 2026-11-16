"""目标节点：清单登记、断点续传复制、提交栅栏、查询门禁、旧快照核对。

关键不变量：

1. **可用性来自清单闭包**，而不是“目录里有多少文件”。门禁每次查询都重新求值。
2. **提交必须带 (epoch, generation) 栅栏**：任何写入都先拿文件锁、重新载入账本，
   再比较已有进度；旧任务的迟到提交被判 ``stale``，不会覆盖新进度，也不会删除
   新文件。
3. **绝不返回看似完整的错误结果**：闭包不全时返回 409/503 并列出缺什么，而不是
   返回空结果或部分结果。
4. **旧快照可读**：历史清单按 generation 保存，历史分片/增量按 epoch + checksum
   命名并保留，``snapshot=<generation>`` 的查询与核对走同一套求值逻辑。
"""

from __future__ import annotations

import copy
import json
import os
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .common import (
    canonical_json,
    exclusive,
    new_id,
    now_iso,
    read_json,
    rows_digest,
    safe_name,
    sha256_bytes,
    sha256_file,
    write_json_atomic,
)
from .httpmini import (
    ApiError,
    ApiServer,
    HttpClient,
    Request,
    TransferAborted,
    download_with_resume,
)
from .shards import apply_chain, read_increment, read_shard

READY = "ready"
INTERRUPTED = "interrupted"
FAILED = "failed"
RUNNING = "running"
STALE = "stale"

SCOPE_FULL = "full"
SCOPE_TABLE = "per-table"

_UNSET = object()


class Superseded(Exception):
    """来源在复制途中压缩合并，目标需要重新拉取清单后重试。"""


class SimulatedCrash(BaseException):
    """测试钩子：模拟进程被杀死，工作线程不写任何终态。"""


# ---------------------------------------------------------------------------
# Ledger
# ---------------------------------------------------------------------------


class Ledger:
    """目标端已确认到达的对象账本。读取前请持锁，写入请同时持 fence 锁。"""

    def __init__(self, path: Path) -> None:
        self.path = path
        data = read_json(path)
        self.data: dict[str, Any] = data or {
            "version": 1,
            "objects": {},
            "schemas": {},
            "dictionaries": {},
            "cols": {},
            "commits": [],
            "generation": 0,
            "epoch": 0,
            "base_epoch": 0,
            "history": [],
            "last_writer": None,
        }
        self.data.setdefault("history", [])
        self.data.setdefault("objects", {})

    def save(self) -> None:
        write_json_atomic(self.path, self.data)

    # -- objects -------------------------------------------------------------
    def object_key(self, table: str, kind: str, name: str, epoch: int) -> str:
        return f"{table}/{kind}/{name}@{epoch}"

    def get_object(self, table: str, kind: str, name: str, epoch: int | None = None) -> dict | None:
        objs = self.data["objects"]
        if epoch is not None:
            return objs.get(self.object_key(table, kind, name, epoch))
        best = None
        for obj in objs.values():
            if obj["table"] == table and obj["kind"] == kind and obj["name"] == name:
                if best is None or int(obj["epoch"]) > int(best["epoch"]):
                    best = obj
        return best

    def objects_of(self, table: str, kind: str) -> list[dict]:
        return sorted(
            (o for o in self.data["objects"].values() if o["table"] == table and o["kind"] == kind),
            key=lambda o: int(o["epoch"]),
        )

    def commit_object(self, obj: dict, writer: dict) -> tuple[bool, str]:
        """提交一个对象，返回 ``(committed, reason)``。

        栅栏规则（调用方持锁）：
          - 对象 epoch 低于账本 ``base_epoch``（来源已压缩合并）→ stale_base；
          - 对象 generation 低于账本已登记的最新清单 → stale_generation；
          - 同 ``(kind, name)`` 已登记了**更高 epoch** → stale_epoch；
          - 同 ``(table, kind, name, epoch)`` 已登记且 checksum 相同 → 幂等成功；
          - 同 key 已登记但 checksum 不同 → conflicting_digest。

        后三种规则共同保证：来源压缩合并、目标重启、两个复制任务抢先完成时，
        迟到的旧副本不能覆盖或回退新进度。
        """
        key = self.object_key(obj["table"], obj["kind"], obj["name"], int(obj["epoch"]))
        existing = self.data["objects"].get(key)
        base_epoch = int(self.data.get("base_epoch") or 0)
        ledger_gen = int(self.data.get("generation") or 0)

        def reject(reason: str, **detail: Any) -> tuple[bool, str]:
            self.data["commits"].append(
                {
                    "at": now_iso(),
                    "object": key,
                    "writer": writer,
                    "result": "stale",
                    "reason": reason,
                    **detail,
                }
            )
            del self.data["commits"][:-400]
            self.save()
            return False, reason

        if existing is not None:
            if existing["sha256"] == obj["sha256"]:
                return True, "idempotent"
            return False, "conflicting_digest"
        if int(obj["epoch"]) < base_epoch:
            return reject("stale_base_epoch", base_epoch=base_epoch)
        if obj["kind"] == "incr" and int(obj.get("base_epoch", 0)) < base_epoch:
            return reject(
                "stale_increment_base",
                base_epoch=base_epoch,
                obj_base_epoch=int(obj.get("base_epoch", 0)),
            )
        if int(obj.get("generation", 0)) < ledger_gen:
            return reject("stale_generation", ledger_generation=ledger_gen)
        current = self.get_object(obj["table"], obj["kind"], obj["name"])
        if current is not None and int(current["epoch"]) > int(obj["epoch"]):
            return reject("stale_epoch", newer_epoch=int(current["epoch"]))
        self.data["objects"][key] = obj
        self.data["last_writer"] = writer
        self.data["commits"].append(
            {"at": now_iso(), "object": key, "writer": writer, "result": "committed"}
        )
        del self.data["commits"][:-400]
        self.save()
        return True, "committed"

    # -- local-scope metadata -------------------------------------------------
    def put_schema(self, table: str, schema_id: str, record: dict) -> None:
        self.data["schemas"].setdefault(table, {})[schema_id] = record
        self.save()

    def get_schema(self, table: str, schema_id: str | None = None) -> dict | None:
        recs = self.data["schemas"].get(table, {})
        if schema_id is not None:
            return recs.get(schema_id)
        if not recs:
            return None
        return recs[sorted(recs)[-1]]

    def put_dictionary(self, table: str, version: str, record: dict) -> None:
        self.data["dictionaries"].setdefault(table, {})[version] = record
        self.save()

    def get_dictionary(self, table: str, version: str | None = None) -> dict | None:
        recs = self.data["dictionaries"].get(table, {})
        if version is not None:
            return recs.get(version)
        if not recs:
            return None
        return recs[sorted(recs)[-1]]

    def put_cols(self, table: str, schema_id: str, cols: list[list[str]]) -> None:
        self.data["cols"][table] = {
            "schema_id": schema_id,
            "cols": cols,
            "registered_at": now_iso(),
        }
        self.save()

    def get_cols(self, table: str, schema_id: str | None = None) -> dict | None:
        rec = self.data["cols"].get(table)
        if rec is None:
            return None
        if schema_id is not None and rec.get("schema_id") != schema_id:
            return None
        return rec

    # -- manifest history -----------------------------------------------------
    def record_manifest(self, bundle: dict) -> None:
        self.data["manifest"] = {
            "generation": int(bundle["generation"]),
            "epoch": int(bundle["epoch"]),
            "registered_at": now_iso(),
        }
        self.data["generation"] = max(
            int(self.data.get("generation", 0)), int(bundle["generation"])
        )
        self.data["epoch"] = max(int(self.data.get("epoch", 0)), int(bundle["epoch"]))
        self.data["base_epoch"] = max(
            int(self.data.get("base_epoch", 0)), int(bundle.get("base_epoch", 0))
        )
        self.save()

    def add_history(self, bundle: dict) -> None:
        """保存整份清单历史。旧快照查询直接从这里取，不依赖来源仍然在线。"""
        hist = self.data.setdefault("history", [])
        entry = {
            "generation": int(bundle["generation"]),
            "epoch": int(bundle["epoch"]),
            "base_epoch": int(bundle.get("base_epoch", 0)),
            "registered_at": now_iso(),
            "source_id": bundle.get("source_id"),
            "tables": {m["table"]: m for m in bundle.get("tables", [])},
        }
        hist[:] = [h for h in hist if int(h["generation"]) != entry["generation"]]
        hist.append(entry)
        hist.sort(key=lambda h: int(h["generation"]))
        del hist[:-40]
        self.save()

    def manifest_at(self, generation: int) -> dict | None:
        for h in self.data.get("history", []):
            if int(h["generation"]) == int(generation):
                return h
        return None

    def latest_snapshot(self) -> dict | None:
        hist = self.data.get("history", [])
        return hist[-1] if hist else None


# ---------------------------------------------------------------------------
# Transfer
# ---------------------------------------------------------------------------


@dataclass
class Transfer:
    id: str
    scope: str
    tables: list[str]
    status: str = RUNNING
    started_at: str = field(default_factory=now_iso)
    finished_at: str | None = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    events: deque = field(default_factory=lambda: deque(maxlen=500))
    progress: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None
    generation: int | None = None
    abort_after: int | None = None
    pace: float = 0.0
    include: list[str] = field(
        default_factory=lambda: ["schema", "dict", "shards", "increments"]
    )
    pinned_generation: int | None = None
    reused_part_bytes: int = 0
    crash_after: int | None = None

    def log(self, message: str = "", /, **extra: Any) -> None:
        self.events.append({"at": now_iso(), "message": message, **extra})

    def view(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "scope": self.scope,
            "tables": self.tables,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "generation": self.generation,
            "abort_after": self.abort_after,
            "include": list(self.include),
            "reused_part_bytes": self.reused_part_bytes,
            "progress": self.progress,
            "error": self.error,
            "result": self.result,
            "events": list(self.events)[-80:],
        }

    def persist(self) -> dict[str, Any]:
        view = self.view()
        view.pop("events", None)
        return view


# ---------------------------------------------------------------------------
# Target state
# ---------------------------------------------------------------------------


class TargetState:
    def __init__(self, root: str | Path, source_url: str | None = None) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.data_dir = self.root / "data"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir = self.root / "incoming"
        self.staging_dir = self.tmp_dir / "staging"
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.fence_path = self.root / ".fence.lock"
        self.ledger_path = self.root / "ledger.json"
        self.lock = threading.RLock()
        self.ledger = Ledger(self.ledger_path)
        self.info_path = self.root / "target.json"
        info = read_json(self.info_path) or {
            "target_id": new_id("tgt"),
            "created_at": now_iso(),
            "source_url": source_url,
        }
        if source_url:
            info["source_url"] = source_url
        write_json_atomic(self.info_path, info)
        self.info = info
        self.transfers: dict[str, Transfer] = {}
        snap = self.ledger.latest_snapshot()
        # 重启后从账本历史恢复“最近一次同步的清单”：resume 不需要重新 sync。
        self.manifests: dict[str, dict] = dict(snap["tables"]) if snap else {}
        self.client = HttpClient(self.source_url) if self.source_url else None
        self.started_at = now_iso()
        self.ready_observed: dict[str, str] = dict(self.info.get("ready_observed") or {})
        self.startup = self._recover()

    # -- helpers --------------------------------------------------------------
    @property
    def source_url(self) -> str | None:
        return self.info.get("source_url")

    def set_source(self, url: str) -> None:
        self.info["source_url"] = url
        write_json_atomic(self.info_path, self.info)
        self.client = HttpClient(url)

    def fence(self):
        return exclusive(self.fence_path)

    def _reload_ledger(self) -> None:
        self.ledger = Ledger(self.ledger_path)

    def object_path(self, table: str, kind: str, name: str, epoch: int, suffix: str = ".db") -> Path:
        safe = [safe_name(table), safe_name(kind), safe_name(name)]
        return self.data_dir / safe[0] / safe[1] / f"{safe[2]}.e{int(epoch):06d}{suffix}"

    def _recover(self) -> dict[str, Any]:
        """目标重启恢复：保留 .part、把 RUNNING 的 transfer 标为 interrupted。

        账本（ledger.json）与 ``.part`` 都在磁盘上，因此重启后已提交的对象不会
        重下，未完成的字节也能从 Range 断点继续。
        """
        abandoned = []
        persisted = dict(self.info.get("transfers") or {})
        for tid, spec in persisted.items():
            if spec.get("status") not in (RUNNING, INTERRUPTED):
                continue
            was_running = spec.get("status") == RUNNING
            tr = Transfer(
                id=tid,
                scope=spec.get("scope", SCOPE_FULL),
                tables=list(spec.get("tables", [])),
                status=INTERRUPTED,
                started_at=spec.get("started_at", now_iso()),
                finished_at=spec.get("finished_at") or now_iso(),
                generation=spec.get("generation"),
                abort_after=spec.get("abort_after"),
                progress=dict(spec.get("progress") or {}),
                include=list(spec.get("include") or ["schema", "dict", "shards", "increments"]),
            )
            if was_running:
                tr.error = {
                    "code": "node_restart",
                    "message": "target node restarted mid-transfer; partial bytes retained",
                }
            else:
                tr.error = spec.get("error") or {
                    "code": "interrupted",
                    "message": "transfer was interrupted before restart",
                }
            tr.log("recovered after restart; ready to resume", was_running=was_running)
            self.transfers[tid] = tr
            spec["status"] = INTERRUPTED
            spec["error"] = tr.error
            if was_running:
                abandoned.append(tid)
        parts = [str(p.relative_to(self.tmp_dir)) for p in self.tmp_dir.rglob("*.part")]
        for meta in self.tmp_dir.rglob("*.part.meta"):
            if not meta.with_suffix("").exists():
                meta.unlink(missing_ok=True)
        if abandoned or parts:
            self.info["abandoned_transfers"] = abandoned
            self.info["recovered_parts"] = parts
            self.info["recovered_at"] = now_iso()
        self.info.setdefault("transfers", {})
        write_json_atomic(self.info_path, self.info)
        return {
            "abandoned_transfers": abandoned,
            "recovered_parts": parts,
            "known_transfers": sorted(self.transfers),
        }

    def _persist_transfer(self, tr: Transfer) -> None:
        self.info.setdefault("transfers", {})[tr.id] = tr.persist()
        write_json_atomic(self.info_path, self.info)

    # -- source manifest ------------------------------------------------------
    def sync_manifests(self, tables: list[str] | None = None) -> dict[str, Any]:
        if self.client is None:
            raise ApiError(400, "no_source", "target has no source_url configured")
        status, bundle = self.client.get_json("/api/bundle")
        if status != 200:
            raise ApiError(502, "source_unavailable", f"source bundle -> {status}")
        want = set(tables) if tables else None
        with self.lock:
            registered = []
            for manifest in bundle["tables"]:
                # 清单元数据总是整体登记：可用性必须基于“这个 generation 应该有哪些
                # 表”，而不是“本次只同步了哪几张表”。tables 只影响返回值/复制范围。
                self.manifests[manifest["table"]] = manifest
                if want is None or manifest["table"] in want:
                    registered.append(manifest["table"])
            with self.fence():
                self._reload_ledger()
                self.ledger.record_manifest(bundle)
                self.ledger.add_history(bundle)
            return {
                "ok": True,
                "generation": int(bundle["generation"]),
                "epoch": int(bundle["epoch"]),
                "base_epoch": int(bundle.get("base_epoch", 0)),
                "tables": registered,
                "source_id": bundle.get("source_id"),
            }

    def manifest_for(self, table: str, generation: int | None = None) -> dict:
        if generation is not None:
            snap = self.ledger.manifest_at(generation)
            if snap is None or table not in snap["tables"]:
                raise ApiError(
                    404,
                    "snapshot_metadata_missing",
                    f"no snapshot metadata for generation {generation}",
                )
            return copy.deepcopy(snap["tables"][table])
        manifest = self.manifests.get(table)
        if manifest is None:
            raise ApiError(
                404, "manifest_missing", f"no manifest for table {table!r}; run /api/sync"
            )
        return manifest

    # -- copying --------------------------------------------------------------
    def start_copy(
        self,
        tables: list[str] | None = None,
        resume: bool = True,
        scope: str = SCOPE_FULL,
        abort_after: int | None = None,
        pace: float = 0.0,
        background: bool = True,
        generation: int | None = None,
        transfer_id: str | None = None,
        include: list[str] | None = None,
        allow_concurrent: bool = False,
        pinned_generation: int | None = None,
        crash_after: int | None = None,
    ) -> Transfer:
        if self.client is None:
            raise ApiError(400, "no_source", "target has no source_url configured")
        if scope not in (SCOPE_FULL, SCOPE_TABLE):
            raise ApiError(400, "bad_scope", f"scope must be {SCOPE_FULL} or {SCOPE_TABLE}")
        kinds = list(include) if include else ["schema", "dict", "shards", "increments"]
        bad = [k for k in kinds if k not in ("schema", "dict", "shards", "increments")]
        if bad:
            raise ApiError(400, "bad_include", f"unknown copy kinds: {bad}")
        with self.lock:
            self._reap()
            active = [t for t in self.transfers.values() if t.status == RUNNING]
            if active and not allow_concurrent:
                raise ApiError(
                    409,
                    "transfer_running",
                    f"transfer {active[0].id} is still running; interrupt it or wait",
                )
            if transfer_id and transfer_id in self.transfers:
                tr = self.transfers[transfer_id]
                if tr.status == READY:
                    raise ApiError(409, "already_ready", f"transfer {transfer_id} already completed")
                tr.stop_event = threading.Event()
                tr.status = RUNNING
                tr.error = None
                tr.finished_at = None
                if abort_after is not None or tr.abort_after is None:
                    tr.abort_after = abort_after
                tr.pace = pace
                tr.log("resume requested")
            else:
                self.sync_manifests(tables)
                selected = list(tables) if tables else list(self.manifests.keys())
                unknown = [t for t in selected if t not in self.manifests]
                if unknown:
                    raise ApiError(404, "unknown_table", f"no manifest for {unknown}")
                tr = Transfer(
                    id=new_id("tr"),
                    scope=scope,
                    tables=selected,
                    abort_after=abort_after,
                    pace=pace,
                    include=kinds,
                    pinned_generation=pinned_generation,
                    crash_after=crash_after,
                )
                tr.generation = int(self.ledger.data.get("generation", 0))
                self.transfers[tr.id] = tr
            self._persist_transfer(tr)
            if background:
                tr.thread = threading.Thread(
                    target=self._run_transfer, args=(tr, resume), daemon=True
                )
                tr.thread.start()
            return tr

    def copy_blocking(self, timeout: float = 600, **kwargs: Any) -> Transfer:
        """非后台复制：阻塞到完成，供 CLI / 测试使用。"""
        tr = self.start_copy(background=True, **kwargs)
        return self.wait(tr.id, timeout=timeout)

    def _reap(self) -> None:
        for tid, tr in list(self.transfers.items()):
            if tr.status == RUNNING and tr.thread is not None and not tr.thread.is_alive():
                tr.status = FAILED
                tr.error = tr.error or {"code": "worker_exited", "message": "worker thread exited"}
                tr.finished_at = tr.finished_at or now_iso()

    def interrupt(self, transfer_id: str) -> dict[str, Any]:
        tr = self.transfers.get(transfer_id)
        if tr is None:
            raise ApiError(404, "unknown_transfer", f"no transfer {transfer_id!r}")
        if tr.status != RUNNING:
            return {"ok": True, "transfer": tr.view(), "already": tr.status}
        tr.stop_event.set()
        tr.log("interrupt requested")
        deadline = time.time() + 20
        while tr.status == RUNNING and time.time() < deadline:
            time.sleep(0.02)
        return {"ok": True, "transfer": tr.view()}

    def resume(
        self,
        transfer_id: str,
        pace: float | None = None,
        abort_after: int | None | object = _UNSET,
    ) -> dict[str, Any]:
        """恢复一个 interrupted 的 transfer。

        ``pace`` 传值则覆盖；``abort_after`` 传值（包括显式 None）则覆盖，缺省时
        保留原值，这样“再中断一次”的测试脚本可以反复使用同一个阈值。
        """
        tr = self.transfers.get(transfer_id)
        if tr is None:
            raise ApiError(404, "unknown_transfer", f"no transfer {transfer_id!r}")
        if tr.status == RUNNING:
            raise ApiError(409, "already_running", "transfer is already running")
        with self.lock:
            active = [t for t in self.transfers.values() if t.status == RUNNING and t.id != tr.id]
            if active:
                raise ApiError(409, "transfer_running", f"transfer {active[0].id} is running")
            tr.stop_event = threading.Event()
            tr.status = RUNNING
            tr.error = None
            tr.finished_at = None
            if abort_after is not _UNSET:
                tr.abort_after = abort_after  # type: ignore[assignment]
            tr.reused_part_bytes = self._part_bytes(tr)
            if pace is not None:
                tr.pace = pace
            tr.log("resumed", resumed_bytes=tr.reused_part_bytes)
            self._persist_transfer(tr)
            tr.thread = threading.Thread(target=self._run_transfer, args=(tr, True), daemon=True)
            tr.thread.start()
        return {"ok": True, "transfer": tr.view()}

    def _part_bytes(self, tr: Transfer) -> int:
        total = 0
        for staged in self.staging_dir.glob("*"):
            if staged.name.endswith(".part"):
                try:
                    total += staged.stat().st_size
                except OSError:
                    pass
        return total

    def _run_transfer(self, tr: Transfer, resume: bool) -> None:
        try:
            attempts = 0
            while True:
                try:
                    result = self._copy_all(tr)
                    break
                except Superseded as exc:
                    attempts += 1
                    if attempts >= 3:
                        raise ApiError(409, "source_compacted", str(exc))
                    tr.log("source compacted mid-copy; re-syncing manifest", attempt=attempts)
                    # 清单已经前进：解除 generation 钉扎，下一次必须用最新清单。
                    tr.pinned_generation = None
                    self.sync_manifests(None)
            tr.result = result
            tr.status = STALE if result.get("stale_tables") else READY
        except TransferAborted as exc:
            tr.status = INTERRUPTED
            tr.error = {"code": "interrupted", "message": str(exc)}
            tr.log("interrupted", detail=str(exc))
        except SimulatedCrash:
            # 模拟进程消失：不写终态、不持久化，目标重启后会看到 RUNNING。
            tr.log("simulated crash; no final state written")
            tr.status = RUNNING
            return
        except ApiError as exc:
            tr.status = FAILED
            tr.error = {"code": exc.code, "message": exc.message, **exc.extra}
            tr.log("failed", code=exc.code, message=exc.message)
        except Exception as exc:  # pragma: no cover - defensive
            tr.status = FAILED
            tr.error = {"code": "internal", "message": f"{type(exc).__name__}: {exc}"}
            tr.log("failed", code="internal", message=str(exc))
        finally:
            tr.finished_at = now_iso()
            with self.lock:
                self._persist_transfer(tr)

    def wait(self, transfer_id: str, timeout: float = 120.0) -> Transfer:
        tr = self.transfers[transfer_id]
        if tr.thread is not None:
            tr.thread.join(timeout=timeout)
        if tr.status == RUNNING:
            raise TimeoutError(f"transfer {transfer_id} still running after {timeout}s")
        return tr

    def _guard(self, tr: Transfer) -> None:
        if tr.stop_event.is_set():
            raise TransferAborted("stop requested")

    def _fetch(
        self,
        tr: Transfer,
        table: str,
        kind: str,
        entry: dict,
        expected_sha: str,
        expected_bytes: int | None,
        epoch: int,
    ) -> Path:
        final = self.object_path(
            table,
            kind,
            entry["name"],
            epoch,
            suffix=".json" if kind in ("schema", "dict") else ".db",
        )
        if final.exists() and sha256_file(final) == expected_sha:
            tr.log(f"{kind}:{entry['name']} already present", table=table)
            return final
        staged = self.staging_dir / (
            f"{safe_name(table)}-{kind}-{safe_name(entry['name'])}.e{epoch:06d}"
        )
        tr.log(f"fetch {kind}:{entry['name']}", table=table, epoch=epoch)
        info = download_with_resume(
            self.client,  # type: ignore[arg-type]
            entry["url"],
            staged,
            expected_sha=expected_sha,
            expected_bytes=expected_bytes,
            stop_event=tr.stop_event,
            abort_after=tr.abort_after,
            pace=tr.pace,
            progress=lambda written, total: self._progress(
                tr, table, kind, entry["name"], written, total
            ),
        )
        if info["resumed_from"] > 0:
            tr.log(
                f"resumed {kind}:{entry['name']} at byte {info['resumed_from']}",
                table=table,
                resumed_from=info["resumed_from"],
            )
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(info["part"], final)
        self._guard(tr)
        return final

    def _progress(
        self, tr: Transfer, table: str, kind: str, name: str, written: int, total: int | None
    ) -> None:
        key = f"{table}/{kind}/{name}"
        tr.progress[key] = {"written": written, "total": total}
        if tr.crash_after is not None and written >= tr.crash_after and kind == "shard":
            tr.crash_after = None
            raise SimulatedCrash(f"crash after {written} bytes of {key}")

    def _copy_all(self, tr: Transfer) -> dict[str, Any]:
        stale_tables: list[str] = []
        committed: list[dict] = []
        for table in tr.tables:
            self._guard(tr)
            manifest = self.manifest_for(table, tr.pinned_generation)
            if "schema" in tr.include:
                self._sync_schema(tr, table, manifest)
            if "dict" in tr.include:
                self._sync_dictionary(tr, table, manifest)
            shard_failed = False
            for shard in (manifest["shards"] if "shards" in tr.include else []):
                self._guard(tr)
                entry = {"name": shard["shard"], "url": shard["url"]}
                path = self._fetch(
                    tr, table, "shard", entry, shard["sha256"], shard["bytes"], int(shard["epoch"])
                )
                ok, reason = self._commit(
                    tr,
                    table,
                    "shard",
                    {
                        "table": table,
                        "kind": "shard",
                        "name": shard["shard"],
                        "epoch": int(shard["epoch"]),
                        "sha256": shard["sha256"],
                        "bytes": shard["bytes"],
                        "rows": shard["rows"],
                        "path": str(path.relative_to(self.root)),
                        "generation": int(manifest["generation"]),
                        "schema_id": manifest["schema"]["schema_id"],
                        "cols": manifest.get("cols", []),
                    },
                )
                if not ok:
                    if reason in ("stale_generation", "stale_base_epoch", "stale_increment_base"):
                        # 本任务持有的清单已经落后于目标账本：重拉清单再试。
                        raise Superseded(f"{table}/{shard['shard']}: {reason}")
                    if reason.startswith("stale"):
                        stale_tables.append(table)
                        tr.log(f"shard {shard['shard']} considered stale", reason=reason)
                        shard_failed = True
                        break
                    raise ApiError(
                        409, "commit_conflict", f"{table}/{shard['shard']}: {reason}"
                    )
                committed.append({"table": table, "kind": "shard", "name": shard["shard"]})
            if shard_failed:
                continue
            for inc in (manifest["increments"] if "increments" in tr.include else []):
                self._guard(tr)
                entry = {"name": inc["incr"], "url": inc["url"]}
                path = self._fetch(
                    tr, table, "incr", entry, inc["sha256"], inc["bytes"], int(inc["epoch"])
                )
                self._validate_increment(path, table, inc, manifest)
                ok, reason = self._commit(
                    tr,
                    table,
                    "incr",
                    {
                        "table": table,
                        "kind": "incr",
                        "name": inc["incr"],
                        "epoch": int(inc["epoch"]),
                        "base_epoch": int(inc["base_epoch"]),
                        "requires": list(inc["requires"]),
                        "sha256": inc["sha256"],
                        "bytes": inc["bytes"],
                        "upserts": inc["upserts"],
                        "deletes": inc["deletes"],
                        "path": str(path.relative_to(self.root)),
                        "generation": int(manifest["generation"]),
                        "schema_id": manifest["schema"]["schema_id"],
                    },
                )
                if not ok:
                    if reason in ("stale_generation", "stale_base_epoch", "stale_increment_base"):
                        raise Superseded(f"{table}/{inc['incr']}: {reason}")
                    if reason.startswith("stale"):
                        stale_tables.append(table)
                        tr.log(f"increment {inc['incr']} considered stale", reason=reason)
                        break
                    raise ApiError(409, "commit_conflict", f"{table}/{inc['incr']}: {reason}")
                committed.append({"table": table, "kind": "incr", "name": inc["incr"]})
        return {
            "committed": committed,
            "stale_tables": sorted(set(stale_tables)),
            "generation": int(self.ledger.data.get("generation", 0)),
            "at": now_iso(),
        }

    def _sync_schema(self, tr: Transfer, table: str, manifest: dict) -> None:
        schema = manifest["schema"]
        existing = self.ledger.get_schema(table, schema["schema_id"])
        if existing and existing.get("sha256") == schema["sha256"]:
            return
        entry = {"name": schema["schema_id"], "url": schema["url"]}
        path = self._fetch(
            tr, table, "schema", entry, schema["sha256"], schema.get("bytes"), 0
        )
        raw = path.read_bytes()
        digest = sha256_bytes(raw)
        if digest != schema["sha256"]:
            raise ApiError(502, "checksum_mismatch", f"schema digest mismatch for {table}")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ApiError(502, "bad_schema", f"schema for {table} is not valid JSON")
        if payload.get("schema_id") != schema["schema_id"]:
            raise ApiError(502, "bad_schema", "schema_id in file disagrees with manifest")
        if [c["name"] for c in payload.get("columns", [])] != [
            c["name"] for c in schema["columns"]
        ]:
            raise ApiError(502, "bad_schema", "schema columns disagree with manifest")
        with self.lock, self.fence():
            self._reload_ledger()
            self.ledger.put_schema(
                table,
                schema["schema_id"],
                {
                    "schema_id": schema["schema_id"],
                    "columns": payload.get("columns", []),
                    "primary_key": payload.get("primary_key"),
                    "sha256": digest,
                    "generation": int(manifest["generation"]),
                    "path": str(path.relative_to(self.root)),
                    "at": now_iso(),
                },
            )
            self.ledger.put_cols(table, schema["schema_id"], manifest.get("cols", []))
        tr.log(f"schema registered for {table}", schema_id=schema["schema_id"])

    def _sync_dictionary(self, tr: Transfer, table: str, manifest: dict) -> None:
        entry = manifest.get("dictionary")
        if not entry:
            return
        existing = self.ledger.get_dictionary(table, entry["version"])
        if existing and existing.get("sha256") == entry["sha256"]:
            return
        fetch_entry = {"name": entry["version"], "url": entry["url"]}
        path = self._fetch(tr, table, "dict", fetch_entry, entry["sha256"], entry["bytes"], 0)
        raw = path.read_bytes()
        if sha256_bytes(raw) != entry["sha256"]:
            raise ApiError(502, "checksum_mismatch", f"dictionary digest mismatch for {table}")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ApiError(502, "bad_dictionary", f"dictionary for {table} is not valid JSON")
        entries_sha = sha256_bytes(canonical_json(payload.get("entries", [])).encode("utf-8"))
        if entries_sha != entry["entries_sha256"]:
            raise ApiError(502, "bad_dictionary", "dictionary entries digest mismatch")
        if len(payload.get("entries", [])) != int(entry["entries"]):
            raise ApiError(502, "bad_dictionary", "dictionary entry count mismatch")
        with self.lock, self.fence():
            self._reload_ledger()
            self.ledger.put_dictionary(
                table,
                entry["version"],
                {
                    "version": entry["version"],
                    "entries": payload["entries"],
                    "sha256": entry["sha256"],
                    "entries_sha256": entry["entries_sha256"],
                    "generation": int(manifest["generation"]),
                    "path": str(path.relative_to(self.root)),
                    "at": now_iso(),
                },
            )
        tr.log(f"dictionary registered for {table}", version=entry["version"])

    def _validate_increment(self, path: Path, table: str, inc: dict, manifest: dict) -> None:
        data = read_increment(path)
        if data.meta.get("table") != table:
            raise ApiError(502, "bad_increment", "increment table mismatch")
        if int(data.meta.get("epoch", -1)) != int(inc["epoch"]):
            raise ApiError(502, "bad_increment", "increment epoch mismatch")
        if int(data.meta.get("base_epoch", -1)) != int(inc["base_epoch"]):
            # 来源在复制途中压缩合并：旧增量的基准已被新分片取代。
            raise Superseded(
                f"increment {inc['incr']} built on epoch {data.meta.get('base_epoch')}, "
                f"manifest expects {inc['base_epoch']}"
            )
        requires = json.loads(data.meta.get("requires", "[]"))
        if sorted(requires) != sorted(inc["requires"]):
            raise Superseded(f"increment {inc['incr']} requires changed")
        if data.meta.get("schema_id") != manifest["schema"]["schema_id"]:
            raise ApiError(409, "schema_conflict", f"increment {inc['incr']} built on another schema")

    def _commit(self, tr: Transfer, table: str, kind: str, obj: dict) -> tuple[bool, str]:
        writer = {
            "transfer": tr.id,
            "epoch": int(obj["epoch"]),
            "table": table,
            "at": now_iso(),
        }
        with self.fence():
            self._reload_ledger()
            ok, reason = self.ledger.commit_object(obj, writer)
        if ok:
            tr.log(f"commit {kind}:{obj['name']}", epoch=obj["epoch"], reason=reason)
        else:
            tr.log(f"commit rejected {kind}:{obj['name']}", epoch=obj["epoch"], reason=reason)
        return ok, reason

    # -- state views ----------------------------------------------------------
    def _note_ready(self, table: str, ready: bool) -> None:
        """记录“这张表第一次在目标上可用”的墙钟时间，供可用时间对比。"""
        if not ready:
            return
        with self.lock:
            if table in self.ready_observed:
                return
            self.ready_observed[table] = now_iso()
            self.info["ready_observed"] = self.ready_observed
            write_json_atomic(self.info_path, self.info)

    def table_state(self, table: str, generation: int | None = None) -> dict[str, Any]:
        manifest = self.manifest_for(table, generation)
        schema_ok = self.ledger.get_schema(table, manifest["schema"]["schema_id"]) is not None
        cols_ok = self.ledger.get_cols(table, manifest["schema"]["schema_id"]) is not None
        dict_entry = manifest.get("dictionary")
        dict_ok = True
        dict_present = None
        if dict_entry:
            rec = self.ledger.get_dictionary(table, dict_entry["version"])
            dict_ok = rec is not None and rec.get("sha256") == dict_entry["sha256"]
            dict_present = rec is not None
        shard_states = []
        not_ready: list[str] = []
        for shard in manifest["shards"]:
            obj = self.ledger.get_object(table, "shard", shard["shard"], int(shard["epoch"]))
            ok = obj is not None and obj["sha256"] == shard["sha256"]
            if not ok:
                not_ready.append(shard["shard"])
            shard_states.append(
                {
                    "shard": shard["shard"],
                    "epoch": int(shard["epoch"]),
                    "ready": ok,
                    "rows": shard.get("rows"),
                    "bytes": shard.get("bytes"),
                }
            )
        incr_states = []
        ready_by_shard = {s["shard"]: s["ready"] for s in shard_states}
        bad_base: list[str] = []
        for inc in manifest["increments"]:
            obj = self.ledger.get_object(table, "incr", inc["incr"], int(inc["epoch"]))
            requires_ready = all(ready_by_shard.get(req, False) for req in inc["requires"])
            if not requires_ready:
                bad_base.append(inc["incr"])
            incr_states.append(
                {
                    "incr": inc["incr"],
                    "epoch": int(inc["epoch"]),
                    "base_epoch": int(inc["base_epoch"]),
                    "requires": list(inc["requires"]),
                    "requires_ready": requires_ready,
                    "ready": obj is not None and obj["sha256"] == inc["sha256"],
                    "upserts": inc.get("upserts"),
                    "deletes": inc.get("deletes"),
                }
            )
        ready = (
            schema_ok
            and cols_ok
            and (dict_ok or dict_entry is None)
            and all(s["ready"] for s in shard_states)
            and all(i["ready"] and i["requires_ready"] for i in incr_states)
        )
        return {
            "table": table,
            "generation": int(manifest["generation"]),
            "epoch": int(manifest["epoch"]),
            "shard_epoch": int(manifest["shard_epoch"]),
            "schema": {"schema_id": manifest["schema"]["schema_id"], "ready": schema_ok},
            "cols": {"ready": cols_ok},
            "dictionary": (
                {
                    "version": dict_entry["version"],
                    "ready": dict_ok,
                    "present": dict_present,
                    "entries": dict_entry.get("entries"),
                }
                if dict_entry
                else None
            ),
            "shards": shard_states,
            "increments": incr_states,
            "ready": bool(ready),
            "ready_since": self.ready_observed.get(table),
            "missing": [
                *(
                    [{"kind": "schema", "detail": "schema not registered"}]
                    if not schema_ok
                    else []
                ),
                *(
                    [{"kind": "cols", "detail": "column metadata not registered"}]
                    if not cols_ok
                    else []
                ),
                *(
                    [
                        {
                            "kind": "dictionary",
                            "detail": f"dictionary {dict_entry['version']} not registered",
                        }
                    ]
                    if dict_entry and not dict_ok
                    else []
                ),
                *(
                    [{"kind": "shards", "detail": f"not ready: {', '.join(not_ready)}"}]
                    if not_ready
                    else []
                ),
                *(
                    [
                        {
                            "kind": "increments",
                            "detail": f"not ready: {', '.join(i['incr'] for i in incr_states if not i['ready'])}",
                        }
                    ]
                    if any(not i["ready"] for i in incr_states)
                    else []
                ),
                *(
                    [
                        {
                            "kind": "increment_base",
                            "detail": f"base shards not ready for: {', '.join(bad_base)}",
                        }
                    ]
                    if bad_base
                    else []
                ),
            ],
        }

    def overview(self, scope: str = SCOPE_FULL) -> dict[str, Any]:
        tables = []
        for table in sorted(self.manifests):
            try:
                state = self.table_state(table)
                self._note_ready(table, bool(state.get("ready")))
                tables.append(state)
            except ApiError as exc:
                tables.append({"table": table, "ready": False, "error": exc.payload()})
        ready_tables = [t for t in tables if t.get("ready")]
        if scope == SCOPE_FULL:
            available = bool(tables) and len(ready_tables) == len(tables)
        else:
            available = bool(ready_tables)
        observed = [self.ready_observed.get(t["table"]) for t in ready_tables]
        observed = [o for o in observed if o]
        full_available_at = (
            max(observed) if ready_tables and len(ready_tables) == len(tables) else None
        )
        latest_writer = self.ledger.data.get("last_writer")
        return {
            "ok": True,
            "scope": scope,
            "generation": int(self.ledger.data.get("generation", 0)),
            "available": available,
            "available_at": (
                full_available_at if scope == SCOPE_FULL else (min(observed) if observed else None)
            ),
            "full_available_at": full_available_at,
            "first_table_available_at": min(observed) if observed else None,
            "tables_ready": len(ready_tables),
            "tables_total": len(tables),
            "tables": tables,
            "source_url": self.source_url,
            "last_writer": latest_writer,
            "target_id": self.info.get("target_id"),
            "target_started_at": self.started_at,
            "now": now_iso(),
        }

    # -- gate -----------------------------------------------------------------
    def evaluate_gate(
        self,
        table: str,
        uses: list[str] | None = None,
        snapshot: int | None = None,
        scope: str = SCOPE_FULL,
    ) -> dict[str, Any]:
        """求值一次查询的依赖闭包。

        返回 ``{decision, missing, consistency, manifest}``：
        - decision = "serve" | "refuse" | "unavailable"
        - missing 中每项带 ``kind`` / ``required``。
        """
        if self.manifests.get(table) is None:
            return {
                "decision": "unavailable",
                "table": table,
                "missing": [{"kind": "manifest", "required": True, "detail": "table not synced"}],
                "consistency": None,
            }
        manifest = self.manifest_for(table, snapshot)
        generation = int(manifest["generation"])
        problems: list[dict[str, Any]] = []
        state = self.table_state(table, snapshot)

        if not state["schema"]["ready"]:
            problems.append(
                {
                    "kind": "schema",
                    "required": True,
                    "detail": f"schema {state['schema']['schema_id']} not registered",
                }
            )
        if not state["cols"]["ready"]:
            problems.append(
                {"kind": "cols", "required": True, "detail": "column metadata not registered"}
            )
        dict_entry = manifest.get("dictionary")
        needs_dict = True
        if uses is not None:
            needs_dict = any(u in ("dict", "dictionary", "codes", "enum") for u in uses)
        if dict_entry and needs_dict and not (state["dictionary"] or {}).get("ready"):
            problems.append(
                {
                    "kind": "dictionary",
                    "required": True,
                    "detail": f"dictionary {dict_entry['version']} not fully registered",
                    "version": dict_entry["version"],
                }
            )
        not_ready = [s["shard"] for s in state["shards"] if not s["ready"]]
        if not_ready:
            problems.append(
                {
                    "kind": "shards",
                    "required": True,
                    "detail": f"shards not ready: {', '.join(not_ready)}",
                    "shards": not_ready,
                }
            )
        missing_incr = [i["incr"] for i in state["increments"] if not i["ready"]]
        base_missing: dict[str, list[str]] = {}
        for inc in state["increments"]:
            if not inc["requires_ready"]:
                base_missing[inc["incr"]] = [
                    r
                    for r in inc["requires"]
                    if not any(s["shard"] == r and s["ready"] for s in state["shards"])
                ]
        if missing_incr:
            problems.append(
                {
                    "kind": "increments",
                    "required": True,
                    "detail": f"increments not ready: {', '.join(missing_incr)}",
                    "increments": missing_incr,
                }
            )
        if base_missing:
            problems.append(
                {
                    "kind": "increment_base",
                    "required": True,
                    "detail": "increment base shards missing: "
                    + ", ".join(f"{k}<-{','.join(v)}" for k, v in base_missing.items()),
                    "bases": base_missing,
                }
            )
        epochs = [i["epoch"] for i in state["increments"]]
        if any(b <= a for a, b in zip(epochs, epochs[1:])):
            problems.append(
                {
                    "kind": "increment_chain",
                    "required": True,
                    "detail": "increment epochs not strictly increasing",
                    "epochs": epochs,
                }
            )
        consistency = {
            "read_mode": "snapshot" if snapshot is not None else "latest",
            "scope": scope,
            "generation": generation,
            "manifest": f"gen-{generation:06d}",
            "epoch": int(manifest["epoch"]),
            "shard_epoch": int(manifest["shard_epoch"]),
            "increment_epochs": epochs,
            "dictionary_version": (dict_entry or {}).get("version"),
            "snapshot": snapshot,
            "boundary": (
                "single-generation: 所有依赖来自同一份清单，查询之间可直接比较"
                if scope == SCOPE_FULL
                else "per-table: 每张表使用自身清单；跨表查询需固定 generation"
            ),
        }
        return {
            "decision": "serve" if not problems else "refuse",
            "table": table,
            "missing": problems,
            "consistency": consistency,
            "manifest": manifest,
        }

    # -- query ----------------------------------------------------------------
    def query(
        self,
        table: str,
        limit: int = 20,
        uses: list[str] | None = None,
        snapshot: int | None = None,
        scope: str = SCOPE_FULL,
        distinct_column: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        gate = self.evaluate_gate(table, uses=uses, snapshot=snapshot, scope=scope)
        if gate["decision"] == "unavailable":
            raise ApiError(
                503,
                "query_unavailable",
                f"table {table!r} is not known on this node",
                missing=gate["missing"],
            )
        if gate["decision"] == "refuse":
            raise ApiError(
                409,
                "query_gate_refused",
                f"query on {table!r} refused: {', '.join(p['kind'] for p in gate['missing'])}",
                missing=gate["missing"],
                consistency=gate["consistency"],
            )
        manifest = gate["manifest"]
        rows = self._materialize(table, manifest)
        columns = [c["name"] for c in manifest["schema"]["columns"]]
        pk = manifest["schema"]["primary_key"]
        rowset = [rows[k] for k in sorted(rows)]
        if distinct_column is not None:
            if distinct_column not in columns:
                raise ApiError(400, "bad_column", f"unknown column {distinct_column!r}")
            idx = columns.index(distinct_column)
            data = sorted({r[idx] for r in rowset}, key=str)
        else:
            data = rowset[: max(0, limit)]
        result = {
            "ok": True,
            "table": table,
            "columns": columns,
            "primary_key": pk,
            "count": len(rowset),
            "digest": rows_digest(rowset),
            "data": data,
            "consistency": gate["consistency"],
            "gate": {"decision": "serve", "missing": []},
        }
        if distinct_column is not None:
            result["distinct_column"] = distinct_column
        return 200, result

    def _materialize(self, table: str, manifest: dict) -> dict[int, list[Any]]:
        """按清单把分片 + 增量求值成行集合。"""
        columns = [c["name"] for c in manifest["schema"]["columns"]]
        pk = manifest["schema"]["primary_key"]
        pk_index = columns.index(pk)
        rows: dict[int, list[Any]] = {}
        for shard in manifest["shards"]:
            obj = self.ledger.get_object(table, "shard", shard["shard"], int(shard["epoch"]))
            if obj is None or obj["sha256"] != shard["sha256"]:
                raise ApiError(
                    409,
                    "query_gate_refused",
                    f"shard {shard['shard']} missing at query time",
                    missing=[{"kind": "shards", "required": True, "shards": [shard["shard"]]}],
                )
            path = self.root / obj["path"]
            if not path.is_file():
                raise ApiError(
                    409,
                    "query_gate_refused",
                    f"shard file gone: {path.name}",
                    missing=[{"kind": "shards", "required": True, "shards": [shard["shard"]]}],
                )
            if sha256_file(path) != obj["sha256"]:
                # 文件被外部篡改/截断：门禁重新校验，绝不以损坏字节回答查询。
                raise ApiError(
                    409,
                    "query_gate_refused",
                    f"shard {shard['shard']} failed checksum at query time; recopy required",
                    missing=[
                        {
                            "kind": "shards",
                            "required": True,
                            "shards": [shard["shard"]],
                            "detail": "checksum mismatch",
                        }
                    ],
                )
            try:
                data = read_shard(path)
            except sqlite3.DatabaseError as exc:
                raise ApiError(
                    409,
                    "query_gate_refused",
                    f"shard {shard['shard']} unreadable: {exc}",
                    missing=[{"kind": "shards", "required": True, "shards": [shard["shard"]]}],
                )
            data = read_shard(path)
            by_name = {c.name: i for i, c in enumerate(data.columns)}
            for raw in data.rows:
                row = [raw[by_name[c]] for c in columns]
                rows[int(row[pk_index])] = row
        incrs = []
        for inc in manifest["increments"]:
            obj = self.ledger.get_object(table, "incr", inc["incr"], int(inc["epoch"]))
            if obj is None or obj["sha256"] != inc["sha256"]:
                raise ApiError(
                    409,
                    "query_gate_refused",
                    f"increment {inc['incr']} missing at query time",
                    missing=[
                        {"kind": "increments", "required": True, "increments": [inc["incr"]]}
                    ],
                )
            incrs.append(read_increment(self.root / obj["path"]))
        return apply_chain(rows, incrs, pk_index)

    # -- verification ---------------------------------------------------------
    def verify(
        self, table: str, snapshot: int | None = None, scope: str = SCOPE_FULL
    ) -> dict[str, Any]:
        """按旧快照核对目标与来源的行集合（逐 key 对比）。"""
        if self.client is None:
            raise ApiError(400, "no_source", "target has no source_url configured")
        manifest = self.manifest_for(table, snapshot)
        generation = int(manifest["generation"])
        gate = self.evaluate_gate(table, uses=None, snapshot=snapshot, scope=scope)
        if gate["decision"] != "serve":
            return {
                "ok": False,
                "table": table,
                "generation": generation,
                "error": "gate_not_serving",
                "missing": gate["missing"],
                "consistency": gate["consistency"],
            }
        local = self._materialize(table, manifest)
        columns = [c["name"] for c in manifest["schema"]["columns"]]
        pk_index = columns.index(manifest["schema"]["primary_key"])
        local_rows = [local[k] for k in sorted(local)]
        status, remote = self.client.get_json(
            f"/api/expected?table={table}&generation={generation}"
        )
        if status != 200:
            raise ApiError(502, "source_unavailable", f"source expected -> {status}")
        remote_rows = remote.get("rowset") or []
        remote_map = {int(r[pk_index]): list(r) for r in remote_rows}
        local_map = {int(r[pk_index]): r for r in local_rows}
        missing = sorted(set(remote_map) - set(local_map))
        extra = sorted(set(local_map) - set(remote_map))
        changed = sorted(
            k for k in set(remote_map) & set(local_map) if remote_map[k] != local_map[k]
        )
        match = not missing and not extra and not changed
        delta = None
        if not match:
            delta = {
                "missing_keys": missing[:50],
                "extra_keys": extra[:50],
                "changed_keys": changed[:50],
                "missing_count": len(missing),
                "extra_count": len(extra),
                "changed_count": len(changed),
            }
            if missing:
                delta["missing_sample"] = [remote_map[k] for k in missing[:5]]
            if changed:
                delta["changed_sample"] = [
                    {"key": k, "source": remote_map[k], "target": local_map[k]}
                    for k in changed[:5]
                ]
        return {
            "ok": True,
            "table": table,
            "generation": generation,
            "epoch": int(manifest["epoch"]),
            "shard_epoch": int(manifest["shard_epoch"]),
            "read_mode": "snapshot" if snapshot is not None else "latest",
            "snapshot": snapshot,
            "scope": scope,
            "source_rows": remote.get("count"),
            "target_rows": len(local_rows),
            "source_digest": remote.get("digest"),
            "target_digest": rows_digest(local_rows),
            "match": match,
            "delta": delta,
            "consistency": gate["consistency"],
        }

    def verify_all(self, snapshot: int | None = None, scope: str = SCOPE_FULL) -> dict[str, Any]:
        results = []
        for table in sorted(self.manifests):
            results.append(self.verify(table, snapshot=snapshot, scope=scope))
        return {
            "ok": True,
            "match": all(r.get("match") for r in results) if results else False,
            "results": results,
            "now": now_iso(),
        }

    def snapshot_info(self) -> dict[str, Any]:
        hist = self.ledger.data.get("history", [])
        return {
            "ok": True,
            "generations": [int(h["generation"]) for h in hist],
            "latest": int(hist[-1]["generation"]) if hist else None,
            "retained_shard_epochs": sorted(
                {
                    int(o["epoch"])
                    for o in self.ledger.data["objects"].values()
                    if o["kind"] == "shard"
                }
            ),
        }


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------


def build_target_app(state: TargetState, log_path: Path | None = None) -> ApiServer:
    api = ApiServer("target")
    api.log_path = log_path

    @api.get(r"/health")
    def health(req: Request):
        return {"ok": True, "role": "target", "target_id": state.info["target_id"]}

    @api.get(r"/api/info")
    def info(req: Request):
        scope = req.q("scope", SCOPE_FULL) or SCOPE_FULL
        data = state.overview(scope=scope)
        data["source_url"] = state.source_url
        data["startup"] = state.startup
        data["transfers"] = [t.view() for t in state.transfers.values()]
        data["snapshots"] = state.snapshot_info()
        return data

    @api.get(r"/api/manifests")
    def manifests(req: Request):
        out = []
        for table in sorted(state.manifests):
            try:
                out.append(state.table_state(table))
            except ApiError as exc:
                out.append({"table": table, "error": exc.payload()})
        return {"ok": True, "manifests": out, "generation": state.ledger.data.get("generation", 0)}

    @api.post(r"/api/sync")
    def sync(req: Request):
        body = req.json()
        tables = body.get("tables")
        return state.sync_manifests(tables if isinstance(tables, list) else None)

    @api.post(r"/api/copy")
    def copy_(req: Request):
        body = req.json()
        tables = body.get("tables")
        include = body.get("include")
        tr = state.start_copy(
            tables=tables if isinstance(tables, list) else None,
            resume=bool(body.get("resume", True)),
            scope=str(body.get("scope", SCOPE_FULL)),
            abort_after=(int(body["abort_after"]) if body.get("abort_after") is not None else None),
            pace=float(body.get("pace", 0.0) or 0.0),
            background=bool(body.get("background", True)),
            transfer_id=body.get("transfer_id"),
            include=[str(k) for k in include] if isinstance(include, list) else None,
            allow_concurrent=bool(body.get("allow_concurrent", False)),
            pinned_generation=(
                int(body["generation"]) if body.get("generation") is not None else None
            ),
            crash_after=(int(body["crash_after"]) if body.get("crash_after") is not None else None),
        )
        if not body.get("background", True):
            tr = state.wait(tr.id, timeout=float(body.get("wait_timeout", 120)))
        return {"ok": tr.status in (READY, RUNNING, STALE), "transfer": tr.view()}

    @api.get(r"/api/transfers")
    def transfers(req: Request):
        return {"ok": True, "transfers": [t.view() for t in state.transfers.values()]}

    @api.get(r"/api/transfers/(?P<tid>[^/]+)")
    def transfer(req: Request, tid: str):
        tr = state.transfers.get(tid)
        if tr is None:
            raise ApiError(404, "unknown_transfer", f"no transfer {tid!r}")
        return {"ok": True, "transfer": tr.view()}

    @api.post(r"/api/transfers/(?P<tid>[^/]+)/interrupt")
    def interrupt(req: Request, tid: str):
        return state.interrupt(tid)

    @api.post(r"/api/transfers/(?P<tid>[^/]+)/resume")
    def resume(req: Request, tid: str):
        body = req.json()
        abort_after = body.get("abort_after", _UNSET)
        return state.resume(
            tid,
            pace=(float(body["pace"]) if body.get("pace") is not None else None),
            abort_after=(int(abort_after) if abort_after not in (_UNSET, None) else abort_after),
        )

    @api.get(r"/api/gate")
    def gate(req: Request):
        table = req.q("table")
        uses = req.qlist("uses", None) if req.q("uses") is not None else None
        snap = req.q("snapshot")
        scope = req.q("scope", SCOPE_FULL) or SCOPE_FULL
        if not table:
            return state.overview(scope=scope)
        result = state.evaluate_gate(
            table,
            uses=uses,
            snapshot=int(snap) if snap else None,
            scope=scope,
        )
        result.pop("manifest", None)
        return result

    @api.post(r"/api/query")
    def query(req: Request):
        body = req.json()
        table = body.get("table")
        if not table:
            raise ApiError(400, "missing_table", "table is required")
        uses = body.get("uses")
        snapshot = body.get("snapshot")
        scope = str(body.get("scope", SCOPE_FULL))
        return state.query(
            str(table),
            limit=int(body.get("limit", 20)),
            uses=[str(u) for u in uses] if isinstance(uses, list) else None,
            snapshot=int(snapshot) if snapshot is not None else None,
            scope=scope,
            distinct_column=body.get("distinct"),
        )

    @api.get(r"/api/verify")
    def verify(req: Request):
        table = req.q("table")
        snap = req.q("snapshot")
        scope = req.q("scope", SCOPE_FULL) or SCOPE_FULL
        generation = int(snap) if snap else None
        if not table:
            return state.verify_all(snapshot=generation, scope=scope)
        return state.verify(table, snapshot=generation, scope=scope)

    @api.get(r"/api/snapshots")
    def snapshots(req: Request):
        return state.snapshot_info()

    @api.post(r"/api/config")
    def config(req: Request):
        body = req.json()
        if body.get("source_url"):
            state.set_source(str(body["source_url"]))
        return {"ok": True, "source_url": state.source_url}

    @api.get(r"/api/events", kind="sse")
    def events(req: Request):
        def gen():
            while not api.stopped:
                with state.lock:
                    payload = {
                        "at": now_iso(),
                        "overview": {
                            "available": state.overview().get("available"),
                            "tables_ready": state.overview().get("tables_ready"),
                            "tables_total": state.overview().get("tables_total"),
                        },
                        "transfers": [t.view() for t in state.transfers.values()],
                    }
                yield payload
                time.sleep(0.4)

        return gen()

    @api.get(r"/", kind="html")
    def index(req: Request):
        candidates = [Path(__file__).resolve().parent.parent / "web" / "index.html"]
        if req.server and req.server.log_path:
            candidates.insert(0, Path(req.server.log_path).parent / "web" / "index.html")
        for candidate in candidates:
            if candidate.is_file():
                return candidate.read_text(encoding="utf-8")
        return "<h1>pointwise-repl</h1><p>web/index.html missing</p>"

    return api
