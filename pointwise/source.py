"""来源节点：业务写入、压缩合并、分片/增量/清单发布、Range 下载。

来源是“真相”的一端，但它也保留历史快照：每次发布都把完整清单写入
``bundles/gen-XXXXXX.json``，旧分片与旧增量文件不删除，因此目标可以在来源
已经前进之后，仍然按旧 generation 求值并核对。
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Iterable

from .common import (
    new_id,
    now_iso,
    read_json,
    sha256_file,
    rows_digest,
    write_json_atomic,
)
from .dataset import Dataset, TableDef
from .httpmini import ApiError, ApiServer, Request, file_response
from .shards import (
    _assign_rows,
    read_increment,
    read_shard,
    shard_names,
    write_increment,
    write_shard,
)

DEFAULT_AUTO_COMPACT = 6
MAX_HISTORY = 200


class SourceState:
    def __init__(self, root: str | Path, dataset: Dataset) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.dataset = dataset
        self.lock = threading.RLock()
        self.info_path = self.root / "source.json"
        self.runtime_path = self.root / "runtime.json"
        self.bundles_dir = self.root / "bundles"
        self.expected_cache: dict[tuple[int, str], dict[str, Any]] = {}

        info = read_json(self.info_path)
        if info is None:
            info = {
                "source_id": new_id("src"),
                "dataset": dataset.name,
                "created_at": now_iso(),
                "auto_compact_every": DEFAULT_AUTO_COMPACT,
            }
            write_json_atomic(self.info_path, info)
        self.info = info

        runtime = read_json(self.runtime_path)
        if runtime is None:
            runtime = {
                "generation": 0,
                "epoch": 0,
                "base_epoch": 0,
                "tables": {
                    name: {
                        "rows": {str(int(r[t.pk_index])): list(r) for r in t.rows},
                        "increments": [],
                        "shards": [],
                    }
                    for name, t in dataset.tables.items()
                },
            }
            write_json_atomic(self.runtime_path, runtime)
        self.runtime = runtime
        for key, default in (("generation", 0), ("epoch", 0), ("base_epoch", 0)):
            self.runtime.setdefault(key, default)
        for tstate in self.runtime["tables"].values():
            tstate.setdefault("increments", [])
            tstate.setdefault("shards", [])
            tstate.setdefault("rows", {})
        if not self.runtime["tables"]:
            raise ValueError("runtime has no tables")

    # -- persistence --------------------------------------------------------
    def _save(self) -> None:
        write_json_atomic(self.runtime_path, self.runtime)

    @property
    def auto_compact_every(self) -> int:
        return int(self.info.get("auto_compact_every", DEFAULT_AUTO_COMPACT))

    # -- publishing ---------------------------------------------------------
    def _next_generation(self) -> int:
        self.runtime["generation"] = int(self.runtime["generation"]) + 1
        return int(self.runtime["generation"])

    def publish(
        self,
        tables: Iterable[str] | None = None,
        note: str = "",
        changed: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """发布一个新 generation 的完整 bundle（所有表的清单）。

        所有表共用同一个 generation；在某张表未变化时，其清单引用的分片/增量
        不变，目标端会因此跳过重复下载。
        """
        with self.lock:
            gen = self._next_generation()
            epoch = int(self.runtime["epoch"])
            base_epoch = int(self.runtime["base_epoch"])
            manifests = []
            for table in self.dataset.tables:
                manifests.append(self._manifest(table, gen))
            bundle = {
                "dataset": self.dataset.name,
                "source_id": self.info["source_id"],
                "generation": gen,
                "epoch": epoch,
                "base_epoch": base_epoch,
                "created_at": now_iso(),
                "note": note,
                "changed": changed or {},
                "tables": manifests,
            }
            path = self.bundles_dir / f"gen-{gen:06d}.json"
            write_json_atomic(path, bundle)
            write_json_atomic(self.bundles_dir / "latest.json", {"generation": gen})
            self._save()
            self._prune_history()
            return {
                "generation": gen,
                "epoch": epoch,
                "base_epoch": base_epoch,
                "bundle_path": str(path),
            }

    def _prune_history(self) -> None:
        gens = sorted(
            int(p.stem.split("-")[1])
            for p in self.bundles_dir.glob("gen-*.json")
        )
        for gen in gens[:-MAX_HISTORY]:
            (self.bundles_dir / f"gen-{gen:06d}.json").unlink(missing_ok=True)

    def _cols(self, table: TableDef) -> list[list[str]]:
        return [[table.name, c.name, "pk" if c.pk else "value"] for c in table.columns]

    def _manifest(self, table_name: str, generation: int) -> dict[str, Any]:
        table = self.dataset.tables[table_name]
        tstate = self.runtime["tables"][table_name]
        increments = []
        for inc in tstate["increments"]:
            increments.append(
                {
                    "incr": inc["incr"],
                    "epoch": inc["epoch"],
                    "base_epoch": inc["base_epoch"],
                    "requires": list(inc["requires"]),
                    "upserts": inc["upserts"],
                    "deletes": inc["deletes"],
                    "sha256": inc["sha256"],
                    "bytes": inc["bytes"],
                    "url": inc["url"],
                    "path": inc["path"],
                }
            )
        last_epoch = increments[-1]["epoch"] if increments else int(self.runtime["base_epoch"])
        schema = table.schema_archive(self.dataset.name)
        schema_path = self.root / "data" / table_name / "schema" / f"{table.schema_id}.json"
        if not schema_path.exists():
            write_json_atomic(schema_path, schema)
        schema_sha = sha256_file(schema_path)
        dict_archive = table.dict_archive(self.dataset.name)
        dict_entry = None
        if dict_archive is not None:
            dict_path = (
                self.root / "data" / table_name / "dict" / f"{table.dictionary.version}.json"
            )
            if not dict_path.exists():
                write_json_atomic(dict_path, dict_archive)
            dict_entry = {
                "version": table.dictionary.version,
                "entries": len(dict_archive["entries"]),
                "entries_sha256": dict_archive["entries_sha256"],
                "sha256": sha256_file(dict_path),
                "bytes": dict_path.stat().st_size,
                "url": f"/data/dict/{table_name}/{table.dictionary.version}",
                "path": str(dict_path.relative_to(self.root)),
            }
        return {
            "dataset": self.dataset.name,
            "table": table_name,
            "generation": generation,
            "epoch": last_epoch,
            "shard_epoch": int(self.runtime["base_epoch"]),
            "schema": {
                "schema_id": table.schema_id,
                "columns": [c.to_dict() for c in table.columns],
                "primary_key": table.pk_name,
                "sha256": schema_sha,
                "bytes": schema_path.stat().st_size,
                "url": f"/data/schema/{table_name}/{table.schema_id}",
                "path": str(schema_path.relative_to(self.root)),
            },
            "cols": self._cols(table),
            "dictionary": dict_entry,
            "shards": [
                {
                    "shard": s["shard"],
                    "epoch": s["epoch"],
                    "rows": s["rows"],
                    "sha256": s["sha256"],
                    "bytes": s["bytes"],
                    "url": s["url"],
                    "path": s["path"],
                }
                for s in tstate["shards"]
            ],
            "increments": increments,
            "source": {"source_id": self.info["source_id"], "generated_at": now_iso()},
        }

    # -- mutations ----------------------------------------------------------
    def mutate(
        self, table_name: str, upserts: list[list[Any]], deletes: list[Any]
    ) -> dict[str, Any]:
        if table_name not in self.dataset.tables:
            raise ApiError(400, "unknown_table", f"unknown table {table_name!r}")
        table = self.dataset.tables[table_name]
        if not isinstance(upserts, list) or not isinstance(deletes, list):
            raise ApiError(400, "bad_mutation", "upserts/deletes must be arrays")
        for row in upserts:
            if not isinstance(row, list) or len(row) != len(table.columns):
                raise ApiError(
                    400,
                    "bad_row",
                    f"row must have {len(table.columns)} values: {row!r}",
                )
        with self.lock:
            tstate = self.runtime["tables"][table_name]
            epoch = int(self.runtime["epoch"]) + 1
            self.runtime["epoch"] = epoch
            base_epoch = int(self.runtime["base_epoch"])
            requires = [s["shard"] for s in tstate["shards"]] or shard_names(table.shards)
            clean_upserts = [[_norm(v) for v in row] for row in upserts]
            clean_deletes = [int(v) for v in deletes]
            incr_id = f"inc-{epoch:06d}"
            rel = Path("data") / table_name / "incr" / f"{incr_id}.db"
            path = self.root / rel
            write_increment(
                path,
                self.dataset.name,
                table,
                incr_id,
                epoch,
                base_epoch,
                requires,
                clean_upserts,
                clean_deletes,
                seq=int(tstate["increments"][-1]["seq"]) + 1 if tstate["increments"] else 1,
                generation=int(self.runtime["generation"]) + 1,
                source_id=self.info["source_id"],
            )
            entry = {
                "incr": incr_id,
                "epoch": epoch,
                "base_epoch": base_epoch,
                "requires": requires,
                "upserts": len(clean_upserts),
                "deletes": len(clean_deletes),
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
                "url": f"/data/incr/{table_name}/{incr_id}",
                "path": str(rel),
                "seq": int(tstate["increments"][-1]["seq"]) + 1 if tstate["increments"] else 1,
            }
            for row in clean_upserts:
                tstate["rows"][str(int(row[table.pk_index]))] = row
            for key in clean_deletes:
                tstate["rows"].pop(str(key), None)
            tstate["increments"].append(entry)
            self._save()
            published = self.publish(
                note=f"mutate:{table_name}",
                changed={"table": table_name, "incr": incr_id},
            )
            result = {
                "ok": True,
                "table": table_name,
                "increment": entry,
                "publication": published,
                "rows": len(tstate["rows"]),
            }
            if len(tstate["increments"]) >= self.auto_compact_every:
                result["compaction"] = self.compact(reason="auto")
            return result

    def compact(self, reason: str = "manual") -> dict[str, Any]:
        """把当前逻辑状态写成新 epoch 的全量分片，并清空增量链。"""
        with self.lock:
            epoch = int(self.runtime["epoch"]) + 1
            self.runtime["epoch"] = epoch
            self.runtime["base_epoch"] = epoch
            for table_name, table in self.dataset.tables.items():
                tstate = self.runtime["tables"][table_name]
                rows = sorted(
                    tstate["rows"].values(), key=lambda r: int(r[table.pk_index])
                )
                buckets = _assign_rows(rows, table.pk_index, table.shards)
                names = shard_names(table.shards)
                shards = []
                for idx, shard in enumerate(names):
                    rel = (
                        Path("data")
                        / table_name
                        / "shards"
                        / f"epoch-{epoch:06d}"
                        / f"{shard}.db"
                    )
                    path = self.root / rel
                    meta = write_shard(
                        path,
                        self.dataset.name,
                        table,
                        shard,
                        epoch,
                        buckets[idx],
                        seq=idx,
                        generation=int(self.runtime["generation"]) + 1,
                        source_id=self.info["source_id"],
                    )
                    shards.append(
                        {
                            "shard": shard,
                            "epoch": epoch,
                            "rows": meta["rows"],
                            "sha256": sha256_file(path),
                            "bytes": path.stat().st_size,
                            "url": f"/data/shards/{table_name}/{epoch}/{shard}.db",
                            "path": str(rel),
                        }
                    )
                tstate["shards"] = shards
                tstate["increments"] = []
            self._save()
            published = self.publish(
                note=f"compact:{reason}", changed={"compact": reason, "epoch": epoch}
            )
            return {"epoch": epoch, **published, "reason": reason}

    # -- reads ---------------------------------------------------------------
    def bundle(self, generation: int | None = None) -> dict[str, Any]:
        gen = generation or int(self.runtime["generation"])
        path = self.bundles_dir / f"gen-{gen:06d}.json"
        data = read_json(path)
        if data is None:
            raise ApiError(404, "generation_not_found", f"generation {gen} not available")
        return data

    def bundle_generations(self) -> list[int]:
        return sorted(
            int(p.stem.split("-")[1]) for p in self.bundles_dir.glob("gen-*.json")
        )

    def latest_bundle(self) -> dict[str, Any]:
        return self.bundle(int(self.runtime["generation"]))

    def expected_rows(
        self, table_name: str, generation: int | None = None, verify_files: bool = True
    ) -> dict[str, Any]:
        """按指定 generation 的清单，从**分片 + 增量文件**重建行集合。

        不复用内存里的最新行：这正是“按旧快照读取”的关键。
        """
        bundle = self.bundle(generation)
        manifest = next(
            (m for m in bundle["tables"] if m["table"] == table_name), None
        )
        if manifest is None:
            raise ApiError(404, "unknown_table", f"table {table_name!r} not in generation")
        cache_key = (int(bundle["generation"]), table_name)
        with self.lock:
            cached = self.expected_cache.get(cache_key)
        if cached is not None:
            return cached
        table = self.dataset.tables[table_name]
        columns = [c["name"] for c in manifest["schema"]["columns"]]
        pk_index = columns.index(manifest["schema"]["primary_key"])
        rows: dict[int, list[Any]] = {}
        for shard in manifest["shards"]:
            path = self.root / shard["path"]
            if not path.is_file():
                raise ApiError(
                    410,
                    "snapshot_gone",
                    f"generation {bundle['generation']} shard {shard['shard']} no longer retained",
                )
            if verify_files:
                digest = sha256_file(path)
                if digest != shard["sha256"]:
                    raise ApiError(
                        500,
                        "source_corrupt",
                        f"shard {shard['shard']} digest mismatch at source",
                    )
            data = read_shard(path)
            by_name = {
                c.name: i for i, c in enumerate(data.columns)
            }
            if set(columns) - set(by_name):
                raise ApiError(500, "source_corrupt", "shard missing manifest columns")
            for raw in data.rows:
                row = [raw[by_name[c]] for c in columns]
                rows[int(row[pk_index])] = row
        for inc in manifest["increments"]:
            path = self.root / inc["path"]
            if not path.is_file():
                raise ApiError(
                    410,
                    "snapshot_gone",
                    f"generation {bundle['generation']} increment {inc['incr']} no longer retained",
                )
            data = read_increment(path)
            if int(data.meta.get("epoch", -1)) != int(inc["epoch"]):
                raise ApiError(500, "source_corrupt", "increment file epoch disagrees with manifest")
            if int(data.meta.get("base_epoch", -1)) != int(inc["base_epoch"]):
                raise ApiError(500, "source_corrupt", "increment base_epoch disagrees with manifest")
            by_name = {name: i for i, name in enumerate(data.columns)}
            for raw in data.upserts:
                row = [raw[by_name[c]] for c in columns]
                rows[int(row[pk_index])] = row
            for key in data.deletes:
                rows.pop(int(key), None)
        ordered = [rows[k] for k in sorted(rows)]
        result = {
            "ok": True,
            "table": table_name,
            "generation": int(bundle["generation"]),
            "epoch": manifest["epoch"],
            "shard_epoch": manifest["shard_epoch"],
            "columns": columns,
            "primary_key": manifest["schema"]["primary_key"],
            "count": len(ordered),
            "digest": rows_digest(ordered),
            "rowset": ordered,
            "increment_epochs": [i["epoch"] for i in manifest["increments"]],
            "source_id": self.info["source_id"],
        }
        with self.lock:
            if len(self.expected_cache) > 64:
                self.expected_cache.clear()
            self.expected_cache[cache_key] = result
        return result

    # -- info ----------------------------------------------------------------
    def summary(self) -> dict[str, Any]:
        tables = {}
        for name, table in self.dataset.tables.items():
            tstate = self.runtime["tables"][name]
            tables[name] = {
                "schema_id": table.schema_id,
                "rows": len(tstate["rows"]),
                "shards": len(tstate["shards"]),
                "shard_epoch": int(self.runtime["base_epoch"]),
                "increments": [i["incr"] for i in tstate["increments"]],
                "epoch": (
                    tstate["increments"][-1]["epoch"]
                    if tstate["increments"]
                    else int(self.runtime["base_epoch"])
                ),
                "dictionary": (
                    {"version": table.dictionary.version, "entries": len(table.dictionary.entries)}
                    if table.dictionary
                    else None
                ),
            }
        return {
            "ok": True,
            "source_id": self.info["source_id"],
            "dataset": self.dataset.name,
            "generation": int(self.runtime["generation"]),
            "epoch": int(self.runtime["epoch"]),
            "base_epoch": int(self.runtime["base_epoch"]),
            "auto_compact_every": self.auto_compact_every,
            "tables": tables,
            "generations": self.bundle_generations(),
            "now": now_iso(),
        }


def _norm(value: Any) -> Any:
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def build_source_app(state: SourceState, log_path: Path | None = None) -> ApiServer:
    api = ApiServer("source")
    api.log_path = log_path
    root = state.root

    @api.get(r"/health")
    def health(req: Request):
        return {"ok": True, "role": "source", "source_id": state.info["source_id"]}

    @api.get(r"/api/info")
    def info(req: Request):
        return state.summary()

    @api.get(r"/api/dataset")
    def dataset(req: Request):
        return {
            "ok": True,
            "dataset": state.dataset.name,
            "tables": {
                name: {
                    "columns": [c.to_dict() for c in t.columns],
                    "shards": t.shards,
                    "rows": len(t.rows),
                    "dictionary": t.dictionary.to_dict() if t.dictionary else None,
                }
                for name, t in state.dataset.tables.items()
            },
        }

    @api.get(r"/api/bundles")
    def bundles(req: Request):
        return {"ok": True, "generations": state.bundle_generations(), "latest": int(state.runtime["generation"])}

    @api.get(r"/api/bundle")
    def bundle(req: Request):
        gen = req.q("generation")
        return state.bundle(int(gen) if gen else None)

    @api.get(r"/api/expected")
    def expected(req: Request):
        table = req.q("table")
        if not table:
            raise ApiError(400, "missing_table", "table is required")
        gen = req.q("generation")
        data = state.expected_rows(table, int(gen) if gen else None)
        if not req.qb("rows", True):
            data = {k: v for k, v in data.items() if k != "rowset"}
        return data

    @api.post(r"/api/mutate")
    def mutate(req: Request):
        body = req.json()
        table = body.get("table")
        if not table:
            raise ApiError(400, "missing_table", "table is required")
        return state.mutate(table, body.get("upserts", []), body.get("deletes", []))

    @api.post(r"/api/compact")
    def compact(req: Request):
        body = req.json()
        return {
            "ok": True,
            "table_state": state.compact(reason=str(body.get("reason", "manual"))),
        }

    @api.get(r"/api/config")
    def config_get(req: Request):
        return {"ok": True, "auto_compact_every": state.auto_compact_every}

    @api.post(r"/api/config")
    def config_post(req: Request):
        body = req.json()
        if "auto_compact_every" in body:
            value = int(body["auto_compact_every"])
            if value < 1:
                raise ApiError(400, "bad_config", "auto_compact_every must be >= 1")
            state.info["auto_compact_every"] = value
            write_json_atomic(state.info_path, state.info)
        return {"ok": True, "auto_compact_every": state.auto_compact_every}

    @api.get(r"/api/rows")
    def rows(req: Request):
        """调试用：直接看某个 generation 的逻辑行。"""
        table = req.q("table")
        if not table:
            raise ApiError(400, "missing_table", "table is required")
        gen = req.q("generation")
        data = state.expected_rows(table, int(gen) if gen else None, verify_files=False)
        limit = int(req.q("limit", "20") or 20)
        return {
            "ok": True,
            "table": table,
            "generation": data["generation"],
            "count": data["count"],
            "digest": data["digest"],
            "rows": data["rowset"][:limit],
        }

    @api.get(r"/data/schema/(?P<table>[^/]+)/(?P<schema_id>[^/]+)", kind="file")
    def schema_file(req: Request, table: str, schema_id: str):
        path = root / "data" / table / "schema" / f"{schema_id}.json"
        return file_response(req, path)

    @api.get(r"/data/dict/(?P<table>[^/]+)/(?P<version>[^/]+)", kind="file")
    def dict_file(req: Request, table: str, version: str):
        path = root / "data" / table / "dict" / f"{version}.json"
        return file_response(req, path)

    @api.get(
        r"/data/shards/(?P<table>[^/]+)/(?P<epoch>\d+)/(?P<shard>[^/]+)\.db",
        kind="file",
    )
    def shard_file(req: Request, table: str, epoch: str, shard: str):
        path = root / "data" / table / "shards" / f"epoch-{int(epoch):06d}" / f"{shard}.db"
        return file_response(req, path)

    @api.get(r"/data/incr/(?P<table>[^/]+)/(?P<incr>[^/]+)", kind="file")
    def incr_file(req: Request, table: str, incr: str):
        path = root / "data" / table / "incr" / f"{incr}.db"
        return file_response(req, path)

    return api
