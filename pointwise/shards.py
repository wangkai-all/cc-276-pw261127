"""分片 / 增量文件的物理格式与读写。

一个分片 = 一个只读 SQLite 文件：

  - ``__pointwise_shard_meta``  (key TEXT PRIMARY KEY, value TEXT)
  - ``__pointwise_cols__``      (table_name, column_name, kind)  -- 列元数据
  - ``__pointwise_rows__``      (id)                             -- 行键集合 R
  - ``<business table>``        (真实列 + pk)

增量 = 一个只读 SQLite 文件：

  - ``__pointwise_incr_meta``   (key, value)  -- epoch / base_epoch / requires ...
  - ``__pointwise_incr__``      (table_name, op, id)  -- 变更键
  - ``__ins_<table>`` / ``__del_<table>``
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

from .dataset import Column, TableDef
from .common import fsync_dir

META = "__pointwise_shard_meta"
COLS = "__pointwise_cols__"
ROWS = "__pointwise_rows__"
INCR_META = "__pointwise_incr_meta"
INCR_KEYS = "__pointwise_incr__"


def shard_bounds(rows: Sequence[Sequence[Any]], pk_index: int, shards: int) -> list[tuple[int, int]]:
    if not rows:
        return [(0, -1) for _ in range(shards)]
    keys = [int(r[pk_index]) for r in rows]
    lo, hi = min(keys), max(keys)
    width = max(1, (hi - lo + shards) // shards)
    bounds = []
    for i in range(shards):
        start = lo + i * width
        end = hi + 1 if i == shards - 1 else lo + (i + 1) * width
        bounds.append((start, end - 1))
    return bounds


def shard_names(shards: int) -> list[str]:
    return [f"s{i:02d}" for i in range(shards)]


def _assign_rows(rows: Sequence[Sequence[Any]], pk_index: int, shards: int) -> list[list[list[Any]]]:
    buckets: list[list[list[Any]]] = [[] for _ in range(shards)]
    if not rows:
        return buckets
    keys = sorted(int(r[pk_index]) for r in rows)
    lo, hi = keys[0], keys[-1]
    span = max(1, hi - lo + 1)
    for r in rows:
        idx = min(shards - 1, int((int(r[pk_index]) - lo) * shards // span))
        buckets[idx].append(list(r))
    return buckets


def _escape(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _sql_type(t: str) -> str:
    t = t.upper()
    return t if t in ("INTEGER", "REAL", "TEXT", "BLOB", "NUMERIC") else "TEXT"


def write_shard(
    path: Path,
    dataset: str,
    table: TableDef,
    shard: str,
    epoch: int,
    rows: Sequence[Sequence[Any]],
    seq: int = 0,
    generation: int = 0,
    source_id: str = "",
) -> dict[str, Any]:
    """原子地写一个分片，返回它的元信息字典（不含 sha256）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    os.close(fd)
    try:
        conn = sqlite3.connect(tmp)
        try:
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute(f"CREATE TABLE {_escape(META)} (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute(
                f"CREATE TABLE {_escape(COLS)} (table_name TEXT, column_name TEXT, kind TEXT)"
            )
            conn.execute(f"CREATE TABLE {_escape(ROWS)} (id INTEGER PRIMARY KEY)")
            col_defs = ", ".join(
                f"{_escape(c.name)} {_sql_type(c.type)}"
                + (" PRIMARY KEY" if c.pk else "")
                for c in table.columns
            )
            conn.execute(f"CREATE TABLE {_escape(table.name)} ({col_defs})")
            placeholders = ",".join("?" for _ in table.columns)
            conn.executemany(
                f"INSERT INTO {_escape(table.name)} VALUES ({placeholders})", rows
            )
            conn.executemany(
                f"INSERT INTO {_escape(ROWS)} (id) VALUES (?)",
                [(int(r[table.pk_index]),) for r in rows],
            )
            conn.executemany(
                f"INSERT INTO {_escape(COLS)} VALUES (?,?,?)",
                [(table.name, c.name, "pk" if c.pk else "value") for c in table.columns],
            )
            # 列元数据跟随分片一起复制：这样“结构已到”可以在物理上被验证。
            meta = {
                "dataset": dataset,
                "table": table.name,
                "shard": shard,
                "epoch": epoch,
                "seq": seq,
                "generation": generation,
                "source_id": source_id,
                "schema_id": table.schema_id,
                "rows": len(rows),
                "columns_json": json.dumps(
                    [c.to_dict() for c in table.columns], ensure_ascii=False
                ),
                "primary_key": table.pk_name,
            }
            conn.executemany(
                f"INSERT INTO {_escape(META)} VALUES (?,?)",
                [(k, str(v)) for k, v in meta.items()],
            )
            conn.commit()
        finally:
            conn.close()
        os.replace(tmp, path)
        fsync_dir(path.parent)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return {
        "shard": shard,
        "epoch": epoch,
        "seq": seq,
        "rows": len(rows),
        "columns": [c.to_dict() for c in table.columns],
        "primary_key": table.pk_name,
    }


def write_increment(
    path: Path,
    dataset: str,
    table: TableDef,
    incr_id: str,
    epoch: int,
    base_epoch: int,
    requires: list[str],
    upserts: Sequence[Sequence[Any]],
    deletes: Sequence[int],
    seq: int = 0,
    generation: int = 0,
    source_id: str = "",
) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    os.close(fd)
    try:
        conn = sqlite3.connect(tmp)
        try:
            conn.execute("PRAGMA journal_mode=OFF")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute(f"CREATE TABLE {_escape(INCR_META)} (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute(
                f"CREATE TABLE {_escape(INCR_KEYS)} (table_name TEXT, op TEXT, id INTEGER)"
            )
            col_defs = ", ".join(
                f"{_escape(c.name)} {_sql_type(c.type)}" for c in table.columns
            )
            conn.execute(f"CREATE TABLE {_escape('ins_' + table.name)} ({col_defs})")
            conn.execute(f"CREATE TABLE {_escape('del_' + table.name)} (id INTEGER PRIMARY KEY)")
            placeholders = ",".join("?" for _ in table.columns)
            conn.executemany(
                f"INSERT INTO {_escape('ins_' + table.name)} VALUES ({placeholders})", upserts
            )
            conn.executemany(
                f"INSERT INTO {_escape('del_' + table.name)} (id) VALUES (?)",
                [(int(i),) for i in deletes],
            )
            conn.executemany(
                f"INSERT INTO {_escape(INCR_KEYS)} VALUES (?,?,?)",
                [(table.name, "upsert", int(r[table.pk_index])) for r in upserts]
                + [(table.name, "delete", int(i)) for i in deletes],
            )
            meta = {
                "dataset": dataset,
                "table": table.name,
                "incr": incr_id,
                "epoch": epoch,
                "base_epoch": base_epoch,
                "requires": json.dumps(requires),
                "seq": seq,
                "generation": generation,
                "source_id": source_id,
                "schema_id": table.schema_id,
                "upserts": len(upserts),
                "deletes": len(deletes),
                "primary_key": table.pk_name,
            }
            conn.executemany(
                f"INSERT INTO {_escape(INCR_META)} VALUES (?,?)",
                [(k, str(v)) for k, v in meta.items()],
            )
            conn.commit()
        finally:
            conn.close()
        os.replace(tmp, path)
        fsync_dir(path.parent)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return {
        "incr": incr_id,
        "epoch": epoch,
        "base_epoch": base_epoch,
        "requires": list(requires),
        "upserts": len(upserts),
        "deletes": len(deletes),
    }


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


@dataclass
class ShardData:
    path: Path
    meta: dict[str, Any]
    columns: list[Column]
    rows: list[list[Any]]


def read_shard(path: Path) -> ShardData:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        meta = {k: v for k, v in conn.execute(f"SELECT key, value FROM {_escape(META)}")}
        table = meta["table"]
        columns = [Column.from_dict(c) for c in json.loads(meta.get("columns_json", "[]"))]
        cols_row = list(
            conn.execute(f"SELECT table_name, column_name FROM {_escape(COLS)} ORDER BY rowid")
        )
        col_names = [c[1] for c in cols_row] or [c.name for c in columns]
        rows = [
            list(r)
            for r in conn.execute(
                f"SELECT {', '.join(_escape(c) for c in col_names)} FROM {_escape(table)}"
            )
        ]
    finally:
        conn.close()
    return ShardData(path=path, meta=meta, columns=columns, rows=rows)


@dataclass
class IncrementData:
    path: Path
    meta: dict[str, Any]
    upserts: list[list[Any]]
    deletes: list[int]
    columns: list[str]


def read_increment(path: Path) -> IncrementData:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        meta = {k: v for k, v in conn.execute(f"SELECT key, value FROM {_escape(INCR_META)}")}
        table = meta["table"]
        ins = f"ins_{table}"
        del_t = f"del_{table}"
        cur = conn.execute(f"SELECT * FROM {_escape(ins)}")
        cols = [d[0] for d in cur.description]
        upserts = [list(r) for r in cur.fetchall()]
        deletes = [int(r[0]) for r in conn.execute(f"SELECT id FROM {_escape(del_t)}")]
    finally:
        conn.close()
    return IncrementData(
        path=path, meta=meta, upserts=upserts, deletes=deletes, columns=cols
    )


def shard_meta_of(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            return {k: v for k, v in conn.execute(f"SELECT key, value FROM {_escape(META)}")}
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return None


def incr_meta_of(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            return {k: v for k, v in conn.execute(f"SELECT key, value FROM {_escape(INCR_META)}")}
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return None


def apply_chain(
    base_rows: dict[int, list[Any]],
    incrs: Iterable[IncrementData],
    pk_index: int,
) -> dict[int, list[Any]]:
    out = dict(base_rows)
    for inc in sorted(incrs, key=lambda i: int(i.meta["epoch"])):
        for row in inc.upserts:
            out[int(row[pk_index])] = list(row)
        for key in inc.deletes:
            out.pop(int(key), None)
    return out
