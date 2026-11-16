"""数据集定义：表、列、字典、初始行与分片规则。

数据集配置是纯 JSON，便于来源节点与测试共用：

```json
{
  "dataset": "shop",
  "tables": {
    "orders": {
      "columns": [{"name":"id","type":"INTEGER","pk":true}, ...],
      "dictionary": {"version":"v1","entries":[[1,"new"],[2,"paid"]]},
      "shards": 2,
      "rows": [[1, "alice", 2, 10, "..."], ...]
    }
  }
}
```
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from .common import canonical_json, sha256_bytes


@dataclass(frozen=True)
class Column:
    name: str
    type: str = "TEXT"
    pk: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "type": self.type, "pk": self.pk}

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "Column":
        return cls(
            name=str(obj["name"]),
            type=str(obj.get("type", "TEXT")),
            pk=bool(obj.get("pk", False)),
        )


@dataclass(frozen=True)
class Dictionary:
    version: str
    entries: tuple[tuple[Any, str], ...] = ()
    columns: tuple[str, ...] = ("code", "label")

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "columns": list(self.columns),
            "entries": [list(e) for e in self.entries],
        }

    @classmethod
    def from_dict(cls, obj: dict[str, Any] | None) -> "Dictionary | None":
        if not obj:
            return None
        return cls(
            version=str(obj["version"]),
            entries=tuple((e[0], str(e[1])) for e in obj.get("entries", [])),
            columns=tuple(obj.get("columns", ["code", "label"])),
        )


@dataclass
class TableDef:
    name: str
    columns: list[Column]
    shards: int = 2
    rows: list[list[Any]] = field(default_factory=list)
    dictionary: Dictionary | None = None

    def __post_init__(self) -> None:
        if not self.columns:
            raise ValueError(f"table {self.name!r} has no columns")
        if not any(c.pk for c in self.columns):
            raise ValueError(f"table {self.name!r} needs a primary key column")
        if self.shards < 1:
            raise ValueError("shards must be >= 1")
        pk_idx = [i for i, c in enumerate(self.columns) if c.pk]
        if len(pk_idx) != 1:
            raise ValueError(f"table {self.name!r} must have exactly one pk column")
        self.pk_index = pk_idx[0]
        width = len(self.columns)
        for r in self.rows:
            if len(r) != width:
                raise ValueError(
                    f"table {self.name!r}: row width {len(r)} != {width}: {r!r}"
                )

    @property
    def pk_name(self) -> str:
        return self.columns[self.pk_index].name

    def schema_payload(self) -> dict[str, Any]:
        return {
            "columns": [c.to_dict() for c in self.columns],
            "primary_key": self.pk_name,
        }

    @property
    def schema_id(self) -> str:
        return sha256_bytes(canonical_json(self.schema_payload()).encode("utf-8"))[:16]

    def schema_archive(self, dataset: str) -> dict[str, Any]:
        return {
            "dataset": dataset,
            "table": self.name,
            "schema_id": self.schema_id,
            **self.schema_payload(),
        }

    def dict_archive(self, dataset: str) -> dict[str, Any] | None:
        if self.dictionary is None:
            return None
        entries = [[code, label] for code, label in self.dictionary.entries]
        return {
            "dataset": dataset,
            "table": self.name,
            "version": self.dictionary.version,
            "columns": list(self.dictionary.columns),
            "entries": entries,
            "entries_sha256": sha256_bytes(canonical_json(entries).encode("utf-8")),
        }

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "columns": [c.to_dict() for c in self.columns],
            "shards": self.shards,
            "rows": self.rows,
        }
        if self.dictionary is not None:
            out["dictionary"] = self.dictionary.to_dict()
        return out

    @classmethod
    def from_dict(cls, name: str, obj: dict[str, Any]) -> "TableDef":
        return cls(
            name=name,
            columns=[Column.from_dict(c) for c in obj["columns"]],
            shards=int(obj.get("shards", 2)),
            rows=[list(r) for r in obj.get("rows", [])],
            dictionary=Dictionary.from_dict(obj.get("dictionary")),
        )


@dataclass
class Dataset:
    name: str
    tables: dict[str, TableDef]

    def to_dict(self) -> dict[str, Any]:
        return {"dataset": self.name, "tables": {k: v.to_dict() for k, v in self.tables.items()}}

    @classmethod
    def from_dict(cls, obj: dict[str, Any]) -> "Dataset":
        tables = {
            name: TableDef.from_dict(name, tobj)
            for name, tobj in obj.get("tables", {}).items()
        }
        if not tables:
            raise ValueError("dataset has no tables")
        return cls(name=str(obj.get("dataset", "dataset")), tables=tables)

    @classmethod
    def load(cls, path: str) -> "Dataset":
        with open(path, "r", encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))


def demo_dataset(orders_rows: int = 4000, payload_len: int = 96, shards: int = 2) -> Dataset:
    """确定性演示数据：customers + orders，orders 带 status 码表与 payload 列。

    payload 用来把分片文件做得足够大，以便演示“传输到一半被真实中断”。
    """
    statuses = [
        (1, "created"),
        (2, "paid"),
        (3, "shipped"),
        (4, "cancelled"),
    ]
    customers = TableDef(
        name="customers",
        columns=[
            Column("id", "INTEGER", pk=True),
            Column("name", "TEXT"),
            Column("tier", "TEXT"),
        ],
        shards=shards,
        dictionary=Dictionary("v1", ((1, "free"), (2, "plus"), (3, "enterprise"))),
        rows=[
            [i, f"customer-{i:05d}", ["free", "plus", "enterprise"][i % 3]]
            for i in range(1, 201)
        ],
    )
    orders = TableDef(
        name="orders",
        columns=[
            Column("id", "INTEGER", pk=True),
            Column("customer_id", "INTEGER"),
            Column("status", "INTEGER"),
            Column("amount", "INTEGER"),
            Column("payload", "TEXT"),
        ],
        shards=shards,
        dictionary=Dictionary("v1", tuple(statuses)),
        rows=[
            [
                i,
                (i % 200) + 1,
                (i % 4) + 1,
                (i * 37) % 1000,
                (f"payload-{i:06d}-" + "x" * max(0, payload_len - 15)),
            ]
            for i in range(1, orders_rows + 1)
        ],
    )
    return Dataset("shop", {"customers": customers, "orders": orders})
