# 本地分析库跨节点复制 —— 设计与一致性说明

本文描述 `pointwise-repl` 的实现取舍。项目包含两个本地服务（`source` / `target`）、
复制清单、查询门禁和网页控制台，可真实中断传输并从断点恢复，并带“按旧快照读取”的核对结果。

## 1. 数据模型

一个分析库由若干**表**组成，每张表由若干**分片（shard）**组成。分片是复制与校验的最小单位。
每个分片文件是一个只读的 SQLite 数据库，内含：

| 表 | 用途 |
| --- | --- |
| `__pointwise_shard_meta` | 分片自身的身份：`dataset` / `table` / `shard` / `schema_id` / `epoch` / `lo` / `hi` |
| 业务表（表名可变） | 真实行数据，带主键 `id` |
| `__pointwise_rows__` | 该分片所有业务行的键集合，用于 `R = rowset(dataset)` |
| `__pointwise_cols__` | 快照列定义（local-scope 状态的一部分） |

此外还有两类 **local-scope** 元数据（不是分布数据，不参与一致性校验，仅本节点生效）：

- **schema**：每张表的结构指纹，`schema_id = sha256(规范化的列定义)`。
- **dictionary**：枚举/码表（code → label），与 schema 一样按 `(table, version)` 存储。
  查询如果引用某个码表，必须确认该版本的字典**已完整到达目标**。

增量（increment）文件也是 SQLite，描述一次变更集：

- 记录**基准分片**（必须先在目标上就绪）和**基准 epoch**；
- upsert / delete 的键集合（`__pointwise_incr__`）；
- 业务表 `__ins_<table>` / `__del_<table>`。

## 2. 复制清单（manifest）

来源端为每个**表**生成一个清单：

```json
{
  "dataset": "shop",
  "table": "orders",
  "schema": {"schema_id": "...", "columns": ["id", "customer_id", ...]},
  "dictionary": {"schema_id": "...", "version": "v1", "entries": 3},
  "shards":  [{"shard": "s00", "epoch": 1, "sha256": "...", "bytes": 8192, "rows": 12}, ...],
  "increments": [
    {"incr": "inc-01", "epoch": 1, "base_epoch": 1, "requires": ["s00", "s01"],
     "sha256": "...", "bytes": 4096, "upserts": 2, "deletes": 1}
  ],
  "cols": [["orders", "id", "id"], ["orders", "amount", "amount"], ...],
  "source": {"generated_at": "...", "source_id": "..."}
}
```

清单只描述“这一版库长什么样”，不代表目标已经拥有其中任何一个字节。
目标按清单逐项拉取，并把每项的真实校验和记录进自己的 **ledger**。

## 3. 门禁（gate）：什么时候“可用”

**可用 ≠ 文件到齐。** 查询门禁对每次查询重新评估清单依赖闭包（而不是看目录里有没有文件）：

| 依赖 | 满足条件 |
| --- | --- |
| 表结构 | 目标上存在**同一 `schema_id`** 的 schema 记录（由清单逐字节校验后登记） |
| 列元数据 | 清单中该表的 `cols` 已登记且引用同一 `schema_id` |
| 字典 | 若清单声明了字典，目标上该 `(table, version)` 字典**完整**（条目数与校验和一致） |
| 基准分片 | 增量 `incr` 的 `requires` 分片全部在目标上**就绪**且属于同一 epoch |
| 增量文件 | 增量文件本身已到，且所有更早 epoch 的增量都已到（链式完整性） |
| 快照读取 | 若查询按旧快照读（`snapshot=<epoch>`），必须存在覆盖该 epoch 的分片+增量链 |

门禁三种决定：

1. **serve**：闭包全部满足。返回 `{ok:true, data}`，并带 `consistency` 信息。
2. **refuse**：闭包**部分**满足，但缺的是结构/字典/基准分片/增量链这类 `required` 项。
   返回 HTTP 409 `{ok:false, error:"query_gate_refused", missing:[...]}`。
   **绝不返回空结果或部分结果冒充完整结果。**
3. **unavailable**：闭包完全不满足（例如表还没登记）。返回 HTTP 503。

有一个容易踩的坑：结构已到、字典未到。如果查询 SELECT 了字典里的枚举列，门禁会
检查字典并拒绝；如果查询不引用字典，则正常服务。字典是否“被引用”由查询的
`uses` 参数声明（默认为表声明的全部字典），也可以传 `uses=[]` 显式声明不依赖。

增量引用基准分片还没到时同理：整条增量链视为不可用，较早 epoch 已可用的部分
**不会**被单独投射成结果，因为那会把“旧状态”冒充成“当前状态”。

## 4. 一致性边界

`consistency` 字段对每个响应写明本次读取实际落在什么边界上：

- `read_mode: "latest"`：在**同一份清单**下求值（分片 + 增量链全部到齐）。
  这是一份单调的、与来源生成时刻对齐的快照；多个查询在同一个 `manifest_generation`
  下读到同一份数据。
- `read_mode: "snapshot"`：读取旧 epoch 的分片并以该 epoch 之前的增量补齐。
  仅当旧 manifest 的全部依赖仍然保留时可行。写入新 epoch 后，旧分片会被**标记为
  回收**（`retired`）而不是立即删除，以保证按旧快照读仍可校验。

页面上展示的 `可用时间` 与 `一致性边界`：

- **整库就绪后开放**（`full-gate`）：可用时刻 = 所有表所有依赖到齐的 max；
  一致性边界 = 单一 generation，最严格，查询之间可直接比较。
- **按依赖逐表开放**（`per-table`）：每张表在自身闭包满足时即可用，可用时刻更早；
  边界变为 per-table generation，跨表 join 可能跨越不同 generation，需要显式固定
  `manifest` 才能对齐。

请求可带 `?scope=per-table`（快速开放）或默认整库。响应总是报告实际使用的 scope。

## 5. 中断与恢复

- **真实中断**：传输走 HTTP Range 断点续传，目标把已收到的部分写成 `<name>.part` +
  `<name>.part.meta`（包含 URL、总长度、清单摘要）。`POST /api/transfers/{id}/interrupt`
  会让进行中的复制线程在一次 1 MiB 读之后停住，保留 `.part`；之后可 `resume`。
- **重启**：`ledger` 与 `.part` 都在磁盘上，目标重启后 `bootstrap` 会扫描未完成的
  transfer 并标记为 `interrupted`，`POST /api/transfers/{id}/resume`（或 `POST /api/copy`
  的自动恢复）继续。已完成项不会重复下载（checksum 校验后直接登记）。
- **来源压缩合并（compaction）**：合并会生成新 epoch 的分片，并**重写增量链的
  `base_epoch`**。目标在提交每个增量前再次校验 `base_epoch` 与 `requires`；
  若来源在复制途中压缩合并，旧增量被声明为过期，目标丢弃该增量并重新拉取清单。
  这样不会把基于旧分片的增量套到新分片上，产生看似成功、实则错位的数据。
- **目标重启** / **两个复制任务抢先完成**：所有提交都经过 `fence` 文件锁
  （`fcntl.flock`，必要时 `msvcrt.locking`）串行化；提交时比较
  `(epoch, manifest_generation)`。分片/增量的写入路径命名包含 epoch 与 checksum，
  **旧任务的迟到提交无法覆盖新进度**：若 ledger 中已有更高 epoch 的同一分片，
  该任务被判为 `stale` 并放弃，不删除也不改写新分片。
- 每次提交后记录 `last_writer`（transfer id + epoch），页面可见谁赢了、谁被判 stale。

## 6. 校验与核对（旧快照）

- 每项下载完成后校验 `sha256`；校验失败则删除 `.part` 并标记失败，绝不登记。
- `POST /api/verify` 计算目标端指针内容：`schema_ids`、`shard_epochs`、`incr_chain`、
  目标行集合 `rowset_target`，与来源端 `GET /api/expected?manifest=...` 的
  `rowset_source` 比对，返回 `match / mismatches`。
- 该核对**读取的是同一个旧快照 epoch**，因此可以在来源已经写入新 epoch、目标尚未追平
  的窗口内，证明“目标上的旧快照与来源上的旧快照逐行一致”。
- 失败路径：删掉目标分片、或目标被中断在增量链中间，核对会报告缺哪些行（按 snapshot
  epoch 求值），网页显示为红。

## 7. 接口

来源（默认 127.0.0.1:18081）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/api/mutate` | 业务写入 + 生成增量；`compact_every` 达阈值自动压缩合并 |
| POST | `/api/compact` | 强制压缩合并 → 新 epoch |
| GET | `/api/manifests` | 所有表的清单摘要 |
| GET | `/api/manifest/{table}` | 单表完整清单（含 sha256） |
| GET | `/api/expected?manifest=<gen>&table=` | 按旧快照求值的来源行集合 |
| GET | `/data/{table}/{shard}?epoch=` | 分片文件（支持 Range） |
| GET | `/data/{table}/incr/{incr}` | 增量文件 |
| GET | `/data/{table}/schema` | 结构 + 字典归档 |

目标（默认 127.0.0.1:18082）：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 健康检查 |
| POST | `/api/sync` | 拉取来源清单（可指定 table） |
| POST | `/api/copy` | 执行复制（`resume=true` 自动续传） |
| POST | `/api/transfers/{id}/interrupt` / `resume` | 真实中断 / 恢复 |
| GET | `/api/manifests` | 目标上的清单与就绪情况 |
| GET | `/api/gate` | 当前可用性（按表、按 scope、缺什么） |
| POST | `/api/query` | 带门禁的 SQL 查询 |
| GET | `/api/verify` | 旧快照核对 |
| GET | `/api/events` | 事件流（SSE） |
| GET | `/` | 网页控制台 |

## 8. 测试

`pytest` 覆盖以下行为（详见 `tests/`）：

1. 结构已到、字典未到 → 引用字典的查询被拒绝（409），不引用字典的查询可服务。
2. 增量引用的基准分片未到 → 整条链不可用，查询被拒绝且不返回“看似完整”的空/旧结果。
3. 传输中断（真实 `.part` 保留）→ resume 后校验和与行数一致。
4. 目标重启 → 未完成 transfer 标为 interrupted，resume 后完成。
5. 来源压缩合并造成 epoch 前进 → 旧增量不会套到新分片上，目标自动重拉清单。
6. 两个复制任务抢先提交 → 只有更高 `(epoch, generation)` 的写入落盘，旧任务 stale。
7. 整库门禁 vs 逐表门禁的可用时间差异与一致性边界。
8. 旧快照核对：删除目标数据后 mismatch，恢复后 match。
