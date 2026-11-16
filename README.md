# pointwise-repl

把一个**本地分析库**从来源节点复制到目标节点：包含表结构、分片和增量；复制途中目标允许只读查询。
"可用"由**查询条件闭包**决定，而不是"文件到了多少"。

纯 Python 标准库实现，无第三方依赖：

```bash
# 一条命令起两个本地节点（来源 :18081，目标 :18082），种入演示数据并完成首次复制
python3 -m pointwise.cli demo --fresh

# 打开控制台
open http://127.0.0.1:18082/
```

或分别启动：

```bash
python3 -m pointwise.cli source --root .pointwise/source --port 18081
python3 -m pointwise.cli target --root .pointwise/target --port 18082 --source http://127.0.0.1:18081
```

## 它解决什么

| 风险 | 本项目的行为 |
| --- | --- |
| 结构已到、字典未到 | 查询若引用字典 → 409 `query_gate_refused`，`missing` 里列出 `dictionary`；不引用字典的查询可服务 |
| 增量引用的基准分片未到 | 整条增量链不可用 → 409，列出 `increments` / `increment_base`；**不**把旧状态冒充当前状态 |
| 只看了"文件数"就认为可用 | 门禁每次按清单闭包重新求值；文件被篡改/截断会被 checksum 拦下 |
| 传输中断只能假装失败 | Range 断点续传：`POST /api/copy {"abort_after": N}` 在**精确字节位**留下 `.part`，resume 从该处继续 |
| 目标重启 | 账本与 `.part` 落盘；重启后 RUNNING 任务标为 interrupted（`node_restart`），可 resume |
| 来源压缩合并 | 新 epoch 使旧增量的 `base_epoch` 过期；目标提交前校验，丢弃并以新清单重试 |
| 两个复制任务抢先完成 | 提交走文件锁 + `(epoch, generation)` 栅栏；旧任务判 `stale_*`，迟到旧副本不覆盖新进度 |
| 无法证明"目标上的旧数据是完整的" | `GET /api/verify?snapshot=<gen>` 在**旧快照**上逐 key 比对来源与目标 |

## 两种开放策略（页面可切换）

- **`full` 整库就绪后开放**：可用时间 = 所有表依赖到齐的 max；一致性边界是单一 generation，查询之间可直接比较；首次可用更晚。
- **`per-table` 按依赖逐表开放**：每张表在自身闭包满足时即可用，可用时间更早；边界是 per-table generation，跨表 join 需固定 `snapshot`。

控制台顶部会同时显示 `first_table_available_at` 与 `full_available_at`，以及当前 `scope` 的边界说明。
门禁接口 `GET /api/gate?table=orders&uses=dict&scope=per-table` 返回 `decision` + `missing` + `consistency`。

## 页面

`web/index.html`（由目标节点 `GET /` 提供）展示：

- 每张表的**依赖闭包**：结构 / 列元数据 / 字典 / 分片 / 增量文件 / 增量基准，缺哪个标红；
- 每张表的可用时间，与整库可用时间的差异；
- 传输进度（真实字节数）、中断阈值、resume 从第几字节继续、stale 提交；
- 查询门禁：放行时给出 `consistency`（manifest/epoch/增量链/边界）；拒绝时只列缺失依赖，不返回半截 data；
- 旧快照核对：来源/目标行数、摘要、缺失/多余/变更 key 的样本。

## 代码结构

```
pointwise/
  common.py     原子读写、sha256、跨进程文件锁、稳定摘要
  dataset.py    数据集/表/列/字典定义；确定性演示数据
  shards.py     分片与增量文件的物理格式（只读 SQLite）与读写
  source.py     来源：写入、压缩合并、清单发布、Range 下载
  target.py     目标：账本、续传复制、提交栅栏、门禁、旧快照核对
  httpmini.py   http.server 路由、Range、SSE、可中断下载
  cli.py        demo / source / target 入口
web/index.html  单页控制台
docs/DESIGN.md  数据模型、门禁规则、一致性与恢复的完整说明
tests/          unittest 用例（无 pytest 依赖）
```

## 测试

```bash
python3 -m unittest discover -s tests -t . -v
```

22 个用例覆盖：

- 结构已到、字典未到（引用/不引用字典两条路径）；
- 增量文件已到、基准分片未到 → 拒绝，不返回部分结果；
- 真实字节中断 → `.part` 保留 → resume 从断点继续 → checksum/行数一致；
- 目标重启（模拟进程被杀）→ 任务标 `node_restart` → resume 完成；
- 来源重启后续传；
- 来源压缩合并 → 旧增量被拒、目标重拉清单、最终核对一致；
- 并发复制 / 迟到旧副本 → 只有更高 epoch+generation 生效，文件不被改写；
- 整库 vs 逐表开放时间与边界；
- 旧快照核对（来源前进后仍能证明旧快照一致）；
- 目标分片被篡改/截断 → 409，而不是 500 或错误数据；
- 已提交对象不重复下载。

## 已知边界

- 分片是整文件传输，未做块级去重；压缩合并会重写全量分片（演示规模下可接受）。
- 查询是"全表物化后切片"，不是 SQL 引擎；适合核对与按快照读取，不适合大表交互式分析。
- 历史保留 40 个 generation 的清单与全部分片文件；更早的快照会被 GC（当前实现只保留，不自动回收）。
- `full` scope 的可用时间基于目标进程观测到的首次就绪时间；进程重启会保留已记录的时间。
