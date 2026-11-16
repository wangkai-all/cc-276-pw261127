"""pointwise-repl: 本地分析库跨节点复制。

两端都是本地 HTTP 服务：
  - ``pointwise.source``  来源端：写业务数据、生成分片/增量/清单，提供 Range 下载。
  - ``pointwise.target``  目标端：拉取清单、断点续传、门禁查询、旧快照核对。

设计说明见 ``docs/DESIGN.md``。
"""

__version__ = "1.0.0"
