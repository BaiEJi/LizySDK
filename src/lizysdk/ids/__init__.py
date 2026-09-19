"""lizysdk.ids —— trace_id 与分布式唯一 ID 生成子模块。

公开 API（均可从 ``lizysdk.ids`` 直接导入）：

- :func:`new_trace_id` —— 小写 hex 的 trace_id（secrets 随机，默认
  16 位、位数可选 8~64）
- :func:`new_uid` —— 通用业务唯一标识 uid（默认 16 位、位数可选 8~64）
- :func:`new_sortable_id` —— ULID 风格 26 字符可排序字符串 ID
  （同进程内单调不减，字典序 == 生成顺序）
- :func:`sortable_id_timestamp` —— 从可排序 ID 反解 Unix 秒时间戳
- :func:`new_id` —— 雪花 64 位整数 ID（模块级默认生成器的快捷方式）
- :func:`new_prefixed_id` —— 带业务前缀的 ID，如 ``"ORD_<snowflake>"``
- :func:`resolve_worker_id` —— worker_id 自动协商（环境变量优先，
  否则本机锁文件占位，用于多实例部署免手工分配）
- :class:`IDGenerator` —— 可配置的雪花生成器（线程安全）
- :class:`ClockBackwardsError` —— 时钟大幅回拨异常（ValueError 子类）
- :data:`DEFAULT_EPOCH_MS` / :data:`MAX_WORKER_ID` / :data:`MAX_SEQUENCE`
  —— 生成器常用常量
"""

from __future__ import annotations

from .snowflake import (
    DEFAULT_EPOCH_MS,
    MAX_SEQUENCE,
    MAX_WORKER_ID,
    ClockBackwardsError,
    IDGenerator,
    new_id,
    new_prefixed_id,
)
from .trace import new_trace_id, new_uid
from .ulid import new_sortable_id, sortable_id_timestamp
from .worker import resolve_worker_id

__all__ = [
    "ClockBackwardsError",
    "DEFAULT_EPOCH_MS",
    "IDGenerator",
    "MAX_SEQUENCE",
    "MAX_WORKER_ID",
    "new_id",
    "new_prefixed_id",
    "new_sortable_id",
    "new_trace_id",
    "new_uid",
    "resolve_worker_id",
    "sortable_id_timestamp",
]
