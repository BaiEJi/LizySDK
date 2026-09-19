"""lizysdk.ids —— trace_id 与分布式唯一 ID 生成子模块。

公开 API（均可从 ``lizysdk.ids`` 直接导入）：

- :func:`new_trace_id` —— 小写 hex 的 trace_id（secrets 随机，默认
  16 位、位数可选 8~64）
- :func:`new_uid` —— 通用业务唯一标识 uid（默认 16 位、位数可选 8~64）
- :func:`new_id` —— 雪花 64 位整数 ID（模块级默认生成器的快捷方式）
- :func:`new_prefixed_id` —— 带业务前缀的 ID，如 ``"ORD_<snowflake>"``
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

__all__ = [
    "ClockBackwardsError",
    "DEFAULT_EPOCH_MS",
    "IDGenerator",
    "MAX_SEQUENCE",
    "MAX_WORKER_ID",
    "new_id",
    "new_prefixed_id",
    "new_trace_id",
    "new_uid",
]
