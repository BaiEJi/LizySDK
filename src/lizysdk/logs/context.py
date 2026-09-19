"""基于 ``contextvars`` 的上下文字段（线程 / 协程隔离）。

通过 :func:`bind_context` 绑定的字段会自动附加到**当前线程 / 协程**内
经 :class:`~lizysdk.logs.PipeLogger` 发出的每一条日志，业务调用侧显式
传入的同名字段优先生效。

隔离性来自 ``contextvars``：线程之间互不可见，``asyncio`` 任务各自持有
上下文快照，天然满足 trace_id 等链路字段的传播需求。

示例::

    from lizysdk.logs import bind_context, clear_context, get_logger

    bind_context(trace_id="abc")        # 此后本（协）线程的日志自带 trace_id=abc
    log = get_logger(__name__)
    log.info("user logged in", user_id=123)
    clear_context()                     # 清空后不再附带
"""

from __future__ import annotations

import contextvars
from typing import Any

from .formatter import validate_fields

__all__ = ["bind_context", "clear_context", "get_context_fields"]

#: 承载上下文字段的 ContextVar；值为不可变约定下的 dict（只整体替换，不原地修改）。
_FIELDS: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "lizysdk_logs_fields", default=None
)


def bind_context(**fields: Any) -> None:
    """绑定上下文字段（合并语义：同名新值覆盖旧值）。

    仅影响当前线程 / 协程；其他线程不受影响。

    Args:
        **fields: 任意业务 kv，key 规则与日志调用侧一致。

    Raises:
        ValueError: key 非法或使用了保留字 ``message``。

    示例::

        bind_context(trace_id="abc", tenant="acme")
    """
    validate_fields(fields)
    current = _FIELDS.get()
    merged = {**(current or {}), **fields}
    _FIELDS.set(merged)


def clear_context() -> None:
    """清空当前线程 / 协程的全部上下文字段。

    示例::

        bind_context(trace_id="abc")
        clear_context()      # 此后日志不再附带 trace_id
    """
    _FIELDS.set(None)


def get_context_fields() -> dict[str, Any]:
    """读取当前上下文字段的快照（内部供 PipeLogger 在发日志时注入）。

    Returns:
        字段字典的浅拷贝；未绑定时返回空字典。
    """
    current = _FIELDS.get()
    return dict(current) if current else {}
