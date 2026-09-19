"""lizysdk：通用 Python 基础工具包（零第三方依赖，Python 3.9+）。

三个子模块：

- :mod:`lizysdk.ids`    —— trace_id / 唯一 ID 生成（位数可选）+ 雪花分布式 ID
- :mod:`lizysdk.logs`   —— 结构化日志（sys_name 标识、pipe/JSON 双格式、轮转、
  后台写入池、打印完即发送 JSON 到远端）
- :mod:`lizysdk.errors` —— 标准化错误体系（错误码 / 模板消息 / 序列化往返）

一行日志的格式契约（``message`` 恒为最后一段）::

    LEVEL||TIMESTAMP||FILE:LINE||sys_name=xxx||k1=v1||k2=v2||message=<文本>

``json_format=True`` 时输出 JSONL；``send_json=True`` 时每条日志打印完即以
JSON POST 到 ``send_url``（body 含 sys_name 与全部字段）。

快速上手::

    import lizysdk as bk

    bk.setup_logging("logs", "app.log", sys_name="order-svc")
    bk.bind_context(trace_id=bk.new_trace_id())

    log = bk.get_logger(__name__)
    log.info("user logged in", user_id=123, action="login")

    raise bk.NotFoundError(params={"resource": "订单"})
"""

from __future__ import annotations

from .errors import (
    AppError,
    AuthError,
    ConflictError,
    ErrorCode,
    InternalError,
    NotFoundError,
    ParamError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    UpstreamTimeoutError,
    ensure,
    wrap,
)
from .ids import (
    ClockBackwardsError,
    DEFAULT_EPOCH_MS,
    IDGenerator,
    MAX_SEQUENCE,
    MAX_WORKER_ID,
    new_id,
    new_prefixed_id,
    new_trace_id,
    new_uid,
)
from .logs import (
    PipeLogger,
    PipeLogFormatter,
    bind_context,
    clear_context,
    flush,
    get_logger,
    parse_line,
    send_stats,
    setup_logging,
)

__version__ = "0.2.0"

__all__ = [
    # ids —— 唯一 ID / trace_id
    "new_trace_id",
    "new_uid",
    "new_id",
    "new_prefixed_id",
    "IDGenerator",
    "ClockBackwardsError",
    "DEFAULT_EPOCH_MS",
    "MAX_WORKER_ID",
    "MAX_SEQUENCE",
    # logs —— 结构化日志
    "setup_logging",
    "get_logger",
    "bind_context",
    "clear_context",
    "flush",
    "send_stats",
    "parse_line",
    "PipeLogger",
    "PipeLogFormatter",
    # errors —— 标准化错误
    "ErrorCode",
    "AppError",
    "ParamError",
    "AuthError",
    "PermissionDeniedError",
    "NotFoundError",
    "ConflictError",
    "RateLimitError",
    "InternalError",
    "ServiceUnavailableError",
    "UpstreamTimeoutError",
    "wrap",
    "ensure",
]
