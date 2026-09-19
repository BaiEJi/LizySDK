"""lizysdk：通用 Python 基础工具包（核心零第三方依赖，Python 3.9+）。

四个子模块：

- :mod:`lizysdk.ids`    —— trace_id / 唯一 ID 生成（位数可选）+ 雪花分布式 ID
  + ULID 可排序 ID + worker_id 自动协商
- :mod:`lizysdk.logs`   —— 结构化日志（sys_name 标识、pipe/JSON 双格式、轮转、
  后台写入池、打印完即发送 JSON 到远端）
- :mod:`lizysdk.errors` —— 标准化错误体系（错误码 / 业务码动态注册表 / 模板消息 /
  序列化往返 / wrap·ensure）
- :mod:`lizysdk.ext`    —— Web 框架适配器（FastAPI/Flask，需 ``pip install lizysdk[web]``）
- :mod:`lizysdk.pools`  —— 统一并发池（线程/协程/进程，初始化选类型，钩子 + 统计）
- :mod:`lizysdk.shell` —— shell 执行包装（安全 argv、超时、结构化命令日志、富错误）
- :mod:`lizysdk.dist` —— 分布式原语 / Redis 能力套件（滑动窗口计数器 /
  分布式锁 / 可重入锁+看门狗 / 领导选举 / 幂等键 / 可靠队列 / 延迟队列 /
  排行榜，可选依赖组 ``pip install lizysdk[redis]``，模块级懒加载）
- :mod:`lizysdk.notify` —— 通知中心（钉钉/飞书/企业微信/邮件/自定义 webhook，
  级别路由、静默期、频控、异步投递与重试）

一行日志的格式契约（``message`` 恒为最后一段）::

    LEVEL||TIMESTAMP||FILE:LINE||sys_name=xxx||k1=v1||k2=v2||message=<文本>

快速上手::

    import lizysdk as bk

    bk.setup_logging("logs", "app.log", sys_name="order-svc")
    bk.bind_context(trace_id=bk.new_trace_id())

    log = bk.get_logger(__name__)
    log.info("user logged in", user_id=123, action="login")

    raise bk.NotFoundError(params={"resource": "订单"})
"""

from __future__ import annotations

from . import ext
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
    register_code,
    registered_codes,
    unregister_code,
    wrap,
)
from .dist import (
    DLock,
    DelayQueue,
    IdempotencyConflictError,
    IdempotencyDoneError,
    IdempotencyError,
    IdempotentKey,
    Job,
    LeaderElector,
    Leaderboard,
    LockError,
    LockNotOwnedError,
    LockTimeoutError,
    RLock,
    ReliableQueue,
    SlidingWindowCounter,
)
from .shell import ShellError, ShellResult, ShellTimeoutError, run
from .ids import (
    ClockBackwardsError,
    DEFAULT_EPOCH_MS,
    IDGenerator,
    MAX_SEQUENCE,
    MAX_WORKER_ID,
    new_id,
    new_prefixed_id,
    new_sortable_id,
    new_trace_id,
    new_uid,
    resolve_worker_id,
    sortable_id_timestamp,
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
from .notify import (
    ChannelError,
    DingTalkChannel,
    EmailChannel,
    FeishuChannel,
    NotifyCenter,
    NotifyError,
    WebhookChannel,
    WeComChannel,
)
from .pools import (
    AsyncPool,
    Pool,
    PoolClosedError,
    PoolError,
    PoolRejectedError,
    ProcessPool,
    ThreadPool,
    create_pool,
    run_all,
)

__all__ = [
    # ids —— 唯一 ID / trace_id
    "new_trace_id",
    "new_uid",
    "new_id",
    "new_prefixed_id",
    "new_sortable_id",
    "sortable_id_timestamp",
    "resolve_worker_id",
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
    "register_code",
    "unregister_code",
    "registered_codes",
    # ext —— Web 框架适配（可选依赖组 lizysdk[web]）
    "ext",
    # pools —— 统一并发池
    "create_pool",
    "run_all",
    "ThreadPool",
    "AsyncPool",
    "ProcessPool",
    "Pool",
    "PoolError",
    "PoolClosedError",
    "PoolRejectedError",
    # shell —— shell 执行包装
    "run",
    "ShellResult",
    "ShellError",
    "ShellTimeoutError",
    # dist —— 分布式原语 / Redis 能力套件（可选依赖组 lizysdk[redis]）
    "SlidingWindowCounter",
    "DLock",
    "RLock",
    "LockError",
    "LockTimeoutError",
    "LockNotOwnedError",
    "LeaderElector",
    "IdempotentKey",
    "IdempotencyError",
    "IdempotencyConflictError",
    "IdempotencyDoneError",
    "ReliableQueue",
    "DelayQueue",
    "Job",
    "Leaderboard",
    # notify —— 通知中心
    "NotifyCenter",
    "DingTalkChannel",
    "FeishuChannel",
    "WeComChannel",
    "EmailChannel",
    "WebhookChannel",
    "NotifyError",
    "ChannelError",
]

__version__ = "0.8.0"
