"""lizysdk.dist —— 分布式原语 / Redis 能力套件（设计文档 docs/redis-suite-design.md）。

属于 ``lizysdk[redis]`` 可选依赖组：**模块级不 import redis**——未安装
redis 的环境 ``import lizysdk.dist`` 依然成功；所有需要 redis 的路径在
函数体内懒加载，未安装时抛带中文安装提示的 ImportError::

    pip install "lizysdk[redis]"

公开 API（均可从 ``lizysdk.dist`` 直接导入）：

限流与锁（v0.6.0）：

- :class:`SlidingWindowCounter` —— 滑动窗口计数器（ZSET+Lua 原子四步；
  ``allow`` 是限流薄糖）
- :class:`DLock` —— 分布式锁（SET NX PX + token + Lua 校验释放/续期；
  **效率锁而非正确性锁**，不可重入）
- :class:`LockError` / :class:`LockTimeoutError` / :class:`LockNotOwnedError`
  —— 锁自持异常族（RLock / LeaderElector 复用）

Redis 能力套件（v0.8.0，算法出处见各模块 docstring）：

- :class:`RLock` —— 可重入锁 + 看门狗（对齐 Redisson RedissonLock：
  hash 重入计数 + 每 TTL/3 自动续期）
- :class:`LeaderElector` —— 领导选举（K8s Lease 语义：独占租约 +
  心跳续期 + 失联自动下台）
- :class:`IdempotentKey` —— 幂等键（Stripe 两态模型：processing→done，
  guard 上下文管理器自动管理键生命周期）
- :class:`ReliableQueue` —— 可靠任务队列（redis.io 官方 pattern：
  LMOVE→processing + LREM ack，at-least-once）
- :class:`DelayQueue` —— 延迟队列（Redisson RDelayedQueue 同构：
  ZSET 到期分 + 原子 Lua 搬运到 ready list）
- :class:`Leaderboard` —— 排行榜（redis.io 官方 ZSET 玩法：1-based
  名次、top/around 窗口）
- :class:`Job` —— 队列任务信封（id / payload / raw / tries）
- :class:`IdempotencyError` / :class:`IdempotencyConflictError` /
  :class:`IdempotencyDoneError` —— 幂等键异常族

快速上手::

    from lizysdk.dist import RLock, ReliableQueue

    with RLock(client, "job:42"):      # 可重入 + 看门狗自动续期
        do_job()

    rq = ReliableQueue(client, "orders")
    rq.push("order:123")
    job = rq.pop(timeout=5)
    rq.ack(job)

本包自持（红线）：不 import lizysdk 其他子包（ids/logs/errors/ext/
pools/shell），日志走标准 logging 由使用方接入。
"""

from __future__ import annotations

from .election import LeaderElector
from .idempotent import (
    IdempotencyConflictError,
    IdempotencyDoneError,
    IdempotencyError,
    IdempotentKey,
)
from .leaderboard import Leaderboard
from .lock import DLock, LockError, LockNotOwnedError, LockTimeoutError
from .queue import DelayQueue, Job, ReliableQueue
from .reentrant_lock import RLock
from .window import SlidingWindowCounter

__all__ = [
    # 限流与锁（v0.6.0）
    "SlidingWindowCounter",
    "DLock",
    "LockError",
    "LockTimeoutError",
    "LockNotOwnedError",
    # Redis 能力套件（v0.8.0）
    "RLock",
    "LeaderElector",
    "IdempotentKey",
    "IdempotencyError",
    "IdempotencyConflictError",
    "IdempotencyDoneError",
    "ReliableQueue",
    "DelayQueue",
    "Job",
    "Leaderboard",
]
