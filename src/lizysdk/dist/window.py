"""lizysdk.dist —— Redis 滑动窗口计数器（ZSET + Lua 原子脚本）。

实现形态（设计文档 §1.2 决策）：对齐 `limits` 库 moving-window 与
`redis.io` 官方限流教程的标准滑动日志——**ZADD 当前时间戳（唯一 member）
→ ZREMRANGEBYSCORE 清理窗口外 → ZCARD 计数 → PEXPIRE 保底过期**，
四步收敛进同一个 Lua 脚本保证原子性；只读计数走第二个 Lua（同口径先
清理再 ZCARD）保证与 ``incr`` 视角一致。

定位是**调用方计数的通用滑动窗口**（不止限流：也可做「近 5 分钟错误数」
这类统计），限流判定只是 :meth:`SlidingWindowCounter.allow` 的薄糖。

本模块自持：仅标准库 + 包内 ``_client``；模块级不 import redis。
"""

from __future__ import annotations

import math
import os
import secrets
import time
from typing import Any

from ._client import client_from_url

__all__ = ["SlidingWindowCounter"]

# ---------------------------------------------------------------------------
# Lua 脚本（模块级常量；redis-py 的 EVAL 每次整段下发，无需预注册）
# ---------------------------------------------------------------------------

#: 脚本一：incr —— 原子四步（写路径）
#:
#: - KEYS[1]: 窗口 ZSET 键（``{prefix}:{key}``）
#: - ARGV[1]: 当前时间戳（秒，浮点字符串）—— 本次事件的 score
#: - ARGV[2]: 唯一 member（``{now}:{pid}:{token}``；amount>1 时循环加 ``:i`` 后缀）
#: - ARGV[3]: 窗口下界（``now - window``；score **<=** 该值的事件出窗）
#: - ARGV[4]: 保底过期毫秒数（``ceil(window*1000)``）
#: - ARGV[5]: 本次事件数 amount（>= 1）
#:
#: 语义注记：TTL 恒 >= window，因此 PEXPIRE 永远不会赶在窗口外清理之前
#: 误删窗口内事件；它只负责清掉「全空后」的孤儿键。
_INCR_LUA = """
local amount = tonumber(ARGV[5])
-- 第 1 步：ZADD 落本次事件（score=now；member 全局唯一，重复调用不覆盖）
for i = 1, amount do
  local member = ARGV[2]
  if amount > 1 then
    member = ARGV[2] .. ':' .. tostring(i)
  end
  redis.call('ZADD', KEYS[1], ARGV[1], member)
end
-- 第 2 步：ZREMRANGEBYSCORE 清掉窗口外历史（含边界：score <= now-window 剔除）
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[3])
-- 第 3 步：ZCARD 取窗口内当前计数（含本次）
local count = redis.call('ZCARD', KEYS[1])
-- 第 4 步：PEXPIRE 保底过期，防孤儿键常驻
redis.call('PEXPIRE', KEYS[1], ARGV[4])
return count
"""

#: 脚本二：count —— 只读路径（不落事件、不续 TTL）
#:
#: - KEYS[1]: 窗口 ZSET 键
#: - ARGV[1]: 窗口下界（``now - window``）
#:
#: 与 incr 同口径**先清理再计数**（放进同一脚本保证原子），否则读到
#: 「脏窗口」会虚高；不 PEXPIRE——TTL 已由最近一次写入设置，读路径无续期职责。
_COUNT_LUA = """
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
return redis.call('ZCARD', KEYS[1])
"""


def _now() -> float:
    """当前时间戳（秒，浮点）。

    独立成模块级函数是刻意的**时间接缝**：测试用它 monkeypatch 冻结/推进
    时间，以离线验证窗口剔除逻辑（见 tests/test_dist.py）。
    """
    return time.time()


class SlidingWindowCounter:
    """Redis 滑动窗口计数器（ZSET + Lua，原子四步）。

    窗口语义：任意时刻 ``count`` 反映 ``(now - window, now]`` 区间内的
    累计事件数（秒级浮点 score，含右端点、剔左端点）。每次 :meth:`incr`
    返回计入本次之后的窗口内计数，全程一个 Lua 脚本原子完成。

    内存注记（设计文档 §1.2 对比表）：滑动日志精度最高但内存 **O(N)**
    （N = 窗口内事件数，每事件一个 ZSET member）。高频 + 大窗口场景
    （如 10 万 QPS × 60s = 600 万 member）请换**固定窗口**（INCR+EXPIRE，
    O(1)）或**令牌桶 / GCRA**（平滑限速，O(1)）——代价是边界精度。

    Args:
        client: redis 客户端实例（``redis.Redis`` / ``fakeredis`` 等兼容
            EVAL 的实现）。注解为 ``Any`` 是因为模块级禁止 import redis。
        window: 窗口长度（秒，> 0），同时决定保底 TTL。
        prefix: 键前缀，实际键为 ``{prefix}:{key}`` 的 ZSET。

    Raises:
        ValueError: window 非数值 / 非正数（中文消息）。

    Example:
        >>> from lizysdk.dist import SlidingWindowCounter      # doctest: +SKIP
        >>> counter = SlidingWindowCounter.from_url("redis://localhost:6379/0", window=60)
        >>> counter.incr("user:1:api")        # 窗口内当前计数（含本次）
        1
        >>> counter.allow("user:1:api", 100)  # 薄糖：incr 后 <= limit
        True
        >>> counter.count("user:1:api")       # 只读，不增
        2
        >>> counter.reset("user:1:api")       # 删除窗口键
    """

    def __init__(
        self,
        client: Any,
        *,
        window: float = 60.0,
        prefix: str = "lizy:swc",
    ) -> None:
        if isinstance(window, bool) or not isinstance(window, (int, float)):
            raise ValueError(f"window 必须为大于 0 的秒数（int/float），当前为 {window!r}")
        if window <= 0:
            raise ValueError(f"window 必须为大于 0 的秒数，当前为 {window!r}")
        self._client = client
        self._window = float(window)
        self._prefix = prefix

    # ------------------------------------------------------------------
    # 构造与元信息
    # ------------------------------------------------------------------

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        window: float = 60.0,
        prefix: str = "lizy:swc",
        **kwargs: Any,
    ) -> "SlidingWindowCounter":
        """懒加载构造：``redis.Redis.from_url(url, **kwargs)`` 建客户端再实例化。

        未安装 redis 时不影响 ``import lizysdk.dist``，直到调用本方法才抛
        带中文安装提示的 ImportError（懒加载红线，见 :mod:`._client`）。

        Args:
            url: 连接串，如 ``"redis://localhost:6379/0"``。
            window: 窗口长度（秒，> 0）。
            prefix: 键前缀。
            **kwargs: 原样透传 ``redis.Redis.from_url``。

        Returns:
            SlidingWindowCounter: 已绑定客户端的计数器。

        Raises:
            ImportError: 未安装 redis（消息含 ``pip install "lizysdk[redis]"``）。
            ValueError: window 非法。

        Example:
            >>> from lizysdk.dist import SlidingWindowCounter   # doctest: +SKIP
            >>> SlidingWindowCounter.from_url("redis://localhost:6379/0", window=300)
            SlidingWindowCounter(window=300.0, prefix='lizy:swc')
        """
        client = client_from_url(url, **kwargs)
        return cls(client, window=window, prefix=prefix)

    @property
    def window(self) -> float:
        """窗口长度（秒，只读）。"""
        return self._window

    @property
    def prefix(self) -> str:
        """键前缀（只读）。"""
        return self._prefix

    def _full_key(self, key: str) -> str:
        """业务 key -> 实际 Redis 键：``{prefix}:{key}``。"""
        return f"{self._prefix}:{key}"

    # ------------------------------------------------------------------
    # 读写路径
    # ------------------------------------------------------------------

    def incr(self, key: str, amount: int = 1) -> int:
        """计入 ``amount`` 个事件，返回窗口内当前计数（含本次，原子）。

        单次调用内四步（ZADD → 清窗口外 → ZCARD → PEXPIRE）由
        ``_INCR_LUA`` 一次 EVAL 原子完成；member 含 ``pid`` 与随机
        token，跨进程/跨线程/同毫秒重复调用都不会互相覆盖。

        Args:
            key: 业务键（自动加 ``{prefix}:`` 前缀）。
            amount: 本次事件数（>= 1 的整数；> 1 时落 ``amount`` 个
                唯一 member，score 同为当前时刻）。

        Returns:
            int: 窗口内当前计数（含本次全部事件）。

        Raises:
            ValueError: amount 非 int 或 < 1（中文消息）。

        Example:
            >>> counter.incr("hits")            # doctest: +SKIP
            1
            >>> counter.incr("hits", amount=3)  # 一次计 3 个事件
            4
        """
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise ValueError(f"amount 必须为 >= 1 的整数，当前为 {amount!r}")
        if amount < 1:
            raise ValueError(f"amount 必须为 >= 1 的整数，当前为 {amount!r}")
        now = _now()
        member = f"{now}:{os.getpid()}:{secrets.token_hex(4)}"
        ttl_ms = math.ceil(self._window * 1000)
        result = self._client.eval(
            _INCR_LUA,
            1,
            self._full_key(key),
            str(now),                 # ARGV[1] score
            member,                   # ARGV[2] 唯一 member 基名
            str(now - self._window),  # ARGV[3] 窗口下界（含边界剔除）
            str(ttl_ms),              # ARGV[4] 保底 TTL（毫秒）
            str(amount),              # ARGV[5] 事件数
        )
        return int(result)

    def count(self, key: str) -> int:
        """只读：窗口内当前计数（不落新事件，不续 TTL）。

        与 :meth:`incr` 同口径——先按当前时刻清理窗口外事件再 ZCARD
        （``_COUNT_LUA`` 原子完成），因此两者视角一致，不会读到脏窗口。

        Args:
            key: 业务键。

        Returns:
            int: 窗口内事件数（键不存在时为 0）。

        Example:
            >>> counter.count("hits")   # doctest: +SKIP
            4
        """
        cutoff = _now() - self._window
        result = self._client.eval(_COUNT_LUA, 1, self._full_key(key), str(cutoff))
        return int(result)

    def allow(self, key: str, limit: int) -> bool:
        """限流薄糖：**先计数再判定** —— ``incr(key) <= limit``。

        语义注记（重要）：这是「计数后判定」，不是「判定后计数」——
        被拒绝的请求**同样占用一个窗口名额**（incr 已经发生）。这在
        「同一调用方超限即惩罚」的场景是期望行为；若需「被拒不计数」
        请改用 :meth:`count` 预判 + 自行取舍。

        Args:
            key: 业务键。
            limit: 窗口内允许的最大计数（>= 1 的整数）。

        Returns:
            bool: 本次计入后仍未超限返回 True，否则 False。

        Raises:
            ValueError: limit 非 int 或 < 1（中文消息）。

        Example:
            >>> counter.allow("user:1:api", 100)   # doctest: +SKIP
            True
        """
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise ValueError(f"limit 必须为 >= 1 的整数，当前为 {limit!r}")
        if limit < 1:
            raise ValueError(f"limit 必须为 >= 1 的整数，当前为 {limit!r}")
        return self.incr(key) <= limit

    def reset(self, key: str) -> None:
        """删除窗口键（整个窗口清零；键不存在也安全）。

        Args:
            key: 业务键。

        Example:
            >>> counter.reset("user:1:api")   # doctest: +SKIP
        """
        self._client.delete(self._full_key(key))

    def __repr__(self) -> str:
        """调试视图（不回显客户端，避免泄露连接信息）。"""
        return (
            f"{self.__class__.__name__}(window={self._window!r}, "
            f"prefix={self._prefix!r})"
        )
