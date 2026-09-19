"""lizysdk.dist —— Redis 分布式锁 DLock + 自持异常族。

实现形态（设计文档 §1.3 决策）：语义对齐 redis-py 内置 Lock——
``SET key token NX PX ttl`` 原子获取；释放/续期走 **Lua compare-token**
脚本（防「GC 停顿/网络延迟导致锁过期后误删他人锁」的经典事故）；
**不可重入**（同一对象未释放再 acquire 按争抢处理）。

单实例语义：v1 面向单节点 Redis；Redlock 多节点多数派列为 v2 演进
（设计文档 §5），届时再讨论时钟漂移与多数派安全性问题。

本模块自持：仅标准库，模块级不 import redis、不依赖 lizysdk 其他子包。
"""

from __future__ import annotations

import math
import random
import secrets
import time
from types import TracebackType
from typing import Any, Optional

__all__ = ["DLock", "LockError", "LockNotOwnedError", "LockTimeoutError"]

# ---------------------------------------------------------------------------
# 异常族（模块内自持）
# ---------------------------------------------------------------------------


class LockError(Exception):
    """锁相关错误的基类。"""


class LockTimeoutError(LockError):
    """阻塞获取超时（在 ``blocking_timeout`` / 覆盖参数期限内未抢到）。"""


class LockNotOwnedError(LockError):
    """操作非当前持有者的锁：锁不存在或 token 不符（防误删他人锁）。"""


# ---------------------------------------------------------------------------
# Lua 脚本（模块级常量）
# ---------------------------------------------------------------------------

#: 释放脚本：compare-token 删除。
#:
#: - KEYS[1]: 锁键（``{prefix}:{name}``）
#: - ARGV[1]: acquire 时生成的随机 token
#:
#: 只有 GET 结果与 token 完全一致（仍归我持有）才 DEL；否则返回 0——
#: 「锁已过期被他人获取」时绝不误删，由调用侧抛 LockNotOwnedError。
_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

#: 续期脚本：compare-token 后 PEXPIRE。
#:
#: - KEYS[1]: 锁键
#: - ARGV[1]: token
#: - ARGV[2]: 新 TTL（毫秒）——PEXPIRE 是**从当前时刻起**设置，
#:   即 redis-py Lock.extend 同款语义（additional_time 秒，非累加剩余时间）
#:
#: token 不符（已失锁）返回 0，由调用侧转为 False。
_EXTEND_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""


class DLock:
    """Redis 分布式锁（单实例：``SET NX PX`` + 随机 token + Lua 校验）。

    **Kleppmann 立场注记（务必阅读）**：本锁是「**效率锁**」而非
    「**正确性锁**」。在 GC 长停顿、网络延迟、进程冻结等场景下，持有者
    可能在锁 TTL 已过、他人已接锁之后仍自认持有（Kleppmann 对 Redlock
    的经典批评同样适用于单实例 TTL 锁）。因此：

    - 适合：防重复调度、防并发浪费、心跳互斥等「偶发重叠只损失效率」的场景；
    - 不适合：账户扣款、库存扣减等「必须严格互斥否则损坏数据」的场景——
      这类场景请在业务侧追加 **fencing token**（单调递增令牌 + 存储侧
      拒绝旧令牌）或数据库唯一约束，不要依赖本锁保证正确性。

    其他语义要点：

    - **不可重入**：同一对象未 release 再 acquire 按争抢处理（阻塞轮询
      直至超时抛 :class:`LockTimeoutError`）；
    - 每次 :meth:`acquire` 生成新 token（``secrets.token_hex(16)``），
      失败的尝试不会覆盖本地已持有的 token；
    - 实例**非线程安全**：请每线程各建自己的 DLock（共享同一 client）。

    Args:
        client: redis 客户端实例（需支持 SET/GET/DEL/EXISTS/EVAL）。
        name: 锁名（非空字符串；实际键为 ``{prefix}:{name}``）。
        timeout: 锁 TTL（秒，> 0）——持有期间最长存活时间，到期自动
            失效，他人可再获取。
        blocking_timeout: :meth:`acquire` 阻塞等待上限（秒，>= 0；
            0 表示立即超时）。
        prefix: 键前缀（默认 ``"lizy:lock"``）。

    Raises:
        ValueError: name 为空 / timeout <= 0 / blocking_timeout < 0（中文消息）。

    Example:
        >>> from lizysdk.dist import DLock                              # doctest: +SKIP
        >>> with DLock(client, "job:42", timeout=10):   # with 即阻塞获取
        ...     do_job()
        >>> lock = DLock(client, "job:42", timeout=10, blocking_timeout=5)
        >>> lock.acquire()              # True；超时抛 LockTimeoutError
        True
        >>> lock.extend(10)             # 续期 10 秒；已失锁返回 False
        True
        >>> lock.release()              # 非持有抛 LockNotOwnedError
        >>> lock.locked()               # 任意持有者视角
        False
    """

    #: 阻塞轮询间隔下限（秒）——与上限一起构成 0.05~0.2s 随机抖动
    _POLL_MIN = 0.05
    #: 阻塞轮询间隔上限（秒）——随机抖动防「惊群」（大量等待者同时重试）
    _POLL_MAX = 0.2

    def __init__(
        self,
        client: Any,
        name: str,
        *,
        timeout: float = 10.0,
        blocking_timeout: float = 10.0,
        prefix: str = "lizy:lock",
    ) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"name 必须为非空字符串，当前为 {name!r}")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError(f"timeout 必须为大于 0 的秒数，当前为 {timeout!r}")
        if (
            isinstance(blocking_timeout, bool)
            or not isinstance(blocking_timeout, (int, float))
            or blocking_timeout < 0
        ):
            raise ValueError(
                f"blocking_timeout 必须为 >= 0 的秒数，当前为 {blocking_timeout!r}"
            )
        self._client = client
        self._name = name
        self._timeout = float(timeout)
        self._blocking_timeout = float(blocking_timeout)
        self._prefix = prefix
        self._token: Optional[str] = None

    # ------------------------------------------------------------------
    # 元信息
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """锁名（只读）。"""
        return self._name

    @property
    def key(self) -> str:
        """实际 Redis 键：``{prefix}:{name}``（只读）。"""
        return f"{self._prefix}:{self._name}"

    @property
    def token(self) -> Optional[str]:
        """当前持有者的随机 token（未获取/已释放为 ``None``，只读）。

        每次 :meth:`acquire` 成功都会换新 token；释放/续期脚本靠它
        校验「锁仍归我」。
        """
        return self._token

    # ------------------------------------------------------------------
    # 获取 / 释放 / 续期 / 查询
    # ------------------------------------------------------------------

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """获取锁：``SET {key} {token} NX PX {ceil(timeout*1000)}``。

        Args:
            blocking: ``True``（默认）在等待期限内轮询重试（间隔
                0.05~0.2s 随机抖动防惊群）；``False`` 立即返回成败。
            timeout: 覆盖构造时的 ``blocking_timeout``（秒，>= 0）；
                仅阻塞模式生效，``None`` 用构造值。

        Returns:
            bool: 获取成功返回 True（仅 ``blocking=False`` 会返回 False）。

        Raises:
            LockTimeoutError: 阻塞模式下等待期满仍未获取（消息含锁名）。
            ValueError: timeout < 0 或类型非法（中文消息）。

        Note:
            不可重入：本对象持锁期间再次 acquire 会像任何竞争者一样
            轮询直至超时抛 :class:`LockTimeoutError`；失败尝试不会覆盖
            本地 token，原持锁状态不受影响。
        """
        if timeout is not None:
            if (
                isinstance(timeout, bool)
                or not isinstance(timeout, (int, float))
                or timeout < 0
            ):
                raise ValueError(f"timeout 必须为 >= 0 的秒数，当前为 {timeout!r}")
            wait = float(timeout)
        else:
            wait = self._blocking_timeout

        token = secrets.token_hex(16)
        ttl_ms = math.ceil(self._timeout * 1000)
        if self._try_set(token, ttl_ms):
            self._token = token
            return True
        if not blocking:
            return False

        deadline = time.monotonic() + wait
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise LockTimeoutError(
                    f"阻塞获取锁 {self.key!r} 超时（等待 {wait} 秒仍未抢到）"
                )
            # 随机抖动轮询：错开等待者的重试时刻，防止惊群
            time.sleep(min(random.uniform(self._POLL_MIN, self._POLL_MAX), remaining))
            if self._try_set(token, ttl_ms):
                self._token = token
                return True

    def release(self) -> None:
        """释放锁：Lua compare-token 删除（防误删他人锁）。

        Raises:
            LockNotOwnedError: 本对象未持有（从未获取/已释放），或键上
                的值已不是本对象 token（TTL 过期后被他人获取、或被外部
                篡改）。两种情况都**不做任何删除**，消息含锁名。

        Note:
            释放失败不会清空本地 token——是否放弃持有由调用方决策
            （例如网络抖动导致的校验失败可重试）。
        """
        if self._token is None:
            raise LockNotOwnedError(
                f"锁 {self.key!r} 尚未被当前对象获取，无法释放"
            )
        result = self._client.eval(_RELEASE_LUA, 1, self.key, self._token)
        if not result:
            raise LockNotOwnedError(
                f"锁 {self.key!r} 已不归当前持有者（不存在或 token 不符），"
                f"拒绝误删他人锁"
            )
        self._token = None

    def extend(self, additional_time: float) -> bool:
        """续期：Lua compare-token 后 ``PEXPIRE`` 从当前时刻起 additional_time 秒。

        Args:
            additional_time: 新 TTL（秒，> 0；是「重设为该值」而非
                「在剩余时间上累加」，对齐 redis-py Lock.extend）。

        Returns:
            bool: 成功 True；本对象未获取 / 已释放 / token 不符
            （已失锁）均返回 **False**（不抛异常——续期失败是可预期的
            竞态结果，调用方据此走「重新获取」分支即可）。

        Raises:
            ValueError: additional_time <= 0 或类型非法（中文消息）。
        """
        if (
            isinstance(additional_time, bool)
            or not isinstance(additional_time, (int, float))
            or additional_time <= 0
        ):
            raise ValueError(
                f"additional_time 必须为大于 0 的秒数，当前为 {additional_time!r}"
            )
        if self._token is None:
            return False
        ttl_ms = math.ceil(float(additional_time) * 1000)
        result = self._client.eval(_EXTEND_LUA, 1, self.key, self._token, str(ttl_ms))
        return bool(result)

    def locked(self) -> bool:
        """锁是否被**任意持有者**持有（EXISTS 视角，不校验 token）。

        Returns:
            bool: 键存在（无论归谁）为 True；用于旁路观察，不能作为
            「归我持有」的判据（那要看 :attr:`token` + 业务确认）。
        """
        return bool(self._client.exists(self.key))

    # ------------------------------------------------------------------
    # 上下文管理器
    # ------------------------------------------------------------------

    def __enter__(self) -> "DLock":
        """进入 with：自动**阻塞获取**（用构造时的 blocking_timeout）。

        获取失败（超时）抛 :class:`LockTimeoutError`（LockError 子类），
        不会带着未持有的锁进入代码块。
        """
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """退出 with：无论代码块正常/异常都尝试释放。

        释放失败（如临界区内 TTL 已过被他人接锁）抛
        :class:`LockNotOwnedError`——这可能遮蔽代码块的原异常，但沉默
        吞掉失锁事实更危险；对齐 redis-py Lock 的行为。
        """
        self.release()

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _try_set(self, token: str, ttl_ms: int) -> bool:
        """原子获取尝试：``SET key token NX PX ttl_ms``。"""
        return bool(self._client.set(self.key, token, nx=True, px=ttl_ms))

    def __repr__(self) -> str:
        """调试视图（不回显 token 与客户端）。"""
        return (
            f"{self.__class__.__name__}(name={self._name!r}, "
            f"timeout={self._timeout!r}, "
            f"blocking_timeout={self._blocking_timeout!r}, "
            f"held={self._token is not None})"
        )
