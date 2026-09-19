"""lizysdk.dist —— Redis 可重入锁 RLock + 看门狗（Redisson 语义对齐件）。

实现形态（设计文档 §1.1 决策）：对齐 Redisson ``RedissonLock``——锁键是
**hash**，field = ``{instance_uuid}:{thread_ident}``（对齐 Redisson 的
clientId:threadId），value = **重入计数**；获取/释放/续期全部收敛为 Lua
脚本（HEXISTS 命中 → HINCRBY +1 + PEXPIRE / 未命中且键不存在 → HSET 1 +
PEXPIRE / 解锁 HINCRBY -1 → 计数 <= 0 则 DEL 否则 PEXPIRE）；未显式指定
lease_time 时启用**看门狗**（默认 30s，每 ``watchdog_timeout/3`` 续期一次，
daemon 线程 + Event 停止），持锁线程存活则锁不丢。

与 :class:`lizysdk.dist.DLock` 的选型指引（二者并存，设计文档 §1.1）：

- 要**可重入 / 看门狗**（长任务不想手动续期）→ 用 RLock；
- 要**极简效率锁**（一次性短临界区，token 模型）→ 用 DLock。

互斥能力两者等价；正确性边界同 DLock 的 Kleppmann 注记（效率锁而非
正确性锁，严格互斥场景需业务侧 fencing token）。

本模块自持：仅标准库 + dist 内 ``lock`` 的异常族；模块级不 import redis、
不依赖 lizysdk 其他子包。
"""

from __future__ import annotations

import math
import random
import threading
import time
import uuid
from types import TracebackType
from typing import Any, Optional

from .lock import LockError, LockNotOwnedError, LockTimeoutError

__all__ = ["RLock"]

# ---------------------------------------------------------------------------
# Lua 脚本（模块级常量）
# ---------------------------------------------------------------------------

#: 加锁脚本：可重入获取（对齐 Redisson RedissonLock#tryLock 的 Lua）。
#:
#: - KEYS[1]: 锁键（``{prefix}:{name}``，hash 结构）
#: - ARGV[1]: field = ``{instance_uuid}:{thread_ident}``（对齐 clientId:threadId）
#: - ARGV[2]: 租期毫秒数（lease_time 或 watchdog_timeout）
#:
#: 三种结果：
#: 1. field 已存在（本线程重入）→ HINCRBY +1 + PEXPIRE，返回 -1（成功）；
#: 2. 键不存在（首次获取）→ HSET field 1 + PEXPIRE，返回 -1（成功）；
#: 3. 键被他人持有 → 返回当前 PTTL（供等待方按 TTL 排期重试；
#:    PTTL 为负的病态情况一律钳为 0，避免与成功哨兵 -1 混淆——成功
#:    约定返回 -1，对齐 Redisson 返回 nil、redis-py eval 转为 None 的
#:    区分思路，用 int 哨兵取代 None 以便类型稳定）。
_TRY_LOCK_LUA = """
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
  redis.call('HINCRBY', KEYS[1], ARGV[1], 1)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return -1
end
if redis.call('EXISTS', KEYS[1]) == 0 then
  redis.call('HSET', KEYS[1], ARGV[1], 1)
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return -1
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then
  return 0
end
return ttl
"""

#: 解锁脚本：可重入释放（对齐 Redisson RedissonLock#unlock 的 Lua）。
#:
#: - KEYS[1]: 锁键
#: - ARGV[1]: field = ``{instance_uuid}:{thread_ident}``
#: - ARGV[2]: 剩余层仍需的租期毫秒数（lease_time 或 watchdog_timeout）
#:
#: 三种结果：
#: 1. field 不存在（非本线程持有：未获取/已释放/已过期/持有者是其他
#:    线程或实例）→ 返回 -1，调用侧抛 :class:`LockNotOwnedError`；
#: 2. HINCRBY -1 后计数 > 0（仍是重入态）→ PEXPIRE 续命，返回剩余计数；
#: 3. 计数 <= 0（完全释放）→ DEL 整键，返回 0。
_UNLOCK_LUA = """
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 0 then
  return -1
end
local counter = redis.call('HINCRBY', KEYS[1], ARGV[1], -1)
if counter > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return counter
end
redis.call('DEL', KEYS[1])
return 0
"""

#: 续期脚本：compare-field 后 PEXPIRE（看门狗与 extend 共用）。
#:
#: - KEYS[1]: 锁键
#: - ARGV[1]: field = ``{instance_uuid}:{thread_ident}``
#: - ARGV[2]: 新 TTL（毫秒）——从当前时刻起「重设为该值」（非累加）
#:
#: field 不存在（已失锁/被他人持有）返回 0，调用侧转为 False。
_RENEW_LUA = """
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""


def _now() -> float:
    """当前时间戳（秒，浮点）。

    独立成模块级函数是刻意的**时间接缝**：测试用它 monkeypatch 冻结/推进
    时间（acquire 的等待截止时间基于它，见 tests/test_dist_locks.py）。
    """
    return time.time()


def _sleep(seconds: float) -> None:
    """线程睡眠（秒）。

    模块级**睡眠接缝**：看门狗/等待方轮询统一经它睡眠，测试 monkeypatch
    加速并发用例（真实 time.sleep 会让看门狗用例变慢）。
    """
    time.sleep(seconds)


#: 看门狗分片睡眠的片长（秒）：每片后检查一次停止事件，保证 stop 延迟
#: 有上界；片长同时是 monkeypatch 加速时的最小颗粒度。
_WATCHDOG_TICK = 0.05


class RLock:
    """Redis 可重入锁（hash 计数 + 看门狗；实现形态对齐 Redisson RedissonLock）。

    核心语义（Redisson 同款）：

    - 键是 **hash**：field = ``{instance_uuid}:{thread_ident}``，value =
      重入计数——同实例同线程重复 :meth:`acquire` 计数 +1，:meth:`release`
      计数 -1，到 0 删除整键；
    - **可重入范围 = 同实例同线程**：跨实例（即使同名）或跨线程都是
      竞争者，与 Redisson 两个 client 互斥语义一致（两个 RLock 实例
      同名互为竞争者）；
    - **看门狗**：``lease_time=None``（默认）时启用——TTL 固定为
      ``watchdog_timeout``（默认 30s），后台 daemon 线程每
      ``watchdog_timeout/3`` 续期一次，持锁线程存活则锁不丢；
      :meth:`release` 到 0 层自动停看门狗；显式给 ``lease_time`` 则固定
      TTL、**无看门狗**，到期自动失效；
    - 互斥能力与 :class:`lizysdk.dist.DLock` 等价；正确性边界同 DLock
      的 Kleppmann 注记（效率锁而非正确性锁）。

    实例内部状态有锁保护，**跨线程共享同一实例是安全的**（行为上互为
    竞争者，与两个实例同名竞争等价）。

    Args:
        client: redis 客户端实例（需支持 EVAL/HSET/HGET/DEL/EXISTS）。
        name: 锁名（非空字符串；实际键为 ``{prefix}:{name}``）。
        lease_time: 显式租期（秒，> 0）。给了则固定 TTL 不续期（无看门狗）；
            ``None``（默认）启用看门狗，TTL = ``watchdog_timeout``。
        watchdog_timeout: 看门狗租期（秒，> 0，默认 30.0，对齐 Redisson
            ``lockWatchdogTimeout``）；仅 ``lease_time=None`` 时生效。
        prefix: 键前缀（非空字符串，默认 ``"lizy:rlock"``）。

    Raises:
        ValueError: name 为空 / lease_time 非法 / watchdog_timeout <= 0 /
            prefix 为空（中文消息）。

    Example:
        >>> from lizysdk.dist import RLock                                # doctest: +SKIP
        >>> lock = RLock(client, "job:42")          # 看门狗模式（30s 续期）
        >>> lock.acquire()                          # 同线程可重入
        True
        >>> lock.acquire()
        True
        >>> lock.reentrant_count()                  # 当前重入层数
        2
        >>> lock.release(); lock.release()          # 归零即删键、停看门狗
        >>> lock = RLock(client, "job:42", lease_time=5)   # 固定 5s，无看门狗
        >>> with lock:                              # with 即阻塞获取
        ...     do_job()
    """

    #: 阻塞轮询间隔下限（秒）——与上限构成 0.05~0.2s 随机抖动
    _POLL_MIN = 0.05
    #: 阻塞轮询间隔上限（秒）——随机抖动防「惊群」（大量等待者同时重试）
    _POLL_MAX = 0.2

    def __init__(
        self,
        client: Any,
        name: str,
        *,
        lease_time: Optional[float] = None,
        watchdog_timeout: float = 30.0,
        prefix: str = "lizy:rlock",
    ) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"name 必须为非空字符串，当前为 {name!r}")
        if lease_time is not None and (
            isinstance(lease_time, bool)
            or not isinstance(lease_time, (int, float))
            or lease_time <= 0
        ):
            raise ValueError(
                f"lease_time 必须为 None 或大于 0 的秒数，当前为 {lease_time!r}"
            )
        if (
            isinstance(watchdog_timeout, bool)
            or not isinstance(watchdog_timeout, (int, float))
            or watchdog_timeout <= 0
        ):
            raise ValueError(
                f"watchdog_timeout 必须为大于 0 的秒数，当前为 {watchdog_timeout!r}"
            )
        if not isinstance(prefix, str) or not prefix:
            raise ValueError(f"prefix 必须为非空字符串，当前为 {prefix!r}")
        self._client = client
        self._name = name
        self._lease_time: Optional[float] = (
            None if lease_time is None else float(lease_time)
        )
        self._watchdog_timeout = float(watchdog_timeout)
        self._prefix = prefix
        #: 每个 RLock 实例一个 uuid：两个实例同名互为竞争者（对齐
        #: Redisson 两个 client 的 clientId 语义）
        self._instance_uuid = uuid.uuid4().hex
        #: 看门狗线程与其停止事件（lease_time=None 时启用）
        self._watchdog_thread: Optional[threading.Thread] = None
        self._watchdog_stop: Optional[threading.Event] = None
        #: 看门狗代持的持有者 field（看门狗线程没有业务线程的 ident，
        #: 必须记录加锁线程的 field 才能续对人的期）
        self._watchdog_field: Optional[str] = None
        self._state_lock = threading.Lock()

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
    def lease_time(self) -> Optional[float]:
        """显式租期（秒；``None`` 表示看门狗模式，只读）。"""
        return self._lease_time

    @property
    def watchdog_timeout(self) -> float:
        """看门狗租期（秒，只读；仅看门狗模式生效）。"""
        return self._watchdog_timeout

    # ------------------------------------------------------------------
    # 获取 / 释放 / 续期 / 查询
    # ------------------------------------------------------------------

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        """获取锁（可重入）：``_TRY_LOCK_LUA`` 原子完成重入/首取/失败三态。

        Args:
            blocking: ``True``（默认）在等待期限内轮询重试（间隔取
                0.05~0.2s 随机抖动与当前 PTTL 的较小值，按 TTL 排期重试）；
                ``False`` 立即返回成败。
            timeout: 阻塞等待上限（秒，>= 0）。``None``（默认）**一直等待**
                （对齐 Redisson ``lock()`` 语义，请确保锁终会释放）。

        Returns:
            bool: 获取成功（含重入）返回 True；仅 ``blocking=False`` 抢不到
            时返回 False。

        Raises:
            LockTimeoutError: 阻塞模式下等待期满仍未获取（消息含键名）。
            ValueError: timeout < 0 或类型非法（中文消息）。

        Note:
            成功且处于看门狗模式时，本方法会确保看门狗线程在运行；
            重入获取（计数 +1）不会重启已有看门狗。
        """
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout < 0
        ):
            raise ValueError(f"timeout 必须为 >= 0 的秒数，当前为 {timeout!r}")
        wait: Optional[float] = None if timeout is None else float(timeout)
        field = self._field()
        ttl_ms = self._effective_lease_ms()
        deadline = None if wait is None else _now() + wait
        while True:
            result = int(
                self._client.eval(_TRY_LOCK_LUA, 1, self.key, field, str(ttl_ms))
            )
            if result == -1:
                self._start_watchdog_if_needed(field)
                return True
            if not blocking:
                return False
            if deadline is not None and _now() >= deadline:
                raise LockTimeoutError(
                    f"阻塞获取可重入锁 {self.key!r} 超时（等待 {wait} 秒仍未抢到）"
                )
            # 抢锁失败：按返回的 PTTL 排期重试（TTL 很短时不必等满抖动间隔）
            ttl_seconds = result / 1000.0 if result > 0 else 0.0
            delay = random.uniform(self._POLL_MIN, self._POLL_MAX)
            if ttl_seconds > 0:
                delay = min(delay, ttl_seconds)
            if deadline is not None:
                delay = min(delay, max(deadline - _now(), 0.0))
            _sleep(max(delay, 0.001))

    def release(self) -> None:
        """释放一层锁：``_UNLOCK_LUA`` 原子完成减计数/续命/删键。

        完全释放（计数到 0）时删除整键并停看门狗；仍是重入态时仅对键
        PEXPIRE 续命（续到有效租期档）。

        Raises:
            LockNotOwnedError: 当前线程并未持有该锁——从未获取/已释放到
                0/TTL 已过期被他人获取/持有者是其他线程或实例。均不做
                任何删除，消息含键名。

        Note:
            显式 lease_time 到期后的 release 属于上述「已失锁」分支；
            看门狗模式下持锁线程存活期间锁不会过期，release 总是可达。
        """
        result = int(
            self._client.eval(
                _UNLOCK_LUA, 1, self.key, self._field(), str(self._effective_lease_ms())
            )
        )
        if result < 0:
            raise LockNotOwnedError(
                f"可重入锁 {self.key!r} 不归当前线程持有"
                f"（未获取/已释放/已过期/持有者为其他线程或实例），拒绝误删"
            )
        if result == 0:
            # 完全释放：停看门狗、清代持 field
            with self._state_lock:
                self._watchdog_field = None
                if self._watchdog_stop is not None:
                    self._watchdog_stop.set()

    def reentrant_count(self) -> int:
        """当前线程对该锁的重入层数（Redis HGET 视角，未持有为 0）。

        Returns:
            int: 当前线程的持有层数；键不存在、已过期或持有者是他人
            均返回 0（以 Redis 为准，不信任本地缓存）。
        """
        value = self._client.hget(self.key, self._field())
        if value is None:
            return 0
        return int(value)

    def is_held_by_current_thread(self) -> bool:
        """锁是否由**当前线程**持有（HEXISTS 视角，重入判定用）。"""
        return bool(self._client.hexists(self.key, self._field()))

    def locked(self) -> bool:
        """锁是否被**任意持有者**持有（EXISTS 视角，不校验 field）。"""
        return bool(self._client.exists(self.key))

    def extend(self, lease_time: float) -> bool:
        """续期：Lua compare-field 后 ``PEXPIRE`` 重设为 lease_time 秒。

        Args:
            lease_time: 新 TTL（秒，> 0；「重设为该值」而非累加剩余时间）。

        Returns:
            bool: 成功 True；当前线程未持有（未获取/已释放/已过期/持有者
            是他人）返回 **False**（不抛异常——续期失败是可预期竞态）。

        Raises:
            ValueError: lease_time <= 0 或类型非法（中文消息）。

        Note:
            看门狗模式下 extend 是一次性重设：下个看门狗周期仍会以
            ``watchdog_timeout`` 覆盖 TTL（对齐 Redisson 看门狗自治语义，
            想改基准租期请重建锁对象）。
        """
        if (
            isinstance(lease_time, bool)
            or not isinstance(lease_time, (int, float))
            or lease_time <= 0
        ):
            raise ValueError(f"lease_time 必须为大于 0 的秒数，当前为 {lease_time!r}")
        ttl_ms = math.ceil(float(lease_time) * 1000)
        result = self._client.eval(_RENEW_LUA, 1, self.key, self._field(), str(ttl_ms))
        return bool(result)

    def force_unlock(self) -> None:
        """强制解锁：直接 ``DEL`` 整键（管理端用，不校验持有者）。

        无论锁归谁、重入几层，一律删除整键；若本实例的看门狗在跑会
        一并停止（代持 field 已不存在，续期必然失败，主动停更干净）。

        Note:
        供运维/管理端打破僵局用；业务代码请走 :meth:`release`。
        """
        self._client.delete(self.key)
        with self._state_lock:
            self._watchdog_field = None
            if self._watchdog_stop is not None:
                self._watchdog_stop.set()

    # ------------------------------------------------------------------
    # 上下文管理器
    # ------------------------------------------------------------------

    def __enter__(self) -> "RLock":
        """进入 with：自动**阻塞获取**（无等待上限，对齐 Redisson ``lock()``）。

        可重入：同线程嵌套 with 会层层计数，退出时层层释放。
        """
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """退出 with：释放一层（嵌套 with 逐层对应）。

        已失锁（如显式 lease_time 在临界区内到期）时释放抛
        :class:`LockNotOwnedError`——不沉默吞掉失锁事实，对齐 DLock 行为。
        """
        self.release()

    # ------------------------------------------------------------------
    # 看门狗（lease_time=None 时启用）
    # ------------------------------------------------------------------

    def _start_watchdog_if_needed(self, field: str) -> None:
        """确保看门狗线程在跑（幂等；已存活则不重启）。

        Args:
            field: 持有者 field（看门狗线程没有业务线程的 ident，必须
                显式代持，否则会用自己的 thread_ident 续期失败）。
        """
        if self._lease_time is not None:
            return  # 显式租期：无看门狗
        with self._state_lock:
            self._watchdog_field = field
            # 存活判定必须同时校验停止事件：上一代看门狗可能仍在**退出途中**
            # （release/force_unlock 已置位其停止事件，但线程还在分片睡眠的
            # 最后一片里没跑完）——只看 is_alive 会误判「看门狗在跑」而跳过
            # 启动新线程，新持锁将无看门狗、TTL 到期即丢锁（压测
            # acquire→release→立即重取场景抓到过，见 tests 回归用例）。
            if (
                self._watchdog_thread is not None
                and self._watchdog_thread.is_alive()
                and self._watchdog_stop is not None
                and not self._watchdog_stop.is_set()
            ):
                return
            self._watchdog_stop = threading.Event()
            self._watchdog_thread = threading.Thread(
                target=self._watchdog_loop,
                name=f"lizysdk-rlock-watchdog-{self._name}",
                daemon=True,
            )
            self._watchdog_thread.start()

    def _stop_watchdog(self) -> None:
        """停看门狗（幂等）：置位停止事件，线程在下一个分片睡眠后退出。"""
        with self._state_lock:
            if self._watchdog_stop is not None:
                self._watchdog_stop.set()

    def _watchdog_loop(self) -> None:
        """看门狗主循环：每 ``watchdog_timeout/3`` 对代持 field 续期一次。

        退出条件（任一）：停止事件置位（release 到 0 层/force_unlock/
        对象回收）；续期发现 field 已不存在（锁过期被他人获取——典型于
        进程冻结超过 TTL 后，对齐 Redisson 看门狗自终止行为）。
        """
        interval = self._watchdog_timeout / 3.0
        stop = self._watchdog_stop
        while stop is not None and not stop.is_set():
            field = self._watchdog_field
            if field is None or not self._renew(field, self._watchdog_timeout):
                break  # 锁没了：看门狗自行退出
            # 分片睡眠：每片检查停止事件（stop 延迟 <= _WATCHDOG_TICK），
            # 同时保持 _sleep 接缝可被测试 monkeypatch 加速
            slept = 0.0
            while slept < interval and not stop.is_set():
                chunk = min(_WATCHDOG_TICK, interval - slept)
                _sleep(chunk)
                slept += chunk

    def _renew(self, field: str, lease_seconds: float) -> bool:
        """对指定 field 续期（compare-field PEXPIRE）；失锁返回 False。"""
        ttl_ms = math.ceil(float(lease_seconds) * 1000)
        result = self._client.eval(_RENEW_LUA, 1, self.key, field, str(ttl_ms))
        return bool(result)

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _field(self) -> str:
        """当前线程的 hash field：``{instance_uuid}:{thread_ident}``。"""
        return f"{self._instance_uuid}:{threading.get_ident()}"

    def _effective_lease_ms(self) -> int:
        """有效租期毫秒：lease_time 优先，否则 watchdog_timeout。"""
        seconds = (
            self._lease_time
            if self._lease_time is not None
            else self._watchdog_timeout
        )
        return math.ceil(float(seconds) * 1000)

    def __del__(self) -> None:  # noqa: D105 兜底回收无需 docstring
        """对象回收时兜底停看门狗（daemon 兜底进程退出，双保险）。"""
        try:
            if self._watchdog_stop is not None:
                self._watchdog_stop.set()
        except Exception:  # noqa: BLE001 解释器关闭期任何失败都静默
            pass

    def __repr__(self) -> str:
        """调试视图（不回显 instance_uuid 与客户端）。"""
        return (
            f"{self.__class__.__name__}(name={self._name!r}, "
            f"lease_time={self._lease_time!r}, "
            f"watchdog_timeout={self._watchdog_timeout!r}, "
            f"held={self.is_held_by_current_thread()})"
        )
