"""lizysdk.dist —— Redis 领导选举 LeaderElector（K8s Lease 语义对齐件）。

实现形态（设计文档 §1.2 决策）：语义对齐 Kubernetes Lease 选举——
**独占租约 + 心跳续期 + 失联自动下台**：

- ``SET key holder_id NX PX lease`` 抢租约（独占）；
- 持有者周期性跑 compare-holder Lua ``PEXPIRE`` 续期（复用锁的校验释放
  思想，防误续他人——心跳只续自己的租约）；
- 续期失败（网络分区/GC 停顿超过 lease 等）→ **先降级为 standby 再回调**
  ``on_losing_leadership``（杜绝「自认领导却无租约」窗口）；降级后继续
  参与竞选，租约空闲时自动重新上位（K8s contender 语义）；
- 主动 :meth:`stop` 下台：compare-holder Lua 释放租约并触发 losing 回调。

异常语义复用 :mod:`lizysdk.dist.lock` 的 :class:`LockError` 族（同子包内
import 允许，设计文档 §1.2）：本模块不新建异常类。

本模块自持：仅标准库 + dist 内 ``lock`` 的异常族；模块级不 import redis、
不依赖 lizysdk 其他子包，日志走标准 logging（logger 名 ``lizysdk.dist``）。
"""

from __future__ import annotations

import logging
import math
import os
import socket
import threading
import time
import uuid
from typing import Any, Callable, Optional

from .lock import LockError

__all__ = ["LeaderElector"]

#: 本模块使用的 logger 名（回调异常、心跳异常均记 warning，不杀线程）
_LOGGER_NAME = "lizysdk.dist"

# ---------------------------------------------------------------------------
# Lua 脚本（模块级常量）
# ---------------------------------------------------------------------------

#: 续期脚本：compare-holder 后 PEXPIRE。
#:
#: - KEYS[1]: 租约键（``{prefix}:{name}``，string 结构，值 = holder_id）
#: - ARGV[1]: 本实例 holder_id
#: - ARGV[2]: 续期毫秒数（ceil(lease*1000)）
#:
#: 只有 GET 结果与 holder_id 完全一致（租约仍归本实例）才 PEXPIRE；
#: 否则返回 0——租约已过期被他人抢走时**绝不误续他人的租约**（复用
#: DLock compare-token 校验释放的思想），调用侧据此触发降级。
_ELECT_RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

#: 释放脚本：compare-holder 后 DEL（主动下台）。
#:
#: - KEYS[1]: 租约键
#: - ARGV[1]: 本实例 holder_id
#:
#: 同上，只有租约仍归本实例才 DEL；返回 0 表示租约已被他人持有
#: （过期被抢），本实例事实上已不是领导，无需也无权释放。
_ELECT_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def _sleep(seconds: float) -> None:
    """线程睡眠（秒）。

    模块级**睡眠接缝**：心跳线程统一经它睡眠，测试 monkeypatch 加速
    心跳周期类用例（真实 time.sleep 会让选举用例变慢）。
    """
    time.sleep(seconds)


#: 心跳分片睡眠的片长（秒）：每片后检查一次停止事件，保证 stop() 延迟
#: 有上界（心跳间隔 lease/3 可能很长，不能整段睡）。
_HEARTBEAT_TICK = 0.05


class LeaderElector:
    """Redis 领导选举器（独占租约 + 心跳续期；K8s Lease 选举语义对齐件）。

    语义要点（K8s Lease 同款）：

    - **独占租约**：``SET NX PX`` 抢到即领导（``is_leader`` 为 True），
      全集群同名最多一个领导；
    - **心跳续期**：后台 daemon 线程每 ``lease/3`` 续期一次（与看门狗
      同节奏，出处同 Redisson 续期比例）；续期失败（网络分区/GC 停顿
      超过 lease）→ **先降级再回调** ``on_losing_leadership``，降级后
      继续竞选，租约空闲时自动重新上位并再次回调 ``on_become_leader``；
    - **回调安全**：两个回调在心跳线程内**同步执行**，抛异常只记
      ``logging.getLogger("lizysdk.dist").warning``，绝不杀心跳线程；
    - 非领导者可 :meth:`leader_id` 查询当前领导者（跟随者发现）。

    Args:
        client: redis 客户端实例（需支持 SET/GET/EVAL）。
        name: 选举名（非空字符串；实际键为 ``{prefix}:{name}``）——
            参与同一次选举的所有实例必须同名。
        on_become_leader: 当选回调（无参可调用对象或 None）。在心跳线程
            内同步执行，异常被吞并记 warning。
        on_losing_leadership: 失去领导回调（无参可调用对象或 None）。
            触发时机：续期失败降级、或 :meth:`stop` 主动下台。同样在
            心跳线程内同步执行（stop 触发时在 stop 调用方线程执行）；
            回调内观察到 ``is_leader`` 必为 False（先降级后回调）。
        lease: 租期（秒，> 0，默认 15.0）。心跳间隔 = ``lease/3``；
            租约到期未续即视为失联，他人可抢。
        holder_id: 持有者标识（非空字符串）。默认
            ``f"{hostname}:{pid}:{uuid4().hex[:8]}"`（进程内每实例唯一）。
        prefix: 键前缀（非空字符串，默认 ``"lizy:elect"``）。

    Raises:
        ValueError: name/prefix/holder_id 为空、lease <= 0、回调不可调用
            （中文消息）。
        LockError: 在心跳线程内（即回调里）调用 :meth:`stop`——那会
            等待心跳线程自身结束造成死锁（中文消息）。

    Example:
        >>> from lizysdk.dist import LeaderElector                         # doctest: +SKIP
        >>> elector = LeaderElector(client, "my-app",
        ...                         on_become_leader=start_jobs,
        ...                         on_losing_leadership=stop_jobs,
        ...                         lease=15.0)
        >>> elector.start()            # 后台竞选 + 心跳线程（daemon）
        >>> elector.is_leader          # 本实例当前是否领导
        False
        >>> elector.leader_id()        # 当前领导者的 holder_id（可能是别人）
        'web-1:4242:9f8e7d6c'
        >>> elector.stop()             # 主动下台：释放租约 + losing 回调
    """

    def __init__(
        self,
        client: Any,
        name: str,
        *,
        on_become_leader: Optional[Callable[[], Any]] = None,
        on_losing_leadership: Optional[Callable[[], Any]] = None,
        lease: float = 15.0,
        holder_id: Optional[str] = None,
        prefix: str = "lizy:elect",
    ) -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"name 必须为非空字符串，当前为 {name!r}")
        if on_become_leader is not None and not callable(on_become_leader):
            raise ValueError(
                f"on_become_leader 必须为可调用对象或 None，当前为 {on_become_leader!r}"
            )
        if on_losing_leadership is not None and not callable(on_losing_leadership):
            raise ValueError(
                f"on_losing_leadership 必须为可调用对象或 None，"
                f"当前为 {on_losing_leadership!r}"
            )
        if isinstance(lease, bool) or not isinstance(lease, (int, float)) or lease <= 0:
            raise ValueError(f"lease 必须为大于 0 的秒数，当前为 {lease!r}")
        if holder_id is None:
            holder_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"
        elif not isinstance(holder_id, str) or not holder_id:
            raise ValueError(f"holder_id 必须为非空字符串，当前为 {holder_id!r}")
        if not isinstance(prefix, str) or not prefix:
            raise ValueError(f"prefix 必须为非空字符串，当前为 {prefix!r}")
        self._client = client
        self._name = name
        self._on_become = on_become_leader
        self._on_losing = on_losing_leadership
        self._lease = float(lease)
        self._holder_id = holder_id
        self._prefix = prefix
        self._leader = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._state_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 元信息
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """选举名（只读）。"""
        return self._name

    @property
    def key(self) -> str:
        """实际 Redis 租约键：``{prefix}:{name}``（只读）。"""
        return f"{self._prefix}:{self._name}"

    @property
    def lease(self) -> float:
        """租期（秒，只读）。"""
        return self._lease

    @property
    def holder_id(self) -> str:
        """本实例的持有者标识（只读，默认 ``hostname:pid:uuid8``）。"""
        return self._holder_id

    @property
    def is_leader(self) -> bool:
        """本实例当前是否为领导（本地视角）。

        为 True 表示最近一次心跳续期成功；网络分区下可能与 Redis 实态
        短暂不一致（心跳周期内会自动纠正——续期失败即降级）。
        """
        return self._leader

    def leader_id(self) -> Optional[str]:
        """查询当前领导者的 holder_id（跟随者发现视角）。

        Returns:
            Optional[str]: 租约键上的持有者 id；无租约（选举空窗）为
            ``None``。本实例是领导时返回自己的 :attr:`holder_id`。
        """
        value = self._client.get(self.key)
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode("utf-8")
        return value

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def start(self) -> None:
        """启动竞选 + 心跳线程（daemon；幂等——重复 start 不再起线程）。

        首个心跳周期立即尝试抢租约；抢不到则每 ``lease/3`` 重试，直到
        上位。已在运行时调用是安全 no-op；stop 后可再次 start（重启）。
        """
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop_event = threading.Event()
            self._thread = threading.Thread(
                target=self._heartbeat_loop,
                name=f"lizysdk-elector-{self._name}",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        """主动下台并停心跳线程（幂等）：释放租约、触发 losing 回调。

        行为顺序：置停止事件等心跳线程退出 → 若仍自认领导，compare-holder
        释放租约（租约已被他人抢走则只记 warning，无键可释放）→ 降级 →
        同步执行 ``on_losing_leadership``（在 stop 调用方线程，异常吞并
        记 warning）。心跳线程先于 stop 观察到失联时，降级与回调已在
        线程内完成，本方法不会重复回调。

        Raises:
            LockError: 在心跳线程内（即选举回调里）调用本方法——join
                自身线程会死锁（异常在回调内抛出并被吞并记 warning）。
        """
        if self._thread is not None and threading.current_thread() is self._thread:
            raise LockError(
                f"不能在 LeaderElector({self._name!r}) 的回调内调用 stop()"
                f"——会等待心跳线程自身结束造成死锁"
            )
        thread = self._thread
        if thread is not None and thread.is_alive():
            self._stop_event.set()
            thread.join(timeout=5.0)
        self._thread = None
        self._demote(release=True)

    # ------------------------------------------------------------------
    # 内部：心跳循环与状态迁移
    # ------------------------------------------------------------------

    def _heartbeat_loop(self) -> None:
        """心跳主循环：抢租约 → 每 lease/3 续期；失败降级后继续竞选。"""
        interval = self._lease / 3.0
        while not self._stop_event.is_set():
            try:
                if self._leader:
                    if not self._renew_lease():
                        # 续期失败：先降级再回调（杜绝自认领导却无租约）
                        self._demote(release=False)
                else:
                    self._try_become_leader()
            except Exception as exc:  # noqa: BLE001 心跳必须扛住一切瞬态错误
                logging.getLogger(_LOGGER_NAME).warning(
                    "LeaderElector(%r) 心跳周期异常（已忽略，线程继续）: %r",
                    self._name,
                    exc,
                )
            self._sleep_stoppable(interval)

    def _try_become_leader(self) -> None:
        """抢租约：``SET key holder_id NX PX lease``；成功则升级并回调。"""
        acquired = bool(
            self._client.set(
                self.key, self._holder_id, nx=True, px=self._lease_ms()
            )
        )
        if not acquired:
            return
        with self._state_lock:
            already = self._leader
            self._leader = True
        if not already:  # 幂等保护：并发迁移只回调一次
            self._run_callback("on_become_leader", self._on_become)

    def _demote(self, release: bool) -> None:
        """降级为 standby：必要时释放租约，先降级再回调 losing。

        Args:
            release: True 表示主动下台（stop 路径），降级前先 compare-holder
                释放租约；False 表示续期失败已实际失去租约（心跳路径），
                无需也无权释放。
        """
        with self._state_lock:
            was_leader = self._leader
            self._leader = False
        if not was_leader:
            return  # 已降过级（或从未上位）：不重复回调
        if release:
            if not self._release_lease():
                logging.getLogger(_LOGGER_NAME).warning(
                    "LeaderElector(%r) 主动下台时租约已不归本实例"
                    f"（holder_id={self._holder_id!r}，可能已过期被抢），无键可释放",
                )
        self._run_callback("on_losing_leadership", self._on_losing)

    def _renew_lease(self) -> bool:
        """compare-holder Lua 续期；租约不归本实例返回 False。"""
        result = self._client.eval(
            _ELECT_RENEW_LUA, 1, self.key, self._holder_id, str(self._lease_ms())
        )
        return bool(result)

    def _release_lease(self) -> bool:
        """compare-holder Lua 释放租约；不归本实例返回 False。"""
        result = self._client.eval(_ELECT_RELEASE_LUA, 1, self.key, self._holder_id)
        return bool(result)

    def _run_callback(self, callback_name: str, callback: Optional[Callable[[], Any]]) -> None:
        """同步执行选举回调；异常只记 warning，绝不杀心跳线程。"""
        if callback is None:
            return
        try:
            callback()
        except Exception as exc:  # noqa: BLE001 回调异常不允许影响心跳
            logging.getLogger(_LOGGER_NAME).warning(
                "LeaderElector(%r) 回调 %s 抛异常（已忽略，心跳线程继续）: %r",
                self._name,
                callback_name,
                exc,
            )

    def _sleep_stoppable(self, seconds: float) -> None:
        """分片睡眠（每片 <= ``_HEARTBEAT_TICK``），片间检查停止事件。"""
        slept = 0.0
        while slept < seconds and not self._stop_event.is_set():
            chunk = min(_HEARTBEAT_TICK, seconds - slept)
            _sleep(chunk)
            slept += chunk

    def _lease_ms(self) -> int:
        """租期毫秒：``ceil(lease*1000)``。"""
        return math.ceil(self._lease * 1000)

    def __repr__(self) -> str:
        """调试视图（不回显 holder_id 与客户端，避免泄露主机信息）。"""
        return (
            f"{self.__class__.__name__}(name={self._name!r}, "
            f"lease={self._lease!r}, "
            f"is_leader={self._leader!r})"
        )
