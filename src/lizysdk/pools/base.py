"""Pool 抽象基类：三种并发池（thread / async / process）的统一底座。

职责收敛（设计文档 §7「三个池共用 base」）：

- **统一接口**：``submit / map / shutdown / stats / add_hook``、上下文管理器、
  只读属性 ``kind / name / workers``；
- **钩子注册表**：八事件（见 :data:`HOOK_EVENTS`），同事件多监听按注册序，
  钩子自身异常一律吞掉并计入 ``stats()["hook_errors"]``；
- **计数器**：submitted / running / succeeded / failed / retried / rejected /
  timed_out / hook_errors，全部在 ``self._lock`` 保护下更新，
  :meth:`Pool.stats` 返回锁保护快照；
- **在途闸门**：``queue_size > 0`` 时限制「已接受 - 未终态」的在途任务数，
  超出按 ``reject_policy`` 走 raise（:class:`~lizysdk.pools.exceptions.PoolRejectedError`）
  或 block（阻塞等待空位，池关闭时被唤醒并抛
  :class:`~lizysdk.pools.exceptions.PoolClosedError`）；
- **重试策略**：``max_retries`` 次数 + ``retry_backoff`` 指数退避
  （``base * 2^(n-1)`` 再加最多 10% 抖动）；
- **超时裁决**（thread / process 的「等待语义」）：:class:`_TaskHandle`
  原子裁决「任务完成」与「超时判定」的竞态——双方先 ``claim_*`` 抢占终态，
  抢到者负责计数、触发钩子、**然后**才把结果/异常发布到 Future，
  保证调用方从 ``Future.result()`` 返回/抛出时对应钩子一定已经触发完毕。

关键取舍（诚实声明）：

- thread / process 的 ``task_timeout`` 是**结果等待超时**：超时后 Future 立即
  得到 :class:`TimeoutError`，但任务本体不会被中断（线程本质如此，进程击杀
  列入 v2）——僵尸任务会继续占用 worker 与在途闸门名额，直到自然结束；
- 被 ``shutdown(cancel_futures=True)`` 或调用方 ``Future.cancel()`` 取消的
  **排队中**任务不计入 succeeded/failed 终态计数（与标准库语义一致）。
"""

from __future__ import annotations

import abc
import random
import threading
import time
from concurrent.futures import Future
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from .exceptions import PoolClosedError, PoolRejectedError

__all__ = ["HOOK_EVENTS", "Pool", "TaskInfo"]

#: 钩子载荷类型：普通 dict（可直接 ``log.info("task done", **info)`` 风格展开）
TaskInfo = Dict[str, Any]

#: 钩子回调类型：接收 TaskInfo dict，返回值被忽略
HookCallable = Callable[[TaskInfo], None]

#: 八个钩子事件（顺序即文档 §5 的生命周期顺序）
HOOK_EVENTS: Tuple[str, ...] = (
    "on_submit",
    "on_start",
    "on_success",
    "on_error",
    "on_retry",
    "on_timeout",
    "on_reject",
    "on_shutdown",
)

_REJECT_POLICIES: Tuple[str, ...] = ("raise", "block")

_MISSING: Any = object()


def _fn_name(fn: Callable[..., Any]) -> str:
    """取函数名用于钩子载荷（无名可调用退化为类名）。"""
    name = getattr(fn, "__name__", None)
    if isinstance(name, str) and name:
        return name
    return type(fn).__name__


def _check_int(value: Any, label: str, minimum: int, *, allow_none: bool = False) -> Optional[int]:
    """校验整数参数（显式排除 bool），非法抛中文 ValueError。"""
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{label} 必须为 int（bool 除外），当前为 {value!r}")
    if value < minimum:
        raise ValueError(f"{label} 不能小于 {minimum}，当前为 {value}")
    return value


def _check_number(
    value: Any, label: str, minimum: float, *, allow_none: bool = False
) -> Optional[float]:
    """校验数值参数（显式排除 bool），非法抛中文 ValueError。"""
    if value is None and allow_none:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} 必须为数值（int/float，bool 除外），当前为 {value!r}")
    if value < minimum:
        raise ValueError(f"{label} 不能小于 {minimum}，当前为 {value}")
    return value


def _normalize_hooks(hooks: Optional[Mapping[str, Any]]) -> Dict[str, List[HookCallable]]:
    """校验并归一化 create_pool(hooks=...) 参数：值为单个回调或回调列表。"""
    normalized: Dict[str, List[HookCallable]] = {}
    if hooks is None:
        return normalized
    if not isinstance(hooks, Mapping):
        raise ValueError(
            "hooks 必须为 dict，形如 {'on_success': 回调} 或 {'on_success': [回调, ...]}"
        )
    for event, listeners in hooks.items():
        if event not in HOOK_EVENTS:
            raise ValueError(
                f"未知钩子事件 {event!r}，合法事件：{', '.join(HOOK_EVENTS)}"
            )
        if callable(listeners):
            items: List[Any] = [listeners]
        elif isinstance(listeners, (list, tuple)):
            items = list(listeners)
        else:
            raise ValueError(
                f"hooks[{event!r}] 必须为单个回调或回调列表，当前为 {listeners!r}"
            )
        for listener in items:
            if not callable(listener):
                raise ValueError(
                    f"hooks[{event!r}] 中存在不可调用元素：{listener!r}"
                )
        normalized[event] = items
    return normalized


class _TaskHandle:
    """单任务终态裁决器（thread / process 池的「等待语义」超时核心）。

    解决的竞态：任务快完成的瞬间超时判定到期——双方都必须**先原子抢占
    终态**（``claim_done`` / ``claim_timeout``），只有抢到的一方才允许
    计数、触发钩子、随后 ``publish`` 把结果或 :class:`TimeoutError`
    写入用户 Future。发布顺序刻意安排在钩子触发**之后**，因此调用方从
    ``Future.result()`` 返回（或抛出）时，对应钩子必定已经执行完毕，
    测试与观测不会出现「结果已到、钩子还没跑」的窗口。
    """

    __slots__ = ("future", "started_mono", "event", "_lock", "_state")

    def __init__(self, future: Future) -> None:
        self.future = future
        #: 任务开始执行的单调时钟（由池在 on_start 时写入）
        self.started_mono: float = 0.0
        self.event = threading.Event()
        self._lock = threading.Lock()
        self._state = "pending"  # pending | done | timed_out

    def claim_done(self) -> bool:
        """原子抢占「正常完成」终态；已被超时抢先则返回 False。"""
        with self._lock:
            if self._state != "pending":
                return False
            self._state = "done"
            return True

    def claim_timeout(self) -> bool:
        """原子抢占「超时」终态；任务已完成或已被抢占则返回 False。"""
        with self._lock:
            if self._state != "pending":
                return False
            self._state = "timed_out"
            return True

    def cancel_pending(self) -> None:
        """抢占终态并把用户 Future 置为取消态。

        用于「排队任务被 shutdown(cancel_futures) / 调用方取消」的收场：
        先抢占终态（令等待中的超时计时线程自然退出、不再计 timed_out），
        再把 PENDING 的 Future 置为 CANCELLED——绝不能让用户 Future 悬置。
        """
        with self._lock:
            if self._state == "pending":
                self._state = "done"
        self.event.set()
        try:
            self.future.cancel()
        except Exception:
            pass

    def publish(self, result: Any = _MISSING, error: Optional[BaseException] = None) -> None:
        """把终态结果写入用户 Future 并置完成事件。

        Future 若已被调用方取消（InvalidStateError），静默忽略——取消的
        任务结果本就无人消费。可重复调用（只第一次生效）。
        """
        with self._lock:
            state = self._state
        if state == "pending":
            return  # 未抢占终态不允许发布（防御）
        try:
            if error is None:
                self.future.set_result(None if result is _MISSING else result)
            else:
                self.future.set_exception(error)
        except Exception:  # InvalidStateError：调用方已 cancel 该 Future
            pass
        finally:
            self.event.set()

    def wait_terminal(self, timeout: float) -> bool:
        """等待任务到达终态（完成或超时判定）；到期内终态返回 True。"""
        return self.event.wait(timeout)


class _MapIterator:
    """惰性保序 map 迭代器，语义同 ``concurrent.futures.Executor.map``。

    fail-fast：任一结果抛异常（或总超时耗尽）时，取消**尚未开始**的
    后续 Future 并向调用方抛出；已开始的任务无法中断（线程本质）。
    """

    __slots__ = ("_futures", "_end_time", "_pos")

    def __init__(self, futures: List[Future], end_time: Optional[float]) -> None:
        self._futures = futures
        self._end_time = end_time
        self._pos = 0

    def __iter__(self) -> "_MapIterator":
        return self

    def __next__(self) -> Any:
        if self._pos >= len(self._futures):
            raise StopIteration
        future = self._futures[self._pos]
        try:
            if self._end_time is None:
                result = future.result()
            else:
                remaining = self._end_time - time.monotonic()
                result = future.result(remaining if remaining > 0 else 0)
        except BaseException:
            self._cancel_rest()
            raise
        self._pos += 1
        return result

    def _cancel_rest(self) -> None:
        """取消后续尚未开始的 Future（尽力而为，语义同标准库）。"""
        rest = self._futures[self._pos:]
        self._futures = self._futures[: self._pos]
        for future in rest:
            future.cancel()


class Pool(abc.ABC):
    """三种并发池的统一抽象基类（不可直接实例化）。

    统一接口（三种池行为一致，见设计文档 §3）：

    - :meth:`submit` —— 提交任务，返回 ``concurrent.futures.Future``
      （async 池为跨线程桥接）
    - :meth:`map` —— 惰性保序迭代、fail-fast，语义同标准库
    - :meth:`shutdown` —— 幂等停机，默认取消未开始任务、等待在途完成
    - :meth:`stats` —— 锁保护的运行统计快照
    - :meth:`add_hook` —— 线程安全地追加事件监听
    - ``__enter__ / __exit__`` —— 上下文管理器，退出即 ``shutdown(wait=True)``
    - 只读属性 :attr:`kind` / :attr:`name` / :attr:`workers`

    钩子契约（八个事件，载荷为 TaskInfo dict）：

    - ``on_submit``  ``{task_id, pool_name, kind, fn_name, queued_at}``
    - ``on_start``   基础键 + ``{attempt, started_at}``
    - ``on_success`` 基础键 + ``{attempt, finished_at, elapsed_ms}``
    - ``on_error``   基础键 + ``{attempt, finished_at, elapsed_ms, error}``
    - ``on_retry``   基础键 + ``{attempt, error, next_delay_ms}``
    - ``on_timeout`` 基础键 + ``{elapsed_ms}``
    - ``on_reject``  ``{task_id, pool_name, kind, fn_name, reason}``
    - ``on_shutdown`` ``{pool_name, kind, stats}``

    钩子在执行线程 / 事件循环内**同步调用**，必须快——慢钩子拖吞吐由
    调用方负责；钩子抛出的任何异常都会被吞掉并计入
    ``stats()["hook_errors"]``，绝不影响任务与池本身。

    ``task_id`` 为池内单调计数（``"t-000123"`` 风格），自持生成、
    不依赖 ids 子包。
    """

    def __init__(
        self,
        *,
        kind: str,
        name: Optional[str],
        workers: int,
        queue_size: int = 0,
        reject_policy: str = "raise",
        task_timeout: Optional[float] = None,
        max_retries: int = 0,
        retry_backoff: float = 0.0,
        initializer: Optional[Callable[..., Any]] = None,
        initargs: Iterable[Any] = (),
        propagate_context: bool = True,
        thread_name_prefix: str = "lizysdk-pool",
        daemon: bool = True,
        hooks: Optional[Mapping[str, Any]] = None,
    ) -> None:
        # ---- 参数校验（中文 ValueError，与 SDK 其余模块一致） ----
        if name is None:
            name = f"pool-{kind}"
        if not isinstance(name, str) or not name:
            raise ValueError(f"name 必须为非空 str，当前为 {name!r}")
        _check_int(workers, "workers", 1)
        _check_int(queue_size, "queue_size", 0)
        if not isinstance(reject_policy, str) or reject_policy not in _REJECT_POLICIES:
            raise ValueError(
                f"reject_policy 必须为 {' 或 '.join(repr(p) for p in _REJECT_POLICIES)}，"
                f"当前为 {reject_policy!r}"
            )
        _check_number(task_timeout, "task_timeout", 0, allow_none=True)
        _check_int(max_retries, "max_retries", 0)
        _check_number(retry_backoff, "retry_backoff", 0)
        if initializer is not None and not callable(initializer):
            raise ValueError(f"initializer 必须为可调用对象或 None，当前为 {initializer!r}")
        if not isinstance(initargs, (tuple, list)):
            raise ValueError(f"initargs 必须为 tuple 或 list，当前为 {initargs!r}")
        if not isinstance(propagate_context, bool):
            raise ValueError(f"propagate_context 必须为 bool，当前为 {propagate_context!r}")
        if not isinstance(thread_name_prefix, str) or not thread_name_prefix:
            raise ValueError(
                f"thread_name_prefix 必须为非空 str，当前为 {thread_name_prefix!r}"
            )
        if not isinstance(daemon, bool):
            raise ValueError(f"daemon 必须为 bool，当前为 {daemon!r}")
        normalized_hooks = _normalize_hooks(hooks)

        # ---- 不可变配置 ----
        self._kind: str = kind
        self._name: str = name
        self._worker_limit: int = workers
        self._queue_size: int = queue_size
        self._reject_policy: str = reject_policy
        self._task_timeout: Optional[float] = task_timeout
        self._max_retries: int = max_retries
        self._retry_backoff: float = retry_backoff
        self._initializer: Optional[Callable[..., Any]] = initializer
        self._initargs: Tuple[Any, ...] = tuple(initargs)
        self._propagate_context: bool = propagate_context
        self._thread_name_prefix: str = thread_name_prefix
        self._daemon: bool = daemon

        # ---- 运行状态（self._lock 保护；钩子派发绝不在持锁状态下进行） ----
        self._lock = threading.Lock()
        #: 在途闸门条件变量（与 self._lock 绑定：block 策略等待 / 排空等待）
        self._gate_cond = threading.Condition(self._lock)
        self._in_flight: int = 0
        self._submitted: int = 0
        self._running: int = 0
        self._succeeded: int = 0
        self._failed: int = 0
        self._retried: int = 0
        self._rejected: int = 0
        self._timed_out: int = 0
        self._hook_errors: int = 0
        self._task_seq: int = 0
        self._shutdown_started: bool = False
        self._shutdown_hook_done: bool = False
        self._hooks: Dict[str, List[HookCallable]] = {event: [] for event in HOOK_EVENTS}
        for event, listeners in normalized_hooks.items():
            self._hooks[event].extend(listeners)

    # ------------------------------------------------------------------
    # 只读属性
    # ------------------------------------------------------------------
    @property
    def kind(self) -> str:
        """池类型：``"thread" | "async" | "process"``（只读）。"""
        return self._kind

    @property
    def name(self) -> str:
        """池名称（进 stats 与钩子载荷，只读）。"""
        return self._name

    @property
    def workers(self) -> int:
        """worker 数量 / 并发令牌数（只读；async 池语义为最大并发任务数）。"""
        return self._worker_limit

    # ------------------------------------------------------------------
    # 钩子
    # ------------------------------------------------------------------
    def add_hook(self, event: str, fn: HookCallable) -> None:
        """追加事件监听（线程安全；同事件多监听按注册序调用）。

        Args:
            event: 事件名，必须属于 :data:`HOOK_EVENTS` 八事件之一。
            fn: 回调，签名为 ``fn(info: dict) -> None``；其抛出的异常会被
                吞掉并计入 ``stats()["hook_errors"]``。

        Raises:
            ValueError: 事件名未知或回调不可调用。

        Example:
            >>> from lizysdk.pools import create_pool
            >>> pool = create_pool("thread")
            >>> pool.add_hook("on_success", lambda info: None)  # doctest: +SKIP
        """
        if event not in HOOK_EVENTS:
            raise ValueError(
                f"未知钩子事件 {event!r}，合法事件：{', '.join(HOOK_EVENTS)}"
            )
        if not callable(fn):
            raise ValueError(f"钩子回调必须可调用，当前为 {fn!r}")
        with self._lock:
            self._hooks[event].append(fn)

    def _dispatch(self, event: str, info: TaskInfo) -> None:
        """按注册序同步调用某事件的全部监听；单个钩子异常吞掉并计数。

        刻意不持锁快照监听列表、不持锁调用钩子——钩子内部可以安全地
        调用 ``stats()`` / ``add_hook()``。
        """
        with self._lock:
            listeners = tuple(self._hooks[event])
        for listener in listeners:
            try:
                listener(info)
            except Exception:
                with self._lock:
                    self._hook_errors += 1

    # ------------------------------------------------------------------
    # 计数与快照
    # ------------------------------------------------------------------
    def stats(self) -> Dict[str, Any]:
        """返回运行统计快照（锁保护，键见设计文档 §6）。

        Returns:
            dict: ``{kind, name, workers, submitted, running, succeeded,
            failed, retried, rejected, timed_out, hook_errors, queue_depth}``。
            其中 ``queue_depth = 在途 - 正在执行``（排队等待执行的任务数）。

        Example:
            >>> from lizysdk.pools import create_pool
            >>> pool = create_pool("thread")
            >>> try:
            ...     sorted(pool.stats())[:4]
            ... finally:
            ...     pool.shutdown()
            ['failed', 'hook_errors', 'kind', 'name']
        """
        with self._lock:
            return {
                "kind": self._kind,
                "name": self._name,
                "workers": self._worker_limit,
                "submitted": self._submitted,
                "running": self._running,
                "succeeded": self._succeeded,
                "failed": self._failed,
                "retried": self._retried,
                "rejected": self._rejected,
                "timed_out": self._timed_out,
                "hook_errors": self._hook_errors,
                "queue_depth": max(0, self._in_flight - self._running),
            }

    def _bump(self, attr: str, delta: int = 1) -> None:
        """计数器原子加减（持锁）。

        ``attr`` 传逻辑名（如 ``"succeeded"``）或私有名（``"_succeeded"``）
        均可，统一归一到私有计数器属性。
        """
        key = attr if attr.startswith("_") else "_" + attr
        with self._lock:
            setattr(self, key, getattr(self, key) + delta)

    def _next_task_id(self) -> str:
        """池内单调任务号（"t-000123" 风格，自持不依赖 ids 子包）。"""
        with self._lock:
            self._task_seq += 1
            return f"t-{self._task_seq:06d}"

    # ------------------------------------------------------------------
    # 在途闸门
    # ------------------------------------------------------------------
    def _gate_acquire(self) -> None:
        """在途闸门：queue_size>0 时限制「已接受-未终态」数量。

        - ``reject_policy="raise"``：满则同步抛
          :class:`~lizysdk.pools.exceptions.PoolRejectedError`；
        - ``reject_policy="block"``：满则阻塞等待空位；池关闭时被唤醒并抛
          :class:`~lizysdk.pools.exceptions.PoolClosedError`。

        必须在 ``submit`` 线程内调用。
        """
        while True:
            with self._gate_cond:
                if self._shutdown_started:
                    raise PoolClosedError(f"池 {self._name!r} 已关闭，不再接受新任务")
                if self._queue_size <= 0 or self._in_flight < self._queue_size:
                    self._in_flight += 1
                    return
                if self._reject_policy == "raise":
                    raise PoolRejectedError(
                        f"池 {self._name!r} 在途任务已达上限 queue_size="
                        f"{self._queue_size}，拒绝策略为 raise"
                    )
                # block：等待空位（gate release 时 notify_all 唤醒；shutdown 亦会唤醒）
                self._gate_cond.wait()

    def _gate_release(self) -> None:
        """在途闸门放行一个名额并唤醒等待者（block 策略 / 排空等待）。"""
        with self._gate_cond:
            self._in_flight -= 1
            self._gate_cond.notify_all()

    def _drain_gate(self) -> None:
        """等待在途任务全部到达终态（shutdown(wait=True) 排空用）。

        注意：任务本体挂死则此等待不会结束——对不可信任务请配置
        ``task_timeout``（async 池为真取消；thread/process 池为等待语义，
        僵尸任务自然结束后才释放名额）。
        """
        with self._gate_cond:
            while self._in_flight > 0:
                self._gate_cond.wait(0.2)

    # ------------------------------------------------------------------
    # 提交公共段（三种池共享）
    # ------------------------------------------------------------------
    def _begin_submit(self, fn: Callable[..., Any]) -> Tuple[str, TaskInfo]:
        """提交公共段：fn 校验 → 在途闸门 → submitted 计数 → on_submit 钩子。

        Returns:
            (task_id, 基础 TaskInfo)——后续事件载荷在其上叠加事件专属键。

        Raises:
            ValueError: fn 不可调用（参数校验优先于池状态）。
            PoolClosedError: 池已关闭。
            PoolRejectedError: 闸门满且策略为 raise（先触发 on_reject 再抛出）。
        """
        if not callable(fn):
            raise ValueError(f"fn 必须为可调用对象，当前为 {fn!r}")
        task_id = self._next_task_id()
        try:
            self._gate_acquire()
        except PoolRejectedError as exc:
            self._bump("rejected")
            self._dispatch(
                "on_reject",
                {
                    "task_id": task_id,
                    "pool_name": self._name,
                    "kind": self._kind,
                    "fn_name": _fn_name(fn),
                    "reason": str(exc),
                },
            )
            raise
        with self._lock:
            self._submitted += 1
        info: TaskInfo = {
            "task_id": task_id,
            "pool_name": self._name,
            "kind": self._kind,
            "fn_name": _fn_name(fn),
            "queued_at": time.time(),
        }
        self._dispatch("on_submit", dict(info))
        return task_id, info

    # ------------------------------------------------------------------
    # 重试策略
    # ------------------------------------------------------------------
    def _retry_delay(self, failed_attempt: int) -> float:
        """计算第 ``failed_attempt`` 次失败后的重试等待秒数。

        指数退避：``base * 2^(n-1)``，叠加 0 ~ 10% 的均匀抖动避免
        惊群；``retry_backoff <= 0`` 时恒为 0（立即重试）。
        """
        if self._retry_backoff <= 0:
            return 0.0
        base = self._retry_backoff * (2 ** (failed_attempt - 1))
        return base + random.uniform(0.0, base * 0.1)

    # ------------------------------------------------------------------
    # 终态公共段（thread / process 池共享；async 池真取消路径自管）
    # ------------------------------------------------------------------
    def _handle_task_success(
        self, handle: _TaskHandle, info: TaskInfo, attempt: int, result: Any
    ) -> None:
        """任务最终成功：抢占终态 → 计数/钩子 → 发布结果 → 释放闸门。

        若超时判定已抢先（claim 失败），仅收尾 running 计数与闸门——
        僵尸任务的最终返回值被丢弃（超时已是它的终态事件）。
        """
        with self._lock:
            self._running -= 1
        if handle.claim_done():
            with self._lock:
                self._succeeded += 1
            self._dispatch(
                "on_success",
                {
                    **info,
                    "attempt": attempt,
                    "finished_at": time.time(),
                    "elapsed_ms": (time.monotonic() - handle.started_mono) * 1000.0,
                },
            )
            handle.publish(result=result)
        self._gate_release()

    def _handle_task_failure(
        self, handle: _TaskHandle, info: TaskInfo, attempt: int, error: BaseException
    ) -> None:
        """任务最终失败（重试耗尽或不可重试）：对偶于成功路径。"""
        with self._lock:
            self._running -= 1
        if handle.claim_done():
            with self._lock:
                self._failed += 1
            self._dispatch(
                "on_error",
                {
                    **info,
                    "attempt": attempt,
                    "finished_at": time.time(),
                    "elapsed_ms": (time.monotonic() - handle.started_mono) * 1000.0,
                    "error": str(error),
                },
            )
            handle.publish(error=error)
        self._gate_release()

    def _spawn_timeout_watcher(self, handle: _TaskHandle, info: TaskInfo) -> None:
        """启动等待语义的超时计时线程（thread / process 池）。

        **诚实声明**：超时后 Future 立即得到 :class:`TimeoutError`，但
        任务本体不会被中断——它会继续占用 worker 与在途名额直到自然
        结束（Python 线程本质；进程击杀列入 v2）。
        """
        thread = threading.Thread(
            target=self._timeout_watch,
            args=(handle, info),
            name=f"{self._thread_name_prefix}-timeout-{info['task_id']}",
            daemon=True,
        )
        thread.start()

    def _timeout_watch(self, handle: _TaskHandle, info: TaskInfo) -> None:
        """计时线程主体：到期且抢占成功才计 timed_out / 触发 on_timeout。"""
        if handle.wait_terminal(timeout=self._task_timeout or 0.0):
            return  # 任务先到终态（正常完成/失败），无事可做
        if handle.claim_timeout():
            with self._lock:
                self._timed_out += 1
            self._dispatch(
                "on_timeout",
                {
                    **info,
                    "elapsed_ms": (time.monotonic() - handle.started_mono) * 1000.0,
                },
            )
            handle.publish(
                error=TimeoutError(
                    f"任务 {info['task_id']} 超过 task_timeout={self._task_timeout}s"
                    f"（等待语义：任务本体不会被中断，见 Pool docstring）"
                )
            )

    # ------------------------------------------------------------------
    # 统一接口
    # ------------------------------------------------------------------
    @abc.abstractmethod
    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        """提交任务，返回 ``concurrent.futures.Future``（抽象方法）。"""

    def map(
        self,
        fn: Callable[..., Any],
        *iterables: Iterable[Any],
        timeout: Optional[float] = None
    ) -> Iterable[Any]:
        """惰性保序 map，语义同 ``concurrent.futures.Executor.map``。

        - 结果按提交顺序产出，迭代时才等待对应 Future（惰性）；
        - ``timeout`` 限制**整个迭代过程**的剩余时间预算（从调用
          ``map()`` 起算，非单任务超时）；
        - fail-fast：任一结果异常即取消尚未开始的后续任务并向调用方
          抛出（已开始的任务无法中断）。

        Args:
            fn: 映射函数（fn 契约随池类型，见各池 docstring）。
            *iterables: 一个或多个可迭代对象（同内置 ``map``）。
            timeout: 总时间预算（秒），``None`` 表示不限制。

        Raises:
            ValueError: ``timeout`` 为负数。
            TimeoutError: 总预算耗尽时由迭代过程抛出。

        Example:
            >>> from lizysdk.pools import create_pool
            >>> with create_pool("thread") as pool:
            ...     list(pool.map(lambda x: x * 2, range(5)))
            [0, 2, 4, 6, 8]
        """
        if timeout is not None:
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
                raise ValueError(f"timeout 必须为数值，当前为 {timeout!r}")
            if timeout < 0:
                raise ValueError(f"timeout 不能为负数，当前为 {timeout}")
            end_time: Optional[float] = timeout + time.monotonic()
        else:
            end_time = None
        futures = [self.submit(fn, *args) for args in zip(*iterables)]
        return _MapIterator(futures, end_time)

    def shutdown(self, wait: bool = True, cancel_futures: bool = True) -> None:
        """幂等停机（可重复调用，语义安全）。

        默认 ``cancel_futures=True``：取消尚未开始的任务、等待在途任务
        完成（与标准库 ``Executor.shutdown`` 一致）；``on_shutdown``
        钩子在**首次**停机流程结束时触发一次（携带最终 stats）。

        Args:
            wait: 是否等待在途任务完成与底层执行器收尾。
            cancel_futures: 是否取消排队中尚未开始的任务。
        """
        with self._lock:
            first = not self._shutdown_started
            self._shutdown_started = True
            # 唤醒 block 策略下阻塞的提交者（它们将抛 PoolClosedError）
            self._gate_cond.notify_all()
        self._teardown(wait=wait, cancel_futures=cancel_futures)
        if first:
            with self._lock:
                hook_pending = not self._shutdown_hook_done
                self._shutdown_hook_done = True
            if hook_pending:
                self._dispatch(
                    "on_shutdown",
                    {"pool_name": self._name, "kind": self._kind, "stats": self.stats()},
                )

    @abc.abstractmethod
    def _teardown(self, wait: bool, cancel_futures: bool) -> None:
        """池特定的停机实现（由幂等的 :meth:`shutdown` 模板调用）。"""

    # ------------------------------------------------------------------
    # 上下文管理器
    # ------------------------------------------------------------------
    def __enter__(self) -> "Pool":
        """进入上下文返回自身。"""
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        """退出上下文即 ``shutdown(wait=True)``，不吞调用方异常。"""
        self.shutdown(wait=True)
        return False

    def __repr__(self) -> str:
        state = "closed" if self._shutdown_started else "open"
        return (
            f"<{type(self).__name__} kind={self._kind!r} name={self._name!r} "
            f"workers={self._worker_limit} {state}>"
        )
