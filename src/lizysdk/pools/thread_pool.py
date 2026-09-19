"""ThreadPool —— 线程池（自管 worker 线程，复用 concurrent.futures 协议）。

实现要点与取舍（诚实声明）：

- **自管 worker 线程而非包装 ``ThreadPoolExecutor``**：标准库执行器自
  3.9 起 worker 线程为**非 daemon** 且不开放控制（bpo-39812），无法满足
  ``daemon`` 配置位；因此本池复用 ``concurrent.futures.Future`` 的完整
  状态协议（PENDING/RUNNING/CANCELLED、``set_running_or_notify_cancel``
  排队取消语义），但 worker 的创建、命名、daemon 位、懒扩容（有积压才
  扩，上限 ``workers``）与停机收尸由本模块自管，钩子/重试/闸门得以
  内聚在任务边界。
- **fn 契约**：任意普通可调用对象（含 lambda / 局部函数 / bound method）。
- **ctx 传播**（``propagate_context=True``，默认）：提交时
  :func:`contextvars.copy_context`，任务内 ``ctx.run(fn, ...)`` 执行——
  trace_id 等上下文随任务进池不丢失，且任务内的修改不影响提交线程。
  同一任务的多次重试串行复用同一 Context（Context 不允许并发重入，
  串行重入是安全的）。
- **task_timeout 等待语义**：超时后 Future 立即得到 :class:`TimeoutError`
  并计 ``timed_out``，但任务**本体不会被中断**（线程无法安全击杀）——
  僵尸任务继续占用 worker 与在途闸门名额直至自然结束，结束时**不会**
  再计 succeeded/failed（超时已是其终态）。需要真取消请用 async 池。
- **重试在 worker 内 sleep**：简单且语义清晰（「任务自己重试」），代价
  是重试等待期间占用 worker——文档明示；不占位的重试属调度器职责。
- ``max_tasks_per_worker``：线程不适用（无内存膨胀回收语义），**忽略**。
- ``initializer/initargs``：每个 worker 线程启动时执行一次；初始化失败
  则池不可用——已排队任务以失败终态收场，后续 ``submit`` 抛
  :class:`~lizysdk.pools.exceptions.PoolError`。

Example:
    >>> from lizysdk.pools import create_pool
    >>> with create_pool("thread", workers=2) as pool:
    ...     fut = pool.submit(pow, 2, 10)
    ...     fut.result()
    1024
"""

from __future__ import annotations

import contextvars
import os
import queue
import threading
import time
from concurrent.futures import Future
from typing import Any, Callable, Dict, List, Optional, Tuple

from .base import Pool, TaskInfo, _TaskHandle
from .exceptions import PoolClosedError, PoolError

__all__ = ["ThreadPool"]

#: 停机哨兵：worker 取到它即退出
_SHUTDOWN: Any = object()


class _WorkItem:
    """队列工作项：函数 + 参数 + 提交线程上下文 + 终态裁决器 + 基础载荷。"""

    __slots__ = ("fn", "args", "kwargs", "ctx", "info", "handle")

    def __init__(
        self,
        fn: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        ctx: Optional[contextvars.Context],
        info: TaskInfo,
        handle: _TaskHandle,
    ) -> None:
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.ctx = ctx
        self.info = info
        self.handle = handle

    def call(self) -> Any:
        """在提交线程的上下文副本中调用用户函数（无传播则直接调用）。"""
        if self.ctx is not None:
            return self.ctx.run(self.fn, *self.args, **self.kwargs)
        return self.fn(*self.args, **self.kwargs)


class ThreadPool(Pool):
    """线程池：统一接口 / 八钩子 / 重试 / 在途闸门 / stats（见基类 docstring）。

    Args:
        name: 池名（默认 ``"pool-thread"``，进 stats 与钩子载荷）。
        workers: worker 线程数上限，默认 ``min(32, cpu数+4)``（对齐标准库）。
        queue_size: 在途任务上限（0=无界），超出走 ``reject_policy``。
        reject_policy: ``"raise"``（默认，抛 PoolRejectedError）或 ``"block"``
            （阻塞等待空位）。
        task_timeout: 单任务超时秒数（**等待语义**，任务本体不中断，见模块
            docstring）；``None`` 不超时。
        max_retries: 失败自动重试次数（重试在同一 worker 内 sleep 等待）。
        retry_backoff: 指数退避基秒（``base * 2^(n-1)`` + 抖动）。
        initializer/initargs: 每个 worker 线程启动时执行；失败则池不可用。
        max_tasks_per_worker: 线程池不适用，**忽略**（仅为统一配置面保留）。
        propagate_context: 是否传播提交线程的 contextvars（默认 True）。
        thread_name_prefix: worker 线程名前缀（默认 ``"lizysdk-pool"``）。
        daemon: worker 线程是否 daemon（默认 True）。
        hooks: 初始钩子表，如 ``{"on_error": 回调}``。

    Raises:
        ValueError: 配置非法（workers<1、未知 reject_policy、负超时等，中文消息）。
    """

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        workers: Optional[int] = None,
        queue_size: int = 0,
        reject_policy: str = "raise",
        task_timeout: Optional[float] = None,
        max_retries: int = 0,
        retry_backoff: float = 0.0,
        initializer: Optional[Callable[..., Any]] = None,
        initargs: Tuple[Any, ...] = (),
        max_tasks_per_worker: Optional[int] = None,  # noqa: ARG001 线程池忽略
        propagate_context: bool = True,
        thread_name_prefix: str = "lizysdk-pool",
        daemon: bool = True,
        hooks: Optional[Dict[str, Any]] = None,
    ) -> None:
        if workers is None:
            workers = min(32, (os.cpu_count() or 1) + 4)
        super().__init__(
            kind="thread",
            name=name,
            workers=workers,
            queue_size=queue_size,
            reject_policy=reject_policy,
            task_timeout=task_timeout,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            initializer=initializer,
            initargs=initargs,
            propagate_context=propagate_context,
            thread_name_prefix=thread_name_prefix,
            daemon=daemon,
            hooks=hooks,
        )
        self._work_queue: "queue.SimpleQueue[Any]" = queue.SimpleQueue()
        #: 保护 线程注册/空闲计数/停机哨兵投放 与 队列排空的互斥（关闭竞态安全）
        self._qlock = threading.Lock()
        self._threads: List[threading.Thread] = []
        self._spawned: int = 0
        self._idle: int = 0
        self._sentinels_queued: bool = False
        self._init_error: Optional[BaseException] = None

    # ------------------------------------------------------------------
    # 提交
    # ------------------------------------------------------------------
    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        """提交普通函数任务，返回 ``concurrent.futures.Future``。

        语义时序：``on_submit``（提交线程）→ 入队 → worker 取出后
        ``set_running_or_notify_cancel``（排队期间被取消则跳过）→
        ``on_start`` → 执行（含重试循环）→ ``on_success`` / ``on_error``
        / ``on_timeout`` 之一 → Future 终态。

        Raises:
            ValueError: ``fn`` 不可调用。
            PoolClosedError: 池已关闭（含 block 策略阻塞期间池被关闭）。
            PoolRejectedError: 在途满且 ``reject_policy="raise"``。
            PoolError: worker 初始化失败后池不可用。
        """
        _task_id, info = self._begin_submit(fn)
        ctx = contextvars.copy_context() if self._propagate_context else None
        future: Future = Future()
        handle = _TaskHandle(future)
        item = _WorkItem(fn, args, kwargs, ctx, info, handle)
        with self._qlock:
            if self._init_error is not None:
                self._gate_release()
                raise PoolError(
                    f"线程池 {self._name!r} 的 worker 初始化失败，池不可用：{self._init_error}"
                )
            if self._sentinels_queued:
                # 与停机排空互斥：绝不把任务排在停机哨兵之后
                self._gate_release()
                raise PoolClosedError(f"池 {self._name!r} 已关闭，不再接受新任务")
            self._work_queue.put(item)
            self._maybe_spawn_locked()
        return future

    # ------------------------------------------------------------------
    # worker 线程
    # ------------------------------------------------------------------
    def _maybe_spawn_locked(self) -> None:
        """懒扩容：有积压且未达上限且无空闲 worker 时再起一个线程。

        调用方必须持有 ``self._qlock``。判定式 ``qsize > idle`` 意为
        「积压超过空闲数」——首任务必然触发，平稳期不空转建线程。
        """
        if self._sentinels_queued or self._spawned >= self._worker_limit:
            return
        if self._work_queue.qsize() <= self._idle:
            return
        self._spawned += 1
        thread = threading.Thread(
            target=self._worker_main,
            name=f"{self._thread_name_prefix}-{self._name}-{self._spawned}",
            daemon=self._daemon,
        )
        self._threads.append(thread)
        thread.start()

    def _worker_main(self) -> None:
        """worker 线程主体：initializer → 循环取件执行 → 哨兵退出。"""
        if self._initializer is not None:
            try:
                self._initializer(*self._initargs)
            except BaseException as exc:  # noqa: BLE001 初始化失败必须让池不可用
                self._on_initializer_failed(exc)
                return
        while True:
            with self._qlock:
                self._idle += 1
            try:
                item = self._work_queue.get()
            finally:
                with self._qlock:
                    self._idle -= 1
            if item is _SHUTDOWN:
                return
            future = item.handle.future
            if not future.set_running_or_notify_cancel():
                # 排队期间被调用方 cancel（map fail-fast / 用户 cancel）
                self._gate_release()
                continue
            try:
                self._run_item(item)
            except BaseException as exc:  # noqa: BLE001 内部错误兜底：不得杀死 worker 或泄漏闸门
                if not item.handle.started_mono:
                    item.handle.started_mono = time.monotonic()
                try:
                    self._handle_task_failure(item.handle, item.info, 1, exc)
                except Exception:
                    self._gate_release()

    def _on_initializer_failed(self, exc: BaseException) -> None:
        """initializer 失败：记录错误并把已排队任务全部以失败终态收场。"""
        with self._qlock:
            if self._init_error is None:
                self._init_error = exc
            doomed: List[_WorkItem] = []
            while True:
                try:
                    pending = self._work_queue.get_nowait()
                except queue.Empty:
                    break
                if pending is not _SHUTDOWN:
                    doomed.append(pending)
        for item in doomed:
            future = item.handle.future
            if not future.set_running_or_notify_cancel():
                self._gate_release()
                continue
            item.handle.started_mono = time.monotonic()
            self._bump("running")
            self._handle_task_failure(
                item.handle, item.info, 1,
                PoolError(f"worker 线程初始化失败：{exc}"),
            )

    # ------------------------------------------------------------------
    # 任务执行（on_start → 重试循环 → 终态）
    # ------------------------------------------------------------------
    def _run_item(self, item: _WorkItem) -> None:
        """执行单个任务：钩子埋点、ctx 执行、重试循环、超时裁决。"""
        info = item.info
        handle = item.handle
        handle.started_mono = time.monotonic()
        self._dispatch(
            "on_start", {**info, "attempt": 1, "started_at": time.time()}
        )
        with self._lock:
            self._running += 1
        if self._task_timeout is not None:
            self._spawn_timeout_watcher(handle, info)
        attempt = 0
        while True:
            attempt += 1
            try:
                result = item.call()
            except Exception as exc:
                if attempt <= self._max_retries:
                    self._schedule_retry(info, attempt, exc)
                    continue
                self._handle_task_failure(handle, info, attempt, exc)
                return
            except BaseException as exc:  # KeyboardInterrupt/SystemExit 不重试
                self._handle_task_failure(handle, info, attempt, exc)
                return
            else:
                self._handle_task_success(handle, info, attempt, result)
                return

    def _schedule_retry(self, info: TaskInfo, failed_attempt: int, exc: Exception) -> None:
        """记录 on_retry 并在 worker 内 sleep 退避（占用 worker，文档明示）。"""
        delay = self._retry_delay(failed_attempt)
        self._bump("retried")
        self._dispatch(
            "on_retry",
            {
                **info,
                "attempt": failed_attempt,
                "error": str(exc),
                "next_delay_ms": delay * 1000.0,
            },
        )
        if delay > 0:
            time.sleep(delay)

    # ------------------------------------------------------------------
    # 停机
    # ------------------------------------------------------------------
    def _teardown(self, wait: bool, cancel_futures: bool) -> None:
        """停收 → （可选）取消排队任务 → 投放停机哨兵 → join worker。"""
        with self._qlock:
            threads = list(self._threads)
            if not self._sentinels_queued:
                self._sentinels_queued = True
                if cancel_futures:
                    while True:
                        try:
                            pending = self._work_queue.get_nowait()
                        except queue.Empty:
                            break
                        if pending is _SHUTDOWN:
                            continue
                        pending.handle.future.cancel()
                        self._gate_release()
                # 每个 worker 一个哨兵（已因初始化失败退出的 worker 多出的
                # 哨兵无害——无人消费即沉底）
                for _ in range(self._spawned):
                    self._work_queue.put(_SHUTDOWN)
        if wait:
            for thread in threads:
                thread.join()
