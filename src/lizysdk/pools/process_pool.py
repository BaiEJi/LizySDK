"""ProcessPool —— 进程池：CPU 密集任务的统一包装。

实现要点与取舍（设计文档 §4/§7，诚实声明）：

- **双执行路径**：
  - 默认包装 :class:`concurrent.futures.ProcessPoolExecutor`
    （``initializer/initargs`` 原生透传）；
  - ``max_tasks_per_worker`` **仅本池生效**（worker 回收防内存膨胀，
    即 ``multiprocessing.Pool`` 的 ``maxtasksperchild`` 语义）。但该参数
    在 ``ProcessPoolExecutor`` 上是 **Python 3.11 才加入**的，为在
    3.9/3.10 上行为一致，配置了该参数时统一改用
    :class:`multiprocessing.Pool(maxtasksperchild=...)` 路线（两平台、
    多版本行为一致，避免「3.11 走原生、3.10 报 TypeError」的分裂）。
- **fn 契约**：模块级、可 pickle 的函数（Windows spawn 下子进程会重新
  import 定义模块；lambda / 局部函数不可用）；args/kwargs 亦须可 pickle。
- **ctx 不传播**：contextvars 不可 pickle，语义上也不可能跨进程——
  ``propagate_context`` 在本池被忽略（文档说明；需要传递的上下文请作为
  显式参数传入 fn）。
- **task_timeout 等待语义**：与线程池相同——超时后 Future 立即得到
  :class:`TimeoutError`，但子进程内的任务本体**不会被中断**（worker
  击杀与重启列入 v2）；僵尸任务自然结束后释放闸门。
- **on_start 语义**：父进程无法感知子进程真实的取件时刻，
  ``on_start`` 在任务提交进执行器时触发（父进程视角），``elapsed_ms``
  同为父进程视角计时。
- **重试在父进程侧调度**：失败回调后由独立的短命退避线程 sleep 并
  重新提交（不能阻塞执行器的回调线程）；重试**不保证**落在同一个
  子进程（与线程池「同一 worker 内重试」不同，诚实声明）。
- ``shutdown(wait=True)``：等待在途任务完成；``cancel_futures=True``
  取消排队未开始的任务。``multiprocessing.Pool`` 路线下 cancel 语义
  只能整体 ``terminate()``（排队与在途一并终止，未决 Future 置为取消），
  随后 ``close()+join()`` 为优雅排空——两条路径的差异在 docstring 标明。
- ``initializer`` 失败：executor 路径表现为 ``BrokenProcessPool``；
  mp.Pool 路径在部分平台上表现为 worker 反复重启、任务无响应——请保证
  initializer 稳定。

Example:
    >>> from lizysdk.pools import create_pool
    >>> def double(x):  # 实际使用必须是模块级函数（可 pickle）
    ...     return x * 2
    >>> with create_pool("process") as pool:  # doctest: +SKIP
    ...     pool.submit(double, 21).result()
    42
"""

from __future__ import annotations

import functools
import multiprocessing
import os
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor
from typing import Any, Callable, Dict, Optional, Tuple

from .base import Pool, TaskInfo, _TaskHandle

__all__ = ["ProcessPool", "process_child_call"]


def process_child_call(
    fn: Callable[..., Any], args: Tuple[Any, ...], kwargs: Dict[str, Any]
) -> Any:
    """子进程入口：反序列化后直接调用用户函数（模块级、可 pickle）。

    独立成模块级函数而非闭包，是 spawn pickle 的硬要求。
    """
    return fn(*args, **kwargs)


class ProcessPool(Pool):
    """进程池：统一接口 / 八钩子 / 在途闸门 / stats（见基类 docstring）。

    Args:
        name: 池名（默认 ``"pool-process"``）。
        workers: 进程数上限，默认 ``os.cpu_count()``。
        queue_size: 在途任务上限（0=无界），超出走 ``reject_policy``。
        reject_policy: ``"raise"``（默认）或 ``"block"``。
        task_timeout: 单任务超时秒数（**等待语义**，子进程任务不中断）。
        max_retries: 失败自动重试次数（父进程侧调度，不保证同 worker）。
        retry_backoff: 指数退避基秒。
        initializer/initargs: 子进程启动时执行（原生透传）。
        max_tasks_per_worker: 每个子进程执行多少任务后回收重建
            （``maxtasksperchild`` 语义；设置后走 multiprocessing.Pool 路线）。
        propagate_context: 进程间不可 pickle，**忽略**（文档说明）。
        thread_name_prefix: 池内部辅助线程（超时计时/重试退避）命名前缀。
        daemon: 池内部辅助线程的 daemon 位（子进程不受此参数影响）。
        hooks: 初始钩子表。

    Raises:
        ValueError: 配置非法（中文消息）。
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
        max_tasks_per_worker: Optional[int] = None,
        propagate_context: bool = True,  # noqa: ARG001 进程池不传播 ctx
        thread_name_prefix: str = "lizysdk-pool",
        daemon: bool = True,
        hooks: Optional[Dict[str, Any]] = None,
    ) -> None:
        if workers is None:
            workers = os.cpu_count() or 1
        super().__init__(
            kind="process",
            name=name,
            workers=workers,
            queue_size=queue_size,
            reject_policy=reject_policy,
            task_timeout=task_timeout,
            max_retries=max_retries,
            retry_backoff=retry_backoff,
            initializer=initializer,
            initargs=initargs,
            propagate_context=True,  # 基类参数校验用；本池语义上忽略
            thread_name_prefix=thread_name_prefix,
            daemon=daemon,
            hooks=hooks,
        )
        if max_tasks_per_worker is not None:
            if (
                isinstance(max_tasks_per_worker, bool)
                or not isinstance(max_tasks_per_worker, int)
                or max_tasks_per_worker < 1
            ):
                raise ValueError(
                    f"max_tasks_per_worker 必须为 >= 1 的 int，当前为 {max_tasks_per_worker!r}"
                )
        self._max_tasks_per_worker: Optional[int] = max_tasks_per_worker
        self._executor: Optional[ProcessPoolExecutor] = None
        self._mp_pool: Optional["multiprocessing.pool.Pool"] = None
        #: mp 路线的未决任务登记（terminate 时把未决 Future 置为取消）
        self._mp_lock = threading.Lock()
        self._mp_pending: "set[_TaskHandle]" = set()
        self._mp_terminated = False
        if self._max_tasks_per_worker is None:
            self._executor = ProcessPoolExecutor(
                max_workers=self._worker_limit,
                initializer=self._initializer,
                initargs=self._initargs,
            )
        else:
            # max_tasks_per_worker 路线：multiprocessing.Pool(maxtasksperchild)
            self._mp_pool = multiprocessing.Pool(
                processes=self._worker_limit,
                initializer=self._initializer,
                initargs=self._initargs,
                maxtasksperchild=self._max_tasks_per_worker,
            )

    # ------------------------------------------------------------------
    # 提交
    # ------------------------------------------------------------------
    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        """提交可 pickle 的模块级函数，返回 ``concurrent.futures.Future``。

        Raises:
            ValueError: ``fn`` 不可调用（pickle 错误则在任务终态以
                异常形式呈现）。
            PoolClosedError: 池已关闭。
            PoolRejectedError: 在途满且 ``reject_policy="raise"``。
        """
        _task_id, info = self._begin_submit(fn)
        future: Future = Future()
        handle = _TaskHandle(future)
        handle.started_mono = time.monotonic()
        if self._task_timeout is not None:
            self._spawn_timeout_watcher(handle, info)
        # on_start 为父进程视角（提交进执行器时），见模块 docstring
        self._dispatch(
            "on_start", {**info, "attempt": 1, "started_at": time.time()}
        )
        with self._lock:
            self._running += 1
        self._launch_attempt(handle, fn, args, kwargs, info, attempt=1)
        return future

    def _launch_attempt(
        self,
        handle: _TaskHandle,
        fn: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        info: TaskInfo,
        attempt: int,
    ) -> None:
        """把一次尝试提交进底层执行器（executor / mp.Pool 双路径）。"""
        if self._executor is not None:
            try:
                inner: Future = self._executor.submit(
                    process_child_call, fn, args, kwargs
                )
            except RuntimeError as exc:
                # 执行器已 broken / 已关闭（含停机期间的重试再提交）
                self._handle_task_failure(handle, info, attempt, exc)
                return
            inner.add_done_callback(
                functools.partial(
                    self._attempt_done, handle, fn, args, kwargs, info, attempt
                )
            )
            return
        assert self._mp_pool is not None
        callback = functools.partial(self._mp_success, handle, info, attempt)
        errback = functools.partial(
            self._mp_failure, handle, fn, args, kwargs, info, attempt
        )
        try:
            with self._mp_lock:
                if self._mp_terminated:
                    raise RuntimeError("进程池已 terminate，不再接受任务")
                self._mp_pool.apply_async(
                    process_child_call, (fn, args, kwargs),
                    callback=callback, error_callback=errback,
                )
                self._mp_pending.add(handle)
        except Exception as exc:  # noqa: BLE001 池不可用需以任务失败收场
            self._handle_task_failure(handle, info, attempt, exc)

    # ------------------------------------------------------------------
    # executor 路线回调（执行器管理线程内执行，禁止阻塞 → 重试走独立线程）
    # ------------------------------------------------------------------
    def _attempt_done(
        self,
        handle: _TaskHandle,
        fn: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        info: TaskInfo,
        attempt: int,
        inner: Future,
    ) -> None:
        """executor 内层 future 完成回调：终态 / 取消 / 失败（含重试判定）。"""
        try:
            if handle.future.cancelled():
                # 调用方已取消用户 Future（如 map fail-fast）：放弃结果
                handle.cancel_pending()
                self._release_task(handle)
                return
            if inner.cancelled():
                # shutdown(cancel_futures=True) 取消了排队任务：
                # 用户 Future 必须同步置取消（绝不能悬置）
                handle.cancel_pending()
                self._release_task(handle)
                return
            exc = inner.exception()
            if exc is None:
                self._handle_task_success(handle, info, attempt, inner.result())
                return
            self._attempt_failed(handle, fn, args, kwargs, info, attempt, exc)
        except BaseException as caught:  # noqa: BLE001 回调线程异常会被执行器吞掉 → 兜底收场
            try:
                self._handle_task_failure(handle, info, attempt, caught)
            except Exception:
                self._gate_release()

    def _attempt_failed(
        self,
        handle: _TaskHandle,
        fn: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        info: TaskInfo,
        attempt: int,
        exc: BaseException,
    ) -> None:
        """一次尝试失败：还有重试额度 → 独立线程退避后重投；否则终态失败。"""
        if attempt <= self._max_retries:
            delay = self._retry_delay(attempt)
            self._bump("retried")
            self._dispatch(
                "on_retry",
                {
                    **info,
                    "attempt": attempt,
                    "error": str(exc),
                    "next_delay_ms": delay * 1000.0,
                },
            )
            thread = threading.Thread(
                target=self._retry_after,
                args=(handle, fn, args, kwargs, info, attempt + 1, delay),
                name=f"{self._thread_name_prefix}-retry-{info['task_id']}",
                daemon=self._daemon,
            )
            thread.start()
            return
        self._handle_task_failure(handle, info, attempt, exc)

    def _retry_after(
        self,
        handle: _TaskHandle,
        fn: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        info: TaskInfo,
        next_attempt: int,
        delay: float,
    ) -> None:
        """重试退避线程：sleep 后把下一次尝试重新提交进执行器。"""
        if delay > 0:
            time.sleep(delay)
        self._launch_attempt(handle, fn, args, kwargs, info, next_attempt)

    # ------------------------------------------------------------------
    # mp.Pool 路线回调（结果处理线程内执行）
    # ------------------------------------------------------------------
    def _mp_success(
        self, handle: _TaskHandle, info: TaskInfo, attempt: int, result: Any
    ) -> None:
        """mp 路线成功回调（partial 前置参数，result 由 mp 回调尾参传入）。"""
        with self._mp_lock:
            self._mp_pending.discard(handle)
        self._handle_task_success(handle, info, attempt, result)

    def _mp_failure(
        self,
        handle: _TaskHandle,
        fn: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        info: TaskInfo,
        attempt: int,
        exc: BaseException,
    ) -> None:
        """mp 路线失败回调：撤销登记后复用重试判定。"""
        with self._mp_lock:
            self._mp_pending.discard(handle)
        if handle.future.cancelled():
            handle.cancel_pending()
            self._release_task(handle)
            return
        self._attempt_failed(handle, fn, args, kwargs, info, attempt, exc)

    # ------------------------------------------------------------------
    # 终态收尾
    # ------------------------------------------------------------------
    def _release_task(self, handle: _TaskHandle) -> None:
        """取消/放弃路径的收尾：回收 running 计数、撤销 mp 登记、释放闸门。

        process 池的 ``running`` 在 submit 时同步递增，因此取消路径也必须
        同步回收（不计 succeeded/failed 终态，与标准库取消语义一致）。
        """
        with self._lock:
            self._running = max(0, self._running - 1)
        if self._mp_pool is not None:
            with self._mp_lock:
                self._mp_pending.discard(handle)
        self._gate_release()

    # ------------------------------------------------------------------
    # 停机
    # ------------------------------------------------------------------
    def _teardown(self, wait: bool, cancel_futures: bool) -> None:
        """executor 路线原生 shutdown；mp 路线 terminate / close+join。"""
        if self._executor is not None:
            self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)
            if wait:
                self._drain_gate()
            return
        assert self._mp_pool is not None
        with self._mp_lock:
            already = self._mp_terminated
            self._mp_terminated = True
            pending = [] if already else list(self._mp_pending)
            self._mp_pending.clear()
        if not already:
            if cancel_futures:
                # mp.Pool 无逐任务取消：整体 terminate（排队与在途一并终止），
                # 未决 Future 置为取消态（诚实文档：与 executor 路线差异）
                self._mp_pool.terminate()
                for handle in pending:
                    handle.cancel_pending()
                    self._release_task(handle)
                self._mp_pool.join()
            else:
                # 优雅排空：close 后等全部任务与回调完成
                self._mp_pool.close()
                if wait:
                    self._mp_pool.join()
                    self._drain_gate()
        elif wait:
            self._mp_pool.join()
