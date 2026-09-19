"""AsyncPool —— 协程池：专用后台事件循环线程 + Semaphore 并发令牌。

实现要点与取舍（设计文档 §7）：

- **专用后台事件循环线程**（daemon，``thread_name_prefix`` 命名）：
  池自带 loop 的所有权模型（参考 :class:`asyncio.Runner`），对外桥接为
  标准 ``concurrent.futures.Future``；
- **并发上限 = 令牌**：``asyncio.Semaphore(workers)`` 承载（anyio
  CapacityLimiter 同思路）——协程不独占线程，令牌语义比固定 worker 集
  更贴切；``workers`` 命名保留为统一配置面，默认 32；
- **task_timeout 真取消**：``asyncio.wait_for`` 到期即取消任务协程
  （这是与 thread/process 池「等待语义」的本质区别），超时 Future 得
  内建 :class:`TimeoutError`（刻意不用 ``asyncio.TimeoutError``——
  Python 3.11 前它不是内建 TimeoutError 的子类，跨版本语义不一致）；
- **ctx 传播**（默认开）：提交时捕获调用方 contextvars，任务协程开始处
  逐 ``var.set`` 注入、结束时 ``var.reset`` 还原（遍历 ``ctx.items()``
  的 py3.9 兼容写法）；注入发生在 Task 自身的上下文副本内，任务之间、
  任务与 loop 基础上下文之间互不污染；
- **shutdown 顺序**：停收（submit 抛 PoolClosedError）→（可选）取消
  未获令牌的任务 → 排空（等待在途任务终态）→ 停 loop → join 线程；
  幂等。``wait=False`` 时由最后一个任务释放闸门时自动停 loop；
- ``max_tasks_per_worker``：协程无 worker 回收语义，**忽略**；
- ``initializer/initargs``：loop 线程启动时执行**一次**；失败则池不可用，
  ``submit`` 抛 :class:`~lizysdk.pools.exceptions.PoolError`。

钩子时序：``on_submit``/``on_reject`` 在提交线程；``on_start``（获得
令牌、即将首次执行时）与其余事件在 loop 线程内同步调用。

Example:
    >>> import asyncio
    >>> from lizysdk.pools import create_pool
    >>> async def add(a, b):
    ...     return a + b
    >>> pool = create_pool("async", workers=4)      # doctest: +SKIP
    >>> pool.submit(add, 1, 2).result()             # 普通线程调用 # doctest: +SKIP
    3
    >>> async def main():                           # doctest: +SKIP
    ...     return await pool.asubmit(add, 3, 4)    # 已在事件循环内
    >>> asyncio.run(main())                         # doctest: +SKIP
    7
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import threading
import time
from concurrent.futures import Future
from typing import Any, Callable, Dict, List, Optional, Tuple

from .base import Pool, TaskInfo, _MISSING
from .exceptions import PoolClosedError, PoolError

__all__ = ["AsyncPool"]


def _inject_context(ctx: Optional[contextvars.Context]) -> List[Tuple[Any, Any]]:
    """在当前协程上下文中注入提交方捕获的 contextvars（py3.9 兼容写法）。

    返回 ``[(var, token), ...]`` 供任务结束时逆序 ``var.reset(token)``。
    """
    if ctx is None:
        return []
    return [(var, var.set(value)) for var, value in ctx.items()]


class _FutureAdapter:
    """把 concurrent.Future 与 asyncio.Future 统一成三件套适配器。

    - concurrent.Future：方法本身线程安全，loop 线程可直接调用；
    - asyncio.Future：必须经其所属事件循环的 ``call_soon_threadsafe``
      跨线程修改（调用方 loop 已关闭时静默放弃——无人等待结果）。
    """

    __slots__ = ("_kind", "_cfut", "_afut", "_caller_loop")

    @classmethod
    def concurrent_future(cls, future: Future) -> "_FutureAdapter":
        adapter = cls.__new__(cls)
        adapter._kind = "c"
        adapter._cfut = future
        adapter._afut = None
        adapter._caller_loop = None
        return adapter

    @classmethod
    def asyncio_future(
        cls, future: "asyncio.Future[Any]", caller_loop: asyncio.AbstractEventLoop
    ) -> "_FutureAdapter":
        adapter = cls.__new__(cls)
        adapter._kind = "a"
        adapter._cfut = None
        adapter._afut = future
        adapter._caller_loop = caller_loop
        return adapter

    def set_result(self, value: Any) -> None:
        """写入结果（Future 已被取消则忽略）。"""
        if self._kind == "c":
            try:
                self._cfut.set_result(value)
            except Exception:  # InvalidStateError：调用方已取消
                pass
        else:
            self._to_caller(self._afut.set_result, value)

    def set_error(self, error: BaseException) -> None:
        """写入异常（Future 已被取消则忽略）。"""
        if self._kind == "c":
            try:
                self._cfut.set_exception(error)
            except Exception:
                pass
        else:
            self._to_caller(self._afut.set_exception, error)

    def try_cancel(self) -> None:
        """尽力把 Future 置为取消态（已终态则不动）。"""
        if self._kind == "c":
            if not self._cfut.done():
                self._cfut.cancel()
        else:
            self._to_caller(self._cancel_if_pending)

    def add_done_callback(self, fn: Callable[[Any], None]) -> None:
        """注册终态回调（concurrent 立即/异步皆可；asyncio 在其 loop 上调度）。"""
        if self._kind == "c":
            self._cfut.add_done_callback(fn)
        else:
            self._afut.add_done_callback(fn)

    def _cancel_if_pending(self) -> None:
        if not self._afut.done():
            self._afut.cancel()

    def _to_caller(self, fn: Callable[..., Any], *args: Any) -> None:
        assert self._caller_loop is not None
        try:
            self._caller_loop.call_soon_threadsafe(fn, *args)
        except RuntimeError:
            pass  # 调用方事件循环已关闭——结果无人消费


class AsyncPool(Pool):
    """协程池：统一接口 / 八钩子 / 真取消超时 / 并发令牌 / stats。

    Args:
        name: 池名（默认 ``"pool-async"``）。
        workers: **最大并发任务数**（令牌语义），默认 32。
        queue_size: 在途任务上限（0=无界），超出走 ``reject_policy``。
        reject_policy: ``"raise"``（默认）或 ``"block"``。
        task_timeout: 单任务超时秒数——**真取消**（``asyncio.wait_for``）。
        max_retries: 失败自动重试次数（重试等待期间持有并发令牌，文档明示）。
        retry_backoff: 指数退避基秒（async 路径用 ``asyncio.sleep`` 等待）。
        initializer/initargs: loop 线程启动时执行一次；失败则池不可用。
        max_tasks_per_worker: 协程池不适用，**忽略**（统一配置面保留）。
        propagate_context: 是否传播提交方 contextvars（默认 True）。
        thread_name_prefix: loop 线程名前缀（默认 ``"lizysdk-pool"``）。
        daemon: loop 线程是否 daemon（默认 True）。
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
        max_tasks_per_worker: Optional[int] = None,  # noqa: ARG001 协程池忽略
        propagate_context: bool = True,
        thread_name_prefix: str = "lizysdk-pool",
        daemon: bool = True,
        hooks: Optional[Dict[str, Any]] = None,
    ) -> None:
        if workers is None:
            workers = 32
        super().__init__(
            kind="async",
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
        self._ready = threading.Event()
        self._stopped = threading.Event()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._sem: Optional[asyncio.Semaphore] = None
        self._init_error: Optional[BaseException] = None
        #: 仅 loop 线程访问：任务注册表（shutdown 取消「未开始」任务用）
        self._pending: Dict["asyncio.Task[Any]", Dict[str, Any]] = {}
        self._loop_thread = threading.Thread(
            target=self._loop_main,
            name=f"{self._thread_name_prefix}-loop-{self._name}",
            daemon=self._daemon,
        )
        self._loop_thread.start()

    # ------------------------------------------------------------------
    # 后台事件循环线程
    # ------------------------------------------------------------------
    def _loop_main(self) -> None:
        """loop 线程主体：initializer → 令牌 → run_forever → 清理关闭。"""
        loop = asyncio.new_event_loop()
        self._loop = loop
        try:
            if self._initializer is not None:
                try:
                    self._initializer(*self._initargs)
                except BaseException as exc:  # noqa: BLE001 初始化失败必须让池不可用
                    self._init_error = exc
            if self._init_error is None:
                # Semaphore 需在运行中的 loop 内创建（3.9 在 __init__ 期取 loop）
                loop.run_until_complete(self._bootstrap())
                self._ready.set()
                loop.run_forever()
                # 停机收尾：取消残留任务（正常排空后应为空集）并清理异步生成器
                leftovers = [task for task in list(self._pending) if not task.done()]
                for task in leftovers:
                    task.cancel()
                if leftovers:
                    loop.run_until_complete(
                        asyncio.gather(*leftovers, return_exceptions=True)
                    )
                loop.run_until_complete(loop.shutdown_asyncgens())
            else:
                self._ready.set()
        finally:
            self._stopped.set()
            loop.close()

    async def _bootstrap(self) -> None:
        """在运行中的事件循环内创建并发令牌。"""
        self._sem = asyncio.Semaphore(self._worker_limit)

    # ------------------------------------------------------------------
    # 提交（submit / asubmit）
    # ------------------------------------------------------------------
    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Future:
        """提交协程函数任务，返回桥接的 ``concurrent.futures.Future``。

        ``fn`` 契约：``async def`` 函数（或调用后返回 coroutine 的函数）；
        普通函数提交后会在 loop 内以 ``TypeError`` 失败终态收场。
        可在任意线程调用。

        Raises:
            ValueError: ``fn`` 不可调用。
            PoolClosedError: 池已关闭。
            PoolRejectedError: 在途满且 ``reject_policy="raise"``。
            PoolError: loop 初始化失败后池不可用。
        """
        _task_id, info = self._begin_submit(fn)
        self._ready.wait()
        if self._init_error is not None:
            self._gate_release()
            raise PoolError(
                f"async 池 {self._name!r} 初始化失败，池不可用：{self._init_error}"
            )
        ctx = contextvars.copy_context() if self._propagate_context else None
        future: Future = Future()
        adapter = _FutureAdapter.concurrent_future(future)
        holder: Dict[str, Any] = {
            "started": False, "task": None, "released": False, "counted": False,
        }
        self._register_cancel_bridge(adapter, holder)
        scheduled = self._try_call_loop(
            functools.partial(self._start_task, holder, adapter, fn, args, kwargs, ctx, info)
        )
        if not scheduled:
            self._gate_release()
            raise PoolClosedError(f"async 池 {self._name!r} 已关闭，不再接受新任务")
        return future

    def asubmit(
        self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any
    ) -> "asyncio.Future[Any]":
        """在调用方事件循环内提交任务，返回 **asyncio Future**（可 await）。

        与 :meth:`submit` 共享闸门 / 计数 / 钩子 / 令牌，仅结果桥接目标
        不同：本方法把结果写回**调用方 loop** 上的 asyncio Future。

        Raises:
            RuntimeError: 当前线程没有运行中的事件循环（请在 ``async def``
                内 ``await``，或改用 :meth:`submit`）。
            PoolClosedError / PoolRejectedError / PoolError: 同 :meth:`submit`。
        """
        try:
            caller_loop = asyncio.get_running_loop()
        except RuntimeError:
            raise RuntimeError(
                "asubmit() 必须在运行中的事件循环内调用（在 async 函数中直接 "
                "await）；普通线程请使用 submit()"
            ) from None
        _task_id, info = self._begin_submit(fn)
        self._ready.wait()
        if self._init_error is not None:
            self._gate_release()
            raise PoolError(
                f"async 池 {self._name!r} 初始化失败，池不可用：{self._init_error}"
            )
        ctx = contextvars.copy_context() if self._propagate_context else None
        afut: "asyncio.Future[Any]" = caller_loop.create_future()
        adapter = _FutureAdapter.asyncio_future(afut, caller_loop)
        holder: Dict[str, Any] = {
            "started": False, "task": None, "released": False, "counted": False,
        }
        self._register_cancel_bridge(adapter, holder)
        scheduled = self._try_call_loop(
            functools.partial(self._start_task, holder, adapter, fn, args, kwargs, ctx, info)
        )
        if not scheduled:
            self._gate_release()
            raise PoolClosedError(f"async 池 {self._name!r} 已关闭，不再接受新任务")
        return afut

    def _try_call_loop(self, callback: Callable[[], Any]) -> bool:
        """线程安全地把回调排入 loop；loop 已停/已关时返回 False。"""
        loop = self._loop
        if loop is None or self._stopped.is_set():
            return False
        try:
            loop.call_soon_threadsafe(callback)
        except RuntimeError:
            return False
        return True

    def _register_cancel_bridge(
        self, adapter: _FutureAdapter, holder: Dict[str, Any]
    ) -> None:
        """用户取消 Future 时，尽力取消池内对应的协程任务。"""

        def _on_user_done(future: Any) -> None:
            if future.cancelled():
                self._try_call_loop(functools.partial(self._cancel_holder, holder))

        try:
            adapter.add_done_callback(_on_user_done)
        except Exception:
            pass  # 已终态等边缘情况：取消桥接失败不影响主流程

    def _cancel_holder(self, holder: Dict[str, Any]) -> None:
        task = holder.get("task")
        if task is not None and not task.done():
            task.cancel()

    # ------------------------------------------------------------------
    # 任务执行（loop 线程内）
    # ------------------------------------------------------------------
    def _start_task(
        self,
        holder: Dict[str, Any],
        adapter: _FutureAdapter,
        fn: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        ctx: Optional[contextvars.Context],
        info: TaskInfo,
    ) -> None:
        """loop 线程入口：创建 Task 并登记（含终态安全网回调）。

        停收竞态说明：新 submit 在闸门处被拒（PoolClosedError），而已过
        闸门的任务**必须**排空完成——loop 在排空（in_flight 清零）之前
        不会被停止，因此这里不按停机状态拒绝执行。

        安全网（``_on_task_done``）覆盖一类微妙竞态：Task 创建后、协程
        首步执行前被取消（如 shutdown 的 cancel_futures），此时协程体
        **根本不会运行**——没有它，在途闸门名额将永久泄漏、排空挂死。
        """
        holder["info"] = info
        task = self._loop.create_task(
            self._run_task(holder, adapter, fn, args, kwargs, ctx, info)
        )
        holder["task"] = task
        self._pending[task] = holder
        task.add_done_callback(functools.partial(self._on_task_done, holder, adapter))

    async def _run_task(
        self,
        holder: Dict[str, Any],
        adapter: _FutureAdapter,
        fn: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        ctx: Optional[contextvars.Context],
        info: TaskInfo,
    ) -> None:
        """任务协程主体：注入 ctx → 争令牌 → 重试循环（真取消超时）→ 终态。"""
        tokens = _inject_context(ctx)
        try:
            try:
                await self._sem.acquire()
            except asyncio.CancelledError:
                # 排队等令牌期间被取消（shutdown / 用户）
                self._finish_cancelled(holder, adapter)
                return
            holder["started"] = True
            with self._lock:
                self._running += 1
            holder["counted"] = True
            t0 = time.monotonic()
            self._dispatch(
                "on_start", {**info, "attempt": 1, "started_at": time.time()}
            )
            cancelled = False
            timed_out = False
            error: Optional[BaseException] = None
            result: Any = _MISSING
            attempt = 0
            try:
                while True:
                    attempt += 1
                    try:
                        coro = fn(*args, **kwargs)
                        if self._task_timeout is None:
                            result = await coro
                        else:
                            # 真取消：到期取消任务协程后抛 asyncio.TimeoutError
                            result = await asyncio.wait_for(coro, self._task_timeout)
                        break
                    except asyncio.CancelledError:
                        raise
                    except asyncio.TimeoutError:
                        timed_out = True
                        break
                    except Exception as exc:
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
                            if delay > 0:
                                await asyncio.sleep(delay)
                            continue
                        error = exc
                        break
            except asyncio.CancelledError:
                cancelled = True
            finally:
                with self._lock:
                    self._running -= 1
                holder["counted"] = False
                self._sem.release()
            finished_at = time.time()
            elapsed_ms = (time.monotonic() - t0) * 1000.0
            if cancelled:
                self._finish_cancelled(holder, adapter)
                return
            if timed_out:
                self._bump("timed_out")
                self._dispatch("on_timeout", {**info, "elapsed_ms": elapsed_ms})
                adapter.set_error(
                    TimeoutError(
                        f"任务 {info['task_id']} 超过 task_timeout="
                        f"{self._task_timeout}s（已真取消：协程被中止）"
                    )
                )
            elif error is not None:
                self._bump("failed")
                self._dispatch(
                    "on_error",
                    {
                        **info,
                        "attempt": attempt,
                        "finished_at": finished_at,
                        "elapsed_ms": elapsed_ms,
                        "error": str(error),
                    },
                )
                adapter.set_error(error)
            else:
                self._bump("succeeded")
                self._dispatch(
                    "on_success",
                    {
                        **info,
                        "attempt": attempt,
                        "finished_at": finished_at,
                        "elapsed_ms": elapsed_ms,
                    },
                )
                adapter.set_result(result)
            self._finish_gate(holder)
        except BaseException as exc:  # noqa: BLE001 内部错误兜底：绝不让用户 Future 永悬
            if not holder.get("released"):
                if holder.pop("counted", False):
                    with self._lock:
                        self._running = max(0, self._running - 1)
                    try:
                        self._sem.release()
                    except RuntimeError:
                        pass
                self._bump("failed")
                self._dispatch(
                    "on_error",
                    {
                        **info,
                        "attempt": 1,
                        "finished_at": time.time(),
                        "elapsed_ms": 0.0,
                        "error": f"pools 内部错误：{exc!r}",
                    },
                )
                adapter.set_error(exc)
                self._finish_gate(holder)
        finally:
            for var, token in reversed(tokens):
                var.reset(token)

    # ------------------------------------------------------------------
    # 终态出口（loop 线程内；released 标记保证闸门恰好释放一次）
    # ------------------------------------------------------------------
    def _finish_gate(self, holder: Dict[str, Any]) -> None:
        """任务终态唯一出口：幂等释放在途闸门；停收后在途清零则自停 loop。"""
        if holder.get("released"):
            return
        holder["released"] = True
        self._gate_release()
        if self._shutdown_started:
            self._stop_if_drained()

    def _finish_cancelled(self, holder: Dict[str, Any], adapter: _FutureAdapter) -> None:
        """任务以取消收场：用户 Future 置取消态，再走唯一闸门出口。"""
        adapter.try_cancel()
        self._finish_gate(holder)

    def _on_task_done(
        self, holder: Dict[str, Any], adapter: _FutureAdapter, task: "asyncio.Task[Any]"
    ) -> None:
        """Task 终态安全网（loop 线程内）：闸门必然释放、Future 必然终态。

        覆盖两类不走协程终态段的路径：
        1. Task 创建后、协程首步前被取消——协程体根本不执行；
        2. 协程内部异常逃逸（终态段自身出错）。
        正常路径已置 ``released``，此处直接跳过。
        """
        self._pending.pop(task, None)
        if holder.get("released"):
            return
        holder["released"] = True
        if not task.cancelled():
            exc = task.exception()
            if exc is not None:
                info = holder.get("info", {})
                try:
                    self._bump("failed")
                    self._dispatch(
                        "on_error",
                        {
                            **info,
                            "attempt": 1,
                            "finished_at": time.time(),
                            "elapsed_ms": 0.0,
                            "error": f"pools 内部错误：{exc!r}",
                        },
                    )
                    adapter.set_error(exc)
                except Exception:  # noqa: BLE001 安全网自身绝不抛
                    pass
            else:
                # 协程正常返回但未走终态段（理论不可达）：透传结果
                adapter.set_result(task.result())
        else:
            adapter.try_cancel()
        self._gate_release()
        if self._shutdown_started:
            self._stop_if_drained()

    def _stop_if_drained(self) -> None:
        """在 loop 线程内：在途清零则停止事件循环。"""
        if self._stopped.is_set() or self._loop is None:
            return
        with self._lock:
            drained = self._in_flight == 0
        if drained:
            self._loop.stop()

    def _cancel_not_started(self) -> None:
        """取消所有尚未获得并发令牌的任务（shutdown cancel_futures 语义）。"""
        for task, holder in list(self._pending.items()):
            if not holder["started"]:
                task.cancel()

    # ------------------------------------------------------------------
    # 停机
    # ------------------------------------------------------------------
    def _teardown(self, wait: bool, cancel_futures: bool) -> None:
        """停收 → （可选）取消未开始任务 → 排空 → 停 loop → join 线程。"""
        self._ready.wait()
        if self._loop is None or self._init_error is not None:
            return  # loop 从未运行（初始化失败）
        if cancel_futures:
            self._try_call_loop(self._cancel_not_started)
        if wait:
            with self._gate_cond:
                while self._in_flight > 0 and not self._stopped.is_set():
                    self._gate_cond.wait(0.2)
            # 排空完成（或在途已被清空）——停 loop 并收尸线程
            if not self._stopped.wait(0.2):
                self._try_call_loop(self._loop.stop)
            self._stopped.wait(5.0)
            self._loop_thread.join(5.0)
        else:
            # 后台自停：未取消的路径由最后一个任务释放闸门时触发；
            # 此刻已无在途则立即停（防止零任务池的 loop 线程泄漏）。
            self._try_call_loop(self._stop_if_drained)
