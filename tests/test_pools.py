"""lizysdk.pools 测试：统一并发池（thread / async / process）× 配置 × 钩子 × 并发正确性。

覆盖范围（设计文档 §8 全落地）：
1. 工厂与配置校验：非法 kind / workers<1 / 未知 reject_policy / 负超时 /
   负 retry / 负 queue_size / 非法 hooks（参数化）；三种 kind 默认值与
   stats 键集；只读属性；Pool 抽象基类不可实例化；
2. thread：结果与异常传播、args/kwargs 透传、ctx 传播（任务内可读且
   不外泄、propagate_context=False 关闭）、max_retries=2 的 on_retry
   序列与最终成功/失败、retry_backoff 生效（前两次失败第三次成功）、
   queue_size 的 raise/block 两策略（block 在池关闭时抛 PoolClosedError）、
   并发 8×50 提交 stats 守恒（submitted==400 且 succeeded+failed==400）、
   shutdown 幂等且 on_shutdown 只触发一次、关闭后 submit 抛
   PoolClosedError、map 保序与 fail-fast、排队任务 cancel 后不执行、
   上下文管理器退出即关、task_timeout 等待语义（Future 得 TimeoutError、
   僵尸跑完不重复计数、池仍可用）、worker 线程命名与 daemon 位、
   initializer 成功可见 / 失败致池不可用；
3. async：submit 返回值桥接（普通线程 .result()）、异常传播、
   task_timeout 真取消（睡眠任务 0.2s 超时被取消、池仍可用）、
   asubmit 在 asyncio.run 内 await、asubmit 无事件循环抛 RuntimeError、
   并发令牌验证（Semaphore(workers=2) 同时运行数峰值==2）、ctx 传播、
   shutdown 排空（20 个快任务全部完成）与取消未开始任务、
   重试 + on_retry、map 保序、reject 策略、非协程函数以 TypeError 失败；
4. process（session 级 fixture 复用池，Windows spawn）：CPU 密集
   （质数计数）结果正确、模块级 initializer 设全局标志在任务内可见、
   用户函数异常传播、stats 记账、map 保序、max_tasks_per_worker=1
   多任务全部成功（mp 回收路径）、shutdown 后 submit 抛 PoolClosedError；
5. hooks：八事件全触发顺序（成功链 / 重试链 / 超时 / 拒绝 / 停机）、
   TaskInfo 载荷键与 task_id 格式（t-000123 风格单调）、钩子抛异常
   不影响任务且 hook_errors 计数、多监听按注册序（config 先、add_hook 后）；
6. run_all：混合任务形态保序、return_exceptions 两模式、async kind、
   空任务列表、非法任务形态抛 ValueError。
"""

from __future__ import annotations

import asyncio
import contextvars
import os
import threading
import time
from concurrent.futures import CancelledError, Future
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest

from lizysdk.pools import (
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

# ---------------------------------------------------------------------------
# 通用辅助
# ---------------------------------------------------------------------------


def wait_until(predicate: Callable[[], bool], timeout: float = 5.0, interval: float = 0.01) -> bool:
    """轮询等待条件成立（防抖动：给异步收敛留出确定性窗口）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def submit_in_thread(pool: Any, fn: Callable[..., Any], *args: Any) -> Dict[str, Any]:
    """在独立线程里 submit，捕获返回的 Future 或抛出的异常。"""
    box: Dict[str, Any] = {}

    def _run() -> None:
        try:
            box["future"] = pool.submit(fn, *args)
        except BaseException as exc:  # noqa: BLE001 测试需要捕获一切
            box["error"] = exc

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    box["thread"] = thread
    return box


class EventRecorder:
    """线程安全的钩子记录器：按事件名收集 (event, info) 与纯事件序。"""

    def __init__(self, events: Tuple[str, ...]) -> None:
        self._lock = threading.Lock()
        self.records: List[Tuple[str, Dict[str, Any]]] = []
        self.hooks = {event: self._make_hook(event) for event in events}

    def _make_hook(self, event: str) -> Callable[[Dict[str, Any]], None]:
        def hook(info: Dict[str, Any]) -> None:
            with self._lock:
                self.records.append((event, dict(info)))

        return hook

    @property
    def names(self) -> List[str]:
        with self._lock:
            return [event for event, _ in self.records]

    def infos(self, event: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [info for name, info in self.records if name == event]


# ---------------------------------------------------------------------------
# 进程池测试用的模块级函数（Windows spawn 必须可导入、可 pickle）
# ---------------------------------------------------------------------------

_PROC_INIT_STATE: Dict[str, str] = {}


def _proc_initializer(token: str) -> None:
    """进程池 initializer：在每个子进程内设置全局标志。"""
    _PROC_INIT_STATE["token"] = token


def _proc_init_token() -> Optional[str]:
    """读取子进程内由 initializer 设置的标志。"""
    return _PROC_INIT_STATE.get("token")


def count_primes(limit: int) -> int:
    """CPU 密集：埃氏筛统计 limit 以内的质数个数。"""
    if limit < 2:
        return 0
    sieve = bytearray([1]) * limit
    sieve[0] = sieve[1] = 0
    i = 2
    while i * i < limit:
        if sieve[i]:
            sieve[i * i :: i] = bytearray(len(sieve[i * i :: i]))
        i += 1
    return sum(sieve)


def proc_echo(x: Any) -> Any:
    """原样返回。"""
    return x


def proc_echo_pid(x: Any) -> Tuple[Any, int]:
    """返回 (x, 子进程 pid)——验证 worker 回收。"""
    return x, os.getpid()


def proc_raise(msg: str) -> None:
    """抛出用户异常。"""
    raise ValueError(msg)


# ===========================================================================
# 1. 工厂与配置校验
# ===========================================================================


@pytest.mark.parametrize("kind", ["Thread", "threads", "", "greenlet", None, 1])
def test_factory_invalid_kind(kind: Any) -> None:
    """非法 kind 抛中文 ValueError。"""
    with pytest.raises(ValueError, match="kind"):
        create_pool(kind)


@pytest.mark.parametrize("kind", ["thread", "async", "process"])
@pytest.mark.parametrize("bad", [0, -1, True, 1.5, "8", [4]])
def test_factory_invalid_workers(kind: str, bad: Any) -> None:
    """workers 必须为 >= 1 的 int（bool 除外），三种池一致。"""
    with pytest.raises(ValueError, match="workers"):
        create_pool(kind, workers=bad)


@pytest.mark.parametrize("bad", ["abort", "", None, 1])
def test_factory_invalid_reject_policy(bad: Any) -> None:
    """未知 reject_policy 抛 ValueError。"""
    with pytest.raises(ValueError, match="reject_policy"):
        create_pool("thread", reject_policy=bad)


def test_factory_negative_numbers_rejected() -> None:
    """负 task_timeout / max_retries / retry_backoff / queue_size 全部拒绝。"""
    with pytest.raises(ValueError, match="task_timeout"):
        create_pool("thread", task_timeout=-0.5)
    with pytest.raises(ValueError, match="max_retries"):
        create_pool("thread", max_retries=-1)
    with pytest.raises(ValueError, match="retry_backoff"):
        create_pool("thread", retry_backoff=-1.0)
    with pytest.raises(ValueError, match="queue_size"):
        create_pool("thread", queue_size=-1)


def test_factory_invalid_hooks_and_add_hook() -> None:
    """hooks 未知事件 / 不可调用元素 / add_hook 非法参数全部拒绝。"""
    with pytest.raises(ValueError, match="未知钩子事件"):
        create_pool("thread", hooks={"on_bogus": lambda info: None})
    with pytest.raises(ValueError, match="不可调用"):
        create_pool("thread", hooks={"on_success": [lambda info: None, 123]})
    with pytest.raises(ValueError, match="回调列表"):
        create_pool("thread", hooks={"on_success": 123})
    with pytest.raises(ValueError, match="hooks"):
        create_pool("thread", hooks=[("on_success", print)])  # type: ignore[arg-type]
    pool = create_pool("thread")
    try:
        with pytest.raises(ValueError, match="未知钩子事件"):
            pool.add_hook("on_no", lambda info: None)
        with pytest.raises(ValueError, match="可调用"):
            pool.add_hook("on_success", "not-callable")  # type: ignore[arg-type]
    finally:
        pool.shutdown()


def test_factory_defaults_names_and_workers() -> None:
    """三种池默认名 / 默认 workers 与设计文档一致。"""
    with ThreadPool() as tp:
        assert tp.kind == "thread"
        assert tp.name == "pool-thread"
        assert tp.workers == min(32, (os.cpu_count() or 1) + 4)
    with AsyncPool() as ap:
        assert ap.kind == "async"
        assert ap.name == "pool-async"
        assert ap.workers == 32
    pp = ProcessPool()
    try:
        assert pp.kind == "process"
        assert pp.name == "pool-process"
        assert pp.workers == os.cpu_count()
    finally:
        pp.shutdown()


def test_stats_shape_and_submit_returns_std_future() -> None:
    """stats 键集与设计文档 §6 完全一致；submit 返回标准库 Future。"""
    expected_keys = {
        "kind", "name", "workers", "submitted", "running", "succeeded",
        "failed", "retried", "rejected", "timed_out", "hook_errors",
        "queue_depth",
    }
    with create_pool("thread") as pool:
        assert set(pool.stats()) == expected_keys
        assert isinstance(pool.submit(lambda: 1), Future)
    with create_pool("async") as pool:
        assert set(pool.stats()) == expected_keys
        assert isinstance(pool.submit(_async_one), Future)


async def _async_one() -> int:
    return 1


def test_readonly_properties() -> None:
    """kind / name / workers 只读。"""
    with create_pool("thread", name="ro", workers=2) as pool:
        for attr in ("kind", "name", "workers"):
            with pytest.raises(AttributeError):
                setattr(pool, attr, "x")


def test_pool_base_not_instantiable() -> None:
    """Pool 是抽象基类，直接实例化抛 TypeError。"""
    with pytest.raises(TypeError):
        Pool(kind="thread", name="x", workers=1)  # type: ignore[abstract]


def test_max_tasks_per_worker_ignored_by_thread_and_async() -> None:
    """max_tasks_per_worker 在 thread/async 池被忽略（统一配置面）。"""
    with create_pool("thread", workers=1, max_tasks_per_worker=1) as pool:
        assert pool.submit(lambda: "ok").result(5) == "ok"
    with create_pool("async", workers=1, max_tasks_per_worker=1) as pool:
        assert pool.submit(_async_one).result(5) == 1


# ===========================================================================
# 2. ThreadPool
# ===========================================================================


def test_thread_result_exception_and_args() -> None:
    """结果与异常传播、args/kwargs 透传。"""
    with create_pool("thread", workers=2) as pool:
        assert pool.submit(pow, 2, 10).result(5) == 1024
        assert pool.submit(pow, 2, 3, mod=100).result(5) == 8  # kwargs 透传
        fut = pool.submit(_raise_value_error, "boom-1")
        with pytest.raises(ValueError, match="boom-1"):
            fut.result(5)
        st = pool.stats()
        assert st["submitted"] == 3 and st["succeeded"] == 2 and st["failed"] == 1


def _raise_value_error(msg: str) -> None:
    raise ValueError(msg)


def test_thread_ctx_propagation() -> None:
    """ctx 传播：任务内可读提交方 ContextVar，任务内修改不外泄。"""
    var = contextvars.ContextVar("pools-thread-ctx", default="unset")
    token = var.set("from-main")
    try:
        def read_var() -> str:
            return var.get()

        def write_var() -> str:
            var.set("from-task")
            return var.get()

        with create_pool("thread", workers=2) as pool:
            assert pool.submit(read_var).result(5) == "from-main"
            assert pool.submit(write_var).result(5) == "from-task"
            assert var.get() == "from-main"  # 任务内 set 不影响提交线程

        with create_pool("thread", workers=1, propagate_context=False) as pool:
            assert pool.submit(read_var).result(5) == "unset"  # 关闭传播
    finally:
        var.reset(token)


def test_thread_retry_success_sequence() -> None:
    """max_retries=2：on_retry 按尝试序触发两次，最终成功。"""
    calls: List[int] = []
    recorder = EventRecorder(("on_submit", "on_start", "on_success", "on_error", "on_retry"))
    lock = threading.Lock()

    def flaky() -> str:
        with lock:
            calls.append(1)
            n = len(calls)
        if n < 3:
            raise ValueError(f"fail-{n}")
        return "recovered"

    with create_pool("thread", workers=1, max_retries=2, retry_backoff=0.0,
                     hooks=recorder.hooks) as pool:
        assert pool.submit(flaky).result(5) == "recovered"
        st = pool.stats()
        assert st["retried"] == 2 and st["succeeded"] == 1 and st["failed"] == 0
        retries = recorder.infos("on_retry")
        assert [info["attempt"] for info in retries] == [1, 2]
        assert [info["error"] for info in retries] == ["fail-1", "fail-2"]
        assert all(info["next_delay_ms"] == 0.0 for info in retries)
        assert recorder.names == ["on_submit", "on_start", "on_retry", "on_retry", "on_success"]


def test_thread_retry_exhaustion() -> None:
    """重试耗尽后 on_error 收场，failed 计数。"""
    recorder = EventRecorder(("on_start", "on_retry", "on_error", "on_success"))
    with create_pool("thread", workers=1, max_retries=1, retry_backoff=0.0,
                     hooks=recorder.hooks) as pool:
        with pytest.raises(ValueError, match="always-fails"):
            pool.submit(_raise_value_error_always).result(5)
        st = pool.stats()
        assert st["failed"] == 1 and st["retried"] == 1 and st["succeeded"] == 0
        assert recorder.names == ["on_start", "on_retry", "on_error"]
        err_info = recorder.infos("on_error")[0]
        assert err_info["attempt"] == 2 and err_info["error"] == "always-fails"


def _raise_value_error_always() -> None:
    raise ValueError("always-fails")


def test_thread_retry_backoff_timing() -> None:
    """retry_backoff 指数退避生效：0.05 基秒 → 两次退避 >= 0.05+0.1。"""
    calls: List[int] = []

    def flaky_third_ok() -> str:
        calls.append(1)
        if len(calls) < 3:
            raise RuntimeError("again")
        return "ok"

    with create_pool("thread", workers=1, max_retries=2, retry_backoff=0.05) as pool:
        t0 = time.monotonic()
        assert pool.submit(flaky_third_ok).result(5) == "ok"
        elapsed = time.monotonic() - t0
    # 第 1 次失败退避 ~0.05s，第 2 次 ~0.1s（含抖动只增不减），留安全余量
    assert elapsed >= 0.12, f"退避未生效：{elapsed:.3f}s"


def test_thread_reject_raise_policy() -> None:
    """queue_size=1 + raise：在途满时同步拒绝并触发 on_reject。"""
    recorder = EventRecorder(("on_reject", "on_submit"))
    release = threading.Event()
    with create_pool("thread", workers=1, queue_size=1,
                     hooks=recorder.hooks) as pool:
        blocker = pool.submit(lambda: release.wait(5) and "b1")
        assert wait_until(lambda: pool.stats()["running"] == 1)
        with pytest.raises(PoolRejectedError, match="queue_size"):
            pool.submit(lambda: "never")
        assert isinstance(PoolRejectedError("x"), RuntimeError)
        st = pool.stats()
        assert st["rejected"] == 1 and st["submitted"] == 1
        release.set()
        assert blocker.result(5) == "b1"
    infos = recorder.infos("on_reject")
    assert len(infos) == 1
    assert set(infos[0]) == {"task_id", "pool_name", "kind", "fn_name", "reason"}
    assert "queue_size" in infos[0]["reason"]


def test_thread_reject_block_policy() -> None:
    """block 策略：在途满时提交阻塞，空位释放后继续执行。"""
    release = threading.Event()

    def blocker() -> str:
        release.wait(5)
        return "b1"

    with create_pool("thread", workers=1, queue_size=1, reject_policy="block") as pool:
        first = pool.submit(blocker)
        assert wait_until(lambda: pool.stats()["running"] == 1)
        box = submit_in_thread(pool, lambda: "b2")
        time.sleep(0.15)
        assert "future" not in box and "error" not in box  # 仍在阻塞等待空位
        release.set()
        box["thread"].join(5)
        assert "future" in box and box["future"].result(5) == "b2"
        assert first.result(5) == "b1"


def test_thread_block_policy_shutdown_raises() -> None:
    """block 阻塞期间池被关闭：提交线程被唤醒并抛 PoolClosedError。"""
    release = threading.Event()

    def blocker() -> str:
        release.wait(5)
        return "b1"

    pool = create_pool("thread", workers=1, queue_size=1, reject_policy="block")
    try:
        first = pool.submit(blocker)
        assert wait_until(lambda: pool.stats()["running"] == 1)
        box = submit_in_thread(pool, lambda: "b2")
        time.sleep(0.15)
        pool.shutdown(wait=False)
        box["thread"].join(5)
        assert isinstance(box.get("error"), PoolClosedError)
        release.set()
        assert first.result(5) == "b1"
    finally:
        release.set()
        pool.shutdown()


def test_thread_concurrency_stats_conservation() -> None:
    """并发 8×50 提交：submitted==400 且 succeeded+failed==400、峰值<=8。"""
    state = {"cur": 0, "peak": 0}
    lock = threading.Lock()

    def work(i: int) -> int:
        with lock:
            state["cur"] += 1
            state["peak"] = max(state["peak"], state["cur"])
        time.sleep(0.005)
        with lock:
            state["cur"] -= 1
        return i

    with create_pool("thread", workers=8) as pool:
        futures = [pool.submit(work, i) for _ in range(8) for i in range(50)]
        assert all(f.result(10) is not None for f in futures)
        st = pool.stats()
        assert st["submitted"] == 400
        assert st["succeeded"] + st["failed"] == 400
        assert st["succeeded"] == 400 and st["failed"] == 0
        assert st["running"] == 0 and st["queue_depth"] == 0
        assert st["rejected"] == 0 and st["retried"] == 0
        assert 1 < state["peak"] <= 8


def test_thread_shutdown_idempotent_hook_once() -> None:
    """shutdown 幂等（连调三次不抛），on_shutdown 只触发一次。"""
    recorder = EventRecorder(("on_shutdown",))
    pool = create_pool("thread", workers=1, hooks=recorder.hooks)
    assert pool.submit(lambda: 1).result(5) == 1
    pool.shutdown()
    pool.shutdown(wait=False)
    pool.shutdown(wait=True, cancel_futures=False)
    assert recorder.names == ["on_shutdown"]
    payload = recorder.infos("on_shutdown")[0]
    assert set(payload) == {"pool_name", "kind", "stats"}
    assert payload["stats"]["succeeded"] == 1


def test_thread_submit_after_shutdown() -> None:
    """关闭后 submit 抛 PoolClosedError（且为 RuntimeError 子类）。"""
    pool = create_pool("thread", workers=1)
    pool.shutdown()
    with pytest.raises(PoolClosedError):
        pool.submit(lambda: 1)
    assert isinstance(PoolClosedError("x"), RuntimeError)
    with pytest.raises(ValueError, match="可调用"):
        pool.submit("not-callable")  # type: ignore[arg-type]


def test_thread_map_ordering_and_fail_fast() -> None:
    """map 保序、惰性；异常处 fail-fast 抛出且取消后续未开始任务。"""
    with create_pool("thread", workers=2) as pool:
        assert list(pool.map(lambda x: x * 2, [5, 3, 1, 0, 9])) == [10, 6, 2, 0, 18]
        assert list(pool.map(lambda a, b: a + b, [1, 2], [10, 20])) == [11, 22]

        def maybe_boom(x: int) -> int:
            if x == 1:
                raise ValueError("map-boom")
            return x

        iterator = pool.map(maybe_boom, [0, 1, 2, 3])
        assert next(iterator) == 0
        with pytest.raises(ValueError, match="map-boom"):
            next(iterator)
    with create_pool("thread") as pool:
        with pytest.raises(ValueError, match="timeout"):
            pool.map(lambda x: x, [1], timeout=-1)


def test_thread_queued_future_cancel_never_runs() -> None:
    """排队中的 Future 可取消，任务本体永不执行（闸门正确释放）。"""
    gate = threading.Event()
    started = threading.Event()

    def blocker() -> str:
        gate.wait(5)
        return "b1"

    def wait_and_flag() -> str:
        started.set()
        gate.wait(5)
        return "ran"

    with create_pool("thread", workers=1) as pool:
        first = pool.submit(blocker)
        second = pool.submit(wait_and_flag)  # 排队
        assert wait_until(lambda: pool.stats()["running"] == 1)
        assert second.cancel() is True
        gate.set()
        assert first.result(5) == "b1"
        time.sleep(0.15)
        assert not started.is_set()  # 已取消：从未执行
        st = pool.stats()
        assert st["submitted"] == 2 and st["succeeded"] == 1 and st["running"] == 0


def test_thread_context_manager_closes() -> None:
    """with 退出即关闭。"""
    pool = create_pool("thread", workers=1)
    with pool as entered:
        assert entered is pool
        assert entered.submit(lambda: "ok").result(5) == "ok"
    with pytest.raises(PoolClosedError):
        pool.submit(lambda: 1)


def test_thread_task_timeout_wait_semantics() -> None:
    """task_timeout 等待语义：Future 快速得 TimeoutError，本体不中断。"""
    zombie_done: List[int] = []

    def slow() -> str:
        time.sleep(0.7)
        zombie_done.append(1)
        return "late"

    with create_pool("thread", workers=1, task_timeout=0.2) as pool:
        t0 = time.monotonic()
        fut = pool.submit(slow)
        with pytest.raises(TimeoutError):
            fut.result(5)
        assert time.monotonic() - t0 < 0.55
        st = pool.stats()
        assert st["timed_out"] == 1 and st["submitted"] == 1
        # 僵尸任务占用 worker 直到自然结束；随后池仍可用
        assert pool.submit(lambda: "ok").result(5) == "ok"
        assert wait_until(lambda: bool(zombie_done), timeout=3)
        final = pool.stats()
        # 超时已是僵尸任务的终态：不重复计 succeeded/failed
        assert final["succeeded"] == 1 and final["failed"] == 0 and final["timed_out"] == 1
        assert final["running"] == 0 and final["queue_depth"] == 0


def test_thread_worker_thread_naming_and_daemon() -> None:
    """worker 线程：daemon 位生效、命名带 thread_name_prefix。

    注：worker 为懒扩容（有积压才扩），单测不做并发到达假设——并发度
    上限由 stats 守恒用例（400 任务峰值验证）覆盖。
    """
    def probe() -> Tuple[str, bool]:
        current = threading.current_thread()
        return current.name, current.daemon

    with create_pool("thread", workers=2, thread_name_prefix="bk-worker") as pool:
        for _ in range(8):
            name, daemon = pool.submit(probe).result(5)
            assert name.startswith("bk-worker-")
            assert daemon is True
    # 非 daemon 配置
    with create_pool("thread", workers=1, daemon=False) as pool:
        assert pool.submit(lambda: threading.current_thread().daemon).result(5) is False


def test_thread_initializer_success() -> None:
    """initializer 在每个 worker 线程启动时执行，任务内可见。"""
    seen: List[int] = []
    lock = threading.Lock()

    def init() -> None:
        with lock:
            seen.append(threading.get_ident())

    with create_pool("thread", workers=2, initializer=init) as pool:
        idents = {pool.submit(lambda: threading.get_ident()).result(5)
                  for _ in range(8)}
        assert idents <= set(seen)


def test_thread_initializer_failure_disables_pool() -> None:
    """initializer 失败：已排队任务以失败收场，后续 submit 抛 PoolError。"""
    def bad_init() -> None:
        raise RuntimeError("init-boom")

    pool = create_pool("thread", workers=1, initializer=bad_init)
    try:
        first = pool.submit(lambda: "x")  # 触发 worker 启动 → init 失败
        with pytest.raises(PoolError, match="init-boom"):
            first.result(5)

        def probe() -> bool:
            try:
                pool.submit(lambda: 1)
                return False
            except PoolError:
                return True

        assert wait_until(probe, timeout=3)
    finally:
        pool.shutdown()


# ===========================================================================
# 3. AsyncPool
# ===========================================================================


async def _async_mul(a: int, b: int) -> int:
    await asyncio.sleep(0)
    return a * b


async def _async_boom() -> None:
    raise ValueError("async-boom")


def test_async_submit_bridge_result_and_exception() -> None:
    """普通线程 submit：concurrent Future 桥接协程结果与异常。"""
    with create_pool("async", workers=2) as pool:
        assert pool.submit(_async_mul, 6, 7).result(5) == 42
        with pytest.raises(ValueError, match="async-boom"):
            pool.submit(_async_boom).result(5)
        st = pool.stats()
        assert st["succeeded"] == 1 and st["failed"] == 1


def test_async_task_timeout_true_cancel() -> None:
    """task_timeout 真取消：0.2s 超时取消协程、池仍可用。"""
    flag: List[str] = []

    async def slow() -> str:
        try:
            await asyncio.sleep(1.0)
            flag.append("done")
        except asyncio.CancelledError:
            flag.append("cancelled")
            raise
        return "never"

    with create_pool("async", workers=1, task_timeout=0.2) as pool:
        t0 = time.monotonic()
        with pytest.raises(TimeoutError):
            pool.submit(slow).result(5)
        assert time.monotonic() - t0 < 0.7
        assert wait_until(lambda: flag == ["cancelled"], timeout=2)
        assert pool.submit(_async_mul, 2, 3).result(5) == 6  # 池仍可用
        st = pool.stats()
        assert st["timed_out"] == 1 and st["succeeded"] == 1 and st["failed"] == 0


def test_async_asubmit_in_event_loop() -> None:
    """asubmit 在 asyncio.run 内 await，结果回写调用方 loop 的 Future。"""
    async def main() -> Tuple[int, int]:
        pool = create_pool("async", name="asub")
        try:
            r1 = await pool.asubmit(_async_mul, 1, 2)
            r2 = await pool.asubmit(_async_mul, r1, 10)
            return r1, r2
        finally:
            pool.shutdown()

    assert asyncio.run(main()) == (2, 20)


def test_async_asubmit_outside_loop_raises() -> None:
    """无事件循环时 asubmit 抛 RuntimeError。"""
    with create_pool("async", workers=1) as pool:
        with pytest.raises(RuntimeError, match="事件循环"):
            pool.asubmit(_async_mul, 1, 2)


def test_async_concurrency_tokens_peak() -> None:
    """Semaphore(workers=2)：同时运行数峰值恰为 2（令牌不超发）。"""
    state = {"cur": 0, "peak": 0}

    async def churn() -> int:
        state["cur"] += 1
        state["peak"] = max(state["peak"], state["cur"])
        await asyncio.sleep(0.15)
        state["cur"] -= 1
        return state["peak"]

    with create_pool("async", workers=2) as pool:
        futures = [pool.submit(churn) for _ in range(6)]
        assert all(f.result(5) is not None for f in futures)
        assert state["peak"] == 2
        st = pool.stats()
        assert st["succeeded"] == 6 and st["running"] == 0


def test_async_ctx_propagation() -> None:
    """async 池 ctx 传播：任务协程内可读提交方变量，修改不外泄。"""
    var = contextvars.ContextVar("pools-async-ctx", default="unset")
    token = var.set("from-main")
    try:
        async def read_var() -> str:
            return var.get()

        async def write_var() -> str:
            var.set("from-task")
            return var.get()

        with create_pool("async", workers=2) as pool:
            assert pool.submit(read_var).result(5) == "from-main"
            assert pool.submit(write_var).result(5) == "from-task"
            assert var.get() == "from-main"
    finally:
        var.reset(token)


def test_async_shutdown_drains_all() -> None:
    """shutdown 排空：20 个快任务在 cancel_futures=False 下全部完成。"""
    async def quick(i: int) -> int:
        await asyncio.sleep(0.01)
        return i * 2

    pool = create_pool("async", workers=4)
    futures = [pool.submit(quick, i) for i in range(20)]
    pool.shutdown(cancel_futures=False)
    assert [f.result(5) for f in futures] == [i * 2 for i in range(20)]
    assert pool.stats()["succeeded"] == 20


def test_async_shutdown_cancels_pending() -> None:
    """默认 cancel_futures=True：在途完成、未开始（未获令牌）被取消。"""
    async def slow() -> str:
        await asyncio.sleep(0.4)
        return "slow"

    async def quick() -> str:
        return "quick"

    pool = create_pool("async", workers=1)
    first = pool.submit(slow)
    assert wait_until(lambda: pool.stats()["running"] == 1)
    second = pool.submit(quick)  # 排队等令牌
    pool.shutdown()
    assert first.result(5) == "slow"
    with pytest.raises(CancelledError):  # concurrent.futures.CancelledError
        second.result(5)
    st = pool.stats()
    assert st["succeeded"] == 1 and st["failed"] == 0


def test_async_retry_with_hook() -> None:
    """async 重试：on_retry 触发后重试成功。"""
    attempts: List[int] = []
    recorder = EventRecorder(("on_retry", "on_success"))

    async def flaky() -> str:
        attempts.append(1)
        if len(attempts) < 2:
            raise ValueError("async-first-fails")
        return "recovered"

    with create_pool("async", workers=1, max_retries=1,
                     hooks=recorder.hooks) as pool:
        assert pool.submit(flaky).result(5) == "recovered"
        st = pool.stats()
        assert st["retried"] == 1 and st["succeeded"] == 1
        retry_info = recorder.infos("on_retry")[0]
        assert retry_info["attempt"] == 1
        assert retry_info["error"] == "async-first-fails"


def test_async_map_and_closed_and_reject() -> None:
    """async map 保序、shutdown 幂等 + 关闭后 submit、闸门拒绝。"""
    async def double(x: int) -> int:
        return x * 2

    async def blocker() -> None:
        await asyncio.sleep(0.4)

    pool = create_pool("async", workers=1)
    assert list(pool.map(double, [1, 2, 3])) == [2, 4, 6]
    pool.shutdown()
    pool.shutdown(wait=False)
    with pytest.raises(PoolClosedError):
        pool.submit(double, 1)

    reject_pool = create_pool("async", workers=1, queue_size=1)
    try:
        fut = reject_pool.submit(blocker)
        assert wait_until(lambda: reject_pool.stats()["running"] == 1)
        with pytest.raises(PoolRejectedError):
            reject_pool.submit(double, 1)
        assert reject_pool.stats()["rejected"] == 1
        fut.result(5)
    finally:
        reject_pool.shutdown()


def test_async_plain_function_fails() -> None:
    """非协程函数提交 async 池：以 TypeError 失败收场（诚实契约）。"""
    def plain() -> int:
        return 1

    with create_pool("async", workers=1) as pool:
        with pytest.raises(TypeError):
            pool.submit(plain).result(5)
        assert pool.stats()["failed"] == 1


# ===========================================================================
# 4. ProcessPool（session 级 fixture 复用，控制 Windows spawn 开销）
# ===========================================================================


@pytest.fixture(scope="session")
def process_pool():
    """整个 process 套件复用一个池（spawn 启动慢）。"""
    pool = create_pool(
        "process", name="proc-suite", workers=2,
        initializer=_proc_initializer, initargs=("tok-42",),
    )
    yield pool
    pool.shutdown()


def test_process_cpu_bound_results(process_pool) -> None:
    """CPU 密集（质数计数）：分块并行结果与串行一致。"""
    chunks = [12000, 9000, 15000, 7000, 11000]
    futures = [process_pool.submit(count_primes, c) for c in chunks]
    results = [f.result(30) for f in futures]
    assert results == [count_primes(c) for c in chunks]
    assert sum(results) == sum(count_primes(c) for c in chunks)


def test_process_initializer_visible(process_pool) -> None:
    """模块级 initializer 设置的全局标志在任务内可见。"""
    for _ in range(2):  # 多跑几次覆盖两个 worker 进程
        assert process_pool.submit(_proc_init_token).result(30) == "tok-42"


def test_process_exception_propagation(process_pool) -> None:
    """用户函数异常跨进程传播。"""
    with pytest.raises(ValueError, match="boom-proc"):
        process_pool.submit(proc_raise, "boom-proc").result(30)


def test_process_stats_and_map(process_pool) -> None:
    """stats 记账与 map 保序。"""
    before = process_pool.stats()
    assert list(process_pool.map(proc_echo, range(5))) == [0, 1, 2, 3, 4]
    after = process_pool.stats()
    assert after["submitted"] - before["submitted"] == 5
    assert after["succeeded"] - before["succeeded"] == 5


def test_process_max_tasks_per_worker_recycle() -> None:
    """max_tasks_per_worker=1（mp 回收路径）：多任务仍全部成功。"""
    with create_pool("process", workers=2, max_tasks_per_worker=1) as pool:
        futures = [pool.submit(proc_echo_pid, i) for i in range(4)]
        results = [f.result(30) for f in futures]
        assert [x for x, _ in results] == [0, 1, 2, 3]
        pids = {pid for _, pid in results}
        assert len(pids) >= 2  # 每进程一个任务即回收，至少两个不同子进程


def test_process_shutdown_then_submit() -> None:
    """进程池 shutdown 后 submit 抛 PoolClosedError。"""
    pool = create_pool("process", workers=1)
    try:
        assert pool.submit(proc_echo, 9).result(30) == 9
    finally:
        pool.shutdown()
    with pytest.raises(PoolClosedError):
        pool.submit(proc_echo, 1)


# ===========================================================================
# 5. 钩子（八事件 / 载荷 / 异常吞噬 / 多监听顺序）
# ===========================================================================

_ALL_EVENTS = (
    "on_submit", "on_start", "on_success", "on_error",
    "on_retry", "on_timeout", "on_reject", "on_shutdown",
)


def test_hooks_full_lifecycle_order_and_payloads() -> None:
    """八事件全触发：成功链 → 重试链 → 超时链 → 拒绝 → 停机，顺序断言。"""
    recorder = EventRecorder(_ALL_EVENTS)
    release = threading.Event()

    def slow() -> None:
        time.sleep(0.6)

    def blocker() -> None:
        release.wait(5)

    pool = create_pool("thread", workers=1, task_timeout=0.2, max_retries=1,
                       retry_backoff=0.0, queue_size=1, hooks=recorder.hooks)
    try:
        # 1) 成功链：提交 → 开始 → 成功
        assert pool.submit(lambda: "ok").result(5) == "ok"
        assert recorder.names[0:3] == ["on_submit", "on_start", "on_success"]
        base = recorder.infos("on_submit")[0]
        assert set(base) == {"task_id", "pool_name", "kind", "fn_name", "queued_at"}
        assert base["task_id"].startswith("t-") and len(base["task_id"]) == 8
        assert base["pool_name"] == "pool-thread" and base["kind"] == "thread"
        start_info = recorder.infos("on_start")[0]
        assert start_info["attempt"] == 1 and "started_at" in start_info
        ok_info = recorder.infos("on_success")[0]
        assert ok_info["attempt"] == 1
        assert isinstance(ok_info["elapsed_ms"], float) and ok_info["elapsed_ms"] >= 0
        assert "finished_at" in ok_info

        # 2) 重试链：提交 → 开始 → 重试 → 失败
        with pytest.raises(ValueError):
            pool.submit(_raise_value_error_always_2).result(5)
        assert recorder.names[3:7] == [
            "on_submit", "on_start", "on_retry", "on_error",
        ]
        retry_info = recorder.infos("on_retry")[0]
        assert retry_info["attempt"] == 1 and "next_delay_ms" in retry_info
        assert retry_info["error"] == "lifecycle-fails"

        # 3) 超时链：提交 → 开始 → 超时（等待语义，僵尸不补成功/失败事件）
        with pytest.raises(TimeoutError):
            pool.submit(slow).result(5)
        assert recorder.names[7:10] == ["on_submit", "on_start", "on_timeout"]
        timeout_info = recorder.infos("on_timeout")[0]
        assert set(timeout_info) >= {"task_id", "elapsed_ms"}
        # 等僵尸释放 worker 与闸门名额（queue_size=1）
        assert wait_until(lambda: pool.stats()["running"] == 0, timeout=3)

        # 4) 拒绝：占住在途名额 → 下一个提交被拒
        blocker_fut = pool.submit(blocker)
        assert wait_until(lambda: pool.stats()["running"] == 1)
        with pytest.raises(PoolRejectedError):
            pool.submit(lambda: 1)
        assert recorder.names[10:13] == ["on_submit", "on_start", "on_reject"]
        reject_info = recorder.infos("on_reject")[0]
        assert set(reject_info) == {"task_id", "pool_name", "kind", "fn_name", "reason"}
        release.set()
        blocker_fut.result(5)
    finally:
        release.set()
        pool.shutdown()
    # 5) 停机：blocker 成功收尾 → on_shutdown 收尾（携带 stats）
    assert recorder.names[13:] == ["on_success", "on_shutdown"]
    shutdown_info = recorder.infos("on_shutdown")[0]
    assert set(shutdown_info) == {"pool_name", "kind", "stats"}
    assert shutdown_info["stats"]["timed_out"] == 1


def _raise_value_error_always_2() -> None:
    raise ValueError("lifecycle-fails")


def test_hook_exception_swallowed_and_counted() -> None:
    """钩子抛异常不影响任务与池，hook_errors 计数增长。"""
    def bad_hook(info: Dict[str, Any]) -> None:
        raise RuntimeError("hook-boom")

    with create_pool("thread", workers=1,
                     hooks={"on_start": bad_hook, "on_success": bad_hook}) as pool:
        assert pool.submit(lambda: "ok").result(5) == "ok"
        assert pool.stats()["hook_errors"] == 2
        # 池仍正常可用
        assert pool.submit(lambda: "again").result(5) == "again"
        assert pool.stats()["hook_errors"] == 4


def test_hooks_multiple_listeners_registration_order() -> None:
    """同事件多监听按注册序：config hooks 在前，add_hook 追加在后。"""
    order: List[str] = []
    lock = threading.Lock()

    def make(name: str) -> Callable[[Dict[str, Any]], None]:
        def hook(info: Dict[str, Any]) -> None:
            with lock:
                order.append(name)
        return hook

    pool = create_pool("thread", workers=1,
                       hooks={"on_success": [make("a"), make("b")]})
    try:
        pool.add_hook("on_success", make("c"))
        assert pool.submit(lambda: 1).result(5) == 1
        assert order == ["a", "b", "c"]
    finally:
        pool.shutdown()


def test_task_id_monotonic_format() -> None:
    """task_id 为池内单调计数（t-000001 风格），跨池互不影响。"""
    ids: List[str] = []
    pool = create_pool("thread", workers=1, hooks={
        "on_submit": lambda info: ids.append(info["task_id"])
    })
    try:
        for i in range(3):
            pool.submit(lambda: i).result(5)
        assert ids == ["t-000001", "t-000002", "t-000003"]
    finally:
        pool.shutdown()


# ===========================================================================
# 6. run_all
# ===========================================================================


def test_run_all_mixed_forms_in_order() -> None:
    """fn / (fn, args) / (fn, args, kwargs) 混合形态保序返回。"""
    def add(a: int, b: int = 0) -> int:
        return a + b

    def zero() -> int:
        return 0

    tasks: List[Any] = [
        (add, (1,)),            # 二元组
        (add, (1,), {"b": 2}),  # 三元组
        zero,                   # 纯函数（零参）
    ]
    assert run_all(tasks, kind="thread", workers=2) == [1, 3, 0]


def test_run_all_return_exceptions_both_modes() -> None:
    """return_exceptions=False 首个异常上抛；True 时异常进结果列表。"""
    def ok(i: int) -> int:
        return i

    tasks: List[Any] = [(ok, (0,)), (_raise_value_error, ("run-boom",)), (ok, (2,))]
    with pytest.raises(ValueError, match="run-boom"):
        run_all(tasks, workers=2)
    results = run_all(tasks, return_exceptions=True, workers=2)
    assert results[0] == 0 and results[2] == 2
    assert isinstance(results[1], ValueError)
    assert str(results[1]) == "run-boom"


def test_run_all_async_kind() -> None:
    """run_all 支持 async kind（协程函数任务）。"""
    assert run_all([(_async_mul, (2, 3)), (_async_mul, (4, 5))],
                   kind="async", workers=2) == [6, 20]


def test_run_all_empty_and_invalid_shape() -> None:
    """空任务列表返回 []；非法形态抛 ValueError。"""
    assert run_all([], workers=1) == []
    assert run_all(iter(()), kind="async") == []
    with pytest.raises(ValueError, match=r"tasks\[1\]"):
        run_all([(lambda: 1), ("not", "callable", "at", "all")])
    with pytest.raises(ValueError, match=r"tasks\[0\]"):
        run_all([42])
    with pytest.raises(ValueError, match="args 必须为"):
        run_all([(lambda x: x, "not-a-tuple")])  # type: ignore[list-item]
    with pytest.raises(ValueError, match="长度必须为"):
        run_all([(lambda: 1, (), {}, ())])
