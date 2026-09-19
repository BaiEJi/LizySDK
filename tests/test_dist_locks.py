"""lizysdk.dist Redis 套件测试：RLock + LeaderElector + IdempotentKey
（fakeredis 全离线，设计文档 §1.1/§1.2/§1.3 与 §4 对应行全落地）。

覆盖范围：
1. 懒加载红线：子进程屏蔽 redis 后 import reentrant_lock / election /
   idempotent 三模块（连同 lizysdk.dist 包链）依然成功——模块级不碰
   redis 的红线可证；
2. RLock（对齐 Redisson RedissonLock）：重入计数（同线程 acquire×3 ->
   hash 计数 3，release×3 归零删键）/ 部分释放续命 TTL / 跨实例互斥与
   交接 / 跨线程互斥（同实例不同 thread ident 互为竞争者）/ 阻塞超时
   LockTimeoutError（消息含键名）/ 阻塞等待前锁释放后成功 / 非阻塞
   立即 False / 看门狗续期（真实时钟缩短 watchdog 观察 PTTL 被周期性
   重设 + monkeypatch _sleep 的确定性续期计数）/ release 到 0 层停
   看门狗 / 显式 lease_time 无看门狗且到期自动失效 / extend 重设 TTL
   与未持有 False / force_unlock 管理端删键 / 未获取与跨线程释放
   LockNotOwnedError / TTL 过期后释放抛错 / 嵌套 with 重入逐层释放 /
   异常路径也释放 / locked 任意持有者视角 / is_held_by_current_thread /
   4 线程争抢临界区恰好串行 / 参数校验 / repr 不回显 instance_uuid；
3. LeaderElector（K8s Lease 语义）：构造与 holder_id 默认格式 / 当选
   回调触发且键值 == holder_id / 心跳续期保住领导（lease 期间 PTTL 被
   重设）/ 租约被夺先降级再回调（回调内 is_leader 已为 False）/ 另一
   实例上位（is_leader 翻转 + losing/become 回调成对）/ 失联后自动
   重新上位（become 二次触发）/ stop 主动下台释放租约 + losing 回调 /
   stop 幂等且未 start 时安全 / start 幂等（重复 start 不再起线程）/
   leader_id 跟随者可见 / 回调抛异常不死心跳线程（续期照常）/ 瞬态
   Redis 异常被心跳吞并重试 / 回调内 stop 抛 LockError（防死锁）且
   线程存活 / 双实例恰好一个领导 / 参数校验 / repr 不回显 holder_id；
4. IdempotentKey（Stripe 两态模型）：异常族与 .result / begin 抢占
   与信封内容（monkeypatch _now 冻结 started）/ processing TTL 生效
   与到期可重试 / guard 首次执行 complete 落 done / processing 中并发
   进入抛 Conflict / done 后进入抛 Done（.result 带缓存结果）/ 临界区
   异常自动 fail 可重试 / 正常退出未 complete 停留 processing（文档化
   行为）/ 同键嵌套 guard Conflict / fail 删自己的坑 / 未 begin 的
   fail 返回 False / done 终态 fail 拒拆 / compare-holder 的 fail 不
   误删他人（跨实例接管场景）/ get 与 is_done 三态 / None 结果用
   is_done 消歧 / 复杂结果 JSON 往返 / complete 覆写与 TTL / 不可
   序列化 result 抛 ValueError / 4 线程并发 guard 恰好一个执行者 /
   guard 句柄手动 fail / 参数校验 / repr。

并发用例线程数受控（4~8）、等待压到最小（短租约/monkeypatch _sleep）；
时间相关断言用模块级 _now/_now+冻结时钟或宽裕阈值防 flaky。
"""

from __future__ import annotations

import json
import logging
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import fakeredis
import pytest

from lizysdk.dist import election as dist_election
from lizysdk.dist import idempotent as dist_idempotent
from lizysdk.dist import reentrant_lock as dist_rlock
from lizysdk.dist.election import LeaderElector
from lizysdk.dist.idempotent import (
    IdempotencyConflictError,
    IdempotencyDoneError,
    IdempotencyError,
    IdempotentKey,
)
from lizysdk.dist.lock import LockError, LockNotOwnedError, LockTimeoutError
from lizysdk.dist.reentrant_lock import RLock

_SRC_DIR = Path(__file__).resolve().parents[1] / "src"


# ---------------------------------------------------------------------------
# 通用 fixture 与辅助
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> Any:
    """每个测试一个全新的 FakeStrictRedis（支持 EVAL / TTL / 线程安全）。"""
    return fakeredis.FakeStrictRedis()


def run_threads(count: int, target: Callable[[], Any]) -> List[threading.Thread]:
    """启动 count 个线程执行 target() 并 join，返回线程列表。"""
    threads = [threading.Thread(target=target, daemon=True) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    return threads


def wait_until(
    predicate: Callable[[], bool], timeout: float = 5.0, interval: float = 0.01
) -> bool:
    """轮询等待 predicate 为 True（默认 5 秒上限）；到期返回最后结果。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


class RenewCountingClient:
    """转发客户端并统计 RLock 续期脚本（_RENEW_LUA）的执行次数。"""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.renew_calls = 0

    def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        if script is dist_rlock._RENEW_LUA:
            self.renew_calls += 1
        return self._inner.eval(script, numkeys, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


class FlakyRenewClient:
    """转发客户端并让前 fail_times 次选举续期脚本抛异常（模拟网络抖动）。"""

    def __init__(self, inner: Any, fail_times: int = 1) -> None:
        self._inner = inner
        self.fail_times = fail_times

    def eval(self, script: str, numkeys: int, *args: Any) -> Any:
        if script is dist_election._ELECT_RENEW_LUA and self.fail_times > 0:
            self.fail_times -= 1
            raise ConnectionError("模拟网络抖动")
        return self._inner.eval(script, numkeys, *args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


# ---------------------------------------------------------------------------
# 懒加载红线
# ---------------------------------------------------------------------------


class TestLazyImport:
    """三个新模块模块级不 import redis（子进程屏蔽 redis 验证）。"""

    def test_modules_import_without_redis(self) -> None:
        """屏蔽 redis/fakeredis 后 import 三模块（连带包链）必须成功。"""
        script = (
            "import sys\n"
            "sys.modules['redis'] = None\n"
            "sys.modules['fakeredis'] = None\n"
            "import lizysdk.dist.reentrant_lock\n"
            "import lizysdk.dist.election\n"
            "import lizysdk.dist.idempotent\n"
            "print('IMPORT_OK')\n"
        )
        env = {**os.environ, "PYTHONPATH": str(_SRC_DIR)}
        proc = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=120,
            env=env,
            cwd=str(_SRC_DIR.parent),
        )
        assert proc.returncode == 0, f"子进程失败:\n{proc.stdout}\n{proc.stderr}"
        assert "IMPORT_OK" in proc.stdout, proc.stdout + proc.stderr


# ---------------------------------------------------------------------------
# RLock（对齐 Redisson RedissonLock）
# ---------------------------------------------------------------------------


class TestRLock:
    """重入计数、互斥、看门狗、显式租期、extend/force_unlock 与校验。"""

    # -- 重入与释放 ---------------------------------------------------------

    def test_reentrant_acquire_increments_hash(self, client: Any) -> None:
        """同线程 acquire×3：hash 计数 3、reentrant_count==3、TTL 已设。"""
        lock = RLock(client, "job:1", watchdog_timeout=30)
        assert lock.acquire(blocking=False) is True
        assert lock.acquire(blocking=False) is True
        assert lock.acquire(blocking=False) is True
        assert client.hvals("lizy:rlock:job:1") == [b"3"]
        assert lock.reentrant_count() == 3
        assert lock.is_held_by_current_thread() is True
        assert 0 < client.pttl("lizy:rlock:job:1") <= 30_000
        for _ in range(3):
            lock.release()

    def test_release_all_levels_deletes_key_and_stops_watchdog(
        self, client: Any
    ) -> None:
        """release×3 归零：键删除、层数 0、看门狗线程退出。"""
        lock = RLock(client, "job:2", watchdog_timeout=30)
        for _ in range(3):
            lock.acquire(blocking=False)
        for _ in range(2):
            lock.release()
        assert lock.reentrant_count() == 1          # 部分释放后仍持有 1 层
        lock.release()
        assert lock.reentrant_count() == 0
        assert lock.locked() is False
        assert client.exists("lizy:rlock:job:2") == 0
        thread = lock._watchdog_thread
        if thread is not None:
            thread.join(timeout=2.0)
            assert thread.is_alive() is False       # 到 0 层看门狗退出

    def test_partial_release_renews_ttl(self, client: Any) -> None:
        """部分释放（计数 > 0）：PEXPIRE 续命回满档（重入态仍持有）。"""
        lock = RLock(client, "job:3", watchdog_timeout=30)
        lock.acquire(blocking=False)
        lock.acquire(blocking=False)
        lock.release()                              # 剩 1 层：PEXPIRE 30000
        pttl = client.pttl("lizy:rlock:job:3")
        assert 25_000 < pttl <= 30_000              # TTL 被重设回满档
        assert client.hvals("lizy:rlock:job:3") == [b"1"]
        lock.release()

    def test_context_manager_nested_reentrant(self, client: Any) -> None:
        """嵌套 with：同线程层层计数，退出逐层释放到 0。"""
        lock = RLock(client, "job:4", watchdog_timeout=30)
        with lock as outer:
            assert outer is lock
            with lock:
                assert lock.reentrant_count() == 2
            assert lock.reentrant_count() == 1
        assert lock.reentrant_count() == 0
        assert lock.locked() is False

    def test_context_manager_releases_on_exception(self, client: Any) -> None:
        """异常路径：代码块抛错也释放一层，且原异常向上传播。"""
        lock = RLock(client, "job:5", watchdog_timeout=30)
        lock.acquire(blocking=False)                # 先造一层重入
        with pytest.raises(RuntimeError, match="boom"):
            with lock:
                raise RuntimeError("boom")
        assert lock.reentrant_count() == 1          # 只释放 with 的那一层
        lock.release()

    # -- 互斥 ---------------------------------------------------------------

    def test_cross_instance_mutex_and_handover(self, client: Any) -> None:
        """跨实例互斥：两实例同名互为竞争者；释放后可交接。"""
        first = RLock(client, "mtx", watchdog_timeout=30)
        second = RLock(client, "mtx", watchdog_timeout=30)
        assert first.acquire(blocking=False) is True
        assert second.acquire(blocking=False) is False
        assert client.hvals("lizy:rlock:mtx") == [b"1"]   # 失败方不写键
        first.release()
        assert second.acquire(blocking=False) is True     # 交接成功
        second.release()

    def test_cross_thread_mutex_same_instance(self, client: Any) -> None:
        """跨线程互斥：同实例不同线程也是竞争者（field 含 thread ident）。"""
        lock = RLock(client, "mtx-t", watchdog_timeout=30)
        assert lock.acquire(blocking=False) is True
        results: Dict[str, Any] = {}

        def contender() -> None:
            results["nb"] = lock.acquire(blocking=False)
            results["held_view"] = lock.is_held_by_current_thread()

        run_threads(1, contender)
        assert results["nb"] is False
        assert results["held_view"] is False        # 他线程视角不持有
        assert lock.is_held_by_current_thread() is True
        lock.release()

    def test_blocking_timeout_raises_lock_timeout(self, client: Any) -> None:
        """阻塞抢锁超时：抛 LockTimeoutError，消息含键名，确实等待过。"""
        holder = RLock(client, "wait:1", watchdog_timeout=30)
        holder.acquire(blocking=False)
        contender = RLock(client, "wait:1", watchdog_timeout=30)
        start = time.monotonic()
        with pytest.raises(LockTimeoutError) as excinfo:
            contender.acquire(timeout=0.2)
        assert time.monotonic() - start >= 0.15     # 确实在等待
        assert "wait:1" in str(excinfo.value)
        holder.release()

    def test_blocking_acquire_succeeds_after_release(self, client: Any) -> None:
        """阻塞等待期间前锁被释放：等待者随后获取成功。"""
        holder = RLock(client, "wait:2", watchdog_timeout=30)
        contender = RLock(client, "wait:2", watchdog_timeout=30)
        held = threading.Event()
        errors: List[BaseException] = []

        def hold_then_release() -> None:
            # 持有者线程内自 acquire 自 release（跨线程 release 是竞争者语义）
            try:
                assert holder.acquire(blocking=False) is True
                held.set()
                time.sleep(0.15)
                holder.release()
            except BaseException as exc:  # noqa: BLE001 测试需要捕获一切
                errors.append(exc)

        releaser = threading.Thread(target=hold_then_release, daemon=True)
        releaser.start()
        assert held.wait(timeout=2.0)                 # 先确保持有者已抢到
        start = time.monotonic()
        assert contender.acquire(timeout=5) is True
        assert time.monotonic() - start >= 0.1        # 确实等待过
        releaser.join()
        assert errors == []
        contender.release()

    def test_non_blocking_false_then_true_after_release(self, client: Any) -> None:
        """非阻塞：被占立即 False；释放后立即 True。"""
        holder = RLock(client, "nb", watchdog_timeout=30)
        holder.acquire(blocking=False)
        contender = RLock(client, "nb", watchdog_timeout=30)
        assert contender.acquire(blocking=False) is False
        holder.release()
        assert contender.acquire(blocking=False) is True
        contender.release()

    def test_concurrent_four_threads_exclusive(self, client: Any) -> None:
        """4 线程（各自实例）争抢同名锁：全部获取且临界区恰好串行。"""
        state = {"inside": 0, "max_inside": 0, "acquired": 0}
        state_guard = threading.Lock()
        barrier = threading.Barrier(4)
        errors: List[BaseException] = []

        def worker() -> None:
            lock = RLock(client, "shared:job", watchdog_timeout=30)
            barrier.wait()
            try:
                if not lock.acquire(timeout=30):
                    errors.append(AssertionError("30 秒内未获取到锁"))
                    return
                try:
                    with state_guard:
                        state["inside"] += 1
                        state["acquired"] += 1
                        state["max_inside"] = max(state["max_inside"], state["inside"])
                    time.sleep(0.02)
                finally:
                    with state_guard:
                        state["inside"] -= 1
                    lock.release()
            except BaseException as exc:  # noqa: BLE001 测试需要捕获一切
                errors.append(exc)

        run_threads(4, worker)
        assert errors == []
        assert state["acquired"] == 4
        assert state["max_inside"] == 1
        assert RLock(client, "shared:job", watchdog_timeout=30).locked() is False

    # -- 看门狗 -------------------------------------------------------------

    def test_watchdog_renews_pttl_realtime(self, client: Any) -> None:
        """看门狗续期：watchdog=0.9s，0.62s 后 PTTL 仍被周期性重设。"""
        lock = RLock(client, "wd:1", watchdog_timeout=0.9)
        assert lock.acquire(blocking=False) is True
        time.sleep(0.62)                            # 走过 2 个续期周期(0.3s)
        assert lock.locked() is True
        pttl = client.pttl("lizy:rlock:wd:1")
        assert pttl > 400                           # 无看门狗此刻 pttl~280
        assert pttl <= 900
        lock.release()

    def test_watchdog_renewal_deterministic(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """看门狗确定性验证：monkeypatch _sleep 推进 2.5 个周期，续期>=3 次。"""
        counting = RenewCountingClient(client)
        lock = RLock(counting, "wd:2", watchdog_timeout=30.0)   # 周期 10s
        slept = {"total": 0.0}

        def fake_sleep(seconds: float) -> None:
            slept["total"] += seconds
            stop = lock._watchdog_stop
            if stop is not None and slept["total"] >= 25.0:     # 2.5 个周期
                stop.set()

        monkeypatch.setattr(dist_rlock, "_sleep", fake_sleep)
        assert lock.acquire(blocking=False) is True
        thread = lock._watchdog_thread
        assert thread is not None
        thread.join(timeout=5.0)
        assert thread.is_alive() is False           # 停止事件生效
        assert counting.renew_calls >= 3            # 立即 1 次 + 每周期 1 次
        pttl = client.pttl("lizy:rlock:wd:2")
        assert pttl > 25_000                        # 最近一次续期重设回满档
        lock.force_unlock()

    def test_watchdog_survives_beyond_ttl_without_release(
        self, client: Any
    ) -> None:
        """看门狗模式：不 release，锁存活时间可超过单个 watchdog 档。"""
        lock = RLock(client, "wd:3", watchdog_timeout=0.6)
        lock.acquire(blocking=False)
        time.sleep(0.9)                             # 超过一档 TTL
        assert lock.locked() is True                # 续期保住了
        assert lock.is_held_by_current_thread() is True
        lock.release()

    def test_rapid_reacquire_gets_fresh_watchdog(self, client: Any) -> None:
        """回归：release 后**立即**重取——旧看门狗线程尚在退出途中（停止
        事件已置位但还没跑完），存活判定若只看 is_alive 会误判「看门狗
        在跑」而不启新线程，新持锁无看门狗、TTL 到期即丢锁（压测场景
        acquire→release→立即重取抓到）。"""
        lock = RLock(client, "wd:4", watchdog_timeout=0.4)   # 续期周期 0.133s
        assert lock.acquire(blocking=False) is True
        first_thread = lock._watchdog_thread
        assert first_thread is not None
        lock.release()                              # 置位停止事件；线程 <=0.05s 后才退出
        assert lock.acquire(blocking=False) is True # 立即重取：必须起**新一代**看门狗
        second_thread = lock._watchdog_thread
        assert second_thread is not None
        assert second_thread is not first_thread    # 新一代（旧线程还在退出途中）
        time.sleep(0.7)                             # 远超单档 TTL 0.4s
        assert lock.locked() is True                # 新看门狗在续期，锁没丢
        assert lock.is_held_by_current_thread() is True
        lock.release()                              # 完全释放不抛（修复前此处 LockNotOwnedError）

    # -- 显式租期（无看门狗）--------------------------------------------------

    def test_lease_time_disables_watchdog_and_expires(self, client: Any) -> None:
        """显式 lease_time：不起看门狗线程；到期自动失效可被他人获取。"""
        lock = RLock(client, "fixed:1", lease_time=0.5)
        assert lock.acquire(blocking=False) is True
        assert lock._watchdog_thread is None        # 无看门狗
        time.sleep(0.65)                            # 等租期过
        assert lock.locked() is False
        contender = RLock(client, "fixed:1", lease_time=10)
        assert contender.acquire(blocking=False) is True
        contender.release()

    def test_lease_time_sets_exact_ttl(self, client: Any) -> None:
        """显式租期 TTL 精确档：lease_time=10 -> pttl <= 10000。"""
        lock = RLock(client, "fixed:2", lease_time=10)
        lock.acquire(blocking=False)
        assert 0 < client.pttl("lizy:rlock:fixed:2") <= 10_000
        lock.release()

    def test_release_after_lease_expiry_raises(self, client: Any) -> None:
        """显式租期过期后 release：抛 LockNotOwnedError，层数归 0。"""
        lock = RLock(client, "fixed:3", lease_time=0.05)
        lock.acquire(blocking=False)
        time.sleep(0.2)
        with pytest.raises(LockNotOwnedError):
            lock.release()
        assert lock.reentrant_count() == 0

    # -- extend / force_unlock ---------------------------------------------

    def test_extend_resets_ttl(self, client: Any) -> None:
        """extend 成功：TTL 重设为新值（重设语义非累加）。"""
        lock = RLock(client, "ext:1", lease_time=5)
        lock.acquire(blocking=False)
        assert client.pttl("lizy:rlock:ext:1") <= 5_000
        assert lock.extend(30) is True
        pttl = client.pttl("lizy:rlock:ext:1")
        assert 20_000 < pttl <= 30_000
        lock.release()

    def test_extend_without_holding_returns_false(self, client: Any) -> None:
        """未持有（从未获取/已释放/被他人持有）时 extend 返回 False。"""
        fresh = RLock(client, "ext:2", lease_time=5)
        assert fresh.extend(10) is False            # 从未获取
        holder = RLock(client, "ext:2", lease_time=5)
        holder.acquire(blocking=False)
        holder.release()
        assert holder.extend(10) is False           # 已释放
        other = RLock(client, "ext:2", lease_time=5)
        other.acquire(blocking=False)
        assert fresh.extend(10) is False            # 持有者是他人
        other.release()

    def test_force_unlock_admin_path(self, client: Any) -> None:
        """force_unlock：管理端直接 DEL；原持有者再 release 抛 NotOwned。"""
        holder = RLock(client, "force:1", watchdog_timeout=30)
        holder.acquire(blocking=False)
        admin = RLock(client, "force:1", watchdog_timeout=30)
        admin.force_unlock()
        assert admin.locked() is False
        with pytest.raises(LockNotOwnedError):
            holder.release()
        assert admin.acquire(blocking=False) is True
        admin.release()

    # -- 释放保护 -----------------------------------------------------------

    def test_release_without_acquire_raises(self, client: Any) -> None:
        """从未获取的实例 release：抛 LockNotOwnedError（消息含键名）。"""
        lock = RLock(client, "ghost:1", watchdog_timeout=30)
        with pytest.raises(LockNotOwnedError) as excinfo:
            lock.release()
        assert "ghost:1" in str(excinfo.value)

    def test_release_from_wrong_thread_raises(self, client: Any) -> None:
        """跨线程释放（同实例）：field 不匹配 -> LockNotOwnedError 不误删。"""
        lock = RLock(client, "ghost:2", watchdog_timeout=30)
        lock.acquire(blocking=False)
        errors: List[BaseException] = []

        def releaser() -> None:
            try:
                lock.release()
            except LockNotOwnedError as exc:
                errors.append(exc)

        run_threads(1, releaser)
        assert len(errors) == 1
        assert client.hvals("lizy:rlock:ghost:2") == [b"1"]   # 键未被误删
        lock.release()

    # -- 观察视角 / 元信息 ----------------------------------------------------

    def test_locked_is_any_holder_view(self, client: Any) -> None:
        """locked() 任意持有者视角：他方实例也能看到 True。"""
        holder = RLock(client, "view:1", watchdog_timeout=30)
        observer = RLock(client, "view:1", watchdog_timeout=30)
        assert observer.locked() is False
        holder.acquire(blocking=False)
        assert observer.locked() is True
        holder.release()
        assert observer.locked() is False

    def test_exception_family_reused_from_lock(self) -> None:
        """RLock 抛的是 dist.lock 的 LockError 族（不新建异常类）。"""
        assert issubclass(LockTimeoutError, LockError)
        assert issubclass(LockNotOwnedError, LockError)

    def test_validation_constructor(self, client: Any) -> None:
        """构造校验：name/lease_time/watchdog_timeout/prefix。"""
        with pytest.raises(ValueError, match="name"):
            RLock(client, "")
        with pytest.raises(ValueError, match="name"):
            RLock(client, 123)  # type: ignore[arg-type]
        for bad_lease in (0, -1, "5", True):
            with pytest.raises(ValueError, match="lease_time"):
                RLock(client, "n", lease_time=bad_lease)  # type: ignore[arg-type]
        for bad_watchdog in (0, -0.5, "30", True):
            with pytest.raises(ValueError, match="watchdog_timeout"):
                RLock(client, "n", watchdog_timeout=bad_watchdog)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="prefix"):
            RLock(client, "n", prefix="")

    def test_validation_acquire_timeout(self, client: Any) -> None:
        """acquire 覆盖参数校验：timeout >= 0。"""
        lock = RLock(client, "v:1", watchdog_timeout=30)
        for bad in (-1, "x", True):
            with pytest.raises(ValueError, match="timeout"):
                lock.acquire(timeout=bad)  # type: ignore[arg-type]

    def test_validation_extend(self, client: Any) -> None:
        """extend 参数校验：lease_time > 0。"""
        lock = RLock(client, "v:2", watchdog_timeout=30)
        for bad in (0, -1, "5", True):
            with pytest.raises(ValueError, match="lease_time"):
                lock.extend(bad)  # type: ignore[arg-type]

    def test_properties_and_repr_no_uuid(self, client: Any) -> None:
        """元信息属性与 repr（不回显 instance_uuid / 客户端）。"""
        lock = RLock(client, "job:42", lease_time=10, prefix="p")
        assert lock.name == "job:42"
        assert lock.key == "p:job:42"
        assert lock.lease_time == 10.0
        assert lock.watchdog_timeout == 30.0
        text = repr(lock)
        assert "job:42" in text and "lease_time=10.0" in text
        assert lock._instance_uuid not in text       # 不回显实例 uuid
        assert "held=False" in text


# ---------------------------------------------------------------------------
# LeaderElector（K8s Lease 语义）
# ---------------------------------------------------------------------------


class TestLeaderElector:
    """竞选/心跳/降级/上位、回调安全、幂等生命周期与参数校验。"""

    def _elector(
        self,
        client: Any,
        name: str = "app",
        lease: float = 0.6,
        **kwargs: Any,
    ) -> LeaderElector:
        """测试用短租约选举器（lease=0.6 -> 心跳 0.2s，等待压最小）。"""
        return LeaderElector(client, name, lease=lease, **kwargs)

    # -- 构造与元信息 ---------------------------------------------------------

    def test_validation_constructor(self, client: Any) -> None:
        """构造校验：name/lease/回调可调用/holder_id/prefix。"""
        with pytest.raises(ValueError, match="name"):
            LeaderElector(client, "")
        with pytest.raises(ValueError, match="name"):
            LeaderElector(client, 42)  # type: ignore[arg-type]
        for bad_lease in (0, -1, "15", True):
            with pytest.raises(ValueError, match="lease"):
                LeaderElector(client, "n", lease=bad_lease)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="on_become_leader"):
            LeaderElector(client, "n", on_become_leader=42)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="on_losing_leadership"):
            LeaderElector(client, "n", on_losing_leadership="x")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="holder_id"):
            LeaderElector(client, "n", holder_id="")
        with pytest.raises(ValueError, match="prefix"):
            LeaderElector(client, "n", prefix="")

    def test_default_holder_id_format(self, client: Any) -> None:
        """默认 holder_id = hostname:pid:uuid8（三段、末段 8 位 hex）。"""
        elector = LeaderElector(client, "n")
        parts = elector.holder_id.split(":")
        assert len(parts) == 3
        assert parts[0] == socket.gethostname()
        assert parts[1] == str(os.getpid())
        assert len(parts[2]) == 8
        int(parts[2], 16)                           # 8 位 hex

    def test_custom_holder_id_injectable(self, client: Any) -> None:
        """holder_id 可注入。"""
        elector = LeaderElector(client, "n", holder_id="worker-1")
        assert elector.holder_id == "worker-1"

    def test_properties_and_repr_no_holder(self, client: Any) -> None:
        """元信息属性与 repr（不回显 holder_id，避免泄露主机信息）。"""
        elector = LeaderElector(client, "my-app", lease=15.0, holder_id="h:1:2")
        assert elector.name == "my-app"
        assert elector.key == "lizy:elect:my-app"
        assert elector.lease == 15.0
        text = repr(elector)
        assert "my-app" in text and "15.0" in text
        assert "h:1:2" not in text

    # -- 竞选与心跳 -----------------------------------------------------------

    def test_becomes_leader_with_callback(self, client: Any) -> None:
        """单实例当选：become 回调触发一次、键值 == holder_id、带租期 TTL。"""
        became: List[int] = []
        elector = self._elector(
            client, on_become_leader=lambda: became.append(1)
        )
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        assert became == [1]
        assert client.get("lizy:elect:app") == elector.holder_id.encode()
        assert 0 < client.pttl("lizy:elect:app") <= 600
        elector.stop()

    def test_heartbeat_renews_keeps_leadership(self, client: Any) -> None:
        """心跳续期：走过多个心跳周期后仍是领导且 PTTL 被重设。"""
        elector = self._elector(client, lease=0.9)   # 心跳 0.3s
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        time.sleep(0.65)                             # 2 个心跳周期
        assert elector.is_leader is True
        assert client.pttl("lizy:elect:app") > 400   # 无续期此刻 ~250
        elector.stop()

    def test_two_instances_exactly_one_leader(self, client: Any) -> None:
        """双实例同竞选：恰好一个领导，leader_id 与胜者 holder_id 一致。"""
        elector_a = self._elector(client, holder_id="a")
        elector_b = self._elector(client, holder_id="b")
        elector_a.start()
        elector_b.start()
        assert wait_until(lambda: elector_a.is_leader != elector_b.is_leader)
        assert elector_a.is_leader != elector_b.is_leader     # 恰好一个
        winner_id = elector_a.leader_id()
        assert winner_id in {elector_a.holder_id, elector_b.holder_id}
        elector_a.stop()
        elector_b.stop()

    def test_leader_id_follower_view(self, client: Any) -> None:
        """leader_id 跟随者可见：未启动的实例能查到当前领导者。"""
        leader = self._elector(client, holder_id="lead:1")
        leader.start()
        assert wait_until(lambda: leader.is_leader)
        follower = self._elector(client, holder_id="follow:1")
        assert follower.leader_id() == "lead:1"      # 不用启动即可查询
        leader.stop()
        assert wait_until(lambda: follower.leader_id() is None)

    # -- 降级与上位 -----------------------------------------------------------

    def test_demote_before_losing_callback(self, client: Any) -> None:
        """租约被夺：先降级再回调——losing 回调内 is_leader 已为 False。"""
        flags: List[bool] = []
        elector: Optional[LeaderElector] = None

        def on_losing() -> None:
            assert elector is not None
            flags.append(elector.is_leader)

        elector = self._elector(client, on_losing_leadership=on_losing)
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        client.delete("lizy:elect:app")              # 模拟失联（租约被夺）
        assert wait_until(lambda: not elector.is_leader)
        assert flags == [False]                      # 降级先于回调
        elector.stop()

    def test_takeover_by_second_instance(self, client: Any) -> None:
        """租约被夺后另一实例上位：双方 is_leader 翻转、回调各自触发。"""
        events: List[str] = []
        elector_a = self._elector(
            client,
            holder_id="a",
            on_become_leader=lambda: events.append("a-up"),
            on_losing_leadership=lambda: events.append("a-down"),
        )
        elector_b = self._elector(
            client,
            holder_id="b",
            on_become_leader=lambda: events.append("b-up"),
        )
        elector_a.start()
        assert wait_until(lambda: elector_a.is_leader)
        elector_b.start()                            # 跟随者等待
        client.delete("lizy:elect:app")              # GC 停顿 > lease，租约丢失
        assert wait_until(lambda: elector_b.is_leader)
        assert wait_until(lambda: not elector_a.is_leader)
        assert "a-down" in events and "b-up" in events
        assert elector_b.leader_id() == "b"
        assert elector_a.leader_id() == "b"          # 双方都看到新领导
        elector_a.stop()
        elector_b.stop()

    def test_reacquire_after_losing(self, client: Any) -> None:
        """失联降级后自动重新上位：become 回调二次触发（ contender 语义）。"""
        became: List[int] = []
        elector = self._elector(client, on_become_leader=lambda: became.append(1))
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        client.delete("lizy:elect:app")              # 租约被夺 -> 降级
        assert wait_until(lambda: not elector.is_leader)
        assert wait_until(lambda: elector.is_leader and len(became) == 2)
        elector.stop()

    # -- stop / start 生命周期 --------------------------------------------------

    def test_stop_releases_lease_and_callbacks(self, client: Any) -> None:
        """stop 主动下台：释放租约、losing 回调、心跳线程退出。"""
        lost: List[int] = []
        elector = self._elector(client, on_losing_leadership=lambda: lost.append(1))
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        thread = elector._thread
        elector.stop()
        assert elector.is_leader is False
        assert lost == [1]
        assert client.exists("lizy:elect:app") == 0  # 租约已释放
        assert elector.leader_id() is None
        if thread is not None:
            thread.join(timeout=2.0)
            assert thread.is_alive() is False

    def test_stop_idempotent_and_before_start(self, client: Any) -> None:
        """stop 幂等：未 start 调用安全；重复 stop 无副作用。"""
        elector = self._elector(client)
        elector.stop()                               # 未 start：no-op
        assert elector.is_leader is False
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        elector.stop()
        elector.stop()                               # 第二次：no-op
        assert elector.is_leader is False
        assert client.exists("lizy:elect:app") == 0

    def test_start_idempotent(self, client: Any) -> None:
        """start 幂等：重复 start 不再起新线程（复用同一线程对象）。"""
        elector = self._elector(client)
        elector.start()
        first = elector._thread
        assert first is not None and first.is_alive()
        elector.start()
        assert elector._thread is first             # 未换线程
        assert threading.active_count() >= 1
        elector.stop()

    def test_restart_after_stop(self, client: Any) -> None:
        """stop 后可重新 start：再次竞选上位。"""
        elector = self._elector(client)
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        elector.stop()
        assert elector.is_leader is False
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        elector.stop()

    # -- 回调与线程安全 ---------------------------------------------------------

    def test_become_callback_exception_thread_survives(
        self, client: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """become 回调抛异常：只记 warning，心跳继续续期、线程存活。"""
        def on_become() -> None:
            raise RuntimeError("boom")

        elector = self._elector(client, lease=0.9, on_become_leader=on_become)
        with caplog.at_level(logging.WARNING, logger="lizysdk.dist"):
            elector.start()
            assert wait_until(lambda: elector.is_leader)
            time.sleep(0.5)                          # 跨多个心跳周期
        assert elector.is_leader is True             # 续期照常
        thread = elector._thread
        assert thread is not None and thread.is_alive()
        assert any(
            "on_become_leader" in rec.getMessage() for rec in caplog.records
        )
        elector.stop()

    def test_losing_callback_exception_recampaigns(self, client: Any) -> None:
        """losing 回调抛异常：线程不死，仍能自动重新上位（二次 become）。"""
        became: List[int] = []

        def on_become() -> None:
            became.append(1)

        def on_losing() -> None:
            raise ValueError("cleanup failed")

        elector = self._elector(
            client,
            on_become_leader=on_become,
            on_losing_leadership=on_losing,
        )
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        client.delete("lizy:elect:app")
        assert wait_until(lambda: len(became) == 2)  # 降级后重新上位
        assert elector.is_leader is True
        elector.stop()

    def test_transient_redis_error_swallowed(self, client: Any) -> None:
        """瞬态 Redis 异常：心跳吞并记 warning，不误降级、线程存活。"""
        flaky = FlakyRenewClient(client, fail_times=1)
        elector = LeaderElector(flaky, "app", lease=0.9)
        elector.start()
        assert wait_until(lambda: elector.is_leader)
        time.sleep(0.5)                              # 跨过注入的那次失败
        assert elector.is_leader is True             # 瞬态错误不降级
        thread = elector._thread
        assert thread is not None and thread.is_alive()
        assert flaky.fail_times == 0                 # 注入的失败确实被触发
        elector.stop()

    def test_stop_from_callback_raises_and_survives(
        self, client: Any, caplog: pytest.LogCaptureFixture
    ) -> None:
        """回调内 stop：抛 LockError（防自 join 死锁），被吞并记 warning。"""
        calls: List[int] = []
        elector_holder: Dict[str, Any] = {}

        def on_become() -> None:
            calls.append(1)
            elector_holder["e"].stop()               # 在心跳线程内调用

        elector = LeaderElector(
            client, "app", lease=0.9, on_become_leader=on_become
        )
        elector_holder["e"] = elector
        with caplog.at_level(logging.WARNING, logger="lizysdk.dist"):
            elector.start()
            assert wait_until(lambda: elector.is_leader)
            time.sleep(0.05)
        assert calls == [1]
        assert any("死锁" in rec.getMessage() for rec in caplog.records)
        time.sleep(0.35)                             # 心跳仍在续期
        assert elector.is_leader is True
        elector.stop()                               # 主线程正常 stop
        assert elector.is_leader is False

    def test_lock_error_family_reused(self) -> None:
        """election 复用 dist.lock 的 LockError 族（不新建异常类）。"""
        assert dist_election.LockError is LockError


# ---------------------------------------------------------------------------
# IdempotentKey（Stripe 两态模型）
# ---------------------------------------------------------------------------


class TestIdempotentKey:
    """两态转移、guard 生命周期、compare-holder fail、查询与校验。"""

    def test_exception_hierarchy(self) -> None:
        """异常族：Conflict/Done 都是 IdempotencyError 子类；Done 带 .result。"""
        assert issubclass(IdempotencyError, Exception)
        assert issubclass(IdempotencyConflictError, IdempotencyError)
        assert issubclass(IdempotencyDoneError, IdempotencyError)
        exc = IdempotencyDoneError("已完成", result=42)
        assert exc.result == 42
        assert IdempotencyDoneError("x").result is None

    # -- begin / processing 态 -----------------------------------------------

    def test_begin_first_wins_envelope_content(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """begin 抢占：首次 True、再次 False；信封内容与冻结时钟一致。"""
        monkeypatch.setattr(dist_idempotent, "_now", lambda: 1_000_000.0)
        idem = IdempotentKey(client)
        assert idem.begin("pay:1") is True
        assert idem.begin("pay:1") is False          # processing 中不可再占
        raw = client.get("lizy:idem:pay:1")
        assert isinstance(raw, bytes)
        envelope = json.loads(raw)
        assert envelope["state"] == "processing"
        assert len(envelope["holder"]) == 16
        int(envelope["holder"], 16)                  # hex token
        assert envelope["started"] == 1_000_000.0    # 走 _now 接缝

    def test_begin_sets_processing_ttl(self, client: Any) -> None:
        """processing TTL 生效：processing_ttl=2 -> pttl <= 2000。"""
        idem = IdempotentKey(client)
        idem.begin("pay:2", processing_ttl=2)
        assert 0 < client.pttl("lizy:idem:pay:2") <= 2_000

    def test_processing_ttl_expiry_allows_retry(self, client: Any) -> None:
        """processing TTL 到期：键自然过期，可重新 begin（崩溃兜底）。"""
        idem = IdempotentKey(client)
        assert idem.begin("pay:3", processing_ttl=0.001) is True  # ex=1s
        time.sleep(1.05)                             # 等过期（真实等待压到最小档）
        assert idem.begin("pay:3") is True

    # -- guard 全流程 -----------------------------------------------------------

    def test_guard_first_execution_completes(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """guard 首次执行：进入即 processing，complete 落 done + 结果。"""
        monkeypatch.setattr(dist_idempotent, "_now", lambda: 2_000_000.0)
        idem = IdempotentKey(client)
        with idem.guard("pay:10") as guard:
            assert guard.key == "pay:10"
            assert idem.is_done("pay:10") is False
            assert idem.get("pay:10") is None        # processing 态无结果
            guard.complete(42, ttl=120)
        assert idem.is_done("pay:10") is True
        assert idem.get("pay:10") == 42
        envelope = json.loads(client.get("lizy:idem:pay:10"))
        assert envelope["state"] == "done"
        assert envelope["result"] == 42
        assert 0 < client.pttl("lizy:idem:pay:10") <= 120_000

    def test_guard_conflict_while_processing(self, client: Any) -> None:
        """并发第二执行者：processing 中进入 guard 抛 Conflict，键不被碰。"""
        idem = IdempotentKey(client)
        assert idem.begin("pay:11") is True          # 第一执行者已占坑
        holder_raw = client.get("lizy:idem:pay:11")
        with pytest.raises(IdempotencyConflictError) as excinfo:
            with idem.guard("pay:11"):
                raise AssertionError("不应进入临界区")
        assert "pay:11" in str(excinfo.value)
        assert client.get("lizy:idem:pay:11") == holder_raw   # 原样未动
        assert idem.is_done("pay:11") is False

    def test_guard_done_returns_cached_result(self, client: Any) -> None:
        """已 done：进入 guard 抛 Done，.result 带缓存结果。"""
        idem = IdempotentKey(client)
        with idem.guard("pay:12") as guard:
            guard.complete({"status": "paid"}, ttl=60)
        with pytest.raises(IdempotencyDoneError) as excinfo:
            with idem.guard("pay:12"):
                raise AssertionError("不应重复执行")
        assert excinfo.value.result == {"status": "paid"}
        assert "pay:12" in str(excinfo.value)

    def test_guard_exception_autofail_allows_retry(self, client: Any) -> None:
        """临界区异常退出：自动 fail（DEL），后续请求可重试。"""
        idem = IdempotentKey(client)
        with pytest.raises(RuntimeError, match="boom"):
            with idem.guard("pay:13"):
                raise RuntimeError("boom")
        assert client.exists("lizy:idem:pay:13") == 0    # 自动 DEL
        with idem.guard("pay:13") as guard:              # 立即可重试
            guard.complete("retried", ttl=60)
        assert idem.get("pay:13") == "retried"

    def test_guard_normal_exit_without_complete_stays_processing(
        self, client: Any
    ) -> None:
        """正常退出但未 complete：键停留 processing 至 TTL（文档化行为）。"""
        idem = IdempotentKey(client)
        with idem.guard("pay:14"):
            pass                                    # 忘了 complete
        envelope = json.loads(client.get("lizy:idem:pay:14"))
        assert envelope["state"] == "processing"
        assert idem.is_done("pay:14") is False
        assert idem.get("pay:14") is None

    def test_guard_nested_same_key_conflicts(self, client: Any) -> None:
        """同键嵌套 guard：自己占的 processing 坑也进不去（单执行者语义）。"""
        idem = IdempotentKey(client)
        with idem.guard("pay:15"):
            with pytest.raises(IdempotencyConflictError):
                with idem.guard("pay:15"):
                    raise AssertionError("不应进入")

    def test_concurrent_guards_single_winner(self, client: Any) -> None:
        """4 线程并发 guard 同键：恰好 1 个执行者，其余 Conflict/Done。"""
        idem = IdempotentKey(client)
        barrier = threading.Barrier(4)
        winners: List[int] = []
        conflicts: List[int] = []
        dones: List[Any] = []
        errors: List[BaseException] = []

        def worker() -> None:
            barrier.wait()
            try:
                with idem.guard("pay:16") as guard:
                    winners.append(1)
                    time.sleep(0.05)                # 拉长 processing 窗口
                    guard.complete(7, ttl=60)
            except IdempotencyConflictError:
                conflicts.append(1)
            except IdempotencyDoneError as exc:
                dones.append(exc.result)
            except BaseException as exc:  # noqa: BLE001 测试需要捕获一切
                errors.append(exc)

        run_threads(4, worker)
        assert errors == []
        assert len(winners) == 1
        assert len(conflicts) + len(dones) == 3
        assert all(done == 7 for done in dones)
        assert idem.is_done("pay:16") is True
        with pytest.raises(IdempotencyDoneError) as excinfo:   # 后来者拿缓存
            with idem.guard("pay:16"):
                raise AssertionError("不应重复执行")
        assert excinfo.value.result == 7

    def test_guard_handle_manual_fail(self, client: Any) -> None:
        """guard 句柄手动 fail：临界区内主动拆坑，退出后可重试。"""
        idem = IdempotentKey(client)
        with idem.guard("pay:17") as guard:
            assert guard.fail() is True
        assert client.exists("lizy:idem:pay:17") == 0
        assert idem.begin("pay:17") is True          # 可重试
        idem.fail("pay:17")

    # -- fail（compare-holder）-------------------------------------------------

    def test_fail_removes_own_hole(self, client: Any) -> None:
        """fail 删自己的坑：DEL 成功，可立即重新 begin。"""
        idem = IdempotentKey(client)
        idem.begin("pay:20")
        assert idem.fail("pay:20") is True
        assert client.exists("lizy:idem:pay:20") == 0
        assert idem.begin("pay:20") is True          # 坑已腾出

    def test_fail_without_begin_returns_false(self, client: Any) -> None:
        """本实例未占用的键 fail 返回 False（不创建键）。"""
        idem = IdempotentKey(client)
        assert idem.fail("ghost") is False
        assert client.exists("lizy:idem:ghost") == 0

    def test_fail_done_state_refuses(self, client: Any) -> None:
        """done 是终态：complete 后 fail 拒绝拆除，键与结果保留。"""
        idem = IdempotentKey(client)
        idem.begin("pay:21")
        idem.complete("pay:21", "ok", ttl=60)
        assert idem.fail("pay:21") is False          # 终态不许拆
        assert idem.is_done("pay:21") is True
        assert idem.get("pay:21") == "ok"

    def test_fail_does_not_delete_others_hole(self, client: Any) -> None:
        """compare-holder：processing 过期被他人接管后，fail 不误删他人的坑。"""
        idem_first = IdempotentKey(client)
        idem_second = IdempotentKey(client, prefix="lizy:idem")
        assert idem_first.begin("pay:22") is True
        # 模拟第一执行者 processing 过期、第二执行者接管
        client.delete("lizy:idem:pay:22")
        assert idem_second.begin("pay:22") is True
        assert idem_first.fail("pay:22") is False    # holder 不符拒绝删除
        assert client.exists("lizy:idem:pay:22") == 1   # 第二执行者的坑还在
        assert idem_second.fail("pay:22") is True    # 本人可拆

    # -- complete / 查询 ---------------------------------------------------------

    def test_get_and_is_done_three_states(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """get/is_done 三态：缺失、processing、done。"""
        monkeypatch.setattr(dist_idempotent, "_now", lambda: 3_000_000.0)
        idem = IdempotentKey(client)
        assert idem.get("k") is None and idem.is_done("k") is False   # 缺失
        idem.begin("k")
        assert idem.get("k") is None and idem.is_done("k") is False   # processing
        idem.complete("k", [1, 2], ttl=60)
        assert idem.get("k") == [1, 2] and idem.is_done("k") is True  # done

    def test_complete_none_result_disambiguation(self, client: Any) -> None:
        """complete(None)：合法结果就是 None，用 is_done 消歧。"""
        idem = IdempotentKey(client)
        idem.begin("k2")
        idem.complete("k2", None, ttl=60)
        assert idem.get("k2") is None
        assert idem.is_done("k2") is True            # 不是 processing 的 None

    def test_complete_roundtrip_complex_result(self, client: Any) -> None:
        """复杂结果 JSON 往返：dict/list/中文/嵌套/bool 均还原。"""
        idem = IdempotentKey(client)
        payload = {"订单": ["a", "b"], "金额": 9.5, "ok": True, "extra": {"n": None}}
        idem.begin("k3")
        idem.complete("k3", payload, ttl=60)
        assert idem.get("k3") == payload

    def test_complete_overwrites_processing_and_sets_ttl(
        self, client: Any
    ) -> None:
        """complete 覆写 processing：state 转 done，TTL 换 done 档。"""
        idem = IdempotentKey(client)
        idem.begin("k4", processing_ttl=60)
        idem.complete("k4", "r", ttl=2)
        assert idem.is_done("k4") is True
        assert 0 < client.pttl("lizy:idem:k4") <= 2_000

    def test_complete_rejects_non_serializable(self, client: Any) -> None:
        """result 不可 JSON 序列化：中文 ValueError，不落键。"""
        idem = IdempotentKey(client)
        idem.begin("k5")
        for bad in (object(), {"s": {1, 2}}, lambda: None):
            with pytest.raises(ValueError, match="JSON"):
                idem.complete("k5", bad)  # type: ignore[arg-type]
        envelope = json.loads(client.get("lizy:idem:k5"))
        assert envelope["state"] == "processing"     # 原状态未被破坏

    # -- 参数校验与 repr ---------------------------------------------------------

    def test_validation_prefix(self, client: Any) -> None:
        """构造校验：prefix 非空。"""
        with pytest.raises(ValueError, match="prefix"):
            IdempotentKey(client, prefix="")

    def test_validation_key_and_ttls(self, client: Any) -> None:
        """方法校验：key 非空 / processing_ttl、ttl > 0。"""
        idem = IdempotentKey(client)
        for method_args in (
            lambda: idem.begin(""),
            lambda: idem.get(""),
            lambda: idem.is_done(""),
            lambda: idem.fail(""),
            lambda: idem.guard(""),
        ):
            with pytest.raises(ValueError, match="key"):
                method_args()
        for bad_ttl in (0, -1, "60", True):
            with pytest.raises(ValueError, match="processing_ttl"):
                idem.begin("k", processing_ttl=bad_ttl)  # type: ignore[arg-type]
            with pytest.raises(ValueError, match="processing_ttl"):
                idem.guard("k", processing_ttl=bad_ttl)  # type: ignore[arg-type]
            with pytest.raises(ValueError, match="^ttl "):
                idem.complete("k", 1, ttl=bad_ttl)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="key"):
            idem.begin(123)  # type: ignore[arg-type]

    def test_repr_and_custom_prefix(self, client: Any) -> None:
        """repr 不回显客户端；自定义 prefix 落到实际键。"""
        idem = IdempotentKey(client, prefix="app:idem")
        assert "app:idem" in repr(idem)
        idem.begin("pay:30")
        assert client.exists("app:idem:pay:30") == 1
        assert client.exists("lizy:idem:pay:30") == 0
        guard = idem.guard("pay:30", processing_ttl=5)
        assert "pay:30" in repr(guard)
        idem.fail("pay:30")
