"""lizysdk.dist 测试：滑动窗口计数器 + 分布式锁（fakeredis 全离线）。

覆盖范围（设计文档 §2.2 / §2.3 / §4 全落地）：
1. 懒加载红线：``import lizysdk.dist`` 不触发 redis 导入（子进程模拟未装
   redis）；load_redis / from_url 未装 redis 时 ImportError 含
   ``pip install "lizysdk[redis]"`` 提示（sys.modules[redis]=None 技巧）；
   client_from_url 在已装环境返回 redis.Redis；
2. 窗口计数器：incr 单调计数 / count 只读不增 / 窗口剔除（monkeypatch
   ``_now`` 冻结推进：t0 计 3 -> 推进 window+ε 后 count==0、再 incr==1；
   含「恰在窗口边界即剔除」）/ allow 边界（==limit 通过、+1 拒绝且被拒
   者占名额）/ amount>1 批量计数 / 多键独立 / reset / TTL 已设
   （ttl>0 且 <= ceil(window*1000) 毫秒）/ 8 线程并发各 incr 100 次计数
   守恒 ==800（时间冻结排除窗口剔除干扰）/ from_url 懒加载构造与 kwargs
   透传 / 参数校验（window / amount / limit）；
3. 分布式锁：获取-释放-再获取流（token 每次 acquire 均不同）/ 互斥
   （同名第二锁 acquire(timeout=0.3) 抛 LockTimeoutError）/ 非阻塞立即
   False / 阻塞等待前锁释放后成功（线程释放 -> 另一锁获取到，且确实
   等待过）/ token 保护（手动 SET 篡改后 release 抛 LockNotOwnedError、
   extend False）/ 未获取与键不存在的 release 两种 LockNotOwnedError /
   TTL 过期自动可再获取（timeout=0.05 + 真实 sleep）/ extend 成功 True
   （pttl 被重设）与过期/未持有 False / 上下文管理器正常与异常路径均
   释放、进入即获取失败抛 LockError 族 / locked() 任意持有者视角 /
   不可重入（未释放再 acquire 按争抢超时，本地 token 不被覆盖，仍可
   释放）/ 4 线程争抢同一锁临界区恰好串行（max_concurrent==1）/
   异常族继承关系与错误消息含锁名 / 参数校验。
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

import fakeredis
import pytest

from lizysdk.dist import (
    DLock,
    LockError,
    LockNotOwnedError,
    LockTimeoutError,
    SlidingWindowCounter,
)
from lizysdk.dist import _client as dist_client
from lizysdk.dist import window as dist_window

_SRC_DIR = Path(__file__).resolve().parents[1] / "src"


# ---------------------------------------------------------------------------
# 通用 fixture 与辅助
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> Any:
    """每个测试一个全新的 FakeStrictRedis（支持 EVAL / TTL / 线程安全）。"""
    return fakeredis.FakeStrictRedis()


def run_threads(count: int, target: Any) -> List[threading.Thread]:
    """启动 count 个线程执行 target() 并 join，返回线程列表。"""
    threads = [threading.Thread(target=target, daemon=True) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    return threads


# ---------------------------------------------------------------------------
# 懒加载红线
# ---------------------------------------------------------------------------


class TestLazyImport:
    """模块级不 import redis；使用路径未装 redis 时给中文安装提示。"""

    def test_module_import_without_redis(self) -> None:
        """子进程模拟未装 redis：import lizysdk.dist 成功，from_url 报中文提示。"""
        script = (
            "import sys\n"
            "sys.modules['redis'] = None\n"       # 模拟未安装（import 即 ImportError）
            "sys.modules['fakeredis'] = None\n"
            "import lizysdk.dist\n"               # 模块级懒加载红线：必须成功
            "print('IMPORT_OK')\n"
            "try:\n"
            "    lizysdk.dist.SlidingWindowCounter.from_url('redis://localhost:6379/0')\n"
            "except ImportError as exc:\n"
            "    ok = 'lizysdk[redis]' in str(exc) and 'pip install' in str(exc)\n"
            "    print('HINT_OK' if ok else 'HINT_BAD:' + str(exc))\n"
            "else:\n"
            "    print('NO_IMPORT_ERROR')\n"
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
        assert "HINT_OK" in proc.stdout, proc.stdout + proc.stderr

    def test_load_redis_import_error_hint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """主测：未装 redis 时 load_redis 的 ImportError 提示文案存在。"""
        monkeypatch.setitem(sys.modules, "redis", None)
        with pytest.raises(ImportError, match=r"pip install \"lizysdk\[redis\]\""):
            dist_client.load_redis()

    def test_from_url_import_error_hint(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """主测：from_url 懒加载路径的 ImportError 提示文案存在。"""
        monkeypatch.setitem(sys.modules, "redis", None)
        with pytest.raises(ImportError) as excinfo:
            SlidingWindowCounter.from_url("redis://localhost:6379/0")
        assert 'pip install "lizysdk[redis]"' in str(excinfo.value)
        assert "未安装" in str(excinfo.value)

    def test_client_from_url_returns_redis_client(self) -> None:
        """已装 redis 的环境：client_from_url 返回 redis.Redis（不立即建连）。"""
        import redis

        created = dist_client.client_from_url("redis://localhost:6379/0")
        assert isinstance(created, redis.Redis)

    def test_from_url_wires_counter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """from_url 把 url/kwargs 透传给 client_from_url 并完成接线。"""
        captured: Dict[str, Any] = {}

        def fake_from_url(url: str, **kwargs: Any) -> Any:
            captured["url"] = url
            captured["kwargs"] = kwargs
            return fakeredis.FakeStrictRedis()

        monkeypatch.setattr(dist_window, "client_from_url", fake_from_url)
        counter = SlidingWindowCounter.from_url(
            "redis://localhost:6379/2", window=30, decode_responses=True
        )
        assert captured["url"] == "redis://localhost:6379/2"
        assert captured["kwargs"] == {"decode_responses": True}
        assert counter.window == 30.0
        assert counter.incr("k") == 1   # 接线后立即可用
        assert counter.count("k") == 1


# ---------------------------------------------------------------------------
# SlidingWindowCounter
# ---------------------------------------------------------------------------


class TestSlidingWindowCounter:
    """计数、窗口剔除、限流薄糖、TTL、并发守恒与参数校验。"""

    def test_incr_monotonic(self, client: Any) -> None:
        """incr 单调递增：1, 2, 3；count 与之一致。"""
        counter = SlidingWindowCounter(client, window=60)
        assert counter.incr("hits") == 1
        assert counter.incr("hits") == 2
        assert counter.incr("hits") == 3
        assert counter.count("hits") == 3

    def test_count_is_readonly(self, client: Any) -> None:
        """count 只读不增：反复 count 恒为 3。"""
        counter = SlidingWindowCounter(client, window=60)
        for _ in range(3):
            counter.incr("hits")
        assert counter.count("hits") == 3
        assert counter.count("hits") == 3
        assert counter.count("hits") == 3

    def test_count_missing_key_is_zero(self, client: Any) -> None:
        """不存在的键 count 为 0（不建键）。"""
        counter = SlidingWindowCounter(client, window=60)
        assert counter.count("ghost") == 0
        assert client.exists("lizy:swc:ghost") == 0

    def test_window_eviction(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """窗口剔除：t0 计 3 -> 推进 window+ε 后 count==0、再 incr==1。"""
        clock = {"now": 1_000_000.0}
        monkeypatch.setattr(dist_window, "_now", lambda: clock["now"])
        counter = SlidingWindowCounter(client, window=5)
        assert counter.incr("k") == 1
        assert counter.incr("k") == 2
        assert counter.incr("k") == 3

        clock["now"] += 5 + 1e-6          # 推过窗口（window+ε）
        assert counter.count("k") == 0    # 旧事件全部出窗
        assert counter.incr("k") == 1     # 重新从 1 计数

    def test_window_boundary_inclusive_eviction(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """边界语义：事件恰好落在 now-window 上（score == cutoff）即剔除。"""
        clock = {"now": 2_000_000.0}
        monkeypatch.setattr(dist_window, "_now", lambda: clock["now"])
        counter = SlidingWindowCounter(client, window=10)
        counter.incr("k")                 # score = t0
        clock["now"] += 10                # cutoff == t0（恰在边界）
        assert counter.count("k") == 0    # score <= cutoff -> 出窗
        assert counter.incr("k") == 1     # 边界时刻即可重新计数

    def test_events_within_window_kept(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """窗口内的事件（score > now-window）不剔除。"""
        clock = {"now": 3_000_000.0}
        monkeypatch.setattr(dist_window, "_now", lambda: clock["now"])
        counter = SlidingWindowCounter(client, window=60)
        counter.incr("k")                 # t0
        clock["now"] += 59.999            # 仍在窗口内
        counter.incr("k")
        assert counter.count("k") == 2

    def test_allow_boundary(self, client: Any) -> None:
        """allow 边界：第 limit 次通过、再 incr 拒绝；被拒者同样占名额。"""
        counter = SlidingWindowCounter(client, window=60)
        assert counter.allow("api", 2) is True    # 计数 1 <= 2
        assert counter.allow("api", 2) is True    # 计数 2 <= 2（==limit 通过）
        assert counter.allow("api", 2) is False   # 计数 3 > 2（+1 拒绝）
        assert counter.count("api") == 3          # 先计数再判定：被拒者已入窗

    def test_incr_amount(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """amount>1：一次计多个事件，返回值含全部本次事件。"""
        monkeypatch.setattr(dist_window, "_now", lambda: 5_000_000.0)
        counter = SlidingWindowCounter(client, window=60)
        assert counter.incr("k", 3) == 3
        assert counter.incr("k") == 4
        assert counter.incr("k", 2) == 6
        assert counter.count("k") == 6

    def test_multi_key_independent(self, client: Any) -> None:
        """多键互不影响，ZSET 各自独立。"""
        counter = SlidingWindowCounter(client, window=60)
        assert counter.incr("a") == 1
        assert counter.incr("b") == 1
        assert counter.incr("a") == 2
        assert counter.count("a") == 2
        assert counter.count("b") == 1
        assert client.exists("lizy:swc:a") == 1
        assert client.exists("lizy:swc:b") == 1

    def test_custom_prefix(self, client: Any) -> None:
        """自定义 prefix 生效到实际 Redis 键。"""
        counter = SlidingWindowCounter(client, window=60, prefix="app:rate")
        counter.incr("u1")
        assert client.exists("app:rate:u1") == 1
        assert client.exists("lizy:swc:u1") == 0

    def test_reset(self, client: Any) -> None:
        """reset 删除窗口键：计数归零、键消失、重复 reset 安全。"""
        counter = SlidingWindowCounter(client, window=60)
        counter.incr("k")
        counter.incr("k")
        assert counter.count("k") == 2
        counter.reset("k")
        assert counter.count("k") == 0
        assert client.exists("lizy:swc:k") == 0
        counter.reset("k")                 # 幂等：键不存在也安全
        assert counter.count("k") == 0

    def test_ttl_set(self, client: Any) -> None:
        """PEXPIRE 保底 TTL 已设：ttl>0 且 pttl <= ceil(window*1000)。"""
        counter = SlidingWindowCounter(client, window=2.5)
        counter.incr("k")
        assert client.ttl("lizy:swc:k") > 0
        pttl = client.pttl("lizy:swc:k")
        assert 0 < pttl <= 2500
        counter.reset("k")

    def test_concurrent_incr_conservation(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """8 线程并发各 incr 100 次：计数守恒 == 800（时间冻结排干扰）。"""
        monkeypatch.setattr(dist_window, "_now", lambda: 7_000_000.0)
        counter = SlidingWindowCounter(client, window=60)
        barrier = threading.Barrier(8)
        errors: List[BaseException] = []

        def worker() -> None:
            barrier.wait()
            try:
                for _ in range(100):
                    counter.incr("hits")
            except BaseException as exc:  # noqa: BLE001 测试需要捕获一切
                errors.append(exc)

        run_threads(8, worker)
        assert errors == []
        assert counter.count("hits") == 800

    def test_validation_window(self, client: Any) -> None:
        """window 必须为 > 0 的数值。"""
        for bad in (0, -1, -0.5):
            with pytest.raises(ValueError, match="window"):
                SlidingWindowCounter(client, window=bad)
        with pytest.raises(ValueError, match="window"):
            SlidingWindowCounter(client, window="60")  # type: ignore[arg-type]

    def test_validation_amount(self, client: Any) -> None:
        """amount 必须为 >= 1 的整数。"""
        counter = SlidingWindowCounter(client, window=60)
        for bad in (0, -2, 1.5, "1"):  # type: ignore[arg-type]
            with pytest.raises(ValueError, match="amount"):
                counter.incr("k", bad)

    def test_validation_limit(self, client: Any) -> None:
        """limit 必须为 >= 1 的整数。"""
        counter = SlidingWindowCounter(client, window=60)
        for bad in (0, -1, 2.5, "5"):  # type: ignore[arg-type]
            with pytest.raises(ValueError, match="limit"):
                counter.allow("k", bad)

    def test_readonly_properties(self, client: Any) -> None:
        """window / prefix 只读属性与 repr。"""
        counter = SlidingWindowCounter(client, window=30, prefix="p")
        assert counter.window == 30.0
        assert counter.prefix == "p"
        assert "window=30.0" in repr(counter)


# ---------------------------------------------------------------------------
# DLock
# ---------------------------------------------------------------------------


class TestDLock:
    """获取/释放/续期、互斥与争抢、token 保护、TTL、上下文管理器与校验。"""

    def test_exception_hierarchy(self) -> None:
        """异常族继承关系。"""
        assert issubclass(LockError, Exception)
        assert issubclass(LockTimeoutError, LockError)
        assert issubclass(LockNotOwnedError, LockError)

    def test_acquire_release_cycle(self, client: Any) -> None:
        """基本流：获取 True -> locked -> 释放 -> 再获取；token 每次不同。"""
        lock = DLock(client, "cyc", timeout=10)
        assert lock.acquire() is True
        first_token = lock.token
        assert first_token is not None
        assert lock.locked() is True
        lock.release()
        assert lock.token is None
        assert lock.locked() is False

        assert lock.acquire() is True
        assert lock.token is not None
        assert lock.token != first_token       # 每次 acquire 新 token
        lock.release()

    def test_acquire_sets_key_with_px(self, client: Any) -> None:
        """获取后键上即有本对象 token，且带 TTL（PX）。"""
        lock = DLock(client, "px", timeout=30)
        lock.acquire()
        assert client.get("lizy:lock:px") is not None
        assert client.get("lizy:lock:px").decode() == lock.token
        pttl = client.pttl("lizy:lock:px")
        assert 0 < pttl <= 30_000
        lock.release()

    def test_mutual_exclusion_timeout(self, client: Any) -> None:
        """互斥：持锁期间同名第二锁阻塞 acquire(timeout=0.3) 抛超时。"""
        holder = DLock(client, "job:42", timeout=30)
        assert holder.acquire() is True

        contender = DLock(client, "job:42", timeout=30)
        start = time.monotonic()
        with pytest.raises(LockTimeoutError) as excinfo:
            contender.acquire(timeout=0.3)
        assert time.monotonic() - start >= 0.25          # 确实在等待
        assert "job:42" in str(excinfo.value)            # 消息含锁名
        assert contender.token is None                   # 失败不产生本地 token
        holder.release()

    def test_non_blocking_returns_false_fast(self, client: Any) -> None:
        """非阻塞：立即返回 False（不等待）。"""
        holder = DLock(client, "nb", timeout=30)
        holder.acquire()
        contender = DLock(client, "nb", timeout=30)
        start = time.monotonic()
        assert contender.acquire(blocking=False) is False
        assert time.monotonic() - start < 0.5
        holder.release()
        assert contender.acquire(blocking=False) is True
        contender.release()

    def test_blocking_acquire_succeeds_after_release(self, client: Any) -> None:
        """阻塞等待期间前锁被释放：等待者随后获取成功。"""
        holder = DLock(client, "wait", timeout=30)
        assert holder.acquire() is True

        def release_soon() -> None:
            time.sleep(0.2)
            holder.release()

        releaser = threading.Thread(target=release_soon, daemon=True)
        releaser.start()

        contender = DLock(client, "wait", timeout=30, blocking_timeout=5)
        start = time.monotonic()
        assert contender.acquire() is True
        elapsed = time.monotonic() - start
        assert elapsed >= 0.1                # 确实等待过（前锁 0.2s 后才释放）
        assert contender.token is not None
        releaser.join()
        contender.release()

    def test_token_protection_release(self, client: Any) -> None:
        """token 保护：外部篡改键值后 release 拒绝误删，抛 LockNotOwnedError。"""
        lock = DLock(client, "job:42", timeout=30)
        assert lock.acquire() is True
        original_token = lock.token

        client.set("lizy:lock:job:42", "forged-by-others")   # 模拟他人接锁
        with pytest.raises(LockNotOwnedError) as excinfo:
            lock.release()
        assert "job:42" in str(excinfo.value)
        assert client.get("lizy:lock:job:42") == b"forged-by-others"  # 未被误删
        assert lock.token == original_token   # 释放失败不清空本地 token

    def test_token_protection_extend(self, client: Any) -> None:
        """token 保护：篡改后 extend 返回 False（不续他人锁）。"""
        lock = DLock(client, "ext-guard", timeout=30)
        lock.acquire()
        client.set("lizy:lock:ext-guard", "forged-by-others", keepttl=True)
        assert lock.extend(10) is False
        assert client.get("lizy:lock:ext-guard") == b"forged-by-others"  # 值未被改写
        pttl = client.pttl("lizy:lock:ext-guard")
        assert 0 < pttl <= 30_000            # TTL 仍为原锁的 30s 档，未被续期触碰

    def test_release_never_acquired(self, client: Any) -> None:
        """未获取过的对象 release：抛 LockNotOwnedError（锁不存在分支）。"""
        lock = DLock(client, "ghost", timeout=10)
        with pytest.raises(LockNotOwnedError):
            lock.release()

    def test_release_after_key_deleted(self, client: Any) -> None:
        """持锁后键被外部删除：release 抛 LockNotOwnedError。"""
        lock = DLock(client, "gone", timeout=30)
        lock.acquire()
        client.delete("lizy:lock:gone")
        with pytest.raises(LockNotOwnedError):
            lock.release()

    def test_ttl_expiry_allows_reacquire(self, client: Any) -> None:
        """TTL 过期自动失锁：极短 timeout + 真实 sleep 后他人可再获取。"""
        holder = DLock(client, "exp", timeout=0.05)
        assert holder.acquire() is True
        time.sleep(0.25)                      # 等锁自然过期
        assert holder.locked() is False

        contender = DLock(client, "exp", timeout=30)
        assert contender.acquire(blocking=False) is True
        contender.release()

    def test_extend_success_resets_ttl(self, client: Any) -> None:
        """extend 成功：True 且 TTL 被重设为 additional_time。"""
        lock = DLock(client, "ext", timeout=10)
        lock.acquire()
        assert client.pttl("lizy:lock:ext") <= 10_000
        assert lock.extend(50) is True
        pttl = client.pttl("lizy:lock:ext")
        assert 40_000 < pttl <= 50_000
        lock.release()

    def test_extend_without_acquire_is_false(self, client: Any) -> None:
        """未获取（本地无 token）时 extend 返回 False，不抛异常。"""
        lock = DLock(client, "ext2", timeout=10)
        assert lock.extend(10) is False

    def test_extend_after_ttl_expiry_is_false(self, client: Any) -> None:
        """锁 TTL 过期后 extend：token 校验失败返回 False。"""
        lock = DLock(client, "ext3", timeout=0.05)
        lock.acquire()
        time.sleep(0.25)
        assert lock.extend(10) is False

    def test_extend_validation(self, client: Any) -> None:
        """additional_time 必须 > 0。"""
        lock = DLock(client, "ext4", timeout=10)
        for bad in (0, -1, "5"):  # type: ignore[arg-type]
            with pytest.raises(ValueError, match="additional_time"):
                lock.extend(bad)

    def test_context_manager_ok(self, client: Any) -> None:
        """上下文管理器：进入即获取，退出即释放（含 token 清空）。"""
        lock = DLock(client, "cm", timeout=10)
        with lock as entered:
            assert entered is lock
            assert lock.locked() is True
            assert lock.token is not None
        assert lock.locked() is False
        assert lock.token is None

    def test_context_manager_releases_on_exception(self, client: Any) -> None:
        """异常路径：代码块抛错也释放，且原异常照常向上传播。"""
        lock = DLock(client, "cm-exc", timeout=10)
        with pytest.raises(RuntimeError, match="boom"):
            with lock:
                raise RuntimeError("boom")
        assert lock.locked() is False
        assert lock.token is None

    def test_context_manager_acquire_failure_raises_lock_error(
        self, client: Any
    ) -> None:
        """with 进入时获取失败：抛 LockTimeoutError（LockError 子类）。"""
        holder = DLock(client, "cm-fail", timeout=30)
        holder.acquire()
        contender = DLock(client, "cm-fail", timeout=30, blocking_timeout=0.2)
        with pytest.raises(LockError):
            with contender:
                raise AssertionError("不应带着未持有的锁进入代码块")
        holder.release()

    def test_locked_is_any_holder_view(self, client: Any) -> None:
        """locked() 是任意持有者视角：他方对象也能看到 True。"""
        holder = DLock(client, "view", timeout=30)
        observer = DLock(client, "view", timeout=30)
        assert observer.locked() is False
        holder.acquire()
        assert observer.locked() is True      # 不归 observer 持有，但键存在
        holder.release()
        assert observer.locked() is False

    def test_not_reentrant(self, client: Any) -> None:
        """不可重入：持锁中再 acquire 按争抢超时；本地 token 不被覆盖，仍可释放。"""
        lock = DLock(client, "re", timeout=30, blocking_timeout=0.2)
        assert lock.acquire() is True
        held_token = lock.token
        with pytest.raises(LockTimeoutError):
            lock.acquire()                    # 未释放再 acquire -> 争抢 -> 超时
        assert lock.token == held_token       # 失败尝试未覆盖本地 token
        lock.release()                        # 原持锁状态完好，可正常释放
        assert lock.locked() is False

    def test_concurrent_four_threads_serialize(self, client: Any) -> None:
        """4 线程争抢同一锁：全部获取成功且临界区恰好串行（max_concurrent==1）。"""
        state = {"inside": 0, "max_inside": 0, "acquired": 0}
        state_guard = threading.Lock()
        barrier = threading.Barrier(4)
        errors: List[BaseException] = []

        def worker() -> None:
            barrier.wait()
            lock = DLock(client, "shared:job", timeout=30, blocking_timeout=30)
            try:
                if not lock.acquire():
                    errors.append(AssertionError("30 秒内未获取到锁"))
                    return
                try:
                    with state_guard:
                        state["inside"] += 1
                        state["acquired"] += 1
                        state["max_inside"] = max(state["max_inside"], state["inside"])
                    time.sleep(0.02)          # 拉长临界区，放大交错可能
                finally:
                    with state_guard:
                        state["inside"] -= 1
                    lock.release()
            except BaseException as exc:  # noqa: BLE001 测试需要捕获一切
                errors.append(exc)

        run_threads(4, worker)
        assert errors == []
        assert state["acquired"] == 4
        assert state["max_inside"] == 1       # 任何时刻至多 1 人在临界区
        assert DLock(client, "shared:job", timeout=5).locked() is False

    def test_validation_constructor(self, client: Any) -> None:
        """构造校验：name 非空 / timeout>0 / blocking_timeout>=0。"""
        with pytest.raises(ValueError, match="name"):
            DLock(client, "", timeout=10)
        with pytest.raises(ValueError, match="timeout"):
            DLock(client, "n", timeout=0)
        with pytest.raises(ValueError, match="timeout"):
            DLock(client, "n", timeout=-1)
        with pytest.raises(ValueError, match="blocking_timeout"):
            DLock(client, "n", timeout=10, blocking_timeout=-0.1)

    def test_validation_acquire_timeout(self, client: Any) -> None:
        """acquire 覆盖参数校验：timeout >= 0。"""
        lock = DLock(client, "v", timeout=10)
        with pytest.raises(ValueError, match="timeout"):
            lock.acquire(timeout=-1)

    def test_readonly_properties_and_repr(self, client: Any) -> None:
        """name / key 只读属性与 repr（不回显 token）。"""
        lock = DLock(client, "job:42", timeout=10, blocking_timeout=5, prefix="p")
        assert lock.name == "job:42"
        assert lock.key == "p:job:42"
        assert lock.token is None
        text = repr(lock)
        assert "job:42" in text and "held=False" in text
        assert lock.acquire() is True
        assert "held=True" in repr(lock)
        assert lock.token not in repr(lock)
        lock.release()

    def test_error_messages_contain_lock_name(self, client: Any) -> None:
        """中文错误消息携带锁名（超时与误删两种）。"""
        holder = DLock(client, "order:99", timeout=30)
        holder.acquire()
        contender = DLock(client, "order:99", timeout=30, blocking_timeout=0.1)
        with pytest.raises(LockTimeoutError) as timeout_info:
            contender.acquire()
        assert "order:99" in str(timeout_info.value)

        forged = DLock(client, "order:99", timeout=30)
        with pytest.raises(LockNotOwnedError) as owner_info:
            forged.release()                  # 从未获取 -> 锁不存在分支
        assert "order:99" in str(owner_info.value)
        holder.release()
