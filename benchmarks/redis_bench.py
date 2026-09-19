"""lizysdk.dist Redis 套件全量压测（fakeredis 后端，衡量 SDK 层开销）。

口径与 bench.py / pool_bench.py 一致：每场景固定操作数、3 轮独立计时
取最优（timeit 惯例，抑制机器快慢态漂移）。

**口径声明（重要）**：本压测后端为 fakeredis（进程内模拟，无网络），
衡量的是 **SDK 层开销**——命令组包、Lua 脚本经 lupa 解释执行、信封
JSON 编解码等；**不是** Redis 服务端吞吐或网络 RTT。真 Redis 数据待
docker 环境就绪后另行补充（见 REDIS_REPORT.md）。

场景（设计文档 §5）：
1. RLock 获取/释放：浅重入 1 层 vs 深重入 5 层
2. RLock 看门狗开/关的持锁开销（稳态续期摊销）
3. ReliableQueue push→pop→ack 全链路：单线程串行 + 4 线程竞争
4. DelayQueue push + move_due 批量搬运 + pop_ready
5. Leaderboard add_score / top(10) / rank / around
6. IdempotentKey begin→complete 全流程
7. 对照组：DLock 获取/释放、SlidingWindowCounter incr（延续 v0.6 可比性）

用法::

    python benchmarks/redis_bench.py --out benchmarks/results/redis_bench.json
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import fakeredis  # noqa: E402

from lizysdk import __version__ as BK_VERSION  # noqa: E402
from lizysdk.dist import (  # noqa: E402
    DLock,
    DelayQueue,
    IdempotentKey,
    Leaderboard,
    RLock,
    ReliableQueue,
    SlidingWindowCounter,
)

REPEAT = 3
N_OP = 2_000          # 常规场景操作数
N_THREAD_OP = 4_000   # 4 线程场景总操作数（每线程 1_000）


def _client() -> Any:
    """每场景全新 fakeredis 实例（进程内、无网络、线程安全）。"""
    return fakeredis.FakeRedis()


def _timeit(fn: Callable[[], Any], repeat: int = REPEAT) -> float:
    """返回最优轮墙钟秒数。"""
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


def _ops(best_s: float, n: int) -> float:
    """最优轮折算吞吐（ops/s）。"""
    return n / best_s if best_s > 0 else float("inf")


# ---- 1. RLock -----------------------------------------------------------------------


def bench_rlock() -> dict[str, Any]:
    out: dict[str, Any] = {}

    # 浅重入 1 层：acquire + release 为一个操作对
    client = _client()
    lock = RLock(client, "bench:rlock")

    def cycle_l1(n: int = N_OP) -> None:
        for _ in range(n):
            lock.acquire()
            lock.release()

    t = _timeit(cycle_l1)
    out["l1_s"] = t
    out["l1_ops"] = _ops(t, N_OP)

    # 深重入 5 层：一次 acquire×5 + release×5 为一个操作组（算 10 个操作）
    def cycle_l5(n: int = N_OP // 10) -> None:
        for _ in range(n):
            for _ in range(5):
                lock.acquire()
            for _ in range(5):
                lock.release()

    t = _timeit(cycle_l5)
    out["l5_s"] = t
    out["l5_ops"] = _ops(t, N_OP)  # n×10 次命令级操作

    return out


def bench_rlock_watchdog() -> dict[str, Any]:
    """看门狗开/关的持锁开销：短看门狗（0.3s，续期间隔 0.1s）持锁 0.5s
    ——后台约续 4~5 次；对照固定 lease_time=0.5s（无看门狗）持同样时长。"""
    cycles = 20
    hold = 0.5

    client = _client()
    wd_lock = RLock(client, "bench:wd", watchdog_timeout=0.3)

    def with_watchdog() -> None:
        for _ in range(cycles):
            wd_lock.acquire()
            time.sleep(hold)
            wd_lock.release()

    t_on = _timeit(with_watchdog, repeat=2)

    fixed_lock = RLock(client, "bench:fixed", lease_time=5.0)

    def fixed_lease() -> None:
        for _ in range(cycles):
            fixed_lock.acquire()
            time.sleep(hold)
            fixed_lock.release()

    t_off = _timeit(fixed_lease, repeat=2)

    per_on_ms = (t_on - hold * cycles) / cycles * 1000
    per_off_ms = (t_off - hold * cycles) / cycles * 1000
    return {
        "cycles": cycles,
        "hold_s": hold,
        "watchdog_on_per_cycle_ms": per_on_ms,
        "watchdog_off_per_cycle_ms": per_off_ms,
        "watchdog_overhead_per_cycle_ms": per_on_ms - per_off_ms,
    }


# ---- 3. ReliableQueue ---------------------------------------------------------------


def bench_queue() -> dict[str, Any]:
    out: dict[str, Any] = {}

    # 串行全链路：push → pop → ack
    rq = ReliableQueue(_client(), "bench:rq")

    def serial_cycle(n: int = N_OP) -> None:
        for i in range(n):
            rq.push(f"payload-{i}")
        for _ in range(n):
            job = rq.pop()
            rq.ack(job)

    t = _timeit(serial_cycle)
    out["serial_s"] = t
    out["serial_ops"] = _ops(t, N_OP * 3)  # push/pop/ack 各算一个操作

    # 4 线程竞争：每线程独立 push/pop/ack 各 1000 次（生产消费混合）
    per_thread = N_THREAD_OP // 4
    rq2 = ReliableQueue(_client(), "bench:rq4")

    def thread_fn(tid: int) -> None:
        for i in range(per_thread):
            rq2.push(f"t{tid}-{i}")
            job = rq2.pop()
            if job is not None:
                rq2.ack(job)

    def threads4() -> None:
        ts = [threading.Thread(target=thread_fn, args=(k,)) for k in range(4)]
        for t_ in ts:
            t_.start()
        for t_ in ts:
            t_.join()

    t = _timeit(threads4)
    out["threads4_s"] = t
    out["threads4_ops"] = _ops(t, per_thread * 4 * 3)

    return out


# ---- 4. DelayQueue ------------------------------------------------------------------


def bench_delay() -> dict[str, Any]:
    dq = DelayQueue(_client(), "bench:dq")
    n = N_OP

    def batch(n: int = n) -> None:
        for i in range(n):
            dq.push(f"delayed-{i}", delay=0.001)  # 立即到期（delay 须 > 0），聚焦搬运与弹出
        moved = 0
        while moved < n:
            moved += dq.move_due(limit=500)
        got = 0
        while got < n:
            if dq.pop_ready() is not None:
                got += 1

    t = _timeit(batch)
    return {"n": n, "batch_s": t, "batch_ops": _ops(t, n * 3)}  # push/搬运/弹出


# ---- 5. Leaderboard -----------------------------------------------------------------


def bench_leaderboard() -> dict[str, Any]:
    lb = Leaderboard(_client(), "bench:lb")
    n = N_OP
    members = [f"player-{i}" for i in range(n)]

    t_add = _timeit(lambda: [lb.add_score(m, (i % 997) + 1) for i, m in enumerate(members)])

    def reads() -> None:
        for i in range(0, n, 4):        # 500 次读
            lb.top(10)
            lb.rank(f"player-{i}")
            lb.around(f"player-{i}", span=5)

    t_read = _timeit(reads)

    return {
        "n_members": n,
        "add_s": t_add,
        "add_ops": _ops(t_add, n),
        "reads": n // 4,
        "read_s": t_read,
        "read_ops": _ops(t_read, (n // 4) * 3),
    }


# ---- 6. IdempotentKey ---------------------------------------------------------------


def bench_idempotent() -> dict[str, Any]:
    idem = IdempotentKey(_client())
    n = N_OP

    def flow(n: int = n) -> None:
        for i in range(n):
            if idem.begin(f"bench:key:{i}"):
                idem.complete(f"bench:key:{i}", result={"ok": True, "i": i})

    t = _timeit(flow)
    return {"n": n, "s": t, "ops": _ops(t, n * 2)}  # begin+complete 各一个操作


# ---- 7. 对照组（延续 v0.6 可比性） ----------------------------------------------------


def bench_control() -> dict[str, Any]:
    out: dict[str, Any] = {}

    client = _client()
    dlock = DLock(client, "bench:dlock")

    def dlock_cycle(n: int = N_OP) -> None:
        for _ in range(n):
            dlock.acquire()
            dlock.release()

    t = _timeit(dlock_cycle)
    out["dlock_s"] = t
    out["dlock_ops"] = _ops(t, N_OP)

    swc = SlidingWindowCounter(_client(), window=60)

    def swc_incr(n: int = N_OP) -> None:
        for _ in range(n):
            swc.incr("bench:swc")

    t = _timeit(swc_incr)
    out["swc_incr_s"] = t
    out["swc_incr_ops"] = _ops(t, N_OP)

    return out


# ---- 主入口 -------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description="lizysdk Redis 套件压测（fakeredis 后端）")
    parser.add_argument("--out", type=Path, default=Path("benchmarks/results/redis_bench.json"))
    args = parser.parse_args()

    print(f"lizysdk {BK_VERSION} Redis 套件压测（fakeredis 进程内后端，每场景 3 轮取最优）")
    data: dict[str, Any] = {
        "meta": {
            "lizysdk_version": BK_VERSION,
            "backend": f"fakeredis {fakeredis.__version__}（进程内，无网络）",
            "python": sys.version,
            "platform": platform.platform(),
            "cpu": platform.processor() or "unknown",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
    }
    for name, fn in [
        ("rlock", bench_rlock),
        ("rlock_watchdog", bench_rlock_watchdog),
        ("queue", bench_queue),
        ("delay", bench_delay),
        ("leaderboard", bench_leaderboard),
        ("idempotent", bench_idempotent),
        ("control", bench_control),
    ]:
        print(f"  running {name} ...", flush=True)
        data[name] = fn()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"结果已写入: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
