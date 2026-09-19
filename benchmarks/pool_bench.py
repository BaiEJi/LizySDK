"""lizysdk.pools 全方位压测：加速比 / 扩展曲线 / 包装开销 / 特性开销。

口径与 bench.py 一致：每场景固定任务数、3 轮独立计时取最优（timeit 惯例，
抑制线程调度噪声），对比维度：

1. IO 密集（1ms sleep 模拟）：串行基线 vs ThreadPool(workers=1/2/4/8/16)
   vs stdlib ThreadPoolExecutor
2. CPU 密集（纯 Python 计算 ~50ms/任务）：串行基线 vs ProcessPool(workers=1/2/4/8)
   vs stdlib ProcessPoolExecutor，另附 ThreadPool(CPU 任务, GIL 无加速佐证)
3. 协程池（1ms asyncio.sleep）：串行 await 基线 vs AsyncPool(workers=8/16/32/64)
   vs 裸 asyncio.gather
4. 提交时延：submit() 调用方开销 vs stdlib executor.submit
5. 特性开销：无配置 / 全钩子 / ctx 传播 / 重试配置 / 超时配置 每任务额外成本

用法::

    python benchmarks/pool_bench.py --out benchmarks/results/pool_bench.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import lizysdk as bk  # noqa: E402
from lizysdk.pools import AsyncPool, ProcessPool, ThreadPool  # noqa: E402

IO_SLEEP = 0.001        # 单任务模拟 IO 时长
IO_N = 2_000            # IO 场景任务数
CPU_LOOPS = 1_500_000   # 单任务计算量（约 50-90ms，视机器而定）
CPU_N = 24              # CPU 场景任务数（进程池启动成本高，控制规模）
AIO_N = 5_000           # 协程场景任务数
REPEAT = 3


def io_task() -> int:
    time.sleep(IO_SLEEP)
    return 1


def cpu_task(loops: int = CPU_LOOPS) -> int:
    """模块级纯计算任务（可 pickle，供进程池使用）。"""
    x = 0
    for i in range(loops):
        x += i * i
    return x


async def aio_task() -> int:
    await asyncio.sleep(IO_SLEEP)
    return 1


def _drain(futs: list[Any]) -> None:
    for f in futs:
        f.result()


def _timeit(fn: Callable[[], Any], repeat: int = REPEAT) -> float:
    """返回最优轮墙钟秒数。"""
    best = float("inf")
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        best = min(best, time.perf_counter() - t0)
    return best


def _calibrate() -> float:
    """测量单任务成本（写入结果，供报告引用）。"""
    t0 = time.perf_counter()
    cpu_task()
    return time.perf_counter() - t0


# ---- 场景 --------------------------------------------------------------------------


def bench_io() -> dict[str, Any]:
    """IO 密集：串行 vs ThreadPool 各规模 vs stdlib。"""
    out: dict[str, Any] = {"task_ms": IO_SLEEP * 1000, "n": IO_N}

    serial = _timeit(lambda: [io_task() for _ in range(IO_N)])
    out["serial_s"] = serial

    for w in (1, 2, 4, 8, 16):
        with bk.create_pool("thread", workers=w, name=f"io-{w}") as pool:
            t = _timeit(lambda p=pool, n=IO_N: _drain([p.submit(io_task) for _ in range(n)]))
        out[f"thread_w{w}_s"] = t
        out[f"thread_w{w}_speedup"] = serial / t

    with ThreadPoolExecutor(max_workers=8) as ex:
        t = _timeit(lambda: _drain([ex.submit(io_task) for _ in range(IO_N)]))
    out["stdlib_thread_w8_s"] = t
    out["stdlib_vs_ours_w8"] = t / out["thread_w8_s"]
    return out


def bench_cpu() -> dict[str, Any]:
    """CPU 密集：串行 vs ProcessPool 各规模 vs stdlib，附线程池 GIL 佐证。"""
    per_task = _calibrate()
    out: dict[str, Any] = {"per_task_ms": per_task * 1000, "n": CPU_N}

    serial = _timeit(lambda: [cpu_task() for _ in range(CPU_N)])
    out["serial_s"] = serial

    for w in (1, 2, 4, 8):
        with bk.create_pool("process", workers=w, name=f"cpu-{w}") as pool:
            t = _timeit(
                lambda p=pool, n=CPU_N: _drain([p.submit(cpu_task) for _ in range(n)])
            )
        out[f"process_w{w}_s"] = t
        out[f"process_w{w}_speedup"] = serial / t

    with bk.create_pool("thread", workers=8) as pool:
        t = _timeit(lambda p=pool, n=CPU_N: _drain([p.submit(cpu_task) for _ in range(n)]))
    out["thread_w8_s"] = t  # GIL：CPU 任务线程池无加速（speedup≈1）
    out["thread_w8_speedup"] = serial / t
    return out


def bench_async() -> dict[str, Any]:
    """协程池：串行 await vs AsyncPool 各规模 vs 裸 gather。"""
    out: dict[str, Any] = {"task_ms": IO_SLEEP * 1000, "n": AIO_N}

    def serial_coro() -> None:
        async def run() -> None:
            for _ in range(AIO_N):
                await aio_task()
        asyncio.run(run())

    out["serial_s"] = _timeit(serial_coro, repeat=2)

    for w in (8, 16, 32, 64):
        with bk.create_pool("async", workers=w, name=f"aio-{w}") as pool:
            t = _timeit(
                lambda p=pool, n=AIO_N: _drain([p.submit(aio_task) for _ in range(n)])
            )
        out[f"async_w{w}_s"] = t
        out[f"async_w{w}_speedup"] = out["serial_s"] / t

    def gather_all() -> None:
        async def run() -> None:
            await asyncio.gather(*(aio_task() for _ in range(AIO_N)))

        asyncio.run(run())

    out["gather_s"] = _timeit(gather_all, repeat=2)
    return out


def bench_submit_latency() -> dict[str, Any]:
    """submit() 调用方单次开销（无界队列、大池、不等待完成）。"""
    n = 20_000

    def noop() -> None:
        return None

    with bk.create_pool("thread", workers=4) as pool:
        t = _timeit(lambda: [pool.submit(noop) for _ in range(n)][-1] and None)
        pool.shutdown(wait=True)
    ours_us = t / n * 1e6

    with ThreadPoolExecutor(max_workers=4) as ex:
        t = _timeit(lambda: [ex.submit(noop) for _ in range(n)][-1] and None)
        ex.shutdown(wait=True)
    stdlib_us = t / n * 1e6

    return {"n": n, "ours_us": ours_us, "stdlib_us": stdlib_us}


def bench_feature_overhead() -> dict[str, Any]:
    """特性开关的每任务额外成本（IO 任务、workers=8、相对裸配置）。"""
    n = 1_000

    def run(pool_kwargs: dict[str, Any]) -> float:
        with bk.create_pool("thread", workers=8, **pool_kwargs) as pool:
            return _timeit(
                lambda p=pool: _drain([p.submit(io_task) for _ in range(n)]), repeat=3
            )

    base = run({})
    hooks_all = {
        "on_submit": lambda i: None, "on_start": lambda i: None,
        "on_success": lambda i: None, "on_error": lambda i: None,
        "on_retry": lambda i: None, "on_timeout": lambda i: None,
        "on_reject": lambda i: None, "on_shutdown": lambda i: None,
    }
    with_hooks = run({"hooks": hooks_all})
    no_ctx = run({"propagate_context": False})
    with_retry = run({"max_retries": 1, "retry_backoff": 0.0})
    with_timeout = run({"task_timeout": 5.0})

    def per_us(t: float) -> float:
        return (t - base) / n * 1e6

    return {
        "n": n, "base_s": base,
        "hooks8_overhead_us": per_us(with_hooks),
        "noctx_delta_us": per_us(no_ctx),
        "retry_cfg_overhead_us": per_us(with_retry),
        "timeout_cfg_overhead_us": per_us(with_timeout),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="lizysdk.pools 压测")
    parser.add_argument("--out", type=Path, default=Path("benchmarks/results/pool_bench.json"))
    args = parser.parse_args()

    print(f"lizysdk {bk.__version__} pools 压测（每场景 3 轮取最优）")
    data: dict[str, Any] = {
        "meta": {
            "lizysdk_version": bk.__version__,
            "python": sys.version,
            "platform": platform.platform(),
            "cpu": platform.processor() or "unknown",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
    }
    for name, fn in [
        ("io_thread", bench_io),
        ("cpu_process", bench_cpu),
        ("async_pool", bench_async),
        ("submit_latency", bench_submit_latency),
        ("feature_overhead", bench_feature_overhead),
    ]:
        print(f"  running {name} ...", flush=True)
        data[name] = fn()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"结果已写入: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
