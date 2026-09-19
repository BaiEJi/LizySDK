"""lizysdk 性能基准（压测）脚本。

聚合口径：每个指标固定迭代次数、5 轮独立计时，取**最优轮** ops/sec
（与 ``timeit`` 取最小耗时的惯例一致：衡量能力上限，抑制线程调度等
环境噪声）；优化前后用同一脚本、同一参数、**同一会话内交错运行**，
保证可比。

用法::

    python benchmarks/bench.py --out benchmarks/results/baseline.json
    python benchmarks/bench.py --out benchmarks/results/after.json
    python benchmarks/bench.py --compare benchmarks/results/baseline.json benchmarks/results/after.json
"""

from __future__ import annotations

import argparse
import contextlib
import json
import platform
import shutil
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import lizysdk as bk  # noqa: E402
from lizysdk.logs import handlers as _log_handlers  # noqa: E402


@contextlib.contextmanager
def _bench_dir(tag: str):
    """基准专用临时目录：退出前先停掉日志 backend 释放文件句柄（Windows 必需）。"""
    d = tempfile.mkdtemp(prefix=f"lizybench-{tag}-")
    try:
        yield Path(d)
    finally:
        try:
            with _log_handlers._BACKEND_LOCK:
                backend = _log_handlers._BACKEND
                _log_handlers._BACKEND = None
            if backend is not None:
                _log_handlers._teardown(backend)
        finally:
            shutil.rmtree(d, ignore_errors=True)

PIPE_LINE = (
    "INFO||2026-09-19T10:30:00||app.py:42||sys_name=bench||"
    "trace_id=0123456789abcdef||user_id=123||action=login||cost_ms=5||"
    "message=user logged in"
)

JSON_LINE = json.dumps(
    {
        "level": "INFO",
        "timestamp": "2026-09-19T10:30:00",
        "file": "app.py",
        "line": 42,
        "sys_name": "bench",
        "trace_id": "0123456789abcdef",
        "user_id": "123",
        "action": "login",
        "cost_ms": "5",
        "message": "user logged in",
    },
    ensure_ascii=False,
)


def _measure(fn: Callable[[], Any], n: int, repeat: int = 5, warmup: int = 1000) -> list[float]:
    """执行 repeat 轮，每轮 n 次调用，返回各轮 ops/sec（调用方取最优轮）。"""
    for _ in range(warmup):
        fn()
    results: list[float] = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        for _ in range(n):
            fn()
        elapsed = time.perf_counter() - t0
        results.append(n / elapsed)
    return results


def _setup(log_dir: Path, **kwargs: Any) -> Any:
    """以给定参数初始化日志，返回可用于打点的 logger。"""
    bk.setup_logging(log_dir, "bench.log", level="DEBUG", console=False,
                     rotation="none", **kwargs)
    return bk.get_logger("bench")


# ---- 指标定义 ----------------------------------------------------------------------


def bench_emit_async(n: int) -> dict[str, Any]:
    """异步写入下 log.info() 的调用方开销（不含后台落盘，flush 在计时外）。"""
    with _bench_dir("emit") as d:
        log = _setup(d / "emit", async_writer=True)
        fields = {"user_id": 123, "action": "login", "cost_ms": 5}

        def call() -> None:
            log.info("user logged in", **fields)

        rounds = _measure(call, n)
        bk.flush(timeout=30)
        return {"rounds": rounds}


def bench_emit_async_threads(n: int) -> dict[str, Any]:
    """4 线程并发 log.info()（异步写入），按墙钟计总吞吐。"""
    with _bench_dir("threads") as d:
        _setup(d / "threads", async_writer=True)
        barrier = threading.Barrier(4)
        per = n // 4

        def worker() -> None:
            log = bk.get_logger("bench")
            barrier.wait()
            for _ in range(per):
                log.info("user logged in", user_id=123, action="login")

        rounds: list[float] = []
        for _ in range(3):
            threads = [threading.Thread(target=worker) for _ in range(4)]
            t0 = time.perf_counter()
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            rounds.append(n / (time.perf_counter() - t0))
        bk.flush(timeout=30)
        return {"rounds": rounds}


def bench_pipeline_sync(n: int) -> dict[str, Any]:
    """同步直写（async_writer=False）端到端：校验→格式化→落盘。"""
    with _bench_dir("sync") as d:
        log = _setup(d / "sync", async_writer=False)

        def call() -> None:
            log.info("user logged in", user_id=123, action="login", cost_ms=5)

        return {"rounds": _measure(call, n)}


def bench_pipeline_json_sync(n: int) -> dict[str, Any]:
    """同步直写 + JSONL 格式（json.dumps 路径）。"""
    with _bench_dir("json") as d:
        log = _setup(d / "json", async_writer=False, json_format=True)

        def call() -> None:
            log.info("user logged in", user_id=123, action="login", cost_ms=5)

        return {"rounds": _measure(call, n)}


def bench_parse_pipe(n: int) -> dict[str, Any]:
    parse = bk.parse_line
    return {"rounds": _measure(lambda: parse(PIPE_LINE), n)}


def bench_parse_json(n: int) -> dict[str, Any]:
    parse = bk.parse_line
    return {"rounds": _measure(lambda: parse(JSON_LINE), n)}


def bench_id_trace(n: int) -> dict[str, Any]:
    fn = bk.new_trace_id
    return {"rounds": _measure(fn, n)}


def bench_id_uid(n: int) -> dict[str, Any]:
    fn = bk.new_uid
    return {"rounds": _measure(fn, n)}


def bench_id_snowflake(n: int) -> dict[str, Any]:
    fn = bk.new_id
    return {"rounds": _measure(fn, n)}


def bench_id_ulid(n: int) -> dict[str, Any]:
    fn = bk.new_sortable_id
    return {"rounds": _measure(fn, n)}


def bench_error_to_dict(n: int) -> dict[str, Any]:
    from lizysdk import ErrorCode

    def call() -> None:
        bk.NotFoundError(params={"resource": "订单"}).to_dict()

    return {"rounds": _measure(call, n), "_unused": ErrorCode.RESOURCE_NOT_FOUND}


METRICS: list[tuple[str, int, Callable[[int], dict[str, Any]]]] = [
    ("log_emit_async_caller", 100_000, bench_emit_async),
    ("log_emit_async_4threads", 100_000, bench_emit_async_threads),
    ("log_pipeline_sync", 20_000, bench_pipeline_sync),
    ("log_pipeline_json_sync", 10_000, bench_pipeline_json_sync),
    ("parse_pipe", 50_000, bench_parse_pipe),
    ("parse_json", 20_000, bench_parse_json),
    ("id_new_trace_id", 100_000, bench_id_trace),
    ("id_new_uid", 100_000, bench_id_uid),
    ("id_new_id_snowflake", 100_000, bench_id_snowflake),
    ("id_new_sortable_id", 100_000, bench_id_ulid),
    ("error_construct_to_dict", 50_000, bench_error_to_dict),
]


def run_all() -> dict[str, Any]:
    data: dict[str, Any] = {
        "meta": {
            "lizysdk_version": bk.__version__,
            "python": sys.version,
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        },
        "metrics": {},
    }
    for name, n, fn in METRICS:
        result = fn(n)
        rounds = result.pop("rounds")
        best = max(rounds)
        data["metrics"][name] = {
            "n": n,
            "ops_per_sec": best,
            "rounds": [round(r, 1) for r in rounds],
            **result,
        }
        print(f"  {name:<28} {best:>12,.0f} ops/s")
    return data


def compare(base_path: Path, after_path: Path) -> int:
    base = json.loads(base_path.read_text(encoding="utf-8"))
    after = json.loads(after_path.read_text(encoding="utf-8"))
    print(f"{'指标':<28} {'基线 ops/s':>14} {'优化后 ops/s':>14} {'提升':>9}")
    geo: list[float] = []
    worst = None
    for name, n, _ in METRICS:
        b = base["metrics"][name]["ops_per_sec"]
        a = after["metrics"][name]["ops_per_sec"]
        gain = (a / b - 1) * 100
        geo.append(a / b)
        if worst is None or gain < worst[1]:
            worst = (name, gain)
        print(f"{name:<28} {b:>14,.0f} {a:>14,.0f} {gain:>+8.1f}%")
    geomean = statistics.geometric_mean(geo) - 1
    print("-" * 70)
    print(f"几何平均提升: {geomean * 100:+.1f}%   最小单项: {worst[0]} {worst[1]:+.1f}%")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="lizysdk 性能基准")
    parser.add_argument("--out", type=Path, help="结果 JSON 输出路径")
    parser.add_argument("--compare", nargs=2, type=Path, metavar=("BASE", "AFTER"),
                        help="对比两份结果文件")
    args = parser.parse_args()

    if args.compare:
        return compare(*args.compare)

    print(f"lizysdk {bk.__version__} 基准测试（每指标 5 轮取最优）")
    data = run_all()
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n结果已写入: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
