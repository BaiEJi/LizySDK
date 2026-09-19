"""聚合基准结果并生成对比报告。

口径：同一代码版本的所有运行文件中，每个指标取**最优轮**（与 bench.py 的
best-of 聚合一致，抑制线程调度与机器状态噪声），再对比两侧。

- 基线：文件名以 ``base`` 开头的结果（v0.3.0 未优化代码）
- 优化后：文件名以 ``after_final`` 开头的结果（最终优化代码）

用法::

    python benchmarks/report.py            # 打印表格与结论
"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"

METRIC_ORDER = [
    "log_emit_async_caller",
    "log_emit_async_4threads",
    "log_pipeline_sync",
    "log_pipeline_json_sync",
    "parse_pipe",
    "parse_json",
    "id_new_trace_id",
    "id_new_uid",
    "id_new_id_snowflake",
    "id_new_sortable_id",
    "error_construct_to_dict",
]


def _best_rounds(pattern: str) -> dict[str, float]:
    best: dict[str, float] = {}
    for path in sorted(RESULTS.glob(pattern)):
        data = json.loads(path.read_text(encoding="utf-8"))
        for name, m in data["metrics"].items():
            value = max(m["rounds"])
            best[name] = max(best.get(name, 0.0), value)
    return best


def main() -> int:
    base = _best_rounds("base*.json")
    after = _best_rounds("after_final*.json")
    if not base or not after:
        print("缺少结果文件：results/base*.json 或 results/after_final*.json")
        return 1

    print(f"{'指标':<28} {'基线 ops/s':>12} {'优化后 ops/s':>13} {'提升':>9}")
    ratios: list[float] = []
    for name in METRIC_ORDER:
        b, a = base[name], after[name]
        gain = (a / b - 1) * 100
        ratios.append(a / b)
        print(f"{name:<28} {b:>12,.0f} {a:>13,.0f} {gain:>+8.1f}%")
    geomean = (statistics.geometric_mean(ratios) - 1) * 100
    print("-" * 68)
    print(f"几何平均提升: {geomean:+.1f}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
