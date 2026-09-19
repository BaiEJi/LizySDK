"""lizysdk 一分钟上手示例：trace_id + 结构化日志 + 标准化错误 协同工作。

在包根目录运行（需先 ``pip install -e .``，或确认 ``src`` 在 ``PYTHONPATH``）::

    python examples/demo.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import lizysdk as bk


def _deduct_stock(order_no: str) -> None:
    """模拟依赖故障：底层异常被 wrap 成标准化错误（保留异常链）。"""
    try:
        raise ConnectionError("redis connection refused")
    except ConnectionError as inner:
        raise bk.wrap(
            inner,
            code=bk.ErrorCode.SERVICE_UNAVAILABLE,
            details={"component": "stock", "order_no": order_no},
        ) from inner


def place_order(order_no: str) -> None:
    """模拟一次业务调用：绑定 trace_id、写结构化日志、抛标准化错误。"""
    log = bk.get_logger("demo.order")
    bk.bind_context(trace_id=bk.new_trace_id())

    try:
        log.info("order received", order_no=order_no, user_id=10086, req_id=bk.new_uid())
        log.info("order created", order_no=order_no, order_id=bk.new_prefixed_id("ORD"))

        try:
            _deduct_stock(order_no)
        except bk.AppError as exc:
            log.error(
                "order failed",
                code=exc.code,
                http_status=exc.http_status,
                component=exc.details["component"],
            )
            raise
    finally:
        bk.clear_context()


def main() -> int:
    log_dir = Path(__file__).resolve().parent / "demo_logs"
    # 可选：json_format=True 输出 JSONL；send_json=True + send_url=... 打印完即发送 JSON
    bk.setup_logging(log_dir, "demo.log", level="DEBUG", sys_name="lizy-demo")

    log = bk.get_logger("demo")
    log.info("lizysdk demo started", version=bk.__version__)

    try:
        place_order("SO-20260919-0001")
    except bk.AppError as exc:
        log.critical("place_order finally failed", error=str(exc))

    bk.flush(5)

    print("=" * 72)
    print(f"日志文件: {log_dir / 'demo.log'}")
    print("=" * 72)
    for line in (log_dir / "demo.log").read_text(encoding="utf-8").splitlines():
        parsed = bk.parse_line(line)
        keys = [k for k in parsed if k not in ("level", "timestamp", "file", "line", "sys_name")]
        print(f"{parsed['level']:<8} {parsed['timestamp']}  {parsed['file']}:{parsed['line']}  [{parsed['sys_name']}]")
        print(f"         kv: {', '.join(keys)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
