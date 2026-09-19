"""lizysdk 全家桶示例：FastAPI 服务接入结构化日志 + 标准化错误 + 业务码。

无需真正启动服务器（用 TestClient 驱动），在包根目录运行::

    python examples/web_demo.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

import lizysdk as bk
from lizysdk.ext import install_fastapi_handler

# 1) 业务错误码：进程启动时注册一次（模板 + HTTP 状态码）
bk.register_code("PAY_BALANCE_NOT_ENOUGH", "余额不足: 还差 {amount} 元", 422)

# 2) Web 适配器：AppError 子类自动转 JSON 响应；未知异常兜底 500
app = install_fastapi_handler(FastAPI(), include_generic=True)


@app.get("/api/orders/{order_no}")
def get_order(order_no: str) -> dict:
    """每个请求绑定独立 trace_id，业务日志自动携带。"""
    bk.bind_context(trace_id=bk.new_trace_id())
    log = bk.get_logger("web")

    log.info("order query", order_no=order_no, order_id=bk.new_prefixed_id("ORD"))

    if order_no == "SO-404":
        raise bk.NotFoundError(params={"resource": f"订单 {order_no}"})
    if order_no == "SO-PAY":
        raise bk.AppError("PAY_BALANCE_NOT_ENOUGH", params={"amount": "3.50"})

    if order_no == "SO-DEDUCT":
        try:
            _deduct(order_no)
        except Exception as inner:  # 演示 wrap：底层异常包装为标准化错误（保留异常链）
            raise bk.wrap(inner, code=bk.ErrorCode.SERVICE_UNAVAILABLE) from inner

    return {"order_no": order_no, "status": "ok", "uid": bk.new_uid()}


def _deduct(order_no: str) -> None:
    raise ConnectionError("redis connection refused")


def main() -> int:
    log_dir = Path(__file__).resolve().parent / "demo_logs"
    bk.setup_logging(log_dir, "web.log", sys_name="web-demo", level="DEBUG", console=False)

    client = TestClient(app, raise_server_exceptions=False)
    print("=" * 72)
    for no in ("SO-001", "SO-404", "SO-PAY", "SO-DEDUCT"):
        resp = client.get(f"/api/orders/{no}")
        print(f"GET /api/orders/{no:<10} -> {resp.status_code}  {resp.json()}")
    bk.flush(5)

    print("=" * 72)
    for line in (log_dir / "web.log").read_text(encoding="utf-8").splitlines():
        p = bk.parse_line(line)
        print(f"{p['level']:<8} {p['timestamp']}  trace={p.get('trace_id', '-')[:8]}  {p['message']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
