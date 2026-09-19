"""lizysdk.logs —— pipe-logfmt / JSONL 结构化日志子模块。

一行日志的固定契约（pipe 格式，分隔符为双竖线 ``||``；``sys_name`` 恒为
``FILE:LINE`` 之后第一段，``message`` 恒为最后一段）::

    LEVEL||TIMESTAMP||FILE:LINE||sys_name=xxx||k1=v1||k2=v2||message=<文本>

示例::

    INFO||2026-09-19T10:30:00||app.py:42||sys_name=app||user_id=123||action=login||message=user logged in

``setup_logging(..., json_format=True)`` 时文件 / 控制台改为输出 JSONL
（每行一个 JSON 对象，字段顺序为固定头 + 业务 kv 原序 + ``message`` 恒存在，
``ensure_ascii=False`` 单行 UTF-8）::

    {"level": "INFO", "timestamp": "2026-09-19T10:30:00", "file": "app.py", "line": 42, "sys_name": "app", "user_id": "123", "action": "login", "message": "user logged in"}

``setup_logging(..., send_json=True, send_url="http://...")`` 时每条日志在
写入（打印）完成后立即以 HTTP POST 把同结构 JSON 发送到远端；发送失败
只计数（:func:`send_stats`），绝不影响本地写盘、绝不向业务抛异常。

快速上手::

    from lizysdk.logs import setup_logging, get_logger, parse_line, send_stats

    setup_logging(
        "logs", "app.log", level="INFO", sys_name="order-svc",
        send_json=True, send_url="http://collector:9000/logs",
    )
    log = get_logger(__name__)
    log.info("user logged in", user_id=123, action="login")
    flush(timeout=5)
    for line in open("logs/app.log", encoding="utf-8"):
        record = parse_line(line)   # pipe 行与 JSON 行都能还原为同结构 dict
"""

from __future__ import annotations

from .context import bind_context, clear_context
from .formatter import PipeLogFormatter, parse_line
from .handlers import flush, send_stats, setup_logging
from .logger import PipeLogger, get_logger

__all__ = [
    "setup_logging",
    "get_logger",
    "bind_context",
    "clear_context",
    "flush",
    "send_stats",
    "parse_line",
    "PipeLogger",
    "PipeLogFormatter",
]
