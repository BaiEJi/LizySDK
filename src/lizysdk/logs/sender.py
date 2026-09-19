"""远端 JSON 日志发送：HTTP POST、永不抛错、计数统计。

配合 :func:`lizysdk.logs.setup_logging` 的 ``send_json`` / ``send_url`` /
``send_timeout`` 参数使用，实现「打印完即发送 JSON 日志到远端」：

- :class:`JsonSender` —— 单条发送通道：``urllib.request`` 发 HTTP POST，
  body 为 JSON 文本、``Content-Type: application/json``；**任何**发送异常
  （连接拒绝 / 超时 / DNS 失败 / 非 2xx）都被吞掉并计入 ``failed``，
  绝不影响本地写盘、绝不向业务调用方抛出；计数器经锁保护，
  :func:`lizysdk.logs.send_stats` 暴露 ``sent`` / ``failed`` / ``last_error``；
- :class:`SendDispatcher` —— 可选的异步派发通道（独立守护线程 + 队列），
  ``async_writer=True`` 时「写完即派发、后台发送」，HTTP 往返不阻塞写入
  线程与业务线程；:meth:`SendDispatcher.wait` / :meth:`SendDispatcher.stop`
  提供 flush / 优雅停机时「尽力排空在途发送」的能力（每条受
  ``send_timeout`` 约束）。

一般无需直接使用本模块，示例（经 setup_logging 启用）::

    from lizysdk.logs import setup_logging, send_stats, get_logger

    setup_logging(
        "logs", "app.log",
        send_json=True, send_url="http://collector:9000/logs",
        send_timeout=3.0,
    )
    get_logger(__name__).info("hello", k=1)   # 写盘后即异步 POST 出去
    send_stats()                              # {"sent": 1, "failed": 0, ...}
"""

from __future__ import annotations

import queue as _queue
import threading
import urllib.request
from typing import Any, Callable, Optional

__all__ = ["JsonSender", "SendDispatcher"]


def _join_queue(q: "_queue.Queue[Any]", timeout: Optional[float]) -> bool:
    """等待队列排空（``timeout=None`` 一直等并恒成功；超时返回 ``False``）。

    借助守护 watcher 线程实现带超时的 ``queue.join()``（``Queue.join``
    本身不支持超时）。
    """
    if timeout is None:
        q.join()
        return True
    done = threading.Event()

    def _join() -> None:
        q.join()
        done.set()

    watcher = threading.Thread(target=_join, daemon=True)
    watcher.start()
    return done.wait(timeout)


class JsonSender:
    """把单行 JSON 日志以 HTTP POST 发送到远端（失败吞掉并计数）。

    线程安全：计数器与 ``last_error`` 经内部锁保护，可被多线程并发调用
    （异步派发线程 / 同步调用线程）。

    示例::

        sender = JsonSender("http://127.0.0.1:9000/collect", timeout=3.0)
        ok = sender.send('{"level": "INFO", "message": "hi"}')
        sender.stats()   # {"sent": 1, "failed": 0, "last_error": None}
    """

    def __init__(self, url: str, timeout: float = 3.0) -> None:
        """初始化发送器。

        Args:
            url: 接收地址（应由调用方保证为 ``http(s)://`` 开头）。
            timeout: 单次发送的超时秒数。
        """
        self._url = url
        self._timeout = timeout
        self._lock = threading.Lock()
        self._sent = 0
        self._failed = 0
        self._last_error: Optional[str] = None

    def send(self, body: str) -> bool:
        """同步发送一条 JSON 日志；返回是否成功，**任何异常都不向外抛**。

        Args:
            body: JSON 文本（与本地 JSON 输出同结构，UTF-8 编码后作为 body）。

        Returns:
            成功（HTTP 2xx）返回 ``True``；任何失败返回 ``False`` 并计数。
        """
        try:
            request = urllib.request.Request(
                self._url,
                data=body.encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                status = response.getcode()
            if not 200 <= status < 300:  # 双保险：urlopen 对非 2xx 本就抛 HTTPError
                raise OSError(f"非 2xx 响应: HTTP {status}")
        except Exception as exc:  # 连接拒绝 / 超时 / DNS / 非 2xx 等一律吞掉
            with self._lock:
                self._failed += 1
                self._last_error = f"{type(exc).__name__}: {exc}"
            return False
        with self._lock:
            self._sent += 1
        return True

    def stats(self) -> dict[str, Any]:
        """返回发送统计快照。

        Returns:
            ``{"sent": int, "failed": int, "last_error": str | None}``，
            ``last_error`` 为最近一次失败的原因（无失败时为 ``None``）。
        """
        with self._lock:
            return {
                "sent": self._sent,
                "failed": self._failed,
                "last_error": self._last_error,
            }


class SendDispatcher:
    """异步发送派发器：无界队列 + 独立守护线程。

    ``submit`` 非阻塞（写完即派发，不阻塞业务/写入线程）；后台线程按
    FIFO 逐条调用 :class:`JsonSender.send`。优雅停机时先投递哨兵再等待
    线程退出，退出前自然排空队列中的剩余待发送项。

    示例::

        dispatcher = SendDispatcher(sender)
        dispatcher.submit(body)      # 立即返回，后台发送
        dispatcher.wait(timeout=5)   # flush 语义：尽力排空在途发送
        dispatcher.stop(timeout=5)   # 停机：先排空再退出
    """

    _SENTINEL = object()

    def __init__(self, sender: JsonSender) -> None:
        """初始化并启动后台发送线程。

        Args:
            sender: 实际执行 HTTP POST 的 :class:`JsonSender`。
        """
        self._sender = sender
        self._queue: "_queue.Queue[str]" = _queue.Queue()
        self._thread = threading.Thread(
            target=self._run, name="lizysdk-logs-sender", daemon=True
        )
        self._thread.start()

    def submit(self, body: str) -> None:
        """非阻塞投递一条待发送 JSON 日志（投递路径自身绝不抛错）。"""
        try:
            self._queue.put_nowait(body)
        except Exception:  # pragma: no cover - 无界队列几乎不可能满
            pass

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if item is self._SENTINEL:
                    return
                self._sender.send(item)
            except Exception:  # pragma: no cover - send 已自带吞错，双保险
                pass
            finally:
                self._queue.task_done()

    def wait(self, timeout: Optional[float] = None) -> bool:
        """等待在途发送完成（``timeout=None`` 一直等；超时返回 ``False``）。"""
        return _join_queue(self._queue, timeout)

    def stop(self, timeout: Optional[float] = None) -> None:
        """投递哨兵并等待发送线程退出（FIFO 保证退出前排空剩余项）。"""
        try:
            self._queue.put_nowait(self._SENTINEL)  # type: ignore[arg-type]
        except Exception:  # pragma: no cover - 无界队列几乎不可能满
            pass
        self._thread.join(timeout)


#: 派发函数的统一签名（同步直发与异步投递共用，由 handlers 层选择）。
DispatchFn = Callable[[str], None]
