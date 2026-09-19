"""文件轮转 handler 工厂、后台写入池与 :func:`setup_logging` 组装层。

职责划分（单一职责）：

- :func:`_make_file_handler` —— 按 ``rotation`` 构造落盘 handler
  （``RotatingFileHandler`` / ``TimedRotatingFileHandler`` / ``FileHandler``）；
- :class:`_SendHandler` —— 「打印完即发送」handler：排在文件/控制台之后，
  记录写出后立即把同结构 JSON 以 HTTP POST 派发到远端（绝不抛错）；
- :class:`_JoinableQueueListener` —— ``pool_size == 1`` 时的单 worker 消费
  线程（基于标准库 ``QueueListener``，补齐 ``task_done`` 以支持
  :func:`flush` 排空等待）；
- :class:`_WorkerPool` —— ``pool_size > 1`` 时的多 worker 写入池
  （``queue.Queue`` + N 守护线程 + 文件写锁，保证行完整不交错）；
- :class:`_Backend` —— 封装一次 ``setup_logging`` 建立的全部运行时资源
  （handler / 队列 / 线程 / 远端发送通道），幂等重建与 atexit 优雅停机
  都围绕它展开。
"""

from __future__ import annotations

import atexit
import logging
import logging.handlers
import queue as _queue
import sys
import threading
import traceback
from pathlib import Path
from typing import Any, Iterable, Optional

from .formatter import (
    DEFAULT_SYS_NAME,
    JsonLogFormatter,
    PipeLogFormatter,
    validate_sys_name,
)
from .sender import DispatchFn, JsonSender, SendDispatcher, _join_queue

__all__ = ["setup_logging", "flush", "send_stats"]

_VALID_ROTATIONS = ("size", "time", "none")
_VALID_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


class _PipeQueueHandler(logging.handlers.QueueHandler):
    """只入队、不改动 record 的 ``QueueHandler``。

    标准库 ``QueueHandler.prepare`` 会就地格式化并**剥离** ``exc_info`` /
    自定义属性，导致后台线程格式化时丢失异常栈与业务 kv。本进程内队列
    传递的是引用而非 pickle，直接原样入队即可。
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """原样返回 record（保留 ``exc_info`` 与 ``bk_fields``）。"""
        return record

    def emit(self, record: logging.LogRecord) -> None:
        """直接原样入队（``prepare`` 恒等，省去一次间接调用）。"""
        self.enqueue(record)

    def enqueue(self, record: logging.LogRecord) -> None:
        """SimpleQueue 友好入队（``put`` 为 C 实现，不持 Python 级互斥锁）。"""
        self.queue.put(record)

    def handle(self, record: logging.LogRecord) -> None:
        """跳过过滤器与 handler 锁直达 enqueue。

        本 handler 是内部管道组件（无过滤器可装），队列入队自身线程安全，
        标准 :meth:`handle` 的 RLock 加解锁在热路径上是纯开销。
        """
        self.emit(record)


class _SendHandler(logging.Handler):
    """「打印完即发送」handler：把记录编码为 JSON 后派发到远端通道。

    排在文件 / 控制台 handler **之后**，从而保证「写入（打印）完成后」才
    发送；发送 body 与 JSON 输出同结构（含 ``sys_name`` 与全部字段），
    ``Content-Type: application/json``。

    派发函数二选一（由 ``async_writer`` 决定）：

    - 异步：:meth:`SendDispatcher.submit`（非阻塞，后台线程发送）；
    - 同步：``JsonSender.send``（调用线程直接发送，内部吞掉一切异常）。

    :meth:`emit` 自身也吞掉一切异常——发送路径的任何失败都绝不影响本地
    写盘、绝不向业务调用方抛出（失败计数见 :func:`send_stats`）。
    """

    def __init__(
        self, json_formatter: JsonLogFormatter, dispatch: DispatchFn
    ) -> None:
        """初始化发送 handler。

        Args:
            json_formatter: 负责把 LogRecord 编码为单行 JSON 的格式化器。
            dispatch: 派发函数（同步直发或异步投递）。
        """
        super().__init__(level=logging.NOTSET)
        self.setFormatter(json_formatter)
        self._dispatch = dispatch

    def emit(self, record: logging.LogRecord) -> None:
        """格式化为 JSON 单行并派发；任何异常静默吞掉（计数在发送层）。"""
        try:
            self._dispatch(self.format(record))
        except Exception:
            pass  # 发送路径绝不影响本地写盘，也绝不向调用方抛错


class _JoinableQueueListener(logging.handlers.QueueListener):
    """后台单消费线程（自持 monitor，配 ``queue.SimpleQueue``）。

    - 队列为 :class:`queue.SimpleQueue`（C 实现）：``put`` 不持 Python
      互斥锁，生产/消费不会因队列锁反复让出 GIL；
    - ``flush`` 采用 :class:`_FlushBarrier` 屏障令牌，而非逐条
      ``task_done``（后者在部分标准库版本缺失，且共享计数锁引入
      GIL 乒乓）；
    - 停机令牌按 FIFO 排在剩余记录之后，退出前天然排空；
    - 单条记录处理异常不会杀死消费线程。
    """

    def __init__(self, q: "_queue.SimpleQueue", *handlers: logging.Handler) -> None:
        super().__init__(q, *handlers)
        self._stop_token = object()

    def _monitor(self) -> None:  # noqa: D102 - 见类 docstring
        q = self.queue
        while True:
            try:
                record = self.dequeue(True)
            except Exception:  # pragma: no cover - 队列本身故障时安全退出
                break
            try:
                if record is self._stop_token:
                    break
                if isinstance(record, _FlushBarrier):
                    record.done()
                    continue
                self.handle(record)
            except Exception:  # 单条失败不拖垮消费线程
                traceback.print_exc(file=sys.stderr)

    def stop(self, timeout: Optional[float] = None) -> None:
        """投递停机令牌并等待消费线程退出（令牌前的记录已全部处理）。"""
        self.queue.put(self._stop_token)
        thread = getattr(self, "_thread", None)
        if thread is not None:
            thread.join(timeout)


class _WorkerPool:
    """多 worker 后台写入池：``queue.Queue`` + N 个守护线程 + 文件写锁。

    每条记录仅被一个 worker 取走（queue 语义），派发期间持有写锁，
    多 worker 下行仍完整、不交错；``Handler.handle`` 自身的 handler 锁
    提供第二重保护（轮转重命名等临界区天然串行）。
    """

    _SHUTDOWN = object()

    def __init__(
        self,
        handlers: Iterable[logging.Handler],
        *,
        pool_size: int,
        q: "_queue.Queue[Any]",
    ) -> None:
        self._queue = q
        self._handlers = tuple(handlers)
        self._write_lock = threading.Lock()
        self._threads = [
            threading.Thread(
                target=self._run, name=f"lizysdk-logs-writer-{i}", daemon=True
            )
            for i in range(pool_size)
        ]
        for t in self._threads:
            t.start()

    def _run(self) -> None:
        # 多 worker 下不能用屏障令牌（令牌被消费 != 更早记录已完成写入，
        # 与等待写锁的在途记录存在竞态），故此路径保留逐条 task_done 计数。
        q = self._queue
        while True:
            record = q.get()
            try:
                if record is self._SHUTDOWN:
                    return
                with self._write_lock:
                    for handler in self._handlers:
                        if record.levelno >= handler.level:
                            handler.handle(record)
            except Exception:  # 单条失败不拖垮 worker
                traceback.print_exc(file=sys.stderr)
            finally:
                q.task_done()

    def stop(self, timeout: Optional[float] = None) -> None:
        """投递 N 个哨兵并等待全部 worker 退出（退出前排空剩余记录）。"""
        for _ in self._threads:
            self._queue.put(self._SHUTDOWN)
        for t in self._threads:
            t.join(timeout)


class _Backend:
    """一次 ``setup_logging`` 建立的全部运行时资源（handler / 队列 / 线程 / 发送通道）。"""

    def __init__(
        self,
        *,
        root_handlers: list[logging.Handler],
        targets: list[logging.Handler],
        drain: "Optional[Any]" = None,
        listener: Optional[logging.handlers.QueueListener] = None,
        pool: Optional[_WorkerPool] = None,
        sender: Optional[JsonSender] = None,
        dispatcher: Optional[SendDispatcher] = None,
    ) -> None:
        self.root_handlers = root_handlers  # 挂在 root 上的入口 handler
        self.targets = targets  # 真正落盘 / 输出的目标 handler
        self.drain = drain  # 屏障令牌排空回调（无队列的同步模式为 None）
        self.listener = listener
        self.pool = pool
        self.sender = sender  # 远端 JSON 发送器（send_json=False 时为 None）
        self.dispatcher = dispatcher  # 异步发送派发线程（仅 async + send_json）
        self.closed = False

    def flush(self, timeout: Optional[float] = None) -> bool:
        """等待后台写入队列与在途发送排空。

        语义（与既有 flush 契约兼容的扩展）：

        - 先等写入队列排空（屏障令牌：调用前已入队的记录全部被 handler
          写出），随后显式刷目标 handler 的流到磁盘；
        - 若启用异步远端发送，再尽力排空在途发送（每条受 ``send_timeout``
          约束；``timeout=None`` 时等到全部完成）。

        Returns:
            ``timeout`` 为 ``None`` 时阻塞等待并恒返回 ``True``；否则全部
            排空返回 ``True``，任一环节超时返回 ``False``。同步模式
            （无队列、无派发线程）恒为 ``True``。
        """
        drained = True
        if not self.closed and self.drain is not None:
            drained = self.drain(timeout)
        if self.dispatcher is not None:
            drained = self.dispatcher.wait(timeout) and drained
        # 队列排空后显式刷目标流：缓冲写 handler 不逐条刷盘，落盘保证由本处承接
        for handler in self.targets:
            try:
                handler.flush()
            except Exception:  # pragma: no cover - flush 失败不应破坏排空语义
                pass
        return drained

    def stop(self, timeout: Optional[float] = None) -> None:
        """停掉后台线程并关闭全部目标 handler（幂等）。"""
        if self.closed:
            return
        self.closed = True
        if self.listener is not None:
            self.listener.stop()  # 哨兵排在剩余记录之后：退出前先排空
        if self.pool is not None:
            self.pool.stop(timeout)
        if self.dispatcher is not None:
            # 写入线程已排空：此处排空发送队列，尽力完成在途发送后退出。
            self.dispatcher.stop(timeout)
        for handler in self.targets:
            handler.close()


#: 当前活跃 backend（模块级单槽，配锁保护；None 表示尚未 setup）。
_BACKEND: Optional[_Backend] = None
_BACKEND_LOCK = threading.RLock()


def _teardown(backend: Optional[_Backend]) -> None:
    """先摘除 root 上的入口 handler（停新流量），再停机落盘、关闭资源。"""
    if backend is None:
        return
    root = logging.getLogger()
    for handler in backend.root_handlers:
        if handler in root.handlers:
            root.removeHandler(handler)
    backend.stop(timeout=10.0)


class _BufferedFileHandler(logging.FileHandler):
    """不逐条 flush 的文件 handler（吞吐优先）。

    标准库 ``StreamHandler.emit`` 每条记录后 ``stream.flush()``，是同步
    落盘热路径的主要开销之一。本类只写缓冲、不逐条刷盘，落盘时机收敛为：
    :func:`flush`（``_Backend`` 会显式刷目标 handler 的流）、轮转
    ``doRollover``（close 会先刷缓冲）、停机 / atexit（close 刷缓冲）。
    代价：进程**硬崩溃**可能丢缓冲尾部（正常停机与 flush 语义不丢）。
    """

    def handle(self, record: logging.LogRecord) -> None:
        """跳过 handler 锁直达 emit（写者串行由后台池/监听线程结构保证）。"""
        self.emit(record)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.stream.write(msg + self.terminator)
        except Exception:
            self.handleError(record)


class _BufferedRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """同 :class:`_BufferedFileHandler`，按大小轮转版（emit 逻辑对齐标准库，仅去掉逐条 flush）。"""

    def handle(self, record: logging.LogRecord) -> None:
        """跳过 handler 锁直达 emit（写者串行由后台池结构保证）。"""
        self.emit(record)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if self.shouldRollover(record):
                self.doRollover()
            msg = self.format(record)
            self.stream.write(msg + self.terminator)
        except Exception:
            self.handleError(record)


class _BufferedTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """同 :class:`_BufferedFileHandler`，按时间轮转版。"""

    def handle(self, record: logging.LogRecord) -> None:
        """跳过 handler 锁直达 emit（写者串行由后台池结构保证）。"""
        self.emit(record)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            if self.shouldRollover(record):
                self.doRollover()
            msg = self.format(record)
            self.stream.write(msg + self.terminator)
        except Exception:
            self.handleError(record)


class _FlushBarrier:
    """flush 屏障令牌：N 个消费者各确认一次后置位事件。

    替代 ``queue.Queue`` 的逐条 ``task_done``/``join``（每次入队都动共享
    计数锁，生产者与消费者在 CPython 下反复让出 GIL）。屏障锁只在消费侧
    短暂持有，**生产路径零额外同步开销**。
    """

    __slots__ = ("_event", "_remaining", "_lock")

    def __init__(self, count: int) -> None:
        self._event = threading.Event()
        self._remaining = count
        self._lock = threading.Lock()

    def done(self) -> None:
        """消费者确认一次；全部确认后置位事件。"""
        with self._lock:
            self._remaining -= 1
            if self._remaining <= 0:
                self._event.set()

    def wait(self, timeout: Optional[float]) -> bool:
        """等待全部确认；超时返回 ``False``。"""
        return self._event.wait(timeout)


def _enqueue_barrier(q: "_queue.SimpleQueue", count: int, timeout: Optional[float]) -> bool:
    """向队列投 ``count`` 个屏障令牌并等待全部被确认（flush 的实现）。"""
    barrier = _FlushBarrier(count)
    for _ in range(count):
        q.put(barrier)
    return barrier.wait(timeout)


def _make_file_handler(
    path: Path,
    *,
    rotation: str,
    max_bytes: int,
    backup_count: int,
    when: str,
    buffered: bool,
) -> logging.Handler:
    """按轮转策略构造落盘 handler（统一 ``utf-8`` 编码）。

    ``buffered=True``（后台写入池模式）用缓冲写 handler：不逐条 flush，
    落盘由 :func:`flush` / 停机 / 轮转承接，吞吐优先；``buffered=False``
    （同步直写模式）用标准库 handler，逐条 flush，调用返回即落盘。
    """
    if rotation == "size":
        cls = _BufferedRotatingFileHandler if buffered else logging.handlers.RotatingFileHandler
        return cls(
            path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
        )
    if rotation == "time":
        cls = (
            _BufferedTimedRotatingFileHandler
            if buffered
            else logging.handlers.TimedRotatingFileHandler
        )
        return cls(path, when=when, backupCount=backup_count, encoding="utf-8")
    cls = _BufferedFileHandler if buffered else logging.FileHandler
    return cls(path, encoding="utf-8")


def setup_logging(
    log_dir: "str | Path" = "logs",
    filename: str = "app.log",
    *,
    level: str = "INFO",
    console: bool = True,
    rotation: str = "size",
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    when: str = "midnight",
    async_writer: bool = True,
    pool_size: int = 1,
    sys_name: str = DEFAULT_SYS_NAME,
    json_format: bool = False,
    send_json: bool = False,
    send_url: "str | None" = None,
    send_timeout: float = 3.0,
) -> None:
    """初始化 lizysdk.logs 全局日志管道（幂等：重复调用安全）。

    重复调用会先清理旧 handler 与后台线程再重建，不会产生重复输出。

    Args:
        log_dir: 日志目录（不存在则自动创建）。
        filename: 日志文件名。
        level: 根日志级别 ``DEBUG/INFO/WARNING/ERROR/CRITICAL``（大小写不敏感）。
        console: 是否同时输出到 stderr。
        rotation: ``"size"``（按大小）| ``"time"``（按时间）| ``"none"``（不轮转）。
        max_bytes: 单文件上限字节，``rotation="size"`` 时生效。
        backup_count: 保留的历史文件个数。
        when: 轮转周期，语义同标准库 ``TimedRotatingFileHandler``
            （如 ``"s"``/``"m"``/``"h"``/``"midnight"``），``rotation="time"``
            时生效。
        async_writer: 是否启用后台写入池（队列 + 消费线程）。
        pool_size: 后台写入线程数（``async_writer=True`` 时生效；
            1 走 ``QueueListener``，>1 走自实现 worker 池）。
        sys_name: 系统标识，注入每一条日志（pipe 行中 ``FILE:LINE`` 之后
            第一段 ``sys_name=xxx``；JSON 行中字段 ``"sys_name"``），
            默认 ``"app"``
        json_format: ``True`` 时文件 / 控制台输出 JSONL（每行一个 JSON
            对象，``ensure_ascii=False`` 单行 UTF-8）；默认 ``False`` 保持
            pipe-logfmt。
        send_json: 是否在每条日志写入（打印）完成后立即以 HTTP POST 把
            同结构 JSON 对象发送到远端（body 含 ``sys_name`` 与全部字段，
            ``Content-Type: application/json``）。发送失败不影响本地写盘、
            不向业务抛异常，仅计数（见 :func:`send_stats`）。
        send_url: 接收地址，``http://`` 或 ``https://`` 开头；
            ``send_json=True`` 时必填。
        send_timeout: 单次发送超时秒数，默认 ``3.0``；flush / atexit 时
            「尽力完成在途发送」也受此约束。

    Raises:
        ValueError: ``rotation`` / ``level`` 非法，``pool_size < 1``，
            ``sys_name`` 不是非空 ``str``，``send_json=True`` 而
            ``send_url`` 不是 ``http(s)://`` 开头的非空 ``str``，
            或 ``send_timeout`` 非正数。

    示例::

        from lizysdk.logs import setup_logging, get_logger, send_stats

        # pipe-logfmt + 异步远端收集
        setup_logging(
            "logs", "app.log", level="INFO", sys_name="order-svc",
            send_json=True, send_url="http://collector:9000/logs",
        )
        get_logger(__name__).info("ready", env="prod")
        send_stats()   # {"sent": ..., "failed": ..., "last_error": ...}
    """
    global _BACKEND

    level_name = str(level).upper()
    if level_name not in _VALID_LEVELS:
        raise ValueError(f"非法 level {level!r}，可选: {_VALID_LEVELS}")
    if rotation not in _VALID_ROTATIONS:
        raise ValueError(f"非法 rotation {rotation!r}，可选: {_VALID_ROTATIONS}")
    if pool_size < 1:
        raise ValueError(f"pool_size 必须 >= 1，得到 {pool_size}")
    validate_sys_name(sys_name)
    if send_json and (
        not isinstance(send_url, str)
        or not send_url
        or not (send_url.startswith("http://") or send_url.startswith("https://"))
    ):
        raise ValueError(
            "send_json=True 时 send_url 必须为 http(s):// 开头的非空 str，"
            f"得到 {send_url!r}"
        )
    if (
        isinstance(send_timeout, bool)
        or not isinstance(send_timeout, (int, float))
        or send_timeout <= 0
    ):
        raise ValueError(f"send_timeout 必须为正数（秒），得到 {send_timeout!r}")

    log_dir_path = Path(log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)
    log_path = log_dir_path / filename

    with _BACKEND_LOCK:
        # 幂等：先拆旧（停后台线程、关旧 handler、清 root），再重建。
        _teardown(_BACKEND)
        _BACKEND = None

        targets: list[logging.Handler] = []
        try:
            file_handler = _make_file_handler(
                log_path,
                rotation=rotation,
                max_bytes=max_bytes,
                backup_count=backup_count,
                when=when,
                buffered=async_writer,
            )
            targets.append(file_handler)
            if console:
                targets.append(logging.StreamHandler(sys.stderr))

            line_formatter: logging.Formatter = (
                JsonLogFormatter(sys_name=sys_name)
                if json_format
                else PipeLogFormatter(sys_name=sys_name)
            )
            for handler in targets:
                handler.setFormatter(line_formatter)

            # 远端发送通道：send_handler 恒用 JSON 格式（body 与 JSON 输出
            # 同结构），与本地 json_format 开关无关。
            sender: Optional[JsonSender] = None
            dispatcher: Optional[SendDispatcher] = None
            send_handler: Optional[_SendHandler] = None
            if send_json:
                sender = JsonSender(send_url, timeout=send_timeout)
                dispatch: DispatchFn
                if async_writer:
                    dispatcher = SendDispatcher(sender)
                    dispatch = dispatcher.submit  # 写完即派发，后台发送
                else:
                    dispatch = sender.send  # 同步直发（内部吞错，只计数）
                send_handler = _SendHandler(
                    JsonLogFormatter(sys_name=sys_name), dispatch
                )

            root = logging.getLogger()
            for stale in root.handlers[:]:
                root.removeHandler(stale)  # 清理历史（含上一次 setup 的）
            root.setLevel(getattr(logging, level_name))

            if async_writer:
                # 发送 handler 排在最后：写入（打印）完成后才发送。
                consumers = list(targets)
                if send_handler is not None:
                    consumers.append(send_handler)
                if pool_size == 1:
                    # 默认路径：SimpleQueue（C 实现，put 不持 Python 互斥锁，
                    # 生产者不因队列锁与消费线程 GIL 乒乓）+ 屏障令牌 flush。
                    q: "_queue.SimpleQueue" = _queue.SimpleQueue()
                    queue_handler = _PipeQueueHandler(q)
                    root.addHandler(queue_handler)
                    listener = _JoinableQueueListener(q, *consumers)
                    listener.start()
                    backend = _Backend(
                        root_handlers=[queue_handler],
                        targets=targets,
                        drain=lambda timeout, _q=q: _enqueue_barrier(_q, 1, timeout),
                        listener=listener,
                        sender=sender,
                        dispatcher=dispatcher,
                    )
                else:
                    # 多 worker：task_done/join 计数保证 flush 语义无竞态
                    q2: "_queue.Queue[Any]" = _queue.Queue()
                    queue_handler = _PipeQueueHandler(q2)
                    root.addHandler(queue_handler)
                    pool = _WorkerPool(consumers, pool_size=pool_size, q=q2)
                    backend = _Backend(
                        root_handlers=[queue_handler],
                        targets=targets,
                        drain=lambda timeout, _q=q2: _join_queue(_q, timeout),
                        pool=pool,
                        sender=sender,
                        dispatcher=dispatcher,
                    )
            else:
                entries = list(targets)
                if send_handler is not None:
                    entries.append(send_handler)
                for handler in entries:
                    root.addHandler(handler)
                backend = _Backend(
                    root_handlers=entries,
                    targets=targets,
                    sender=sender,
                )

            _BACKEND = backend
        except BaseException:
            for handler in targets:
                handler.close()
            raise


def flush(timeout: Optional[float] = None) -> bool:
    """等待后台写入队列排空落盘（启用异步远端发送时一并尽力排空在途发送）。

    Args:
        timeout: 最长等待秒数；``None`` 表示一直等。

    Returns:
        队列已排空（或本为同步直写）返回 ``True``，超时返回 ``False``；
        尚未 :func:`setup_logging` 时恒为 ``True``。

    示例::

        log.info("bye")
        assert flush(timeout=5)
    """
    backend = _BACKEND
    if backend is None:
        return True
    return backend.flush(timeout)


def send_stats() -> "dict[str, Any]":
    """返回远端 JSON 发送的统计快照。

    Returns:
        ``{"sent": int, "failed": int, "last_error": str | None}``：

        - ``sent``       —— 成功（HTTP 2xx）送达的条数；
        - ``failed``     —— 失败条数（连接拒绝 / 超时 / DNS / 非 2xx 等）；
        - ``last_error`` —— 最近一次失败的 ``类型: 信息``，无失败时 ``None``。

        未启用 ``send_json`` 或尚未 :func:`setup_logging` 时返回全零快照。

    示例::

        setup_logging("logs", send_json=True, send_url="http://h/p")
        log.info("hi")
        flush(timeout=5)
        stats = send_stats()
        assert stats["sent"] == 1 and stats["failed"] == 0
    """
    backend = _BACKEND
    if backend is None or backend.sender is None:
        return {"sent": 0, "failed": 0, "last_error": None}
    return backend.sender.stats()


def _atexit_shutdown() -> None:
    """atexit 钩子：排空队列与在途发送、停机、关闭 handler，退出不丢日志。"""
    global _BACKEND
    with _BACKEND_LOCK:
        backend = _BACKEND
        _BACKEND = None
    if backend is not None:
        backend.flush(10.0)
        _teardown(backend)


atexit.register(_atexit_shutdown)
