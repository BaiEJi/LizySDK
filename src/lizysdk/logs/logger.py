""":class:`PipeLogger` —— 支持结构化 kv 的 Logger。

继承标准库 ``logging.Logger``，仅重载 ``debug/info/warning/error/critical``
等方法的签名为 ``(message, *args, exc_info=False, **fields)``：业务 kv 经
``extra`` 注入自定义 LogRecord 属性（``bk_fields``），由
:class:`~lizysdk.logs.formatter.PipeLogFormatter` 在格式化时拼装。
``FILE:LINE`` 通过直接捕获调用栈帧修正到**真实调用方**（跨 Python 版本
确定性成立，不受 ``findCaller`` 的 ``stacklevel`` 语义变化影响）。

用法::

    from lizysdk.logs import get_logger, setup_logging

    setup_logging("logs", level="INFO")
    log = get_logger(__name__)
    log.info("user logged in", user_id=123, action="login")
    log.error("db failed", exc_info=True)      # 异常栈转义后并入 message
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from .context import get_context_fields
from .formatter import FIELDS_ATTR, validate_fields

__all__ = ["PipeLogger", "get_logger"]


class PipeLogger(logging.Logger):
    """支持 ``log.info("msg", key=value)`` 结构化字段的 Logger。

    签名约定（消息形参名沿用标准库的 ``msg``）::

        debug/info/warning/error/critical(msg, *args,
                                          exc_info=False, **fields)

    .. note::
        消息形参不叫 ``message`` 是有意的：``message`` 是输出契约中的
        保留字段，若形参占用该名，``log.info("m", message="x")`` 会在
        参数绑定阶段抛 ``TypeError`` 而非契约要求的 ``ValueError``。

    - ``msg`` 支持 ``%`` 风格惰性格式化（同标准库）；
    - ``exc_info`` 同标准库语义：``True`` 取当前异常，也可传异常实例或
      ``(type, value, tb)`` 元组；异常栈经转义追加进 ``message``；
    - ``fields`` 即业务 kv，非法 key / 保留字 ``message`` 抛 ``ValueError``；
    - 上下文字段（:func:`~lizysdk.logs.bind_context`）先于调用侧字段，
      同名时以调用侧为准。
    """

    def _emit(
        self,
        level: int,
        msg: Any,
        args: tuple[Any, ...],
        exc_info: Any,
        fields: dict[str, Any],
    ) -> None:
        """统一的发出路径：校验 -> 捕获调用方帧 -> 合并上下文 -> handle。

        帧深度约定：调用链恒为 ``用户代码 -> debug/info/... -> _emit``，
        故 ``sys._getframe(2)`` 即真实调用方。
        """
        validate_fields(fields)
        if msg is None:
            raise ValueError("缺少日志消息文本（第一个位置参数）")
        if not self.isEnabledFor(level):
            return

        frame = sys._getframe(2)
        try:
            pathname = frame.f_code.co_filename
            lineno = frame.f_lineno
            func = frame.f_code.co_name
        finally:
            del frame  # 避免引用循环

        exc: tuple[type, BaseException, Any] | None = None
        if exc_info:
            if isinstance(exc_info, BaseException):
                exc = (type(exc_info), exc_info, exc_info.__traceback__)
            elif isinstance(exc_info, tuple):
                exc = exc_info
            else:  # True 或其他真值：取当前正在处理的异常
                exc = sys.exc_info()
            if exc == (None, None, None):
                exc = None  # exc_info=True 但不在 except 块中

        merged = {**get_context_fields(), **fields}
        record = self.makeRecord(
            self.name,
            level,
            pathname,
            lineno,
            msg,
            args,
            exc,
            func,
            {FIELDS_ATTR: merged},
        )
        self.handle(record)

    # ---- 公开日志方法（签名不可偏离契约） ---------------------------------

    def debug(self, msg: Any = None, *args: Any, exc_info: Any = False, **fields: Any) -> None:
        """DEBUG 级别：``log.debug("hi", key=value)``。"""
        self._emit(logging.DEBUG, msg, args, exc_info, fields)

    def info(self, msg: Any = None, *args: Any, exc_info: Any = False, **fields: Any) -> None:
        """INFO 级别：``log.info("user logged in", user_id=123)``。"""
        self._emit(logging.INFO, msg, args, exc_info, fields)

    def warning(self, msg: Any = None, *args: Any, exc_info: Any = False, **fields: Any) -> None:
        """WARNING 级别：``log.warning("slow query", cost_ms=900)``。"""
        self._emit(logging.WARNING, msg, args, exc_info, fields)

    def error(self, msg: Any = None, *args: Any, exc_info: Any = False, **fields: Any) -> None:
        """ERROR 级别：``log.error("db failed", exc_info=True)``。"""
        self._emit(logging.ERROR, msg, args, exc_info, fields)

    def critical(self, msg: Any = None, *args: Any, exc_info: Any = False, **fields: Any) -> None:
        """CRITICAL 级别：``log.critical("core down")``。"""
        self._emit(logging.CRITICAL, msg, args, exc_info, fields)

    def exception(self, msg: Any = None, *args: Any, exc_info: Any = True, **fields: Any) -> None:
        """ERROR 级别且默认 ``exc_info=True``（须在 except 块中调用）。"""
        self._emit(logging.ERROR, msg, args, exc_info, fields)

    def log(self, level: int, msg: Any = None, *args: Any, exc_info: Any = False, **fields: Any) -> None:
        """按级别发出：``log.log(logging.INFO, "msg", key=value)``。"""
        self._emit(level, msg, args, exc_info, fields)


def _rebuild_as_pipe(name: str, old: Any) -> PipeLogger:
    """把既有普通 Logger 替换为 PipeLogger（兜底路径）。

    当 ``name`` 在 ``setLoggerClass(PipeLogger)`` 之前已被创建为普通
    Logger 时，复刻标准库 Manager 的父子关系修复逻辑后原位替换。
    """
    manager = logging.Logger.manager
    new = PipeLogger(name)
    new.manager = manager
    if isinstance(old, logging.Logger):
        new.setLevel(old.level)
        new.propagate = old.propagate
        for handler in old.handlers:
            new.addHandler(handler)
    manager.loggerDict[name] = new
    parent: logging.Logger = logging.getLogger()
    parts = name.split(".")
    for i in range(len(parts) - 1, 0, -1):
        ancestor = manager.loggerDict.get(".".join(parts[:i]))
        if isinstance(ancestor, logging.Logger):
            parent = ancestor
            break
    new.parent = parent
    return new


def get_logger(name: str = "app") -> PipeLogger:
    """获取（或创建）名为 ``name`` 的 :class:`PipeLogger` 实例。

    同名返回同一实例（由标准库 Manager 托管），日志沿 ``logging`` 层级
    传播到 root，由 :func:`~lizysdk.logs.setup_logging` 安装的 handler 输出。

    Args:
        name: logger 名称，建议传 ``__name__``。

    Returns:
        :class:`PipeLogger` 实例。

    示例::

        log = get_logger(__name__)
        log.info("user logged in", user_id=123, action="login")
    """
    logging.setLoggerClass(PipeLogger)
    logger = logging.getLogger(name)
    if not isinstance(logger, PipeLogger):
        logger = _rebuild_as_pipe(name, logger)
    return logger  # type: ignore[return-value]
