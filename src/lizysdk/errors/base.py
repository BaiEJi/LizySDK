"""AppError：lizysdk 标准化异常基类。

设计要点：

- 用 ``code`` / ``message`` / ``http_status`` / ``details`` / ``timestamp`` 五元组
  完整描述一次错误，可直接序列化进日志或 HTTP 响应体；
- ``message`` 未显式给出时，用 ``params`` 渲染错误码自带的消息模板
  （占位符缺键时安全降级，不抛 ``KeyError``）；
- ``to_dict`` / ``from_dict`` 支持结构化往返，``from_dict`` 依据 ``type``
  字段还原具体子类，未知 ``type`` 落回 ``AppError`` 本体；
- 通过 ``__init_subclass__`` 自动登记子类，供 ``from_dict`` 按类名查找。
"""

from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any

from .codes import ErrorCode

__all__ = ["AppError"]

# 「类名 -> 异常类」注册表：from_dict 据此把 ``type`` 字段还原成具体子类。
_TYPE_REGISTRY: dict[str, type[AppError]] = {}


class _SafeFormatDict(dict):
    """渲染消息模板用的安全字典：缺失键原样保留占位符，不抛 ``KeyError``。

    >>> _SafeFormatDict({"a": 1})["missing"]
    '{missing}'
    """

    def __missing__(self, key: str) -> str:
        return "{" + key + "}"


def _resolve_code(code: ErrorCode | str) -> tuple[str, str | None, int]:
    """把 ``ErrorCode`` 或裸字符串解析为 ``(码名, 模板, HTTP 状态码)``。

    裸字符串恰好命中已知错误码时沿用其模板与状态码；
    否则模板为 ``None``、状态码按 500（INTERNAL_ERROR 语义）兜底。
    """
    if isinstance(code, ErrorCode):
        return code.value, code.template, code.http_status
    code_str = str(code)
    try:
        matched = ErrorCode(code_str)
    except ValueError:
        return code_str, None, 500
    return matched.value, matched.template, matched.http_status


def _render(template: str, params: dict[str, Any] | None) -> str:
    """用 ``params`` 安全渲染消息模板。

    缺失的占位符原样保留；遇到模板本身的格式问题（如意外的位置占位符）
    时降级返回原始模板，绝不因渲染抛错。
    """
    try:
        return template.format_map(_SafeFormatDict(params or {}))
    except (KeyError, IndexError, ValueError):
        return template


class AppError(Exception):
    """应用标准异常基类。

    示例：
        >>> from lizysdk.errors import AppError, ErrorCode
        >>> err = AppError(ErrorCode.PARAM_MISSING, params={"param": "user_id"})
        >>> err.message
        '缺少必填参数: user_id'
        >>> str(err)
        '[PARAM_MISSING] 缺少必填参数: user_id'
        >>> err.to_dict()["http_status"]
        400

    :param code: 错误码（``ErrorCode`` 成员或码名字符串），默认 ``INTERNAL_ERROR``
    :param message: 显式消息；为 ``None`` 时用 ``params`` 渲染 ``code`` 的模板
    :param details: 附加上下文（如 trace_id、resource_id），传入后做浅拷贝
    :param params: 渲染消息模板的命名参数
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """子类定义即自动登记到类型注册表（下划线开头的私有基类除外）。"""
        super().__init_subclass__(**kwargs)
        if not cls.__name__.startswith("_"):
            _TYPE_REGISTRY[cls.__name__] = cls

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.INTERNAL_ERROR,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        if code is None:  # 防御性兜底：等价于默认值
            code = ErrorCode.INTERNAL_ERROR
        code_str, template, http_status = _resolve_code(code)
        if message is None:
            message = _render(template, params) if template is not None else code_str

        self._code: str = code_str
        self._message: str = message
        self._http_status: int = http_status
        self._details: dict = dict(details) if details else {}
        self._timestamp: str = datetime.now(timezone.utc).isoformat()

        # args 传递 (code, message)，保证 str/pickle 语义正常
        super().__init__(code_str, message)

    # ---- 只读属性 ----

    @property
    def code(self) -> str:
        """错误码名（字符串形式，如 ``"PARAM_MISSING"``）。"""
        return self._code

    @property
    def message(self) -> str:
        """最终消息（显式消息或渲染后的模板）。"""
        return self._message

    @property
    def http_status(self) -> int:
        """对应 HTTP 状态码。"""
        return self._http_status

    @property
    def details(self) -> dict:
        """附加上下文（构造时已做浅拷贝）。"""
        return self._details

    @property
    def timestamp(self) -> str:
        """错误发生时刻（UTC ISO8601 字符串）。"""
        return self._timestamp

    # ---- 表示 ----

    def __str__(self) -> str:
        """``"[CODE] message"`` 形式。"""
        return f"[{self._code}] {self._message}"

    def __repr__(self) -> str:
        """可读形式：``类名(code=..., message=..., http_status=...)``。"""
        return (
            f"{type(self).__name__}("
            f"code={self._code!r}, "
            f"message={self._message!r}, "
            f"http_status={self._http_status})"
        )

    # ---- 序列化往返 ----

    def to_dict(self) -> dict:
        """导出可直接 ``json.dumps`` 的字典（details 为深拷贝，互不影响）。

        示例：
            >>> from lizysdk.errors import AppError
            >>> AppError("CUSTOM_CODE", "自定义消息").to_dict()["code"]
            'CUSTOM_CODE'
        """
        return {
            "type": type(self).__name__,
            "code": self._code,
            "message": self._message,
            "http_status": self._http_status,
            "details": copy.deepcopy(self._details),
            "timestamp": self._timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> AppError:
        """从 :meth:`to_dict` 产出的字典重建异常。

        以 ``d["type"]`` 为准在类型注册表中还原具体子类；
        ``type`` 缺失或未知时落回 ``AppError`` 本体。

        示例：
            >>> from lizysdk.errors import AppError
            >>> err = AppError.from_dict(
            ...     {"type": "AppError", "code": "RATE_LIMITED", "message": "慢点"}
            ... )
            >>> err.code, err.message, err.http_status
            ('RATE_LIMITED', '慢点', 429)

        :param d: 含 ``type/code/message/http_status/details/timestamp`` 的字典
        :return: 重建的异常实例
        """
        type_name = d.get("type")
        target: type[AppError] = (
            _TYPE_REGISTRY.get(type_name) if isinstance(type_name, str) else None
        ) or AppError

        details = d.get("details")
        if not isinstance(details, dict):
            details = None
        err = target(
            d.get("code", ErrorCode.INTERNAL_ERROR.value),
            d.get("message"),
            details=details,
        )
        status = d.get("http_status")
        if isinstance(status, int) and not isinstance(status, bool):
            err._http_status = status
        timestamp = d.get("timestamp")
        if isinstance(timestamp, str) and timestamp:
            err._timestamp = timestamp
        return err

    # ---- pickle / copy 支持 ----

    def __reduce__(self) -> tuple[Any, ...]:
        """完整保真地 pickle：保留 code/message/details/http_status/timestamp。"""
        state = {
            "_http_status": self._http_status,
            "_details": dict(self._details),
            "_timestamp": self._timestamp,
        }
        return type(self), (self._code, self._message), state


_TYPE_REGISTRY[AppError.__name__] = AppError
