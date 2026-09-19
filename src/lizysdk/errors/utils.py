"""错误工具函数：异常包装（wrap）与断言式抛错（ensure）。"""

from __future__ import annotations

from typing import Any

from .base import AppError
from .codes import ErrorCode

__all__ = ["wrap", "ensure"]


def wrap(
    exc: BaseException,
    message: str | None = None,
    *,
    code: ErrorCode | str = ErrorCode.INTERNAL_ERROR,
    details: dict | None = None,
) -> AppError:
    """把任意异常包装为 :class:`~lizysdk.errors.base.AppError`。

    - 保持异常链：``err.__cause__ is exc``；
    - 原异常的 ``str`` 记入 ``details["original"]``，原异常类名记入
      ``details["original_type"]``（调用方已提供同名键时不覆盖）；
    ``message`` 为 ``None`` 时按 ``code`` 的模板渲染默认消息。

    示例：
        >>> from lizysdk.errors import ErrorCode, wrap
        >>> orig = None
        >>> try:
        ...     raise ValueError("boom")
        ... except ValueError as exc:
        ...     orig = exc
        ...     err = wrap(exc, code=ErrorCode.UPSTREAM_TIMEOUT)
        >>> err.code, err.http_status, err.details["original"]
        ('UPSTREAM_TIMEOUT', 504, 'boom')
        >>> err.__cause__ is orig
        True

    :param exc: 被包装的原始异常
    :param message: 显式消息，默认 ``None``（用 ``code`` 模板）
    :param code: 错误码，默认 ``INTERNAL_ERROR``
    :param details: 附加上下文，将与 ``original`` 等键合并
    :return: 包装后的 ``AppError``（尚未 raise，由调用方抛出或记录）
    """
    merged = dict(details) if details else {}
    merged.setdefault("original", str(exc))
    merged.setdefault("original_type", type(exc).__name__)
    wrapped = AppError(code, message, details=merged)
    wrapped.__cause__ = exc
    return wrapped


def ensure(condition: bool, error: AppError | ErrorCode | str, **kwargs: Any) -> None:
    """断言式抛错：``condition`` 为 ``False`` 时抛出 ``error``。

    - ``error`` 为错误码（枚举或码名字符串）时构造 ``AppError`` 抛出：
      ``kwargs`` 作为 ``params`` 渲染模板；其中名为 ``details`` 的 dict
      参数会被取出并作为 ``details``；
    - ``error`` 为 ``AppError`` 实例（含子类实例）时直接抛出该实例；
      若附带 ``kwargs``，则合并进 ``details`` 后以同类重建抛出（不改动原实例）。

    示例：
        >>> from lizysdk.errors import ErrorCode, ensure
        >>> ensure("a" == "a", ErrorCode.PARAM_MISSING, param="name") is None
        True
        >>> ensure(False, ErrorCode.PARAM_MISSING, param="name")
        Traceback (most recent call last):
            ...
        lizysdk.errors.base.AppError: [PARAM_MISSING] 缺少必填参数: name

    :param condition: 断言条件
    :param error: ``AppError`` 实例、``ErrorCode`` 成员或码名字符串
    :param kwargs: 传码时作为 ``params``（``details`` 键特殊处理）；传实例时合并进 ``details``
    :raises AppError: ``condition`` 为假时
    """
    if condition:
        return None
    if isinstance(error, AppError):
        if not kwargs:
            raise error
        merged = {**error.details, **kwargs}
        raise type(error)(error.code, error.message, details=merged)
    params = dict(kwargs)
    details = params.pop("details") if isinstance(params.get("details"), dict) else None
    raise AppError(error, params=params, details=details)
