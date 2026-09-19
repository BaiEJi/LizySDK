"""标准错误子类：为常见 HTTP 语义预置默认 code 与 http_status。

所有子类构造参数与 :class:`~lizysdk.errors.base.AppError` 完全一致，
仅替换 ``code`` 的默认值；``http_status`` 由默认 ``code`` 推导而来：

- ``ParamError``            400 / ``PARAM_INVALID``
- ``AuthError``             401 / ``CREDENTIALS_ERROR``
- ``PermissionDeniedError`` 403 / ``PERMISSION_DENIED``
- ``NotFoundError``         404 / ``RESOURCE_NOT_FOUND``
- ``ConflictError``         409 / ``RESOURCE_ALREADY_EXISTS``
- ``RateLimitError``        429 / ``RATE_LIMITED``
- ``InternalError``         500 / ``INTERNAL_ERROR``
- ``ServiceUnavailableError`` 503 / ``SERVICE_UNAVAILABLE``
- ``UpstreamTimeoutError``  504 / ``UPSTREAM_TIMEOUT``
"""

from __future__ import annotations

from .base import AppError
from .codes import ErrorCode

__all__ = [
    "ParamError",
    "AuthError",
    "PermissionDeniedError",
    "NotFoundError",
    "ConflictError",
    "RateLimitError",
    "InternalError",
    "ServiceUnavailableError",
    "UpstreamTimeoutError",
]


class ParamError(AppError):
    """参数错误（400 / ``PARAM_INVALID``）。

    示例：
        >>> err = ParamError(params={"param": "page"})
        >>> err.code, err.http_status, err.message
        ('PARAM_INVALID', 400, '参数无效: page')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.PARAM_INVALID,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)


class AuthError(AppError):
    """认证错误（401 / ``CREDENTIALS_ERROR``）。

    示例：
        >>> err = AuthError()
        >>> err.code, err.http_status, err.message
        ('CREDENTIALS_ERROR', 401, '用户名或密码错误')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.CREDENTIALS_ERROR,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)


class PermissionDeniedError(AppError):
    """权限不足（403 / ``PERMISSION_DENIED``）。

    示例：
        >>> err = PermissionDeniedError(params={"required_permission": "admin"})
        >>> err.code, err.http_status, err.message
        ('PERMISSION_DENIED', 403, '权限不足: admin')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.PERMISSION_DENIED,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)


class NotFoundError(AppError):
    """资源不存在（404 / ``RESOURCE_NOT_FOUND``）。

    示例：
        >>> err = NotFoundError(params={"resource": "订单"})
        >>> err.code, err.http_status, err.message
        ('RESOURCE_NOT_FOUND', 404, '资源不存在: 订单')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.RESOURCE_NOT_FOUND,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)


class ConflictError(AppError):
    """资源冲突（409 / ``RESOURCE_ALREADY_EXISTS``）。

    示例：
        >>> err = ConflictError(params={"resource": "用户名"})
        >>> err.code, err.http_status, err.message
        ('RESOURCE_ALREADY_EXISTS', 409, '资源已存在: 用户名')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.RESOURCE_ALREADY_EXISTS,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)


class RateLimitError(AppError):
    """触发限流（429 / ``RATE_LIMITED``）。

    示例：
        >>> err = RateLimitError()
        >>> err.code, err.http_status, err.message
        ('RATE_LIMITED', 429, '请求过于频繁，请稍后重试')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.RATE_LIMITED,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)


class InternalError(AppError):
    """内部错误（500 / ``INTERNAL_ERROR``）。

    示例：
        >>> err = InternalError()
        >>> err.code, err.http_status, err.message
        ('INTERNAL_ERROR', 500, '内部服务器错误')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.INTERNAL_ERROR,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)


class ServiceUnavailableError(AppError):
    """服务暂不可用（503 / ``SERVICE_UNAVAILABLE``）。

    示例：
        >>> err = ServiceUnavailableError()
        >>> err.code, err.http_status, err.message
        ('SERVICE_UNAVAILABLE', 503, '服务暂不可用')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.SERVICE_UNAVAILABLE,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)


class UpstreamTimeoutError(AppError):
    """上游服务超时（504 / ``UPSTREAM_TIMEOUT``）。

    示例：
        >>> err = UpstreamTimeoutError()
        >>> err.code, err.http_status, err.message
        ('UPSTREAM_TIMEOUT', 504, '上游服务超时')
    """

    def __init__(
        self,
        code: ErrorCode | str = ErrorCode.UPSTREAM_TIMEOUT,
        message: str | None = None,
        *,
        details: dict | None = None,
        params: dict | None = None,
    ) -> None:
        super().__init__(code, message, details=details, params=params)
