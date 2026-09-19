"""lizysdk.errors —— 标准化错误体系。

分层结构：

- :mod:`lizysdk.errors.codes`    —— ``ErrorCode`` 错误码注册表（码名/模板/HTTP 状态）
- :mod:`lizysdk.errors.base`     —— ``AppError`` 异常基类（模板渲染、序列化往返）
- :mod:`lizysdk.errors.standard` —— 按 HTTP 语义预置的标准子类
- :mod:`lizysdk.errors.utils`    —— ``wrap`` 异常包装 / ``ensure`` 断言式抛错

快速上手：
    >>> from lizysdk.errors import AppError, ErrorCode, NotFoundError, wrap, ensure
    >>> err = NotFoundError(params={"resource": "订单"})
    >>> str(err)
    '[RESOURCE_NOT_FOUND] 资源不存在: 订单'
"""

from __future__ import annotations

from .base import AppError
from .codes import ErrorCode
from .standard import (
    AuthError,
    ConflictError,
    InternalError,
    NotFoundError,
    ParamError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    UpstreamTimeoutError,
)
from .utils import ensure, wrap

__all__ = [
    "ErrorCode",
    "AppError",
    "ParamError",
    "AuthError",
    "PermissionDeniedError",
    "NotFoundError",
    "ConflictError",
    "RateLimitError",
    "InternalError",
    "ServiceUnavailableError",
    "UpstreamTimeoutError",
    "wrap",
    "ensure",
]
