"""标准化错误码注册表。

每个错误码由三要素构成：

- 码名：枚举成员的 ``value``（如 ``"PARAM_MISSING"``），也是对外暴露的稳定标识；
- 消息模板：中文模板，支持 ``{param}`` 形式的命名占位符；
- HTTP 状态码：错误语义对应的 HTTP 状态。

分组：参数(400) / 认证(401) / 授权(403) / 资源(404, 409) / 限流(429) / 系统(500, 503, 504)。

参考设计：github.com/BaiJi/LzyTools ``basic_tool/errors``。
"""

from __future__ import annotations

from enum import Enum

__all__ = ["ErrorCode"]


class ErrorCode(str, Enum):
    """声明式错误码枚举。

    继承自 ``str``，因此成员可直接与字符串比较、参与 JSON 序列化：

    >>> ErrorCode.PARAM_MISSING == "PARAM_MISSING"
    True
    >>> str(ErrorCode.PARAM_MISSING)
    'PARAM_MISSING'
    >>> ErrorCode.PARAM_MISSING.template
    '缺少必填参数: {param}'
    >>> ErrorCode.PARAM_MISSING.http_status
    400
    >>> import json
    >>> json.dumps({"code": ErrorCode.TOKEN_EXPIRED})
    '{"code": "TOKEN_EXPIRED"}'
    """

    def __new__(cls, code: str, template: str, http_status: int) -> ErrorCode:
        """创建错误码成员。

        :param code: 码名字符串，同时作为枚举值
        :param template: 支持命名占位符的中文消息模板
        :param http_status: 对应的 HTTP 状态码
        """
        obj = str.__new__(cls, code)
        obj._value_ = code
        obj.template = template
        obj.http_status = http_status
        return obj

    def __str__(self) -> str:
        """返回码名字符串，避免 ``"ErrorCode.XXX"`` 形式带来的歧义。"""
        return str(self.value)

    # ---- 参数错误（400）----
    PARAM_MISSING = ("PARAM_MISSING", "缺少必填参数: {param}", 400)
    PARAM_INVALID = ("PARAM_INVALID", "参数无效: {param}", 400)
    PARAM_TYPE_ERROR = (
        "PARAM_TYPE_ERROR",
        "参数类型错误: {param} 应为 {expected_type}",
        400,
    )

    # ---- 认证错误（401）----
    TOKEN_EXPIRED = ("TOKEN_EXPIRED", "令牌已过期", 401)
    TOKEN_INVALID = ("TOKEN_INVALID", "令牌无效", 401)
    CREDENTIALS_ERROR = ("CREDENTIALS_ERROR", "用户名或密码错误", 401)

    # ---- 授权错误（403）----
    PERMISSION_DENIED = ("PERMISSION_DENIED", "权限不足: {required_permission}", 403)
    ACCESS_FORBIDDEN = ("ACCESS_FORBIDDEN", "禁止访问: {resource}", 403)

    # ---- 资源错误（404 / 409）----
    RESOURCE_NOT_FOUND = ("RESOURCE_NOT_FOUND", "资源不存在: {resource}", 404)
    RESOURCE_ALREADY_EXISTS = ("RESOURCE_ALREADY_EXISTS", "资源已存在: {resource}", 409)
    VERSION_CONFLICT = ("VERSION_CONFLICT", "版本冲突: {resource} 已被修改", 409)

    # ---- 限流（429）----
    RATE_LIMITED = ("RATE_LIMITED", "请求过于频繁，请稍后重试", 429)

    # ---- 系统错误（500 / 503 / 504）----
    INTERNAL_ERROR = ("INTERNAL_ERROR", "内部服务器错误", 500)
    SERVICE_UNAVAILABLE = ("SERVICE_UNAVAILABLE", "服务暂不可用", 503)
    UPSTREAM_TIMEOUT = ("UPSTREAM_TIMEOUT", "上游服务超时", 504)
