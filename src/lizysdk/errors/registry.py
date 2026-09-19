"""业务错误码动态注册表。

在 :class:`~lizysdk.errors.codes.ErrorCode` 声明式枚举之外，允许在运行期
（如应用启动、读取配置）动态登记业务错误码，供 ``AppError`` 按码名解析
模板与 HTTP 状态码：

- ``register_code``     —— 注册业务码（与内置 ``ErrorCode`` 重名时需显式 ``overwrite``）；
- ``unregister_code``   —— 注销业务码（幂等，主要供测试 / 重载配置用）；
- ``registered_codes``  —— 返回注册表快照（深拷贝，改快照不影响注册表本身）。

``AppError`` 对码名的解析优先级为：**动态注册表 -> ErrorCode 枚举 -> 未知兜底(500)**；
被 ``overwrite`` 覆盖的内置码同样以注册表的模板 / 状态为准。

线程安全：注册 / 注销 / 查询全程持锁（``RLock``，可重入）。

示例：
    >>> from lizysdk.errors import register_code, registered_codes
    >>> from lizysdk.errors.base import AppError
    >>> register_code("BIZ_ORDER_REJECTED", "订单被拒绝: {order_id}", 422)
    'BIZ_ORDER_REJECTED'
    >>> err = AppError("BIZ_ORDER_REJECTED", params={"order_id": "o-9"})
    >>> err.message, err.http_status
    ('订单被拒绝: o-9', 422)
"""

from __future__ import annotations

import re
import threading
from typing import Any, Optional, Tuple

from .codes import ErrorCode

__all__ = ["register_code", "unregister_code", "registered_codes"]

#: 码名规约：大写字母开头，仅含大写字母 / 数字 / 下划线，总长 2~64。
_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{1,63}$")

#: 全局注册表锁：注册 / 注销 / 查询均持锁，RLock 允许同线程重入。
_LOCK = threading.RLock()

#: 注册表本体：{code: {"template": str, "http_status": int}}。
_REGISTRY: dict[str, dict[str, Any]] = {}


def register_code(
    code: str,
    template: str,
    http_status: int = 400,
    *,
    overwrite: bool = False,
) -> str:
    """注册一个业务错误码，返回码名本身（便于链式书写）。

    校验规则（不合法抛 ``ValueError``，中文消息）：

    - ``code``：非空 ``str`` 且匹配 ``^[A-Z][A-Z0-9_]{1,63}$``
      （大写字母开头，总长 2~64，仅含大写字母 / 数字 / 下划线）；
    - ``template``：非空 ``str``（纯空白字符串同样视为非法）；
    - ``http_status``：``int``（不接受 ``bool``）且在 100~599 之间。

    与已有定义同名时：

    - 与注册表中已有业务码重名：默认抛 ``ValueError``，``overwrite=True`` 覆盖；
    - 与内置 ``ErrorCode`` 成员重名：默认抛 ``ValueError``；``overwrite=True``
      允许覆盖内置定义（此后 ``AppError`` 解析以注册表为准）。

    示例：
        >>> from lizysdk.errors import register_code
        >>> register_code("BIZ_BALANCE_SHORT", "余额不足: 需要 {need}", 402)
        'BIZ_BALANCE_SHORT'
        >>> register_code("BIZ_BALANCE_SHORT", "余额不足（覆盖）", 402, overwrite=True)
        'BIZ_BALANCE_SHORT'

    :param code: 业务错误码名（如 ``"BIZ_ORDER_REJECTED"``）
    :param template: 中文消息模板，支持 ``{name}`` 命名占位符
    :param http_status: 对应 HTTP 状态码，默认 400
    :param overwrite: 同名时是否覆盖已有注册项或内置 ``ErrorCode`` 定义
    :return: 码名字符串
    :raises ValueError: 任一参数非法，或同名且未声明 ``overwrite``
    """
    if not isinstance(code, str) or not _CODE_PATTERN.match(code):
        raise ValueError(
            f"错误码非法：须为匹配 ^[A-Z][A-Z0-9_]{{1,63}}$ 的非空字符串"
            f"（大写字母开头，总长 2~64，仅含大写字母/数字/下划线），实际为 {code!r}"
        )
    if not isinstance(template, str) or not template.strip():
        raise ValueError(f"消息模板非法：须为非空字符串（纯空白同样非法），实际为 {template!r}")
    if (
        not isinstance(http_status, int)
        or isinstance(http_status, bool)
        or not 100 <= http_status <= 599
    ):
        raise ValueError(f"HTTP 状态码非法：须为 100~599 之间的 int（不接受 bool），实际为 {http_status!r}")

    with _LOCK:
        if code in _REGISTRY and not overwrite:
            raise ValueError(f"错误码 {code!r} 已注册；如需覆盖请传 overwrite=True")
        if code in ErrorCode.__members__ and not overwrite:
            raise ValueError(f"错误码 {code!r} 与内置 ErrorCode 成员同名；如需覆盖内置定义请传 overwrite=True")
        _REGISTRY[code] = {"template": template, "http_status": http_status}
    return code


def unregister_code(code: str) -> None:
    """注销一个业务错误码（幂等：码不存在时不抛错）。

    主要供测试清理与运行期重载配置使用；注销被 ``overwrite`` 覆盖的内置码后，
    ``AppError`` 解析自动回落到内置 ``ErrorCode`` 定义。

    示例：
        >>> from lizysdk.errors import register_code, unregister_code
        >>> register_code("BIZ_TMP", "临时码")
        'BIZ_TMP'
        >>> unregister_code("BIZ_TMP")   # 注销后回落未知兜底（500）
        >>> unregister_code("BIZ_TMP")   # 幂等，不抛错

    :param code: 业务错误码名；非 ``str`` 或未注册时静默忽略
    """
    if not isinstance(code, str):
        return
    with _LOCK:
        _REGISTRY.pop(code, None)


def registered_codes() -> dict[str, dict[str, Any]]:
    """返回注册表快照：``{code: {"template": ..., "http_status": ...}}``。

    返回的是拷贝（内层亦为新 dict），修改快照不会影响注册表本身；
    仅包含动态注册的业务码，不含内置 ``ErrorCode`` 成员。

    示例：
        >>> from lizysdk.errors import register_code, registered_codes
        >>> register_code("BIZ_SNAPSHOT", "示例", 418)
        'BIZ_SNAPSHOT'
        >>> registered_codes()["BIZ_SNAPSHOT"] == {"template": "示例", "http_status": 418}
        True

    :return: 注册表快照字典
    """
    with _LOCK:
        return {code: dict(entry) for code, entry in _REGISTRY.items()}


def _lookup_code(code: str) -> Optional[Tuple[str, int]]:
    """按码名查注册表，返回 ``(template, http_status)``；未命中返回 ``None``。

    仅供 :mod:`lizysdk.errors.base` 内部解析裸字符串码 /
    被覆盖的 ``ErrorCode`` 成员时使用（避免 base 与 registry 循环导入）。

    :param code: 码名字符串
    :return: ``(template, http_status)`` 元组或 ``None``
    """
    with _LOCK:
        entry = _REGISTRY.get(code)
        if entry is None:
            return None
        return entry["template"], entry["http_status"]
