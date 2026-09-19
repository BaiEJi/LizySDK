"""随机 hex ID 生成：trace_id（链路追踪）与 uid（业务唯一标识）。

两者都基于密码学安全随机源（:mod:`secrets`）生成小写十六进制随机
字符串，位数可选（8 ~ 64 之间任意整数位，默认 16 位），不依赖任何
第三方库。语义上刻意区分：

- :func:`new_trace_id` —— 链路追踪 ID，贯穿单次请求的日志 / 错误 /
  上下文传递全链路（配合 ``lizysdk.logs`` 的 ``bind_context`` 使用）；
- :func:`new_uid` —— 通用业务唯一标识，如订单号、会话 ID、脱敏
  主键等不与追踪语义绑定的场景。

二者共享同一份私有随机实现（:func:`_random_hex`），仅语义与用途不同。

历史变更：``new_trace_id`` 的默认长度由 32 位调整为 16 位（64 bit
熵已满足绝大多数链路追踪场景），依赖 32 位的旧代码请显式传
``length=32``。
"""

from __future__ import annotations

import secrets

__all__ = ["new_trace_id", "new_uid"]

#: 合法位数下限：8 位 hex 仅 32 bit 熵，再短则碰撞空间失去实用价值
_MIN_LENGTH: int = 8
#: 合法位数上限：64 位 hex = 256 bit 熵，远超任何去中心化场景所需
_MAX_LENGTH: int = 64


def _random_hex(length: int) -> str:
    """生成精确 ``length`` 位的小写十六进制随机字符串（模块内私有辅助）。

    :func:`secrets.token_hex` 按字节产码，只能直接得到偶数长度
    （每字节恰编码为 2 个 hex 字符），因此先按 ``ceil(length / 2)``
    字节生成、再截断到精确长度，奇数位数同样支持。

    Args:
        length: 目标十六进制字符位数，必须为 int（bool 除外）且
            满足 ``8 <= length <= 64``。

    Returns:
        str: 长度精确等于 ``length`` 的字符串，仅包含 ``0-9`` 与 ``a-f``。

    Raises:
        ValueError: ``length`` 非 int（或为 bool），或不在
            ``[8, 64]`` 范围内。

    Example:
        >>> len(_random_hex(16))
        16
        >>> len(_random_hex(9))  # 奇数位：生成 5 字节再截断
        9
    """
    # bool 是 int 的子类，必须先于 int 判断显式排除
    if isinstance(length, bool) or not isinstance(length, int):
        raise ValueError(
            f"length 必须为 int（bool 除外），合法范围为 "
            f"[{_MIN_LENGTH}, {_MAX_LENGTH}]；当前类型为 {type(length).__name__}"
        )
    if not _MIN_LENGTH <= length <= _MAX_LENGTH:
        raise ValueError(
            f"length 必须在 [{_MIN_LENGTH}, {_MAX_LENGTH}] 内，当前为 {length}"
        )
    # ceil(length/2) 字节编码为偶数位 hex，再截断为精确位数
    nbytes = (length + 1) // 2
    return secrets.token_hex(nbytes)[:length]


def new_trace_id(length: int = 16) -> str:
    """生成用于链路追踪的 trace_id：``length`` 位小写十六进制字符串。

    内部经 :func:`secrets.token_hex` 使用操作系统级密码学安全随机源，
    结果不可预测。默认 16 位 = 64 bit 熵：按生日碰撞公式估算，累计
    生成 2^32（约 43 亿）个 trace_id 碰撞概率仍不足 50%，概率意义上
    可视为全局唯一，无需中心化协调。

    Args:
        length: trace_id 的十六进制字符位数，可取 ``[8, 64]`` 内任意
            整数（含奇数位，内部生成后截断到精确长度），默认 16。

    Returns:
        str: 长度精确为 ``length`` 的字符串，仅包含 ``0-9`` 与 ``a-f``。

    Raises:
        ValueError: ``length`` 非 int（或为 bool），或不在 ``[8, 64]`` 内。

    Note:
        默认长度在 lizysdk 中由 32 位改为 16 位；需要 32 位的旧代码
        请显式传入 ``length=32``。

    Example:
        >>> tid = new_trace_id()
        >>> len(tid)
        16
        >>> tid == tid.lower()
        True
        >>> len(new_trace_id(32))
        32
        >>> new_trace_id(7)
        Traceback (most recent call last):
            ...
        ValueError: length 必须在 [8, 64] 内，当前为 7
    """
    return _random_hex(length)


def new_uid(length: int = 16) -> str:
    """生成通用唯一字符串 ID（uid）：``length`` 位小写十六进制字符串。

    与 :func:`new_trace_id` 共用同一份随机实现，仅语义不同：

    - ``trace_id`` 面向链路追踪，贯穿单次请求的日志 / 错误 / 上下文；
    - ``uid`` 面向业务唯一标识（订单号、会话 ID、脱敏主键等），
      不与追踪语义绑定，避免在业务表中混入追踪概念。

    Args:
        length: uid 的十六进制字符位数，可取 ``[8, 64]`` 内任意整数
            （含奇数位，内部生成后截断到精确长度），默认 16。

    Returns:
        str: 长度精确为 ``length`` 的字符串，仅包含 ``0-9`` 与 ``a-f``。

    Raises:
        ValueError: ``length`` 非 int（或为 bool），或不在 ``[8, 64]`` 内。

    Example:
        >>> uid = new_uid()
        >>> len(uid)
        16
        >>> len(new_uid(8))
        8
        >>> set(new_uid(24)) <= set("0123456789abcdef")
        True
    """
    return _random_hex(length)
