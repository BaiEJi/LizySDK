r"""lizysdk.notify 异常族：NotifyError（子系统基类）与 ChannelError（渠道发送失败）。

设计契约（``docs/notify-design.md`` §2/§4）：

- 渠道发送失败统一抛 :class:`ChannelError`，中文消息携带**渠道名 /
  HTTP 状态 / 响应体摘要**三要素，并作为属性供程序化读取；
- 参数校验类错误直接抛 :class:`ValueError`（中文），不属于本异常族；
- :class:`NotifyError` 目前直接抛出的场景是通知中心生命周期错误
  （如 :meth:`lizysdk.notify.NotifyCenter.shutdown` 之后继续投递）。
"""

from __future__ import annotations

from typing import Optional

__all__ = ["ChannelError", "NotifyError"]

#: 错误消息中响应体摘要的最大长度（空白折叠后按字符截断）
_BRIEF_LIMIT: int = 160


def _summarize(text: str, limit: int = _BRIEF_LIMIT) -> str:
    """折叠空白并截断到 ``limit`` 个字符，作为错误消息里的响应摘要。

    Args:
        text: 原始响应文本（可能含换行 / 大量空白）。
        limit: 摘要最大长度，超出部分以 ``…`` 结尾。

    Returns:
        str: 适合单行展示的摘要。
    """
    collapsed = " ".join(text.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 1] + "…"


class NotifyError(Exception):
    """通知子系统（lizysdk.notify）的基异常。

    渠道发送失败见子类 :class:`ChannelError`；本类直接抛出的场景主要
    是通知中心的生命周期错误（停机后继续投递等）。

    Example:
        >>> from lizysdk.notify import ChannelError, NotifyError
        >>> issubclass(ChannelError, NotifyError), issubclass(NotifyError, Exception)
        (True, True)
    """


class ChannelError(NotifyError):
    r"""单个渠道发送失败的统一异常（设计文档 §2）。

    由各渠道 ``send()`` 在**发送失败**时抛出——HTTP 渠道为响应判定失败
    （钉钉 / 企微 ``errcode != 0``、飞书新版 ``code`` 与旧版
    ``StatusCode`` 均非 0、自定义 webhook 非 2xx、响应非合法 JSON），
    邮件渠道为 SMTP 异常，另含网络层异常（连接拒绝 / 超时等）的统一
    包装。中文消息形如::

        渠道[dingtalk]发送失败：errcode=310000，errmsg=sign not match；
        HTTP 200；响应摘要：{"errcode": 310000, ...}

    Attributes:
        channel: 渠道名（内置渠道为 ``"dingtalk"`` / ``"feishu"`` /
            ``"wecom"`` / ``"email"`` / ``"webhook"``，自定义渠道为其
            ``name`` 属性）。
        status: HTTP 状态码；网络层异常等无状态可谈时为 None。
        detail: 响应体原文（仅在可取得时提供，供日志排查）。

    Example:
        >>> from lizysdk.notify import ChannelError
        >>> exc = ChannelError("feishu", "code=19021，msg=sign match fail",
        ...                    status=200, detail='{"code": 19021}')
        >>> exc.channel, exc.status
        ('feishu', 200)
        >>> str(exc).startswith("渠道[feishu]发送失败")
        True
        >>> "HTTP 200" in str(exc)
        True
    """

    def __init__(
        self,
        channel: str,
        message: str,
        *,
        status: Optional[int] = None,
        detail: str = "",
    ) -> None:
        """组装携带渠道名 / HTTP 状态 / 响应摘要的中文错误。

        Args:
            channel: 渠道名。
            message: 失败原因（如 ``errcode=310000，errmsg=...``）。
            status: HTTP 状态码，无则为 None。
            detail: 响应体原文（可选）。
        """
        self.channel = channel
        self.status = status
        self.detail = detail
        parts = [f"渠道[{channel}]发送失败：{message}"]
        if status is not None:
            parts.append(f"HTTP {status}")
        if detail:
            parts.append(f"响应摘要：{_summarize(detail)}")
        super().__init__("；".join(parts))
