r"""lizysdk.notify 渠道层：钉钉 / 飞书 / 企业微信 / 邮件 / 自定义 Webhook。

设计契约 ``docs/notify-design.md`` §2 的逐条落实——五个内置渠道对齐
各官方机器人 API，payload 结构、标题拼接规则、签名算法与成功判定
**逐字节可测**：

- **钉钉** :class:`DingTalkChannel`：markdown，``text = "<title>\n<body>"``；
  加签 ``sign = quote_plus(b64(hmac_sha256(key=secret, msg=f"{ts_ms}\n{secret}")))``
  拼接 ``&timestamp={ts_ms}&sign={sign}``（**毫秒**时间戳）；成功判定
  响应 JSON ``errcode == 0``。
- **飞书** :class:`FeishuChannel`：text，``content.text = "<title>\n<body>"``；
  签名与钉钉**相反——空消息签名**：``b64(hmac_sha256(key=f"{ts}\n{secret}",
  msg=b""))`` 进 body（**秒**级时间戳，``timestamp`` 为 str）；成功判定
  新版 ``code == 0`` 或旧版 ``StatusCode == 0``。
- **企业微信** :class:`WeComChannel`：markdown，``content = "**<title>**\n<body>"``
  （标题加粗；官方注：markdown 仅子集语法）；无签名；成功判定
  ``errcode == 0``。
- **邮件** :class:`EmailChannel`：``MIMEText(body, "plain", "utf-8")``、
  ``Subject=title``、发 ``to`` 列表；SMTP/SMTP_SSL + LOGIN；SMTP 无异常
  即成功。
- **自定义** :class:`WebhookChannel`：默认 payload
  ``{"title", "content", "level", "ts"}``，``payload_builder(title, body,
  level)`` 可定制；成功判定 HTTP 2xx。

离线化接缝（测试注入点，设计文档 §2 备注）：

- ``transport``：HTTP 渠道统一走
  ``transport(url, payload, headers, timeout) -> (status, text)``，默认
  :func:`_default_transport`（urllib POST JSON，
  ``Content-Type: application/json``、utf-8）；
- ``mailer``：邮件渠道走 ``mailer(msg_bytes, sender, to)``，默认
  :func:`_default_mailer`（smtplib 封装）；
- 时间：模块级 :func:`_now`（签名时间戳等共用，测试 monkeypatch 冻结用）。

工程红线：纯标准库；不 import 其他 lizysdk 子包；日志只走标准
``logging.getLogger("lizysdk.notify")`` 通道；渠道发送失败统一抛
:class:`lizysdk.notify.ChannelError`。
"""

from __future__ import annotations

import base64
import functools
import hashlib
import hmac
import json
import logging
import smtplib
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from email.header import Header
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import quote_plus

from .exceptions import ChannelError

__all__ = [
    "DingTalkChannel",
    "FeishuChannel",
    "WeComChannel",
    "EmailChannel",
    "WebhookChannel",
]

#: 子包统一日志通道（对 lizysdk.logs 零 import 依赖，由根 handler 统一捕获）
logger = logging.getLogger("lizysdk.notify")

#: HTTP transport 契约：``f(url, payload, headers, timeout) -> (状态码, 响应文本)``
Transport = Callable[[str, Dict[str, Any], Optional[Dict[str, str]], float], Tuple[int, str]]
#: 邮件 mailer 契约：``f(MIME 字节串, 发件人, 收件人列表) -> None``
Mailer = Callable[[bytes, str, Sequence[str]], None]
#: 自定义 payload 构造器契约：``f(title, body, level) -> dict``
PayloadBuilder = Callable[[str, str, str], Dict[str, Any]]


def _now() -> float:
    """当前 Unix 时间戳（秒）——测试冻结接缝。

    静默期 / 频控（:mod:`lizysdk.notify.center`）与渠道签名时间戳共用
    同一语义的接缝；测试用 ``monkeypatch.setattr`` 冻结以获得确定性。
    """
    return time.time()


def _check_str(value: Any, name: str, *, allow_empty: bool = False) -> str:
    """校验 ``value`` 为非空 str（``allow_empty=True`` 时仅要求类型）。"""
    if not isinstance(value, str):
        raise ValueError(
            f"{name} 必须为非空 str，当前类型为 {type(value).__name__}：{value!r}"
        )
    if not allow_empty and not value.strip():
        raise ValueError(f"{name} 不能为空或纯空白字符串")
    return value


def _check_url(value: Any, name: str = "webhook") -> str:
    """校验 ``value`` 为 http(s):// 开头的非空 URL 字符串。"""
    _check_str(value, name)
    if not (value.startswith("http://") or value.startswith("https://")):
        raise ValueError(f"{name} 必须以 http:// 或 https:// 开头：{value!r}")
    return value


def _check_timeout(timeout: Any, name: str = "timeout") -> float:
    """校验 ``timeout`` 为正数秒（bool 除外），返回 float 形式。"""
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise ValueError(
            f"{name} 必须为正数秒（int/float），"
            f"当前类型为 {type(timeout).__name__}：{timeout!r}"
        )
    if timeout <= 0:
        raise ValueError(f"{name} 必须为正数秒，当前为 {timeout}")
    return float(timeout)


def _check_callable(value: Any, name: str) -> None:
    """校验 ``value`` 为可调用对象（None 由调用方先处理）。"""
    if not callable(value):
        raise ValueError(
            f"{name} 必须为可调用对象，当前类型为 {type(value).__name__}：{value!r}"
        )


def _default_transport(
    url: str,
    payload: Dict[str, Any],
    headers: Optional[Dict[str, str]],
    timeout: float,
) -> Tuple[int, str]:
    """默认 HTTP transport：urllib POST JSON。

    Args:
        url: 目标 URL（渠道已把签名参数拼进 URL 时直接使用）。
        payload: JSON 对象（dict）。
        headers: 调用方附加头（如自定义 webhook 的鉴权头）；与默认
            ``Content-Type: application/json`` 合并，同名键以调用方为准。
        timeout: 超时秒数。

    Returns:
        Tuple[int, str]: ``(HTTP 状态码, 响应文本)``——4xx/5xx 也作为
        「响应」返回（交由各渠道的成功判定处理），不在此抛错。
    """
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    merged: Dict[str, str] = {"Content-Type": "application/json"}
    if headers:
        merged.update(headers)
    request = urllib.request.Request(url, data=data, headers=merged, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            return int(resp.status), resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        # HTTPError 也携带响应体：读出后按 (状态码, 响应文本) 契约返回
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:  # pragma: no cover - 错误体读取失败的兜底
            body = ""
        return int(exc.code), body


def _default_mailer(
    host: str,
    port: int,
    user: str,
    password: str,
    use_ssl: bool,
    timeout: float,
    msg_bytes: bytes,
    sender: str,
    to: Sequence[str],
) -> None:
    """默认 mailer：SMTP/SMTP_SSL + LOGIN，``with`` 语义确保连接关闭。

    Args:
        host: SMTP 服务器地址。
        port: 端口（SSL 默认 465）。
        user: 登录用户名（通常即发件邮箱）。
        password: 登录口令 / 授权码。
        use_ssl: True 走 :class:`smtplib.SMTP_SSL`，False 走
            :class:`smtplib.SMTP`。
        timeout: 连接与读写超时秒数。
        msg_bytes: 完整 MIME 消息字节串（含 Subject/From/To 头）。
        sender: 发件人地址。
        to: 收件人地址列表。
    """
    client_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with client_cls(host, port, timeout=timeout) as client:
        client.login(user, password)
        client.sendmail(sender, list(to), msg_bytes)


class _BaseChannel:
    """渠道基类：统一 ``send(title, body, level)`` 契约与公共校验。

    通知中心按**鸭子类型**扇出——任何实现了 ``send(title, body,
    level)`` 的对象都可以注册为渠道（不必继承本类）；内置五渠道继承
    它以复用参数校验与 HTTP 辅助方法。

    Note:
        级别 ``level`` 在渠道层是**透传元数据**（仅自定义 webhook 默认
        payload 会携带），合法性校验由 :class:`lizysdk.notify.NotifyCenter`
        负责；渠道只校验 ``title`` / ``body`` 自身。
    """

    #: 渠道名（出现在 ChannelError 消息与 per-channel 统计中）
    name: str = "base"

    def send(self, title: str, body: str, level: str = "info") -> None:
        """发送一条通知；失败抛 :class:`lizysdk.notify.ChannelError`。"""
        raise NotImplementedError

    def _check_title_body(self, title: Any, body: Any) -> Tuple[str, str]:
        """校验 ``title`` 为非空 str、``body`` 为 str（允许空串）。"""
        _check_str(title, "title")
        if not isinstance(body, str):
            raise ValueError(
                f"body 必须为 str（允许空串），当前类型为 {type(body).__name__}：{body!r}"
            )
        return title, body

    def _post_json(
        self,
        url: str,
        payload: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> Tuple[int, str]:
        """经 transport 发送 POST，任何网络层异常统一包装为 ChannelError。"""
        try:
            return self._transport(url, payload, headers, self.timeout)  # type: ignore[attr-defined]
        except Exception as exc:
            raise ChannelError(self.name, f"HTTP 请求异常：{exc}") from exc

    def _json_body(self, status: int, text: str) -> Dict[str, Any]:
        """把响应文本解析为 JSON 对象，非 JSON / 非对象统一 ChannelError。"""
        try:
            data = json.loads(text)
        except ValueError as exc:
            raise ChannelError(self.name, "响应不是合法 JSON", status=status, detail=text) from exc
        if not isinstance(data, dict):
            raise ChannelError(self.name, "响应 JSON 不是对象", status=status, detail=text)
        return data


class DingTalkChannel(_BaseChannel):
    r"""钉钉自定义机器人渠道（markdown 消息，可选加签）。

    消息格式（设计文档 §2，逐字节对齐官方机器人 API）::

        {"msgtype": "markdown",
         "markdown": {"title": "<title>", "text": "<title>\n<body>"}}

    加签（**注意与飞书相反**：钉钉以 secret 为 key、``f"{ts_ms}\n{secret}"``
    为消息；ts 为**毫秒**）::

        sign = quote_plus(base64(hmac_sha256(key=secret, msg=f"{ts_ms}\n{secret}")))
        url  = webhook + f"&timestamp={ts_ms}&sign={sign}"

    成功判定：响应 JSON ``errcode == 0``。

    Args:
        webhook: 机器人 webhook 地址（须以 ``http(s)://`` 开头，通常
            已含 ``?access_token=...``，因此签名参数以 ``&`` 拼接）。
        secret: 加签密钥（``SEC`` 开头）；None 表示不加签。
        timeout: HTTP 超时秒数。
        transport: 注入的 HTTP transport（测试离线化用），None 用
            :func:`_default_transport`。

    Raises:
        ValueError: 参数非法（URL 格式 / 超时 / transport 不可调用）。
        ChannelError: 发送失败（网络异常 / 响应非 JSON / errcode 非 0）。

    Example:
        >>> seen = []
        >>> ch = DingTalkChannel(
        ...     "https://oapi.dingtalk.com/robot/send?access_token=tok",
        ...     transport=lambda u, p, h, t: (seen.append((u, p, h, t)),
        ...                                    (200, '{"errcode": 0}'))[1])
        >>> ch.send("构建失败", "main 分支流水线红了", level="error")
        >>> seen[0][1]["markdown"]["text"]
        '构建失败\nmain 分支流水线红了'
        >>> seen[0][1]["msgtype"]
        'markdown'
    """

    name = "dingtalk"

    def __init__(
        self,
        webhook: str,
        *,
        secret: Optional[str] = None,
        timeout: float = 10.0,
        transport: Optional[Transport] = None,
    ) -> None:
        _check_url(webhook, "webhook")
        if secret is not None:
            _check_str(secret, "secret")
        _check_timeout(timeout)
        if transport is None:
            transport = _default_transport
        else:
            _check_callable(transport, "transport")
        self.webhook = webhook
        self.secret = secret
        self.timeout: float = float(timeout)
        self._transport: Transport = transport

    def send(self, title: str, body: str, level: str = "info") -> None:
        """发送 markdown 消息；可选加签拼 URL；``errcode != 0`` 抛错。

        Args:
            title: 通知标题（非空 str，进 markdown.title 并拼进 text 首行）。
            body: 通知正文（str，拼接在标题之后）。
            level: 级别元数据（钉钉 payload 不携带，仅保持统一签名）。

        Raises:
            ValueError: title / body 非法。
            ChannelError: 发送失败。
        """
        self._check_title_body(title, body)
        payload: Dict[str, Any] = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": f"{title}\n{body}"},
        }
        url = self.webhook
        if self.secret:
            ts_ms = int(_now() * 1000)
            digest = hmac.new(
                self.secret.encode("utf-8"),
                f"{ts_ms}\n{self.secret}".encode("utf-8"),
                hashlib.sha256,
            ).digest()
            sign = quote_plus(base64.b64encode(digest))
            url = f"{self.webhook}&timestamp={ts_ms}&sign={sign}"
        status, text = self._post_json(url, payload)
        data = self._json_body(status, text)
        if data.get("errcode") != 0:
            raise ChannelError(
                self.name,
                f"errcode={data.get('errcode')}，errmsg={data.get('errmsg', '')}",
                status=status,
                detail=text,
            )


class FeishuChannel(_BaseChannel):
    r"""飞书自定义机器人渠道（text 消息，可选签名校验）。

    消息格式（设计文档 §2）::

        {"msg_type": "text", "content": {"text": "<title>\n<body>"}}

    签名（**注意与钉钉相反——空消息签名**：以 ``f"{ts}\n{secret}"`` 为
    key、空字节串为消息；ts 为**秒**，body 增补两个字段）::

        body["timestamp"] = str(ts)
        body["sign"] = base64(hmac_sha256(key=f"{ts}\n{secret}", msg=b""))

    成功判定：新版响应 ``code == 0`` 或旧版 ``StatusCode == 0``。

    Args:
        webhook: 机器人 webhook 地址（``http(s)://`` 开头）。
        secret: 签名密钥；None 表示不签名。
        timeout: HTTP 超时秒数。
        transport: 注入的 HTTP transport，None 用默认 urllib 实现。

    Raises:
        ValueError: 参数非法。
        ChannelError: 发送失败（含 code / StatusCode 非 0、无成功标记）。

    Example:
        >>> seen = []
        >>> ch = FeishuChannel(
        ...     "https://open.feishu.cn/open-apis/bot/v2/hook/tok",
        ...     transport=lambda u, p, h, t: (seen.append(p),
        ...                                    (200, '{"code": 0}'))[1])
        >>> ch.send("标题", "内容")
        >>> seen[0]["content"]["text"]
        '标题\n内容'
        >>> seen[0]["msg_type"]
        'text'
    """

    name = "feishu"

    def __init__(
        self,
        webhook: str,
        *,
        secret: Optional[str] = None,
        timeout: float = 10.0,
        transport: Optional[Transport] = None,
    ) -> None:
        _check_url(webhook, "webhook")
        if secret is not None:
            _check_str(secret, "secret")
        _check_timeout(timeout)
        if transport is None:
            transport = _default_transport
        else:
            _check_callable(transport, "transport")
        self.webhook = webhook
        self.secret = secret
        self.timeout: float = float(timeout)
        self._transport: Transport = transport

    def send(self, title: str, body: str, level: str = "info") -> None:
        """发送 text 消息；可选在 body 增补 timestamp/sign；非 0 抛错。

        Args:
            title: 通知标题（非空 str，与正文拼为 ``"<title>\n<body>"``）。
            body: 通知正文（str）。
            level: 级别元数据（飞书 payload 不携带）。

        Raises:
            ValueError: title / body 非法。
            ChannelError: 发送失败。
        """
        self._check_title_body(title, body)
        payload: Dict[str, Any] = {
            "msg_type": "text",
            "content": {"text": f"{title}\n{body}"},
        }
        if self.secret:
            ts = int(_now())
            digest = hmac.new(
                f"{ts}\n{self.secret}".encode("utf-8"), b"", hashlib.sha256
            ).digest()
            payload["timestamp"] = str(ts)
            payload["sign"] = base64.b64encode(digest).decode("ascii")
        status, text = self._post_json(self.webhook, payload)
        data = self._json_body(status, text)
        # 新版权重高于旧版：任一成功标记为 0 即成功（设计文档 §2）
        if data.get("code") == 0 or data.get("StatusCode") == 0:
            return
        if "code" in data:
            reason = f"code={data.get('code')}，msg={data.get('msg', '')}"
        elif "StatusCode" in data:
            reason = (
                f"StatusCode={data.get('StatusCode')}，"
                f"StatusMessage={data.get('StatusMessage', '')}"
            )
        else:
            reason = "响应缺少成功标记字段（code / StatusCode）"
        raise ChannelError(self.name, reason, status=status, detail=text)


class WeComChannel(_BaseChannel):
    r"""企业微信群机器人渠道（markdown 消息，无签名）。

    消息格式（设计文档 §2，标题以 ``**`` 加粗）::

        {"msgtype": "markdown", "markdown": {"content": "**<title>**\n<body>"}}

    签名：无（webhook URL 中的 key 即凭证）。官方注：markdown 仅支持
    子集语法（标题 / 加粗 / 引用 / 代码块 / 链接等，无表格与图片）。

    成功判定：响应 JSON ``errcode == 0``。

    Args:
        webhook: 群机器人 webhook 地址（``http(s)://`` 开头）。
        timeout: HTTP 超时秒数。
        transport: 注入的 HTTP transport，None 用默认 urllib 实现。

    Raises:
        ValueError: 参数非法。
        ChannelError: 发送失败（网络异常 / 响应非 JSON / errcode 非 0）。

    Example:
        >>> seen = []
        >>> ch = WeComChannel(
        ...     "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=tok",
        ...     transport=lambda u, p, h, t: (seen.append(p),
        ...                                    (200, '{"errcode": 0}'))[1])
        >>> ch.send("标题", "内容")
        >>> seen[0]["markdown"]["content"]
        '**标题**\n内容'
    """

    name = "wecom"

    def __init__(
        self,
        webhook: str,
        *,
        timeout: float = 10.0,
        transport: Optional[Transport] = None,
    ) -> None:
        _check_url(webhook, "webhook")
        _check_timeout(timeout)
        if transport is None:
            transport = _default_transport
        else:
            _check_callable(transport, "transport")
        self.webhook = webhook
        self.timeout: float = float(timeout)
        self._transport: Transport = transport

    def send(self, title: str, body: str, level: str = "info") -> None:
        """发送 markdown 消息（标题加粗）；``errcode != 0`` 抛错。

        Args:
            title: 通知标题（非空 str，以 ``**`` 包裹后拼在 content 首行）。
            body: 通知正文（str）。
            level: 级别元数据（企微 payload 不携带）。

        Raises:
            ValueError: title / body 非法。
            ChannelError: 发送失败。
        """
        self._check_title_body(title, body)
        payload: Dict[str, Any] = {
            "msgtype": "markdown",
            "markdown": {"content": f"**{title}**\n{body}"},
        }
        status, text = self._post_json(self.webhook, payload)
        data = self._json_body(status, text)
        if data.get("errcode") != 0:
            raise ChannelError(
                self.name,
                f"errcode={data.get('errcode')}，errmsg={data.get('errmsg', '')}",
                status=status,
                detail=text,
            )


class EmailChannel(_BaseChannel):
    r"""邮件渠道（SMTP / SMTP_SSL + LOGIN，纯文本正文）。

    消息格式（设计文档 §2）：``MIMEText(body, "plain", "utf-8")``，
    ``Subject`` 为 title（中文经 RFC 2047 编码），``From`` 为 sender
    （默认取 ``user``），``To`` 为收件人列表逗号连接；发送
    ``msg.as_bytes()``。成功判定：SMTP 调用无异常。

    Args:
        host: SMTP 服务器地址。
        port: 端口，默认 465（SSL 隐式加密端口）。
        user: 登录用户名（默认同时作为发件人）。
        password: 登录口令 / 授权码。
        to: 收件人——单个地址或地址列表（至少一个）。
        use_ssl: True（默认）走 :class:`smtplib.SMTP_SSL`，False 走
            :class:`smtplib.SMTP`。
        sender: 显式发件人地址，None 取 ``user``。
        timeout: SMTP 连接与读写超时秒数。
        mailer: 注入的邮件发送器 ``f(msg_bytes, sender, to)``（测试
            离线化用），None 用 :func:`_default_mailer`（smtplib 封装）。

    Raises:
        ValueError: 参数非法（host/user/password/to/port/use_ssl/mailer）。
        ChannelError: SMTP 发送异常（登录失败 / 投递被拒等）。

    Example:
        >>> sent = []
        >>> ch = EmailChannel("smtp.example.com", user="bot@example.com",
        ...                   password="pw", to=["ops@example.com"],
        ...                   mailer=lambda raw, sender, to: sent.append((raw, sender, to)))
        >>> ch.send("磁盘告警", "data 盘使用率 91%")
        >>> sent[0][1], sent[0][2]
        ('bot@example.com', ['ops@example.com'])
        >>> from email import message_from_bytes
        >>> message_from_bytes(sent[0][0]).get_payload(decode=True).decode("utf-8")
        'data 盘使用率 91%'
    """

    name = "email"

    def __init__(
        self,
        host: str,
        port: int = 465,
        *,
        user: str,
        password: str,
        to: Union[str, Sequence[str]],
        use_ssl: bool = True,
        sender: Optional[str] = None,
        timeout: float = 10.0,
        mailer: Optional[Mailer] = None,
    ) -> None:
        _check_str(host, "host")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError(f"port 必须为 1~65535 的整数，当前为 {port!r}")
        _check_str(user, "user")
        _check_str(password, "password")
        if isinstance(to, str):
            recipients: List[str] = [to]
        elif isinstance(to, Sequence):
            recipients = list(to)
        else:
            raise ValueError(
                f"to 必须为 str 或 str 列表，当前类型为 {type(to).__name__}：{to!r}"
            )
        if not recipients:
            raise ValueError("to 不能为空：至少需要一个收件人地址")
        for addr in recipients:
            _check_str(addr, "to 中的收件人地址")
        if not isinstance(use_ssl, bool):
            raise ValueError(f"use_ssl 必须为 bool，当前为 {use_ssl!r}")
        if sender is None:
            sender = user
        else:
            _check_str(sender, "sender")
        _check_timeout(timeout)
        if mailer is None:
            bound_mailer: Mailer = functools.partial(
                _default_mailer, host, port, user, password, use_ssl, float(timeout)
            )
        else:
            _check_callable(mailer, "mailer")
            bound_mailer = mailer
        self.host = host
        self.port = port
        self.user = user
        self.to: List[str] = recipients
        self.use_ssl = use_ssl
        self.sender = sender
        self.timeout: float = float(timeout)
        self._mailer: Mailer = bound_mailer

    def send(self, title: str, body: str, level: str = "info") -> None:
        """构造 MIME 纯文本邮件并经 mailer 发出；SMTP 异常统一抛错。

        Args:
            title: 邮件主题（非空 str，中文经 ``Header(title, "utf-8")``
                编码）。
            body: 邮件正文（str，``"plain"`` / utf-8）。
            level: 级别元数据（邮件头不携带）。

        Raises:
            ValueError: title / body 非法。
            ChannelError: mailer 抛出的任何异常（SMTP 登录 / 投递失败等）。
        """
        self._check_title_body(title, body)
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = Header(title, "utf-8")
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.to)
        raw = msg.as_bytes()
        try:
            self._mailer(raw, self.sender, list(self.to))
        except Exception as exc:
            raise ChannelError(self.name, f"SMTP 发送异常：{exc}") from exc


class WebhookChannel(_BaseChannel):
    r"""自定义 webhook 渠道（默认 JSON 结构 + 可定制 payload 与 headers）。

    默认 payload（设计文档 §2）::

        {"title": <title>, "content": <body>, "level": <level>, "ts": <_now()>}

    ``payload_builder(title, body, level) -> dict`` 可整体定制结构；
    ``headers`` 自由附加（如鉴权头），与默认 transport 的
    ``Content-Type: application/json`` 合并、同名键以用户为准。签名机制
    完全自定义（在 headers / payload_builder 内自行实现）。成功判定：
    HTTP 2xx。

    Args:
        url: 目标地址（``http(s)://`` 开头）。
        headers: 附加请求头（str 到 str 的映射），None 表示无。
        payload_builder: 自定义 payload 构造器，None 用默认四键结构。
        timeout: HTTP 超时秒数。
        transport: 注入的 HTTP transport，None 用默认 urllib 实现。

    Raises:
        ValueError: 参数非法。
        ChannelError: 发送失败（builder 异常 / 返回非 dict / 非 2xx /
            网络异常）。

    Example:
        >>> seen = []
        >>> ch = WebhookChannel(
        ...     "https://hooks.example.com/x",
        ...     payload_builder=lambda t, b, lv: {"what": t, "detail": b, "sev": lv},
        ...     transport=lambda u, p, h, t: (seen.append(p), (200, "ok"))[1])
        >>> ch.send("标题", "内容", level="critical")
        >>> seen[0]
        {'what': '标题', 'detail': '内容', 'sev': 'critical'}
    """

    name = "webhook"

    def __init__(
        self,
        url: str,
        *,
        headers: Optional[Mapping[str, str]] = None,
        payload_builder: Optional[PayloadBuilder] = None,
        timeout: float = 10.0,
        transport: Optional[Transport] = None,
    ) -> None:
        _check_url(url, "url")
        _check_timeout(timeout)
        copied_headers: Optional[Dict[str, str]]
        if headers is None:
            copied_headers = None
        else:
            if not isinstance(headers, Mapping):
                raise ValueError(
                    f"headers 必须为 str 到 str 的映射，"
                    f"当前类型为 {type(headers).__name__}：{headers!r}"
                )
            copied_headers = {}
            for key, value in headers.items():
                _check_str(key, "headers 的键")
                _check_str(value, f"headers[{key!r}] 的值", allow_empty=True)
                copied_headers[key] = value
        if payload_builder is not None:
            _check_callable(payload_builder, "payload_builder")
        if transport is None:
            transport = _default_transport
        else:
            _check_callable(transport, "transport")
        self.url = url
        self.headers: Optional[Dict[str, str]] = copied_headers
        self.timeout: float = float(timeout)
        self._payload_builder: Optional[PayloadBuilder] = payload_builder
        self._transport: Transport = transport

    def send(self, title: str, body: str, level: str = "info") -> None:
        """按默认结构或 payload_builder 组装 payload 并 POST；非 2xx 抛错。

        Args:
            title: 通知标题（非空 str）。
            body: 通知正文（str）。
            level: 级别（默认进 payload 的 ``level`` 键）。

        Raises:
            ValueError: title / body 非法。
            ChannelError: 发送失败。
        """
        self._check_title_body(title, body)
        if self._payload_builder is None:
            payload: Dict[str, Any] = {
                "title": title,
                "content": body,
                "level": level,
                "ts": _now(),
            }
        else:
            try:
                built = self._payload_builder(title, body, level)
            except Exception as exc:
                raise ChannelError(self.name, f"payload_builder 执行异常：{exc}") from exc
            if not isinstance(built, Mapping):
                raise ChannelError(
                    self.name,
                    f"payload_builder 必须返回 dict，实际为 {type(built).__name__}",
                )
            payload = dict(built)
        status, text = self._post_json(self.url, payload, self.headers)
        if not 200 <= status < 300:
            raise ChannelError(
                self.name, f"HTTP 状态码非 2xx：{status}", status=status, detail=text
            )
