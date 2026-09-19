r"""lizysdk.notify 测试：五渠道 payload/签名/成功判定、中心语义、异步与统计。

覆盖范围（设计文档 §5，**全离线**——transport/mailer 注入 +
monkeypatch smtplib，零真实网络）：

1. 渠道 payload 逐字节断言：钉钉 markdown text 拼接、飞书 text、企微
   title 加粗、WebhookChannel 默认四键结构与 payload_builder 定制、
   headers / timeout 透传；
2. 签名：钉钉 URL 拼接（timestamp/sign）与飞书 body 签名在冻结 _now
   后与同算法重算逐字节一致、同参数确定性重发、**钉钉与飞书算法产物
   互不相同**（防串用回归钩子）；
3. 成功判定三分支：钉钉 errcode / 飞书新版 code + 旧版 StatusCode /
   企微 errcode；失败抛 ChannelError（渠道名 / 状态码 / 响应摘要），
   非 JSON 响应与网络层异常同样统一为 ChannelError；
4. EmailChannel：注入 mailer 断言 MIME（Subject / To / 正文 / 发件人）；
   默认 mailer 分支 monkeypatch smtplib 假对象验证 SMTP_SSL 与 SMTP
   两种形态的初始化 / 登录 / sendmail 序列；
5. 中心语义：级别校验参数化、路由命中 / 回退全渠道 / 显式 channels /
   notify_to、静默期跨午夜两形态（22-08 与 08-12）含边界与 except_levels
   豁免、频控窗口抑制 / 豁免 / 过期边界 / LRU 上限 / 静默期不占频控、
   同步模式返回 {渠道: bool} 不抛发送异常、构造与各类参数校验；
6. 异步：入队即返回 + flush 全送达、失败重试 N 次计 failed /
   last_error、重试后成功计 retried、单渠道失败隔离（含非 ChannelError
   异常）、flush 超时返回 False、shutdown 幂等 + 停机后拒收、atexit
   钩子排空与注册、分发线程为守护线程；
7. stats：各计数器与 per-channel 计数准确（含 quiet_suppressed /
   cooldown_suppressed）；
8. 工程红线：公开导出完整、异常族继承、logger 命名、不 import 其他
   lizysdk 子包。
"""

from __future__ import annotations

import atexit
import base64
import hashlib
import hmac
import re
import time
from email import message_from_bytes
from email.header import decode_header, make_header
from pathlib import Path
from urllib.parse import quote_plus

import pytest

import lizysdk.notify as notify_pkg
from lizysdk.notify import (
    ChannelError,
    DingTalkChannel,
    EmailChannel,
    FeishuChannel,
    NotifyCenter,
    NotifyError,
    WebhookChannel,
    WeComChannel,
)
from lizysdk.notify import center as center_mod
from lizysdk.notify import channels as channels_mod

#: 签名测试共用的密钥与冻结时间戳（秒）
SECRET = "SEC9f8e7d6c5b3a1"
FROZEN_TS = 1758268800.25


# --------------------------------------------------------------------------- #
# 测试辅助（全部离线：假 transport / 假 mailer / 假渠道）
# --------------------------------------------------------------------------- #


def freeze_time(monkeypatch: pytest.MonkeyPatch, ts: float) -> None:
    """冻结 channels 与 center 两处模块级 ``_now`` 接缝。"""
    monkeypatch.setattr(channels_mod, "_now", lambda: ts)
    monkeypatch.setattr(center_mod, "_now", lambda: ts)


def ts_at(hour: int, minute: int) -> float:
    """构造本地时间为 ``hour:minute`` 的 Unix 时间戳。

    日期固定 2026-03-15（全球主要时区均非 DST 切换日），配合
    ``time.mktime`` 使 ``time.localtime(ts)`` 的时分精确可预期，
    从而静默期测试与机器时区无关。
    """
    st = time.struct_time((2026, 3, 15, hour, minute, 0, 0, 0, -1))
    return time.mktime(st)


def _ding_sign(secret: str, ts_ms: int) -> str:
    """与实现同算法重算钉钉签名（独立复刻，作逐字节对照）。"""
    digest = hmac.new(
        secret.encode("utf-8"),
        f"{ts_ms}\n{secret}".encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return quote_plus(base64.b64encode(digest))


def _feishu_sign(secret: str, ts: int) -> str:
    """与实现同算法重算飞书签名（空消息签名，独立复刻对照）。"""
    digest = hmac.new(
        f"{ts}\n{secret}".encode("utf-8"), b"", hashlib.sha256
    ).digest()
    return base64.b64encode(digest).decode("ascii")


class FakeTransport:
    """记录调用并按脚本回复的假 transport（离线替代 urllib）。"""

    def __init__(self, replies=None, raise_exc=None):
        self.calls = []  # [(url, payload, headers, timeout), ...]
        self.replies = list(replies or [])
        self.raise_exc = raise_exc

    def __call__(self, url, payload, headers, timeout):
        self.calls.append((url, payload, headers, timeout))
        if self.raise_exc is not None:
            raise self.raise_exc
        if self.replies:
            item = self.replies.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return 200, '{"errcode": 0}'


class FakeChannel:
    """记录发送、可注入失败次数 / 延迟 / 异常类型的假渠道。"""

    def __init__(self, *, fail_times=0, delay=0.0, exc=None):
        self.sent = []  # [(title, body, level), ...]
        self.fail_times = fail_times
        self.delay = delay
        self.exc = exc if exc is not None else ChannelError("fake", "注入失败")
        self.attempts = 0

    def send(self, title, body, level="info"):
        self.attempts += 1
        if self.delay:
            time.sleep(self.delay)
        if self.attempts <= self.fail_times:
            raise self.exc
        self.sent.append((title, body, level))


# --------------------------------------------------------------------------- #
# 钉钉渠道
# --------------------------------------------------------------------------- #


class TestDingTalkChannel:
    """payload 逐字节、加签 URL、成功判定三分支。"""

    def test_payload_byte_exact_without_secret(self):
        fake = FakeTransport()
        ch = DingTalkChannel(
            "https://oapi.dingtalk.com/robot/send?access_token=tok", transport=fake
        )
        ch.send("CPU 告警", "使用率 95%", level="warning")
        url, payload, headers, timeout = fake.calls[0]
        assert url == "https://oapi.dingtalk.com/robot/send?access_token=tok"
        assert payload == {
            "msgtype": "markdown",
            "markdown": {"title": "CPU 告警", "text": "CPU 告警\n使用率 95%"},
        }
        assert payload["markdown"]["text"] == "CPU 告警" + "\n" + "使用率 95%"
        assert headers is None
        assert timeout == 10

    def test_custom_timeout_and_url_passthrough(self):
        fake = FakeTransport()
        ch = DingTalkChannel("https://oapi.dingtalk.com/x", timeout=3.5, transport=fake)
        ch.send("t", "b")
        assert fake.calls[0][3] == 3.5
        assert fake.calls[0][0] == "https://oapi.dingtalk.com/x"

    def test_signature_url_byte_exact(self, monkeypatch):
        freeze_time(monkeypatch, FROZEN_TS)
        fake = FakeTransport()
        webhook = "https://oapi.dingtalk.com/robot/send?access_token=tok"
        ch = DingTalkChannel(webhook, secret=SECRET, transport=fake)
        ch.send("t", "b")
        ts_ms = int(FROZEN_TS * 1000)
        expected = f"{webhook}&timestamp={ts_ms}&sign={_ding_sign(SECRET, ts_ms)}"
        assert fake.calls[0][0] == expected
        # URL 必含 timestamp= 与 sign= 查询段（且以 & 拼接）
        assert "&timestamp=" in fake.calls[0][0]
        assert "&sign=" in fake.calls[0][0]

    def test_signature_deterministic_under_frozen_clock(self, monkeypatch):
        freeze_time(monkeypatch, FROZEN_TS)
        fake = FakeTransport()
        ch = DingTalkChannel("https://oapi.dingtalk.com/x", secret=SECRET, transport=fake)
        ch.send("t", "b")
        ch.send("t", "b")  # 冻结时间下两次签名应完全一致
        assert fake.calls[0][0] == fake.calls[1][0]

    def test_success_errcode_zero(self):
        fake = FakeTransport(replies=[(200, '{"errcode": 0, "errmsg": "ok"}')])
        ch = DingTalkChannel("https://oapi.dingtalk.com/x", transport=fake)
        ch.send("t", "b")  # 不抛即成功

    def test_failure_errcode_nonzero_raises_channel_error(self):
        body = '{"errcode": 310000, "errmsg": "sign not match"}'
        fake = FakeTransport(replies=[(200, body)])
        ch = DingTalkChannel("https://oapi.dingtalk.com/x", transport=fake)
        with pytest.raises(ChannelError) as ei:
            ch.send("t", "b")
        exc = ei.value
        assert exc.channel == "dingtalk"
        assert exc.status == 200
        assert "310000" in str(exc)
        assert "sign not match" in str(exc)
        assert "dingtalk" in str(exc)
        assert "HTTP 200" in str(exc)

    def test_non_json_response_raises_channel_error(self):
        fake = FakeTransport(replies=[(502, "<html>Bad Gateway</html>")])
        ch = DingTalkChannel("https://oapi.dingtalk.com/x", transport=fake)
        with pytest.raises(ChannelError) as ei:
            ch.send("t", "b")
        assert ei.value.status == 502
        assert "JSON" in str(ei.value)
        assert "Bad Gateway" in ei.value.detail

    def test_transport_network_error_wrapped(self):
        fake = FakeTransport(raise_exc=OSError("connection refused"))
        ch = DingTalkChannel("https://oapi.dingtalk.com/x", transport=fake)
        with pytest.raises(ChannelError, match="connection refused"):
            ch.send("t", "b")


# --------------------------------------------------------------------------- #
# 飞书渠道
# --------------------------------------------------------------------------- #


class TestFeishuChannel:
    """payload 逐字节、空消息签名、新旧两版成功判定。"""

    def test_payload_byte_exact_without_secret(self):
        fake = FakeTransport(replies=[(200, '{"code": 0}')])
        ch = FeishuChannel("https://open.feishu.cn/open-apis/bot/v2/hook/tok", transport=fake)
        ch.send("CPU 告警", "使用率 95%")
        url, payload, headers, timeout = fake.calls[0]
        assert url == "https://open.feishu.cn/open-apis/bot/v2/hook/tok"
        assert payload == {"msg_type": "text", "content": {"text": "CPU 告警\n使用率 95%"}}
        assert headers is None
        assert timeout == 10

    def test_signature_body_byte_exact(self, monkeypatch):
        freeze_time(monkeypatch, FROZEN_TS)
        fake = FakeTransport(replies=[(200, '{"code": 0}')])
        ch = FeishuChannel("https://open.feishu.cn/x", secret=SECRET, transport=fake)
        ch.send("t", "b")
        ts = int(FROZEN_TS)  # 飞书用秒级时间戳
        payload = fake.calls[0][1]
        assert set(payload) == {"msg_type", "content", "timestamp", "sign"}
        assert payload["timestamp"] == str(ts)
        assert payload["sign"] == _feishu_sign(SECRET, ts)

    def test_success_new_style_code_zero(self):
        fake = FakeTransport(replies=[(200, '{"code": 0, "msg": "success"}')])
        ch = FeishuChannel("https://open.feishu.cn/x", transport=fake)
        ch.send("t", "b")  # 新版权：code == 0

    def test_success_old_style_status_code_zero(self):
        fake = FakeTransport(
            replies=[(200, '{"StatusCode": 0, "StatusMessage": "success", "data": {}}')]
        )
        ch = FeishuChannel("https://open.feishu.cn/x", transport=fake)
        ch.send("t", "b")  # 旧版权：无 code 键、StatusCode == 0

    def test_failure_new_style_code_nonzero(self):
        fake = FakeTransport(replies=[(200, '{"code": 19021, "msg": "sign match fail"}')])
        ch = FeishuChannel("https://open.feishu.cn/x", transport=fake)
        with pytest.raises(ChannelError) as ei:
            ch.send("t", "b")
        assert ei.value.channel == "feishu"
        assert ei.value.status == 200
        assert "19021" in str(ei.value)
        assert "feishu" in str(ei.value)

    def test_failure_old_style_status_code_nonzero(self):
        fake = FakeTransport(
            replies=[(200, '{"StatusCode": 10004, "StatusMessage": "sign match fail"}')]
        )
        ch = FeishuChannel("https://open.feishu.cn/x", transport=fake)
        with pytest.raises(ChannelError, match="10004"):
            ch.send("t", "b")

    def test_failure_no_success_marker(self):
        fake = FakeTransport(replies=[(200, '{"foo": "bar"}')])
        ch = FeishuChannel("https://open.feishu.cn/x", transport=fake)
        with pytest.raises(ChannelError, match="code"):
            ch.send("t", "b")


class TestSignatureCrossCheck:
    """钉钉 / 飞书签名算法互不相同——防串用的回归钩子（设计文档 §5）。"""

    def test_dingtalk_and_feishu_signs_differ(self, monkeypatch):
        freeze_time(monkeypatch, FROZEN_TS)
        ding_fake, feishu_fake = (
            FakeTransport(),
            FakeTransport(replies=[(200, '{"code": 0}')]),
        )
        DingTalkChannel("https://oapi.dingtalk.com/x", secret=SECRET, transport=ding_fake).send("t", "b")
        FeishuChannel("https://open.feishu.cn/x", secret=SECRET, transport=feishu_fake).send("t", "b")
        ding_url = ding_fake.calls[0][0]
        ding_sign = ding_url.split("&sign=", 1)[1]
        feishu_sign = feishu_fake.calls[0][1]["sign"]
        # 同 secret、同冻结时间，两渠道算法产物必须互不相同
        assert ding_sign != feishu_sign
        # 且各自与同算法独立重算一致
        assert ding_sign == _ding_sign(SECRET, int(FROZEN_TS * 1000))
        assert feishu_sign == _feishu_sign(SECRET, int(FROZEN_TS))


# --------------------------------------------------------------------------- #
# 企业微信渠道
# --------------------------------------------------------------------------- #


class TestWeComChannel:
    """title 加粗拼接与 errcode 判定。"""

    def test_payload_byte_exact_with_bold_title(self):
        fake = FakeTransport()
        ch = WeComChannel("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=tok", transport=fake)
        ch.send("CPU 告警", "使用率 95%")
        url, payload, headers, timeout = fake.calls[0]
        assert url == "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=tok"
        assert payload == {"msgtype": "markdown", "markdown": {"content": "**CPU 告警**\n使用率 95%"}}
        assert headers is None
        assert timeout == 10

    def test_success_errcode_zero(self):
        fake = FakeTransport(replies=[(200, '{"errcode": 0, "errmsg": "ok"}')])
        ch = WeComChannel("https://qyapi.weixin.qq.com/x", transport=fake)
        ch.send("t", "b")

    def test_failure_errcode_nonzero(self):
        fake = FakeTransport(replies=[(200, '{"errcode": 40001, "errmsg": "invalid credential"}')])
        ch = WeComChannel("https://qyapi.weixin.qq.com/x", transport=fake)
        with pytest.raises(ChannelError) as ei:
            ch.send("t", "b")
        assert ei.value.channel == "wecom"
        assert ei.value.status == 200
        assert "40001" in str(ei.value)
        assert "invalid credential" in str(ei.value)


# --------------------------------------------------------------------------- #
# 自定义 webhook 渠道
# --------------------------------------------------------------------------- #


class TestWebhookChannel:
    """默认结构、payload_builder 定制、headers 透传、2xx 判定。"""

    def test_default_payload_structure(self, monkeypatch):
        freeze_time(monkeypatch, 1750000000.5)
        fake = FakeTransport()
        ch = WebhookChannel("https://hooks.example.com/x", transport=fake)
        ch.send("标题", "内容", level="warning")
        url, payload, headers, timeout = fake.calls[0]
        assert payload == {
            "title": "标题",
            "content": "内容",
            "level": "warning",
            "ts": 1750000000.5,
        }
        assert set(payload) == {"title", "content", "level", "ts"}
        assert url == "https://hooks.example.com/x"
        assert headers is None
        assert timeout == 10

    def test_payload_builder_custom_structure_and_headers(self):
        fake = FakeTransport()
        ch = WebhookChannel(
            "https://hooks.example.com/x",
            headers={"X-Token": "tok", "Content-Type": "text/plain"},
            payload_builder=lambda t, b, lv: {"event": t, "text": b, "sev": lv},
            transport=fake,
        )
        ch.send("t", "b", level="error")
        assert fake.calls[0][1] == {"event": "t", "text": "b", "sev": "error"}
        # 注入 transport 收到的正是用户 headers（合并 Content-Type 是默认
        # transport 的职责）
        assert fake.calls[0][2] == {"X-Token": "tok", "Content-Type": "text/plain"}

    @pytest.mark.parametrize("status", [200, 201, 204])
    def test_success_any_2xx(self, status):
        fake = FakeTransport(replies=[(status, "ok")])
        ch = WebhookChannel("https://hooks.example.com/x", transport=fake)
        ch.send("t", "b")  # 2xx 即成功

    @pytest.mark.parametrize("status", [302, 404, 500, 503])
    def test_failure_non_2xx(self, status):
        fake = FakeTransport(replies=[(status, "nope")])
        ch = WebhookChannel("https://hooks.example.com/x", transport=fake)
        with pytest.raises(ChannelError) as ei:
            ch.send("t", "b")
        assert ei.value.status == status
        assert "2xx" in str(ei.value)
        assert ei.value.detail == "nope"

    def test_payload_builder_non_dict_result(self):
        fake = FakeTransport()
        ch = WebhookChannel(
            "https://hooks.example.com/x",
            payload_builder=lambda t, b, lv: [t],
            transport=fake,
        )
        with pytest.raises(ChannelError, match="payload_builder"):
            ch.send("t", "b")

    def test_payload_builder_exception_wrapped(self):
        fake = FakeTransport()
        ch = WebhookChannel(
            "https://hooks.example.com/x",
            payload_builder=lambda t, b, lv: (_ for _ in ()).throw(RuntimeError("boom")),
            transport=fake,
        )
        with pytest.raises(ChannelError, match="boom"):
            ch.send("t", "b")


# --------------------------------------------------------------------------- #
# 邮件渠道
# --------------------------------------------------------------------------- #


class TestEmailChannel:
    """MIME 构造（注入 mailer）与默认 mailer 的 smtplib 序列。"""

    def test_mime_via_injected_mailer(self):
        box = []
        ch = EmailChannel(
            "smtp.x.com",
            user="bot@x.com",
            password="pw",
            to=["a@x.com", "b@x.com"],
            mailer=lambda raw, sender, to: box.append((raw, sender, to)),
        )
        ch.send("订单告警", "SO-001 扣减失败", level="critical")
        assert len(box) == 1
        raw, sender, to = box[0]
        assert sender == "bot@x.com"  # 默认发件人 = 登录用户
        assert to == ["a@x.com", "b@x.com"]
        msg = message_from_bytes(raw)
        assert str(make_header(decode_header(msg["Subject"]))) == "订单告警"
        assert msg.get_payload(decode=True).decode("utf-8") == "SO-001 扣减失败"
        assert msg["From"] == "bot@x.com"
        assert msg["To"] == "a@x.com, b@x.com"
        assert msg.get_content_type() == "text/plain"
        assert msg.get_content_charset() == "utf-8"

    def test_single_recipient_str_and_custom_sender(self):
        box = []
        ch = EmailChannel(
            "smtp.x.com",
            user="bot@x.com",
            password="pw",
            to="only@x.com",
            sender="noreply@x.com",
            mailer=lambda raw, sender, to: box.append((raw, sender, to)),
        )
        ch.send("标题", "正文")
        raw, sender, to = box[0]
        assert sender == "noreply@x.com"
        assert to == ["only@x.com"]  # str 收件人归一化为列表
        assert message_from_bytes(raw)["To"] == "only@x.com"

    def test_mailer_exception_wrapped_as_channel_error(self):
        def boom(raw, sender, to):
            raise Exception("auth failed")

        ch = EmailChannel(
            "smtp.x.com", user="bot@x.com", password="pw", to=["a@x.com"], mailer=boom
        )
        with pytest.raises(ChannelError) as ei:
            ch.send("t", "b")
        assert ei.value.channel == "email"
        assert ei.value.status is None
        assert "auth failed" in str(ei.value)

    def test_default_mailer_ssl_sequence(self, monkeypatch):
        events = []

        class FakeSMTP_SSL:
            def __init__(self, host, port, timeout=None):
                events.append(("ssl_init", host, port, timeout))

            def __enter__(self):
                events.append(("enter",))
                return self

            def __exit__(self, *exc):
                events.append(("exit",))
                return False

            def login(self, user, password):
                events.append(("login", user, password))

            def sendmail(self, sender, to, msg_bytes):
                events.append(("sendmail", sender, list(to), msg_bytes))

        class FakeSMTP:
            def __init__(self, *args, **kwargs):
                events.append(("plain_init", args, kwargs))

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def login(self, *args):
                pass

            def sendmail(self, *args):
                pass

        monkeypatch.setattr(channels_mod.smtplib, "SMTP_SSL", FakeSMTP_SSL)
        monkeypatch.setattr(channels_mod.smtplib, "SMTP", FakeSMTP)
        ch = EmailChannel(
            "smtp.x.com", 465, user="bot@x.com", password="pw",
            to=["a@x.com"], timeout=7.5,
        )
        ch.send("磁盘告警", "data 盘使用率 91%")
        # 序列：SSL 初始化 → 进入上下文 → 登录 → sendmail → 退出上下文
        assert [e[0] for e in events] == ["ssl_init", "enter", "login", "sendmail", "exit"]
        assert events[0] == ("ssl_init", "smtp.x.com", 465, 7.5)
        assert events[2] == ("login", "bot@x.com", "pw")
        # sendmail 收到的消息体是合法 MIME（Subject / 收件人正确）
        raw = events[3][3]
        msg = message_from_bytes(raw)
        assert str(make_header(decode_header(msg["Subject"]))) == "磁盘告警"
        assert events[3][1] == "bot@x.com"
        assert events[3][2] == ["a@x.com"]

    def test_default_mailer_plain_smtp_branch(self, monkeypatch):
        events = []

        class FakeSMTP:
            def __init__(self, host, port, timeout=None):
                events.append(("plain_init", host, port, timeout))

            def __enter__(self):
                events.append(("enter",))
                return self

            def __exit__(self, *exc):
                events.append(("exit",))
                return False

            def login(self, user, password):
                events.append(("login", user, password))

            def sendmail(self, sender, to, msg_bytes):
                events.append(("sendmail", sender, list(to), msg_bytes))

        class FakeSMTP_SSL:
            def __init__(self, *args, **kwargs):
                events.append(("ssl_init", args, kwargs))

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def login(self, *args):
                pass

            def sendmail(self, *args):
                pass

        monkeypatch.setattr(channels_mod.smtplib, "SMTP_SSL", FakeSMTP_SSL)
        monkeypatch.setattr(channels_mod.smtplib, "SMTP", FakeSMTP)
        ch = EmailChannel(
            "smtp.x.com", 587, user="bot@x.com", password="pw",
            to=["a@x.com"], use_ssl=False,
        )
        ch.send("标题", "正文")
        kinds = [e[0] for e in events]
        # 非 SSL 形态：只走 smtplib.SMTP，绝不触碰 SMTP_SSL
        assert kinds == ["plain_init", "enter", "login", "sendmail", "exit"]
        assert events[0] == ("plain_init", "smtp.x.com", 587, 10.0)


# --------------------------------------------------------------------------- #
# 渠道参数校验
# --------------------------------------------------------------------------- #


class TestChannelValidation:
    """渠道构造参数与 send 的 title/body 校验（统一 ValueError）。"""

    @pytest.mark.parametrize(
        "factory",
        [
            lambda: DingTalkChannel("ftp://x"),
            lambda: DingTalkChannel(""),
            lambda: DingTalkChannel("https://x", timeout=0),
            lambda: DingTalkChannel("https://x", timeout=-1),
            lambda: DingTalkChannel("https://x", transport=123),
            lambda: DingTalkChannel("https://x", secret=""),
            lambda: FeishuChannel("notaurl"),
            lambda: FeishuChannel("https://x", secret=123),
            lambda: WeComChannel("https://x", timeout="slow"),
            lambda: EmailChannel("smtp.x.com", user="u", password="p", to=[]),
            lambda: EmailChannel("smtp.x.com", user="u", password="p", to=""),
            lambda: EmailChannel("smtp.x.com", user="u", password="p", to=[""]),
            lambda: EmailChannel("smtp.x.com", user="u", password="p", to=123),
            lambda: EmailChannel("smtp.x.com", 0, user="u", password="p", to=["a@x"]),
            lambda: EmailChannel("smtp.x.com", 70000, user="u", password="p", to=["a@x"]),
            lambda: EmailChannel("", user="u", password="p", to=["a@x"]),
            lambda: EmailChannel("smtp.x.com", user="", password="p", to=["a@x"]),
            lambda: EmailChannel("smtp.x.com", user="u", password="p", to=["a@x"], use_ssl="yes"),
            lambda: EmailChannel("smtp.x.com", user="u", password="p", to=["a@x"], mailer="x"),
            lambda: EmailChannel("smtp.x.com", user="u", password="p", to=["a@x"], timeout=0),
            lambda: WebhookChannel("https://x", headers=["a"]),
            lambda: WebhookChannel("https://x", headers={"": "v"}),
            lambda: WebhookChannel("https://x", payload_builder="x"),
            lambda: WebhookChannel("x"),
        ],
        ids=lambda f: "",
    )
    def test_ctor_invalid_params_raise_value_error(self, factory):
        with pytest.raises(ValueError):
            factory()

    def test_send_title_body_validation(self):
        ch = DingTalkChannel("https://x", transport=FakeTransport())
        with pytest.raises(ValueError, match="title"):
            ch.send("", "b")
        with pytest.raises(ValueError, match="title"):
            ch.send("  ", "b")
        with pytest.raises(ValueError, match="body"):
            ch.send("t", 123)
        # 空正文允许（title 非空即可）
        ch.send("t", "")


# --------------------------------------------------------------------------- #
# 中心：参数校验与注册
# --------------------------------------------------------------------------- #


class TestCenterValidation:
    """级别 / 标题 / 构造参数 / add_channel / route / 显式 channels 校验。"""

    @pytest.mark.parametrize(
        "bad_level",
        ["debug", "INFO", "fatal", "", " info", "critical ", None, 1, b"critical"],
    )
    def test_notify_level_validation(self, bad_level):
        center = NotifyCenter(async_send=False)
        center.add_channel("c", FakeChannel())
        with pytest.raises(ValueError, match="level"):
            center.notify("t", "b", level=bad_level)

    @pytest.mark.parametrize(
        "bad_level",
        ["warn", "CRITICAL", "", None, 2],
    )
    def test_notify_to_level_validation(self, bad_level):
        center = NotifyCenter(async_send=False)
        center.add_channel("c", FakeChannel())
        with pytest.raises(ValueError, match="level"):
            center.notify_to("c", "t", "b", level=bad_level)

    def test_title_body_validation(self):
        center = NotifyCenter(async_send=False)
        center.add_channel("c", FakeChannel())
        with pytest.raises(ValueError, match="title"):
            center.notify("", "b")
        with pytest.raises(ValueError, match="title"):
            center.notify("   ", "b")
        with pytest.raises(ValueError, match="title"):
            center.notify(None, "b")
        with pytest.raises(ValueError, match="body"):
            center.notify("t", 123)

    def test_ctor_params_validation(self):
        with pytest.raises(ValueError, match="retry"):
            NotifyCenter(retry=-1)
        with pytest.raises(ValueError, match="retry"):
            NotifyCenter(retry=1.5)
        with pytest.raises(ValueError, match="retry"):
            NotifyCenter(retry=True)
        with pytest.raises(ValueError, match="retry_backoff"):
            NotifyCenter(retry_backoff=-0.1)
        with pytest.raises(ValueError, match="retry_backoff"):
            NotifyCenter(retry_backoff="fast")
        with pytest.raises(ValueError, match="async_send"):
            NotifyCenter(async_send="yes")

    def test_add_channel_validation(self):
        center = NotifyCenter(async_send=False)
        with pytest.raises(ValueError, match="name"):
            center.add_channel("", FakeChannel())
        center.add_channel("c", FakeChannel())
        with pytest.raises(ValueError, match="已注册"):
            center.add_channel("c", FakeChannel())
        with pytest.raises(ValueError, match="send"):
            center.add_channel("bad", object())

    def test_route_validation(self):
        center = NotifyCenter(async_send=False)
        center.add_channel("c1", FakeChannel())
        with pytest.raises(ValueError, match="level"):
            center.route("debug", channels=["c1"])
        with pytest.raises(ValueError, match="未注册"):
            center.route("error", channels=["nope"])
        with pytest.raises(ValueError, match="不能为空"):
            center.route("error", channels=[])
        with pytest.raises(ValueError, match="单个字符串"):
            center.route("error", channels="c1")

    def test_notify_channels_param_validation(self):
        center = NotifyCenter(async_send=False)
        center.add_channel("c1", FakeChannel())
        with pytest.raises(ValueError, match="未注册"):
            center.notify("t", "b", channels=["nope"])
        with pytest.raises(ValueError, match="不能为空"):
            center.notify("t", "b", channels=[])
        with pytest.raises(ValueError, match="单个字符串"):
            center.notify("t", "b", channels="c1")

    def test_notify_to_unknown_channel(self):
        center = NotifyCenter(async_send=False)
        center.add_channel("c", FakeChannel())
        with pytest.raises(ValueError, match="未注册"):
            center.notify_to("nope", "t", "b")

    def test_flush_timeout_validation(self):
        center = NotifyCenter(async_send=False)
        with pytest.raises(ValueError, match="timeout"):
            center.flush(timeout=0)
        with pytest.raises(ValueError, match="timeout"):
            center.flush(timeout=-1)


# --------------------------------------------------------------------------- #
# 中心：路由语义
# --------------------------------------------------------------------------- #


class TestRouting:
    """路由命中 / 未命中回退全渠道 / 显式覆盖 / notify_to。"""

    def _center(self):
        center = NotifyCenter(async_send=False)
        c1, c2 = FakeChannel(), FakeChannel()
        center.add_channel("c1", c1)
        center.add_channel("c2", c2)
        return center, c1, c2

    def test_route_hit_targets_only_routed_channels(self):
        center, c1, c2 = self._center()
        center.route("error", channels=["c1"])
        assert center.notify("t", "b", level="error") == {"c1": True}
        assert c1.sent == [("t", "b", "error")]
        assert c2.sent == []

    def test_route_miss_falls_back_to_all_channels(self):
        center, c1, c2 = self._center()
        center.route("error", channels=["c1"])
        # info 级别无路由 → 发全部渠道
        assert center.notify("t", "b", level="info") == {"c1": True, "c2": True}
        assert len(c1.sent) == 1 and len(c2.sent) == 1

    def test_explicit_channels_override_route(self):
        center, c1, c2 = self._center()
        center.route("error", channels=["c1"])
        assert center.notify("t", "b", level="error", channels=["c2"]) == {"c2": True}
        assert c1.sent == [] and c2.sent == [("t", "b", "error")]

    def test_notify_to_targets_single_channel(self):
        center, c1, c2 = self._center()
        assert center.notify_to("c2", "t", "b") == {"c2": True}
        assert c1.sent == []
        assert c2.sent == [("t", "b", "info")]

    def test_route_overwrite_by_second_call(self):
        center, c1, c2 = self._center()
        center.route("error", channels=["c1"])
        center.route("error", channels=["c2"])  # 覆盖旧路由
        assert center.notify("t", "b", level="error") == {"c2": True}
        assert c1.sent == [] and c2.sent == [("t", "b", "error")]


# --------------------------------------------------------------------------- #
# 中心：同步模式与 stats
# --------------------------------------------------------------------------- #


class TestSyncMode:
    """同步模式返回 {渠道: bool}、不抛发送异常、stats 计数准确。"""

    def test_sync_returns_dict_and_swallows_exceptions(self):
        center = NotifyCenter(async_send=False, retry=0, retry_backoff=0.0)
        ok = FakeChannel()
        bad = FakeChannel(fail_times=10**9)  # 恒失败（ChannelError）
        ugly = FakeChannel(fail_times=10**9, exc=RuntimeError("任意异常"))
        center.add_channel("ok", ok)
        center.add_channel("bad", bad)
        center.add_channel("ugly", ugly)
        # 不抛发送异常（含非 ChannelError），失败渠道值为 False
        assert center.notify("t", "b") == {"ok": True, "bad": False, "ugly": False}
        st = center.stats()
        assert st["sent"] == 1
        assert st["failed"] == 2
        assert st["last_error"] is not None and "ugly" in st["last_error"]
        assert st["channels"] == {
            "ok": {"sent": 1, "failed": 0},
            "bad": {"sent": 0, "failed": 1},
            "ugly": {"sent": 0, "failed": 1},
        }

    def test_sync_retry_then_success(self):
        center = NotifyCenter(async_send=False, retry=2, retry_backoff=0.0)
        flaky = FakeChannel(fail_times=1)
        center.add_channel("flaky", flaky)
        assert center.notify("t", "b") == {"flaky": True}
        st = center.stats()
        assert st["sent"] == 1 and st["failed"] == 0
        assert st["retried"] == 1
        assert flaky.attempts == 2
        # last_error 记录最近一次失败（含重试中途失败），不因成功清除
        assert st["last_error"] is not None

    def test_stats_keys_exact_shape(self, monkeypatch):
        center = NotifyCenter(async_send=False, retry=0, retry_backoff=0.0)
        ok = FakeChannel()
        bad = FakeChannel(fail_times=10**9)
        center.add_channel("ok", ok)
        center.add_channel("bad", bad)
        center.set_cooldown(600)
        freeze_time(monkeypatch, 1000.0)
        assert center.notify("t1", "b") == {"ok": True, "bad": False}
        assert center.notify("t1", "b") == {}  # 频控抑制
        st = center.stats()
        assert set(st) == {
            "sent", "failed", "retried",
            "quiet_suppressed", "cooldown_suppressed",
            "last_error", "channels",
        }
        assert st["sent"] == 1 and st["failed"] == 1
        assert st["retried"] == 0
        assert st["quiet_suppressed"] == 0 and st["cooldown_suppressed"] == 1
        assert st["channels"]["ok"] == {"sent": 1, "failed": 0}
        assert st["channels"]["bad"] == {"sent": 0, "failed": 1}

    def test_stats_includes_zero_count_channels(self):
        center = NotifyCenter(async_send=False)
        center.add_channel("idle", FakeChannel())
        assert center.stats()["channels"] == {"idle": {"sent": 0, "failed": 0}}

    def test_sync_flush_is_trivially_true(self):
        center = NotifyCenter(async_send=False)
        center.add_channel("c", FakeChannel())
        center.notify("t", "b")
        assert center.flush() is True


# --------------------------------------------------------------------------- #
# 中心：静默期
# --------------------------------------------------------------------------- #


class TestQuietHours:
    """静默期两形态（跨午夜 / 当日）含边界、豁免、入队前抑制。"""

    def _center(self):
        center = NotifyCenter(async_send=False)
        ch = FakeChannel()
        center.add_channel("c", ch)
        return center, ch

    @pytest.mark.parametrize(
        "hour, minute, suppressed",
        [
            (22, 0, True),   # 起点含
            (23, 30, True),
            (0, 5, True),    # 次日凌晨
            (7, 59, True),
            (8, 0, False),   # 终点不含
            (12, 0, False),
            (21, 59, False),
        ],
    )
    def test_cross_midnight_window_22_to_08(self, monkeypatch, hour, minute, suppressed):
        center, ch = self._center()
        center.set_quiet_hours("22:00", "08:00")
        freeze_time(monkeypatch, ts_at(hour, minute))
        result = center.notify("标题", "内容", level="warning")
        if suppressed:
            assert result == {}
            assert ch.sent == []
            assert center.stats()["quiet_suppressed"] == 1
        else:
            assert result == {"c": True}
            assert ch.sent == [("标题", "内容", "warning")]
            assert center.stats()["quiet_suppressed"] == 0

    @pytest.mark.parametrize(
        "hour, minute, suppressed",
        [
            (8, 0, True),    # 起点含
            (9, 30, True),
            (11, 59, True),
            (12, 0, False),  # 终点不含
            (7, 59, False),
            (12, 1, False),
        ],
    )
    def test_same_day_window_08_to_12(self, monkeypatch, hour, minute, suppressed):
        center, ch = self._center()
        center.set_quiet_hours("08:00", "12:00")
        freeze_time(monkeypatch, ts_at(hour, minute))
        result = center.notify("标题", "内容")
        if suppressed:
            assert result == {} and ch.sent == []
            assert center.stats()["quiet_suppressed"] == 1
        else:
            assert result == {"c": True} and len(ch.sent) == 1

    def test_except_levels_exempt(self, monkeypatch):
        center, ch = self._center()
        center.set_quiet_hours("22:00", "08:00", except_levels=("critical",))
        freeze_time(monkeypatch, ts_at(23, 30))
        # critical 豁免：正常发送
        assert center.notify("紧急", "b", level="critical") == {"c": True}
        # warning 不豁免：被抑制
        assert center.notify("普通", "b", level="warning") == {}
        st = center.stats()
        assert st["quiet_suppressed"] == 1
        assert ch.sent == [("紧急", "b", "critical")]

    def test_quiet_suppresses_notify_to_as_well(self, monkeypatch):
        center, ch = self._center()
        center.set_quiet_hours("22:00", "08:00")
        freeze_time(monkeypatch, ts_at(23, 30))
        assert center.notify_to("c", "标题", "内容") == {}
        assert ch.sent == []
        assert center.stats()["quiet_suppressed"] == 1

    def test_async_suppression_skips_enqueue(self, monkeypatch):
        center = NotifyCenter(async_send=True)
        ch = FakeChannel()
        center.add_channel("c", ch)
        center.set_quiet_hours("22:00", "08:00")
        freeze_time(monkeypatch, ts_at(23, 30))
        assert center.notify("t", "b") is None
        # 入队前抑制：即时计数、队列恒空、分发线程从未启动
        assert center.stats()["quiet_suppressed"] == 1
        assert center.flush() is True
        assert center.stats()["sent"] == 0
        assert center._worker is None
        center.shutdown()

    @pytest.mark.parametrize(
        "start, end",
        [
            ("9:00", "10:00"),     # 非 HH:MM 补零格式
            ("24:00", "08:00"),    # 小时越界
            ("08:60", "09:00"),    # 分钟越界
            ("8:00", "9:00"),
            ("abc", "08:00"),
            ("08:00:00", "09:00"),
            ("", "08:00"),
            (800, "08:00"),
            ("08:00", "08:00"),    # 起止相同
        ],
    )
    def test_set_quiet_hours_validation(self, start, end):
        center = NotifyCenter(async_send=False)
        with pytest.raises(ValueError):
            center.set_quiet_hours(start, end)

    def test_set_quiet_hours_except_levels_validation(self):
        center = NotifyCenter(async_send=False)
        with pytest.raises(ValueError, match="except_levels"):
            center.set_quiet_hours("22:00", "08:00", except_levels="critical")
        with pytest.raises(ValueError, match="except_levels"):
            center.set_quiet_hours("22:00", "08:00", except_levels=("fatal",))


# --------------------------------------------------------------------------- #
# 中心：频控
# --------------------------------------------------------------------------- #


class TestCooldown:
    """同标题窗口抑制、豁免、边界过期、LRU 上限、与静默期的次序。"""

    def _center(self):
        center = NotifyCenter(async_send=False)
        ch = FakeChannel()
        center.add_channel("c", ch)
        return center, ch

    def test_same_title_suppressed_different_title_passes(self, monkeypatch):
        center, ch = self._center()
        center.set_cooldown(600)
        freeze_time(monkeypatch, 1000.0)
        assert center.notify("同题", "a") == {"c": True}
        assert center.notify("同题", "b") == {}  # 窗口内第二次被抑制
        assert center.notify("异题", "c") == {"c": True}
        st = center.stats()
        assert st["cooldown_suppressed"] == 1
        assert st["sent"] == 2
        assert ch.sent == [("同题", "a", "info"), ("异题", "c", "info")]

    @pytest.mark.parametrize(
        "delta, suppressed",
        [(1.0, True), (300.0, True), (599.9, True), (600.0, False), (900.0, False)],
    )
    def test_window_expiry_boundaries(self, monkeypatch, delta, suppressed):
        center, ch = self._center()
        center.set_cooldown(600)
        freeze_time(monkeypatch, 1000.0)
        center.notify("t", "b")
        freeze_time(monkeypatch, 1000.0 + delta)
        result = center.notify("t", "b")
        if suppressed:
            assert result == {} and len(ch.sent) == 1
        else:
            assert result == {"c": True} and len(ch.sent) == 2

    def test_except_levels_exempt(self, monkeypatch):
        center, ch = self._center()
        center.set_cooldown(600, except_levels=("critical",))
        freeze_time(monkeypatch, 1000.0)
        for _ in range(3):
            assert center.notify("紧急", "b", level="critical") == {"c": True}
        assert center.notify("普通", "b") == {"c": True}
        assert center.notify("普通", "b") == {}  # 非豁免级别仍受频控
        st = center.stats()
        assert st["cooldown_suppressed"] == 1
        assert st["sent"] == 4

    def test_lru_cap_1024(self, monkeypatch):
        center, ch = self._center()
        center.set_cooldown(10**9)  # 窗口极大：只有 LRU 淘汰能让旧标题再次通过
        freeze_time(monkeypatch, 1000.0)
        for i in range(1025):
            assert center.notify(f"t{i}", "b") == {"c": True}
        # 记录上限：1025 条记录只保留最近 1024 条
        assert len(center._cooldown_seen) == 1024
        # t1 仍在缓存内 → 抑制（证明上限没有清掉全部记录）
        assert center.notify("t1", "b") == {}
        # t0（最早）已被 LRU 淘汰 → 放行，且记录后总数仍封顶
        assert center.notify("t0", "b") == {"c": True}
        assert len(center._cooldown_seen) == 1024

    def test_quiet_suppression_does_not_consume_cooldown(self, monkeypatch):
        center, ch = self._center()
        center.set_quiet_hours("22:00", "08:00")
        center.set_cooldown(10**9)  # 若静默期误记频控，出窗后必被抑制
        freeze_time(monkeypatch, ts_at(23, 30))
        assert center.notify("t", "b") == {}  # 静默期抑制
        freeze_time(monkeypatch, ts_at(12, 0))  # 出静默期
        assert center.notify("t", "b") == {"c": True}  # 未被频控拦截
        st = center.stats()
        assert st["quiet_suppressed"] == 1
        assert st["cooldown_suppressed"] == 0

    @pytest.mark.parametrize("bad_window", [0, -5, "x", None])
    def test_set_cooldown_validation(self, bad_window):
        center = NotifyCenter(async_send=False)
        with pytest.raises(ValueError):
            center.set_cooldown(bad_window)

    def test_set_cooldown_except_levels_validation(self):
        center = NotifyCenter(async_send=False)
        with pytest.raises(ValueError, match="except_levels"):
            center.set_cooldown(600, except_levels="critical")
        with pytest.raises(ValueError, match="except_levels"):
            center.set_cooldown(600, except_levels=("warn",))


# --------------------------------------------------------------------------- #
# 中心：异步分发 / 重试 / 停机
# --------------------------------------------------------------------------- #


class TestAsyncMode:
    """入队即返回、flush 送达、重试计数、失败隔离、停机与 atexit。"""

    def test_enqueue_returns_immediately_then_flush_delivers(self):
        center = NotifyCenter(async_send=True)
        slow = FakeChannel(delay=0.3)
        center.add_channel("slow", slow)
        started = time.perf_counter()
        assert center.notify("t", "b") is None  # 入队即返回
        assert time.perf_counter() - started < 0.2
        assert center.flush(timeout=5) is True
        assert slow.sent == [("t", "b", "info")]
        center.shutdown()

    def test_flush_delivers_all_channels_and_messages(self):
        center = NotifyCenter(async_send=True)
        a, b = FakeChannel(), FakeChannel()
        center.add_channel("a", a)
        center.add_channel("b", b)
        center.notify("t1", "b1", level="error")
        center.notify("t2", "b2")
        assert center.flush(timeout=5) is True
        assert sorted(a.sent) == [("t1", "b1", "error"), ("t2", "b2", "info")]
        assert b.sent == a.sent
        st = center.stats()
        assert st["sent"] == 4 and st["failed"] == 0
        assert st["channels"] == {"a": {"sent": 2, "failed": 0}, "b": {"sent": 2, "failed": 0}}
        center.shutdown()

    def test_retry_n_times_then_failed(self):
        center = NotifyCenter(async_send=True, retry=2, retry_backoff=0.0)
        bad = FakeChannel(fail_times=10**9)
        center.add_channel("bad", bad)
        center.notify("t", "b")
        assert center.flush(timeout=5) is True
        st = center.stats()
        assert st["failed"] == 1
        assert st["retried"] == 2  # 重试 2 次（共 3 次尝试）
        assert st["sent"] == 0
        assert bad.attempts == 3
        assert st["last_error"] is not None and "bad" in st["last_error"]
        assert st["channels"]["bad"] == {"sent": 0, "failed": 1}
        center.shutdown()

    def test_retry_then_success_counts_retried(self):
        center = NotifyCenter(async_send=True, retry=2, retry_backoff=0.0)
        flaky = FakeChannel(fail_times=1)
        center.add_channel("flaky", flaky)
        center.notify("t", "b")
        assert center.flush(timeout=5) is True
        st = center.stats()
        assert st["sent"] == 1 and st["failed"] == 0 and st["retried"] == 1
        assert flaky.attempts == 2
        center.shutdown()

    def test_single_channel_failure_does_not_affect_others(self):
        center = NotifyCenter(async_send=True, retry=0, retry_backoff=0.0)
        bad = FakeChannel(fail_times=10**9)  # ChannelError 恒败
        ugly = FakeChannel(fail_times=10**9, exc=RuntimeError("任意异常也不击穿"))
        good = FakeChannel()
        center.add_channel("bad", bad)
        center.add_channel("ugly", ugly)
        center.add_channel("good", good)
        center.notify("t1", "b")
        center.notify("t2", "b")  # 后续通知不受前面失败影响
        assert center.flush(timeout=5) is True
        st = center.stats()
        assert st["sent"] == 2
        assert st["failed"] == 4
        assert sorted(m[0] for m in good.sent) == ["t1", "t2"]
        assert st["channels"]["good"] == {"sent": 2, "failed": 0}
        assert st["channels"]["bad"] == {"sent": 0, "failed": 2}
        center.shutdown()

    def test_flush_timeout_returns_false_while_busy(self):
        center = NotifyCenter(async_send=True, retry=0, retry_backoff=0.0)
        slow = FakeChannel(delay=0.4)
        center.add_channel("slow", slow)
        center.notify("t", "b")
        assert center.flush(timeout=0.05) is False  # 在途未完成
        assert center.flush(timeout=5) is True      # 之后排空
        assert center.stats()["sent"] == 1
        center.shutdown()

    def test_worker_thread_is_daemon(self):
        center = NotifyCenter(async_send=True, retry=0, retry_backoff=0.0)
        center.add_channel("c", FakeChannel())
        center.notify("t", "b")
        assert center.flush(timeout=5) is True
        assert center._worker is not None
        assert center._worker.daemon is True  # 守护线程：不阻塞进程退出
        center.shutdown()

    def test_shutdown_idempotent_and_rejects_after(self):
        center = NotifyCenter(async_send=True, retry=0, retry_backoff=0.0)
        center.add_channel("c", FakeChannel())
        center.notify("t", "b")
        assert center.flush(timeout=5) is True
        center.shutdown()
        center.shutdown()  # 幂等，不抛
        with pytest.raises(NotifyError, match="停机"):
            center.notify("t2", "b")
        with pytest.raises(NotifyError, match="停机"):
            center.notify_to("c", "t3", "b")

    def test_sync_center_shutdown_rejects_too(self):
        center = NotifyCenter(async_send=False)
        center.add_channel("c", FakeChannel())
        center.shutdown()
        with pytest.raises(NotifyError, match="停机"):
            center.notify("t", "b")

    def test_atexit_hook_drains_queue(self):
        center = NotifyCenter(async_send=True, retry=0, retry_backoff=0.0)
        ch = FakeChannel()
        center.add_channel("c", ch)
        center.notify("t", "b")
        center._atexit_flush()  # 模拟进程退出时触发的 atexit 钩子
        assert center.stats()["sent"] == 1
        assert ch.sent == [("t", "b", "info")]
        assert center._closed is True
        center._atexit_flush()  # 重复调用安全（内部 flush + 幂等 shutdown）

    def test_atexit_registered_only_for_async_center(self, monkeypatch):
        registered = []
        monkeypatch.setattr(
            atexit, "register", lambda fn: registered.append(fn) or fn
        )
        async_center = NotifyCenter(async_send=True)
        assert any(fn == async_center._atexit_flush for fn in registered)
        registered.clear()
        NotifyCenter(async_send=False)  # 同步模式无在途队列，不注册
        assert registered == []


# --------------------------------------------------------------------------- #
# 导出与工程红线
# --------------------------------------------------------------------------- #


class TestExportsAndRedlines:
    """公开导出、异常族、logger 命名、子包隔离红线。"""

    def test_public_exports(self):
        expected = {
            "NotifyCenter",
            "DingTalkChannel",
            "FeishuChannel",
            "WeComChannel",
            "EmailChannel",
            "WebhookChannel",
            "NotifyError",
            "ChannelError",
        }
        assert set(notify_pkg.__all__) == expected
        for name in expected:
            assert hasattr(notify_pkg, name)

    def test_exception_family(self):
        assert issubclass(NotifyError, Exception)
        assert issubclass(ChannelError, NotifyError)

    def test_channel_name_attributes(self):
        assert DingTalkChannel("https://x", transport=FakeTransport()).name == "dingtalk"
        assert FeishuChannel("https://x", transport=FakeTransport()).name == "feishu"
        assert WeComChannel("https://x", transport=FakeTransport()).name == "wecom"
        assert (
            EmailChannel(
                "s", user="u", password="p", to=["a@x"], mailer=lambda *a: None
            ).name
            == "email"
        )
        assert WebhookChannel("https://x", transport=FakeTransport()).name == "webhook"

    def test_logger_names(self):
        # 子包日志统一走 "lizysdk.notify" 通道（被 logs 子包根 handler 捕获）
        assert channels_mod.logger.name == "lizysdk.notify"
        assert center_mod.logger.name == "lizysdk.notify"

    def test_no_import_of_other_lizysdk_subpackages(self):
        # 红线：notify 子包禁止 import 其他 lizysdk 子包（相对导入除外）。
        # 只匹配模块顶层（列 0）的绝对导入语句，docstring 里缩进的示例代码不算。
        pkg_dir = Path(notify_pkg.__file__).resolve().parent
        pattern = re.compile(r"^(?:from|import)\s+lizysdk[\.\s]", re.M)
        for py in sorted(pkg_dir.glob("*.py")):
            source = py.read_text(encoding="utf-8")
            assert not pattern.search(source), (
                f"{py.name} 违反子包隔离红线：出现对 lizysdk 的绝对导入"
            )
