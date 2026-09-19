r"""lizysdk.notify —— 通知中心子模块（多渠道通知，零依赖、纯标准库）。

统一 ``notify()`` 多渠道扇出（钉钉 / 飞书 / 企业微信 / 邮件 / 自定义
webhook）+ 级别路由、静默期（跨午夜）、同标题频控（LRU 上限 1024）、
异步投递与指数退避重试、统计——设计契约见 ``docs/notify-design.md``。

公开 API：

- :class:`NotifyCenter` —— 通知中心：路由 / 静默期 / 频控 / 异步分发 / 统计
- :class:`DingTalkChannel` / :class:`FeishuChannel` / :class:`WeComChannel` /
  :class:`EmailChannel` / :class:`WebhookChannel` —— 五个内置渠道
- :class:`NotifyError` / :class:`ChannelError` —— 异常族

典型用法（完整示例见 :class:`lizysdk.notify.NotifyCenter`）::

    from lizysdk.notify import NotifyCenter, DingTalkChannel, EmailChannel

    center = NotifyCenter()                                   # 默认异步投递
    center.add_channel("ops", DingTalkChannel(webhook="...", secret="SEC..."))
    center.add_channel("mail", EmailChannel(host="smtp.x.com", user="bot@x.com",
                                            password="...", to=["a@x.com"]))

    center.route("error", channels=["ops", "mail"])           # 级别路由
    center.set_quiet_hours("22:00", "08:00", except_levels=("critical",))
    center.set_cooldown(600, except_levels=("critical",))     # 同标题 10 分钟一次

    center.notify("订单异常", "SO-001 扣减失败", level="error")  # 入队即返回
    center.notify_to("ops", "标题", "内容")                     # 直发指定渠道
    center.stats()                                             # 各类计数
    center.flush(timeout=10)                                   # 排空在途通知
    center.shutdown()                                          # 幂等停机

本子包对其他 lizysdk 子包零 import 依赖（日志只走标准
``logging.getLogger("lizysdk.notify")`` 通道，由 lizysdk.logs 的根
handler 统一格式捕获）；实现见 :mod:`lizysdk.notify.channels` 与
:mod:`lizysdk.notify.center`。
"""

from __future__ import annotations

from .center import NotifyCenter
from .channels import (
    DingTalkChannel,
    EmailChannel,
    FeishuChannel,
    WebhookChannel,
    WeComChannel,
)
from .exceptions import ChannelError, NotifyError

__all__ = [
    "NotifyCenter",
    "DingTalkChannel",
    "FeishuChannel",
    "WeComChannel",
    "EmailChannel",
    "WebhookChannel",
    "NotifyError",
    "ChannelError",
]
