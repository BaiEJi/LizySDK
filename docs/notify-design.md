# lizysdk.notify —— 通知中心设计文档

> 版本：v1（2026-09-20）· 目标版本 lizysdk 0.7.0
> 定位：统一 `notify()` 多渠道通知（钉钉 / 飞书 / 企业微信 / 邮件 / 自定义 webhook），
> 级别路由、静默期、同标题频控、异步投递与重试——**纯标准库零依赖**（webhook 走
> urllib、邮件走 smtplib）。

## 1. 开源参考与选型对比

| 参考 | 取 | 舍 |
|---|---|---|
| [Apprise](https://github.com/caronc/apprise)（最流行的 Python 通知库，100+ 服务） | 统一对象 + 一次调用多渠道扇出；tag 路由思想 → 我们的 `route(level, channels)` | **URL 魔法串配置**（`discord://token@id`）——隐式解析与我们的显式风格冲突；插件广度——五个内置渠道覆盖国内主流，够用为止 |
| [notifiers](https://github.com/notifiers/notifiers) | Provider 即类的分层 | JSON-Schema 声明框架——对五个渠道过度设计 |
| Grafana 告警路由模型 | **mute timings（静默期）**与**路由规则（级别→渠道）**两个概念直接借鉴 | 告警评估引擎（那是监控系统的事） |
| ntfy（自托管推送服务） | —— | 需要独立服务进程，超出 SDK 边界 |

**决策**：显式渠道类 + 中心对象扇出 + Grafana 式路由/静默期；每渠道契约对齐
[钉钉](https://open.dingtalk.com)/[飞书](https://open.feishu.cn)/[企业微信](https://developer.work.weixin.qq.com)
官方机器人 API（payload 与签名逐字节可测）。

## 2. 渠道契约（对齐官方文档）

| 渠道 | 消息格式 | 签名 | 成功判定 |
|---|---|---|---|
| `DingTalkChannel(webhook, *, secret=None, timeout=10, transport=None)` | `{"msgtype":"markdown","markdown":{"title":<title>,"text":<title>\n<body>}}` | 加签：`sign=quote_plus(b64(hmac_sha256(key=secret, msg=f"{ts_ms}\n{secret}")))` 拼接 `&timestamp={ts_ms}&sign={sign}` | 响应 JSON `errcode==0` |
| `FeishuChannel(webhook, *, secret=None, timeout=10, transport=None)` | `{"msg_type":"text","content":{"text":"<title>\n<body>"}}` | 签名：body 增 `"timestamp": str(ts_sec)` 与 `"sign": b64(hmac_sha256(key=f"{ts}\n{secret}", msg=""))` ——**注意与钉钉相反：飞书是空消息签名** | `code==0`（新版）或 `StatusCode==0`（旧版） |
| `WeComChannel(webhook, *, timeout=10, transport=None)` | `{"msgtype":"markdown","markdown":{"content":"**<title>**\n<body>"}}` | 无（webhook key 即凭证；官方注：markdown 仅子集语法） | `errcode==0` |
| `EmailChannel(host, port=465, *, user, password, to, use_ssl=True, sender=None, timeout=10, mailer=None)` | `MIMEText(body, "plain", "utf-8")`，Subject=title；发 `to` 列表 | SMTP/SMTP_SSL + LOGIN | SMTP 无异常即成功 |
| `WebhookChannel(url, *, headers=None, payload_builder=None, timeout=10, transport=None)` | 默认 `{"title","content","level","ts"}`；`payload_builder(title, body, level)->dict` 可定制 | 自定义（headers 自由） | HTTP 2xx |

- **transport 注入（测试离线化的关键）**：HTTP 渠道接受 `transport: Callable[[url, payload, headers, timeout], tuple[int, str]]`，默认 urllib POST JSON（`Content-Type: application/json`，utf-8）
- **mailer 注入**：EmailChannel 接受 `mailer: Callable[[msg_bytes, sender, to], None]`，默认 smtplib 封装
- 渠道异常统一 `ChannelError(NotifyError)`，携带渠道名 / HTTP 状态 / 响应体摘要（中文）

## 3. NotifyCenter API

```python
from lizysdk.notify import NotifyCenter, DingTalkChannel, FeishuChannel, EmailChannel

center = NotifyCenter()                                   # async_send=True 默认异步
center.add_channel("ops", DingTalkChannel(webhook=..., secret="SEC..."))
center.add_channel("mail", EmailChannel(host="smtp.x.com", user="bot@x.com",
                                        password="...", to=["a@x.com"]))

center.route("error", channels=["ops", "mail"])           # 级别路由
center.set_quiet_hours("22:00", "08:00", except_levels=("critical",))   # 静默期(跨午夜)
center.set_cooldown(600, except_levels=("critical",))     # 同标题 10 分钟内只发一次

center.notify("订单异常", "SO-001 扣减失败", level="error") # 路由到 error 渠道；异步入队即返回
center.notify_to("ops", "标题", "内容")                     # 直发指定渠道
center.stats()    # {"sent","failed","retried","quiet_suppressed","cooldown_suppressed","last_error","channels":{name:{"sent","failed"}}}
center.flush(timeout=10)                                   # 排空在途通知
center.shutdown()                                          # 幂等停机（atexit 钩子保底）
```

语义细则：

- **级别**：`info | warning | error | critical`（与 logging 对齐），非法抛 ValueError
- **路由**：`notify(channels=None)` 时按已注册的 `route(level, ...)` 解析；该级别无路由
  则发**全部渠道**；`notify_to` 指定渠道优先
- **静默期**：本地时间 `HH:MM` 窗口（支持跨午夜），抑制 `except_levels` 之外的级别，
  计入 `quiet_suppressed`（发送动作整个跳过，含异步入队）
- **频控**：同 `title` 在窗口内第二次及以后被抑制（`except_levels` 豁免），
  计入 `cooldown_suppressed`；窗口内记录有上限（防内存泄漏，LRU 上限 1024）
- **异步模式**（默认）：单守护分发线程 + `queue.Queue`，逐条按渠道扇出，**单渠道失败
  隔离**不影响其他渠道与后续通知；重试 `retry` 次指数退避（`retry_backoff` 基秒）
- **同步模式**（`async_send=False`）：内联发送，返回 `{渠道名: 成功与否}`，
  **不抛发送异常**（失败进 stats 与 `last_error`）；参数非法仍抛 ValueError
- 幂等 `shutdown`；atexit 自动 flush+停机；进程退出不丢已入队通知（尽力）

## 4. 工程约定

- 纯标准库；**禁止 import 其他 lizysdk 子包**；日志走标准
  `logging.getLogger("lizysdk.notify")`（被 logs 子包统一格式捕获）
- 异常族：`NotifyError(Exception)`、`ChannelError(NotifyError)`；参数校验 ValueError（中文）
- 时间接缝：模块级 `_now()`（`time.time()`）供测试冻结（静默期/频控/签名时间戳共用）
- 中心与渠道的锁保护 stats 计数；静默期/频控判断在**入队前**（异步模式下也立即抑制）

## 5. 测试计划（全离线：transport/mailer 注入，零真实网络）

- **渠道 payload 逐字节断言**：钉钉/飞书/企微/自定义 webhook 的 JSON 结构与字段值
  （标题拼接规则各渠道不同）；钉钉与飞书**签名确定性**（冻结 `_now` 后与同算法重算
  一致，且钉钉/飞书算法互不相同——防串用的回归钩子）；钉钉 errcode!=0 / 飞书旧版
  StatusCode / 企微 errcode 失败路径
- **EmailChannel**：MIME 构造（Subject/正文/to 列表）经注入 mailer 断言；默认 mailer
  走 smtplib 的分支用 monkeypatch SMTP_SSL 假对象验证登录与发送序列
- **中心语义**：级别校验、路由命中/未命中回退全渠道、notify_to 指定、静默期跨午夜
  （22:00-08:00 与 08:00-12:00 两形态）与 except_levels 豁免、频控窗口内抑制与豁免、
  频控 LRU 上限、同步模式返回 {渠道: bool} 且不抛
- **异步与重试**：入队即返回 + flush 后全部送达；失败渠道重试 N 次后计入 failed、
  `last_error` 更新；**单渠道失败不影响同批其他渠道**；shutdown 幂等与 atexit 排空
- **stats**：各计数器准确（含 quiet/cooldown suppressed 与 per-channel 计数）
- 防抖：`tests/test_notify.py` 连跑 3 轮全绿；全量套件零回归

## 6. 演进

v2 候选：Apprise 风格 URL 配置串、Telegram/Slack/短信渠道、模板引擎联动
（消息模板 + 渠道适配）、静默期按星期/节假日、通知聚合（同类合并摘要）。
