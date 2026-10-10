# lizysdk 全方位文档

> 版本：**0.8.0**（本文档与该版本源码逐一对齐；API 签名取自源码内省，压测数据取自
> [benchmarks/](../benchmarks/) 各报告）
> 仓库：<https://github.com/BaiEJi/LizySDK> · Python **3.9 ~ 3.13**（全部实测）· 核心零第三方依赖

---

## 目录

1. [项目概览](#1-项目概览)
2. [安装与环境](#2-安装与环境)
3. [一分钟上手](#3-一分钟上手)
4. [子包详解](#4-子包详解)
   - [4.1 ids —— ID 生成](#41-ids--id-生成)
   - [4.2 logs —— 结构化日志](#42-logs--结构化日志)
   - [4.3 errors —— 标准化错误](#43-errors--标准化错误)
   - [4.4 ext —— Web 框架适配](#44-ext--web-框架适配)
   - [4.5 pools —— 统一并发池](#45-pools--统一并发池)
   - [4.6 shell —— 命令执行包装](#46-shell--命令执行包装)
   - [4.7 dist —— Redis 能力套件](#47-dist--redis-能力套件)
   - [4.8 notify —— 通知中心](#48-notify--通知中心)
5. [架构总则与工程红线](#5-架构总则与工程红线)
6. [性能与压测](#6-性能与压测)
7. [多版本支持矩阵](#7-多版本支持矩阵)
8. [选型与最佳实践](#8-选型与最佳实践)
9. [FAQ](#9-faq)
10. [变更记录](#10-变更记录)

---

## 1. 项目概览

lizysdk 是一个**通用 Python 基础工具包**：把后端服务日常重复造的轮子——ID、日志、
错误、并发、命令执行、Redis 协作、告警通知——收敛为一套风格统一、可组合、
开箱即用的 API。

| 子包 | 一句话定位 | 依赖 |
|---|---|---|
| `lizysdk.ids` | trace_id / 随机 uid / 雪花 ID / ULID 可排序 ID / worker_id 协商 | 零 |
| `lizysdk.logs` | pipe / JSON 双格式结构化日志，轮转、后台写入池、打印完即发送 | 零 |
| `lizysdk.errors` | 错误码 + 模板消息 + 序列化往返 + 业务码注册表 + wrap/ensure | 零 |
| `lizysdk.ext` | FastAPI / Flask 全局异常适配器 | `[web]` |
| `lizysdk.pools` | 线程 / 协程 / 进程统一并发池，八事件钩子、重试、超时、背压 | 零（进程池用 stdlib） |
| `lizysdk.shell` | 安全 argv 命令执行，超时终止，富错误，结构化命令日志 | 零 |
| `lizysdk.dist` | Redis 能力套件：限流 / 锁×2 / 选举 / 幂等 / 队列×2 / 排行榜 | `[redis]`（懒加载） |
| `lizysdk.notify` | 钉钉/飞书/企微/邮件/webhook 通知中心，路由/静默期/频控 | 零 |

**设计哲学**：核心零依赖（装上就能用）；重能力走可选组按需安装；模块级懒加载
（没装可选依赖时 `import lizysdk` 及其子包永远成功）；每个分布式组件的算法
对齐业界开源实现并在 docstring 标注出处，不自创语义。

## 2. 安装与环境

```bash
pip install lizysdk                 # 核心（零第三方依赖）
pip install "lizysdk[web]"          # + FastAPI/Flask 适配器
pip install "lizysdk[redis]"        # + Redis 能力套件（redis>=4.2）
pip install "lizysdk[web,redis]"    # 组合
```

开发环境：`pip install -e ".[dev]"`（pytest / fastapi / flask / httpx / redis /
fakeredis\[lua\]）。Python **3.9 ~ 3.13 全部实测**（见 §7）。

## 3. 一分钟上手

```python
import lizysdk as bk

# 1) 日志：pipe 格式 + sys_name 标识 + 后台写入池
bk.setup_logging("logs", "app.log", sys_name="order-svc", level="INFO")
log = bk.get_logger(__name__)

# 2) trace 贯通：一条请求一个 trace_id，日志自动携带
bk.bind_context(trace_id=bk.new_trace_id())     # 16 位 hex，可选位数
log.info("user logged in", user_id=123, action="login")
# -> INFO||2026-10-10T10:30:00||app.py:42||sys_name=order-svc||trace_id=...||user_id=123||action=login||message=user logged in

# 3) 标准化错误：错误码 + 模板 + 序列化往返
raise bk.NotFoundError(params={"resource": "订单"})   # [RESOURCE_NOT_FOUND] 资源不存在: 订单

# 4) 并发池：一行切换线程/协程/进程
with bk.create_pool("thread", workers=8) as pool:
    results = [f.result() for f in [pool.submit(fetch, url) for url in urls]]

# 5) Redis 能力（装了 lizysdk[redis]）
from lizysdk import RLock, ReliableQueue
with RLock(client, "job:42"):      # 可重入 + 看门狗自动续期
    run_job()
```

完整可运行示例：[examples/demo.py](../examples/demo.py)（基础三件套）、
[examples/web_demo.py](../examples/web_demo.py)（FastAPI 全家桶，直接运行）。

---

## 4. 子包详解

### 4.1 ids —— ID 生成

四种 ID 各司其职：**trace_id/uid**（随机，无语义）、**雪花 ID**（趋势递增、
可反解时间）、**ULID**（字典序 = 时间序，适合做主键/文件名）、**worker_id 协商**
（多进程部署下雪花 ID 的机器位自动分配）。

| API | 签名 | 说明 |
|---|---|---|
| `new_trace_id` | `(length: int = 16)` | 小写 hex 随机串（secrets），日志贯通默认 16 位 |
| `new_uid` | `(length: int = 16)` | 通用随机 uid（与 trace_id 同源不同途） |
| `new_id` | `()` | 雪花 64 位整数（模块级默认生成器快捷方式） |
| `new_prefixed_id` | `(prefix: str)` | `"ORD_<snowflake>"` 带业务前缀 |
| `new_sortable_id` | `()` | ULID（26 位 Crockford，字典序=时间序，毫秒精度） |
| `sortable_id_timestamp` | `(uid: str) -> float` | 从 ULID 反解创建时间戳（秒） |
| `IDGenerator` | `(worker_id=0, epoch=1704067200000)` | 可配置雪花生成器（线程安全） |
| `resolve_worker_id` | `(default=0, *, env_var="LIZYSDK_WORKER_ID", lock_dir=None, stale_after_seconds=86400.0)` | worker_id 自动协商：环境变量优先，否则文件锁抢占 0~1023 |
| `ClockBackwardsError` | — | 时钟大幅回拨（ValueError 子类） |
| 常量 | `MAX_WORKER_ID=1023` · `MAX_SEQUENCE=4095` · `DEFAULT_EPOCH_MS=1704067200000` | 10bit 机器位 / 12bit 序列 / 2024-01-01 纪元 |

```python
import lizysdk as bk

bk.new_trace_id()                # '9f8e7d6c5b4a3210'
bk.new_trace_id(32)              # 32 位
bk.new_id()                      # 1234567890123456789（趋势递增）
bk.new_prefixed_id("ORD")        # 'ORD_1234567890123456789'
uid = bk.new_sortable_id()       # '01ARZ3NDEKTSV4RRFFQ69G5FAV'
bk.sortable_id_timestamp(uid)    # 1760073600.123（可反解时间）

# 多进程部署：每个进程自动拿不冲突的机器位（环境变量 LIZYSDK_WORKER_ID 可强制）
worker_id = bk.resolve_worker_id()
gen = bk.IDGenerator(worker_id=worker_id)
```

要点：雪花时钟小幅回拨自动等待补偿、大幅回拨抛 `ClockBackwardsError`；
随机 ID 用线程本地批量熵缓冲（高并发下 ~117 万次/秒，见 §6）。

### 4.2 logs —— 结构化日志

**行契约**（pipe 格式，`message` 恒为最后一段）：

```
LEVEL||TIMESTAMP||FILE:LINE||sys_name=xxx||k1=v1||k2=v2||message=<文本>
```

JSONL 格式（`json_format=True`）为同字段的 JSON 对象，供服务端检索。

| API | 说明 |
|---|---|
| `setup_logging(log_dir="logs", filename="app.log", *, level="INFO", console=True, rotation="size", max_bytes=10MB, backup_count=5, when="midnight", async_writer=True, pool_size=1, sys_name="app", json_format=False, send_json=False, send_url=None, send_timeout=3.0)` | 一次性初始化（幂等）；`rotation="size"/"time"/"none"`；`async_writer=True` 走后台写入池（调用方零 IO 等待），`False` 同步逐条刷盘（**打印完即落盘**契约） |
| `get_logger(name="app") -> PipeLogger` | 取 logger；`log.info("msg", k1=v1, k2=v2)` 关键字直传字段 |
| `bind_context(**fields)` / `clear_context()` | 进程级上下文字段（如 trace_id），之后每条日志自动携带 |
| `flush(timeout=None) -> bool` | 异步模式下强制排空写入池 |
| `send_stats()` | send_json 发送统计（sent/failed/dropped/last_error…） |
| `parse_line(line) -> dict` | pipe 行解析回字典（含 message/level/file/字段表） |

```python
import lizysdk as bk

bk.setup_logging("logs", "app.log", sys_name="order-svc",
                 rotation="size", max_bytes=50*1024*1024, backup_count=10)

# 打印完即发送 JSON 到日志服务端（配合 lizylog 服务端，见 §9）
bk.setup_logging("logs", "app.jsonl", sys_name="order-svc", json_format=True,
                 send_json=True, send_url="http://log-svc:8000/ingest")

log = bk.get_logger("order")
bk.bind_context(trace_id=bk.new_trace_id(), env="prod")
log.info("order created", order_id="SO-001", amount=99.9)
log.error("payment failed", code="E502", message="upstream timeout")  # message 显式给值优先
```

要点：同步模式每条 `flush`（调用返回即落盘）；异步模式池化写入 + 缓冲 handler
（`flush()`/停机/轮转承接落盘），4 线程并发打点 ~6.2 万条/秒（§6）。
转义规则：字段值含 `\n`/`||`/`=` 时转义，`parse_line` 逆向还原。

### 4.3 errors —— 标准化错误

`ErrorCode`（10 个标准码，str 枚举可直接 JSON 序列化）× `AppError` 族（模板渲染 +
`to_dict`/`from_dict` 序列化往返）× 注册表（业务码动态注册）× 工具（`wrap`/`ensure`）。

| API | 说明 |
|---|---|
| `AppError(code=INTERNAL_ERROR, message=None, *, details=None, params=None)` | 基类；`params` 渲染进中文模板，`details` 附带结构化上下文 |
| 标准子类 | `ParamError` `AuthError` `PermissionDeniedError` `NotFoundError` `ConflictError` `RateLimitError` `InternalError` `ServiceUnavailableError` `UpstreamTimeoutError` |
| `wrap(exc, message=None, *, code=INTERNAL_ERROR, details=None)` | 任意异常包成 AppError（details 自动记 `original_type`） |
| `ensure(condition, error, **kwargs)` | 断言式抛错（error 接受 码/实例 两种形态） |
| `register_code(code, template, http_status=400, *, overwrite=False)` | 注册业务码；`unregister_code` / `registered_codes()` |

```python
import lizysdk as bk

raise bk.NotFoundError(params={"resource": "订单"})
# str -> "[RESOURCE_NOT_FOUND] 资源不存在: 订单"；.http_status == 404

bk.register_code("ORDER_DUP", "订单重复: {order_id}", http_status=409)
err = bk.AppError("ORDER_DUP", params={"order_id": "SO-001"})

data = err.to_dict()                    # 序列化（type 随子类自动注册）
err2 = bk.AppError.from_dict(data)      # 往返还原同子类

try:
    risky()
except Exception as exc:
    raise bk.wrap(exc, code=bk.UpstreamTimeoutError) from None

bk.ensure(user.is_admin, bk.PermissionDeniedError)
```

要点：模板渲染缺键保留 `{占位符}` 原样、绝不抛 KeyError；`from_dict` 靠
`__init_subclass__` 类型注册表还原子类，未知 type 回落 `AppError` 本体。

### 4.4 ext —— Web 框架适配

把 AppError 自动转成统一 JSON 错误响应（`code/message/details/http_status`），
未知异常按 500 包装。需 `pip install "lizysdk[web]"`（导入也是懒加载）。

```python
# FastAPI
from fastapi import FastAPI
from lizysdk.ext import install_fastapi_handler
app = FastAPI()
install_fastapi_handler(app, include_generic=True)   # 返回 app 可链式

# Flask
from flask import Flask
from lizysdk.ext import install_flask_handler
app = Flask(__name__)
install_flask_handler(app, include_generic=True)
```

### 4.5 pools —— 统一并发池

三种执行模型一个 API 面：`submit()` 返回 concurrent.futures 兼容的 Future，
`map()` 批量提交保序，`shutdown()` 优雅停机；统一配置重试/超时/背压/上下文
传播/八事件钩子（on_submit/on_start/on_success/on_error/on_retry/on_timeout/
on_reject/on_shutdown）。

| API | 签名要点 |
|---|---|
| `create_pool(kind, *, name=None, workers=None, queue_size=0, reject_policy="raise", task_timeout=None, max_retries=0, retry_backoff=0.0, initializer=None, initargs=(), max_tasks_per_worker=None, propagate_context=True, hooks=None)` | `kind="thread"/"async"/"process"`；`workers=None` 自动（CPU 核数） |
| `ThreadPool` / `AsyncPool` / `ProcessPool` | 同参数直接构造 |
| `run_all(tasks, *, kind="thread", return_exceptions=False, workers=None, **config)` | 一次性批量执行（自动建池关池）；`tasks` 为 `(fn, args, kwargs)` 三元组可迭代 |
| 异常族 | `PoolError` · `PoolClosedError` · `PoolRejectedError`（背压满且 reject_policy="raise"） |

```python
import lizysdk as bk

# IO 密集：线程池（近理想线性扩展，见 §6）
with bk.create_pool("thread", workers=16) as pool:
    futs = [pool.submit(fetch, url, timeout=10) for url in urls]
    results = [f.result() for f in futs]

# CPU 密集：进程池（真并行；线程池在 CPU 任务下无加速——GIL）
with bk.create_pool("process", workers=8) as pool:
    outs = pool.map(heavy_compute, items)          # 保序

# 协程池
async def main():
    with bk.create_pool("async", workers=64) as pool:
        futs = [pool.submit(async_fetch, u) for u in urls]
        results = [await f for f in futs]

# 一次性批量 + 失败容忍
results = bk.run_all([(fetch, (u,), {}) for u in urls],
                     kind="thread", workers=8, return_exceptions=True)
```

要点：进程池任务须可 pickle（模块级函数）；`propagate_context=True` 时
logging 上下文/trace 自动带进 worker；`queue_size>0` + `reject_policy`
构成背压（`"raise"` 抛 PoolRejectedError / `"drop"` 丢弃返回 cancelled）。

### 4.6 shell —— 命令执行包装

**恒 `shell=False`**（安全红线）：字符串 argv 走 `shlex.split`，天然免疫注入；
不支持管道/重定向（需要时用 Python 组合）。超时走 `subprocess.run(timeout=...)`
（Windows TerminateProcess），超时/非零退出抛富错误（带部分输出）。

| API | 签名 |
|---|---|
| `run` | `(argv, *, timeout=None, check=False, capture=True, input=None, env=None, cwd=None, encoding="utf-8", log=True) -> ShellResult` |
| `ShellResult` | `(argv, returncode, stdout, stderr, elapsed_ms)` 命名元组 |
| `ShellError` | 非零退出（`check=True` 时抛）；携带 argv/returncode/stdout/stderr/elapsed_ms |
| `ShellTimeoutError` | 超时；携带已捕获的部分输出 |

```python
from lizysdk.shell import run, ShellError, ShellTimeoutError

r = run(["git", "log", "--oneline", "-5"])          # str 序列会 shlex.split
print(r.returncode, r.stdout, r.elapsed_ms)

try:
    run("ping -n 100 example.com".split(), timeout=3, check=True)
except ShellTimeoutError as exc:
    print("部分输出:", exc.stdout)
```

每次执行自动记结构化命令日志（标准 logging，logger 名 `lizysdk.shell`，
`log=False` 关闭）。

### 4.7 dist —— Redis 能力套件

需 `pip install "lizysdk[redis]"`。**模块级懒加载**：未装 redis 时
`import lizysdk.dist` 照常成功，用到时才抛带中文安装提示的 ImportError。
单实例 Redis 语义；**每个组件算法对齐开源实现**（出处即选型依据）：

| 组件 | 对齐实现 | 核心语义 |
|---|---|---|
| `SlidingWindowCounter` | limits 库 / redis.io 限流教程 | ZSET+Lua 原子四步，`(now-window, now]` 精确滑动窗口 |
| `DLock` | redis-py Lock | 不可重入效率锁：`SET NX PX` + token + Lua compare-token 释放/续期 |
| `RLock` | Redisson RedissonLock | hash 重入计数（field=实例:线程）+ 看门狗每 TTL/3 自动续期 |
| `LeaderElector` | Kubernetes Lease | 独占租约 + 心跳续期（lease/3）+ 失联**先降级再回调** + 自动重选 |
| `IdempotentKey` | Stripe Idempotency Key | processing→done 两态；并发重复 Conflict(409)、已完成拿缓存结果 |
| `ReliableQueue` | redis.io Reliable queue | LPUSH / LMOVE→processing / LREM ack / recover 清扫；at-least-once |
| `DelayQueue` | Redisson RDelayedQueue | ZSET score=到期时间 + 原子 Lua 搬运（ZREM 成功才入队，不重不漏） |
| `Leaderboard` | redis.io Leaderboard | ZSET 计分/名次/top/around；**1-based 名次**，同分按 member 字典序 |

```python
from lizysdk import (
    SlidingWindowCounter, DLock, RLock, LeaderElector, IdempotentKey,
    IdempotencyConflictError, IdempotencyDoneError,
    ReliableQueue, DelayQueue, Leaderboard,
)

# --- 限流：滑动窗口 -------------------------------------------------------
counter = SlidingWindowCounter(client, window=60)        # 或 .from_url("redis://...")
counter.incr("user:1:api")                               # -> 窗口内当前计数
if counter.allow("user:1:api", limit=100):               # incr 后 <= limit
    serve()

# --- 不可重入效率锁（短临界区） ---------------------------------------------
with DLock(client, "cache:refresh", timeout=10):         # TTL 10s 崩溃自动过期
    refresh()
# lock.extend(10) 续期；token 保护防误删他人锁

# --- 可重入锁 + 看门狗（长任务） --------------------------------------------
with RLock(client, "job:42"):                            # 默认 30s 档自动续期
    with RLock(client, "job:42"):                        # 同线程嵌套层层计数
        run_job()
# reentrant_count() 查层数；lease_time=5 则固定 5s 无看门狗；force_unlock() 管理端强删

# --- 领导选举（多实例只跑一份定时任务） --------------------------------------
elector = LeaderElector(client, "cron-leader",
                        on_become_leader=start_jobs,
                        on_losing_leadership=stop_jobs, lease=15)
elector.start()                  # daemon 心跳线程，幂等
elector.is_leader                # 本实例是否领导
elector.leader_id()              # 当前领导 holder_id（跟随者发现）
elector.stop()                   # 主动下台（释放租约 + losing 回调）

# --- 幂等键（支付/下单防重复） ----------------------------------------------
idem = IdempotentKey(client)
try:
    with idem.guard("pay:order:123", processing_ttl=60) as g:
        result = do_pay()
        g.complete(result, ttl=86400)     # 落 done + 缓存结果
except IdempotencyConflictError:
        ...                                # 并发重复：409 语义
except IdempotencyDoneError as e:
    cached = e.result                     # 已完成：直接拿缓存

# --- 可靠队列（at-least-once，消费方需幂等） ----------------------------------
rq = ReliableQueue(client, "orders")
job_id = rq.push("order:123")             # 返回任务 id
job = rq.pop(timeout=5)                   # Job(id, payload, raw, tries)；无货 None
rq.ack(job)                               # 处理成功：LREM 确认
rq.nack(job)                              # 处理失败：回队重投（tries+1，id 不变）
rq.recover()                              # 清扫：崩溃者残留全量搬回
rq.qsize(), rq.processing_size()

# --- 延迟队列 ----------------------------------------------------------------
dq = DelayQueue(client, "reminders")
job_id = dq.push("sms:send:42", delay=30) # 30 秒后到期，返回 id
job = dq.pop_ready(timeout=5)             # 先搬到期项再弹；dq.move_due() 可独立定时调用
dq.cancel(job_id)                         # 未到期可取消（已搬运则 False）
dq.due_size(), dq.ready_size()

# --- 排行榜 ------------------------------------------------------------------
lb = Leaderboard(client, "game:1:score")
lb.add_score("alice", 100)                # ZADD 覆盖；incr_score 加减分
lb.rank("alice")                          # 1-based；未上榜 None
lb.top(10)                                # [(member, score)] 降序
lb.around("alice", span=2)                # 「我的排名」窗口（贴边自动收缩）
```

**诚实边界**（对齐 Kleppmann 对 TTL 锁的批评）：锁/选举是**效率工具**而非正确性
保证——GC 长停顿/网络分区下可能偶发重叠；严格互斥场景请业务侧加 fencing token
或数据库约束。多节点 Redlock 列为 v2。队列 at-least-once 意味着**可能重复投递**。

### 4.8 notify —— 通知中心

五渠道统一 `notify()`：Apprise 式统一扇出 + Grafana 式路由/静默期/频控。
零第三方依赖（邮件走 stdlib smtplib）。消息格式与签名**逐字节对齐官方 API**
（钉钉/飞书签名算法不同，有防串用回归钩子）。

| 渠道 | 构造签名要点 | 签名 |
|---|---|---|
| `DingTalkChannel` | `(webhook, *, secret=None, timeout=10, transport=None)` | HMAC-SHA256(secret, `"{ts_ms}\n{secret}"`) → b64 → quote_plus 拼 URL |
| `FeishuChannel` | `(webhook, *, secret=None, timeout=10, transport=None)` | HMAC-SHA256(key=`"{ts}\n{secret}"`, msg=`b""`) → b64 进 body（ts 秒） |
| `WeComChannel` | `(webhook, *, timeout=10, transport=None)` | 无签名 |
| `EmailChannel` | `(host, port=465, *, user, password, to, use_ssl=True, sender=None, timeout=10, mailer=None)` | SMTP/SMTPS |
| `WebhookChannel` | `(url, *, headers=None, payload_builder=None, timeout=10, transport=None)` | 自定义 payload |

```python
import lizysdk as bk

center = bk.NotifyCenter()                                  # async_send=True, retry=2
center.add_channel("ops", bk.DingTalkChannel(webhook="https://oapi.dingtalk.com/robot/send?access_token=xxx", secret="SEC..."))
center.add_channel("mail", bk.EmailChannel(host="smtp.x.com", user="bot@x.com", password="***", to=["a@x.com"]))

center.route("error", channels=["ops", "mail"])             # 级别路由
center.set_quiet_hours("22:00", "08:00", except_levels=("critical",))   # 静默期(跨午夜)
center.set_cooldown(600, except_levels=("critical",))       # 同标题 10 分钟一次

center.notify("订单异常", "SO-001 扣减失败", level="error")  # 异步入队即返回
center.notify_to("ops", "标题", "内容")                      # 直发指定渠道
center.stats()      # sent/failed/retried/quiet_suppressed/cooldown_suppressed/last_error
center.flush(timeout=10)                                    # 排空待发队列
center.shutdown()                                           # 停机排空（atexit 自动）
```

要点：抑制判断（静默期/频控）在**入队前**（省队列）；单渠道失败隔离；
重试指数退避；`transport(url, payload, headers, timeout)->(status, text)` 与
`mailer(msg_bytes, sender, to)` 是注入接缝（测试全离线）。

---

## 5. 架构总则与工程红线

面向贡献者（使用者也值得了解的边界承诺）：

1. **核心零依赖**：ids/logs/errors/pools/shell/notify 只用标准库；ext/dist 的
   第三方 import 一律函数体内懒加载——未装可选组时 `import lizysdk.*` 恒成功，
   用到才抛带 `pip install "lizysdk[redis]"` 中文提示的 ImportError。
2. **子包自持**：子包之间不互相 import（唯一例外：dist 内部锁异常族复用）；
   日志统一走标准 logging（logger 名 `lizysdk.*`）。
3. **py3.9+ 兼容**：全包 `from __future__ import annotations`；不用 3.10+ 语法。
4. **Lua 一律模块级常量** + 注释块说明 KEYS/ARGV；原子性由脚本保证。
5. **时间/睡眠接缝**：dist 各模块的 `_now()`/`_sleep()` 是刻意的测试接缝
   （monkeypatch 冻结/推进时间做离线验证），勿内联。
6. **参数校验全量前置**：构造器与方法入口校验，非法抛中文 ValueError。
7. **类型完备**：py.typed 随包发布，全公开 API 带注解与中文 docstring
   （含 Google 风格 Args/Returns/Raises/Example）。
8. **测试离线**：825 个测试零真实网络（fakeredis\[lua\] + transport/mailer 注入
   + tmp_path），并发用例 3 轮防 flaky。

## 6. 性能与压测

> 方法论统一：固定操作数、**多轮独立计时取最优**（timeit 惯例）；同机交错对比
> 消除机器快慢态；本机噪声 ±20%，结论只看量级与相对比例。原始数据见
> [benchmarks/results/](../benchmarks/results/)，三份报告：REPORT.md（logs/ids）、
> pool_bench、REDIS_REPORT.md。

**并发池**（[pool_bench.py](../benchmarks/pool_bench.py)，IO=1ms sleep、CPU=103ms 纯计算）：

| 场景 | 结果 |
|---|---|
| ThreadPool 16 worker（IO 密集） | **×15.92 加速、效率 100%**（近理想线性） |
| ProcessPool 8 worker（CPU 密集） | **×4.19**；对照 ThreadPool 跑 CPU 任务 ×1.01（GIL 佐证） |
| AsyncPool 8~64 worker（协程） | **×130~145** |
| `submit()` 调用方开销 | 3.8 µs（stdlib ThreadPoolExecutor 14.7 µs 的 1/3.9） |

**日志与 ID**（[benchmarks/REPORT.md](../benchmarks/REPORT.md)，v0.4.0 优化
11 项指标几何平均 **+74.4%**，354 测试零回归）：

| 指标 | 优化后 |
|---|---:|
| 异步打点调用方开销 | 62,090 ops/s |
| 4 线程并发打点总吞吐 | 61,967 ops/s（+351.7%） |
| `new_trace_id` | 1,166,808 ops/s |

**Redis 套件**（[redis_bench.py](../benchmarks/redis_bench.py) /
[REDIS_REPORT.md](../benchmarks/REDIS_REPORT.md)；**fakeredis 进程内口径**——衡量
SDK 层开销，非 Redis 服务端吞吐/网络 RTT；Lua 经 lupa 每次解释执行，真 Redis
有 EVALSHA 缓存，Lua 类场景相对劣势会收窄）：

| 场景 | 吞吐 (ops/s) | 单操作 |
|---|---:|---:|
| IdempotentKey begin→complete | 13,073 | 0.08 ms |
| Leaderboard 写 / ReliableQueue 全链路 | ~8,200 | 0.12 ms |
| DelayQueue 批量搬运 | 5,076 | 0.20 ms |
| DLock 对照 / RLock 深重入 | ~2,600 | 0.39 ms |
| RLock 浅循环（看门狗线程启停主导） | 820 | 1.22 ms |

看门狗稳态持锁开销每周期仅 +0.1ms（续期是后台异步 EVAL）。压测过程抓到并
修复 1 个看门狗真 bug（release 后立即重取丢锁），详见 REDIS_REPORT.md §5。

## 7. 多版本支持矩阵

声明并**全部实测**（conda 环境驱动 `scripts/ci_matrix.sh`，全量 825 测试）：

| Python | 3.9 | 3.10 | 3.11 | 3.12 | 3.13 |
|---|---|---|---|---|---|
| 状态 | ✅ | ✅ | ✅ | ✅ | ✅ |

pyproject classifiers 同步声明 3.9~3.13；源码不用任何 3.10+ 语法。

## 8. 选型与最佳实践

- **锁**：毫秒级短临界区 / 高频抢锁 → `DLock`（无线程、token 模型）；
  秒级以上长任务 / 需要同线程重入 → `RLock`（看门狗续期）。严格互斥正确性
  场景（扣款/扣库存）→ 业务侧 fencing token，不要依赖任何 TTL 锁。
- **队列**：普通任务分发 → `ReliableQueue`（at-least-once，**消费方必须幂等**，
  可与 `IdempotentKey` 组合）；定时/重试延迟 → `DelayQueue`（到期搬运原子不重
  不漏，`move_due` 可挂外部定时器）。需要「到期进可靠队列」自行桥接两个队列。
- **并发池**：IO 密集线程池、CPU 密集进程池、已有 asyncio 事件循环用协程池；
  高并发提交关注 `queue_size` 背压与 `reject_policy`。
- **日志**：默认异步写入池（性能）；对「打印完即落盘」有强需求（审计类）用
  `async_writer=False`。生产建议 `json_format=True + send_json=True` 直发日志服务端。
- **通知**：`critical` 级别务必加进 `except_levels`（静默期/频控都豁免）。
- **部署**：多进程跑雪花 ID 先 `resolve_worker_id()`；用 k8s 多副本跑定时任务
  用 `LeaderElector` 包住调度入口。

## 9. FAQ

**Q: 日志服务端用什么？**
自研 **lizylog**（配套项目：SQLite/PG 双存储 + 检索 API，与 `send_json=True`
的 JSONL 格式对接；其设计文档已并入本仓库 git 历史的 `docs/log-server-design.md`
提交记录）。量级不大时 SQLite 足够，PG 用于多实例汇聚。SDK 侧只需
`send_url` 指向服务端 ingest 接口。

**Q: 为什么 `RLock` 快速取放比 `DLock` 慢？**
看门狗模式下每次「取-放到 0 层」循环伴随一次线程启停（~1ms）；深重入或长持锁
场景摊销后可忽略（§6）。这是 Redisson 式看门狗在 Python 线程模型下的固有成本。

**Q: 队列会丢消息吗？**
`ReliableQueue` 保证 at-least-once：worker 崩溃后任务留在 processing 列表，
`recover()` 清扫搬回重投——代价是**可能重复**，消费方需幂等（推荐
`IdempotentKey` 护住处理逻辑）。

**Q: 可靠队列的阻塞 pop 为什么是小步循环？**
`pop(timeout>0)` 用 BLMOVE 小步循环：fakeredis 的 BLMOVE 空队列不阻塞（测试
环境语义一致），真 Redis 下 BLMOVE 本身服务端阻塞、循环只是 deadline 兜底。
SDK 不提供永久阻塞入口（timeout=0 即非阻塞）。

**Q: 进程池任务有什么限制？**
任务函数与参数必须可 pickle（模块级函数；lambda/闭包不行）——这是跨进程的
本质约束，非 SDK 限制。

## 10. 变更记录

- **0.8.0** —— dist 扩为 Redis 能力套件：`RLock`（Redisson）/ `LeaderElector`
  （K8s Lease）/ `IdempotentKey`（Stripe）/ `ReliableQueue`（redis.io）/
  `DelayQueue`（Redisson RDelayedQueue）/ `Leaderboard`（redis.io）；150 新测试
  （总 825）；fakeredis 口径压测（benchmarks/REDIS_REPORT.md）；修复看门狗
  快速重取丢锁 bug 与 .gitignore 误排除 dist 子包问题；**文档整合为本单册**
  （原五份设计文档并入 git 历史）。
- **0.7.0** —— 新增 notify 通知中心（五渠道/路由/静默期/频控/异步重试）。
- **0.6.0** —— 新增 shell 执行包装与 dist（滑动窗口计数器 + DLock）；
  多版本支持矩阵建立。
- **0.5.0** —— 新增 pools 统一并发池。
- **0.4.0** —— 性能专项优化（几何平均 +74.4%，benchmarks 体系建立）。
- **0.3.0** —— errors 业务码注册表与 Web 适配器；ids ULID 与 worker_id 协商。
- **0.2.0** —— trace_id 位数可选；日志 sys_name/JSONL/send_json。
- **0.1.0** —— 首版（ids/logs/errors 三件套）。
