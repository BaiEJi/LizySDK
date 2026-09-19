# lizysdk

通用 Python 基础工具包：**唯一 ID / trace_id 生成 · 结构化日志（pipe/JSON 双格式 + 远程发送）· 标准化错误体系（业务码注册表 + Web 框架适配）**。

- 核心纯标准库，**零第三方依赖**，`import lizysdk` 即用；Web 适配器走可选依赖组 `lizysdk[web]`
- Python **3.9+**，全量类型注解（随包发布 `py.typed`）
- 线程安全（ID 生成与日志写入均经并发验证）
- **354 个测试**全绿
- 仓库：<https://github.com/BaiEJi/LizySDK>

## 安装

```bash
pip install -e .            # 核心功能（零依赖）
pip install -e .[web]       # + FastAPI/Flask 异常适配器
pip install -e .[dev]       # + 测试依赖（pytest/fastapi/flask/httpx）
```

## 一分钟上手

```python
import lizysdk as bk

bk.setup_logging("logs", "app.log", sys_name="order-svc")   # 1. 初始化日志
bk.bind_context(trace_id=bk.new_trace_id())                 # 2. 绑定链路标识
log = bk.get_logger(__name__)
log.info("user logged in", user_id=123, action="login")     # 3. 结构化打点
raise bk.NotFoundError(params={"resource": "订单"})          # 4. 标准化抛错
```

产出（`logs/app.log`）：

```
INFO||2026-09-19T10:30:00||app.py:10||sys_name=order-svc||trace_id=a3f0c2d4e5f60718||user_id=123||action=login||message=user logged in
```

---

## 使用指南

### 1. ID 生成（`lizysdk.ids`）

**怎么选**：

| 场景 | 用哪个 | 输出示例 |
|---|---|---|
| 链路追踪标识 | `new_trace_id()` | `'a3f0c2d4e5f60718'`（16 位 hex，位数可调 8~64） |
| 业务唯一标识（字符串） | `new_uid()` | `'8a149354d2bdf82c'`（位数可调） |
| 业务单号 | `new_prefixed_id("ORD")` | `'ORD_7350283750400000001'` |
| 数据库主键（可排序字符串） | `new_sortable_id()` | `'01m2vn4zkfvms6dx8mzgst03wt'`（26 位 ULID，字典序=时间序） |
| 高性能整数 ID | `new_id()` | `7350283750400000001`（雪花 64 位） |

```python
from lizysdk import new_trace_id, new_uid, new_sortable_id, sortable_id_timestamp

new_trace_id()          # 默认 16 位
new_trace_id(32)        # 位数可选：8~64（含奇数位），非法位数抛 ValueError
uid = new_uid(20)       # 通用唯一 ID，同样位数可选

sid = new_sortable_id()          # ULID：生成顺序 == 字典序（同毫秒内单调递增）
sortable_id_timestamp(sid)       # 反解 Unix 秒（含毫秒），1663568712.345
```

**雪花与多实例部署**：

```python
from lizysdk import IDGenerator, resolve_worker_id

# 多实例免手工分配 worker_id：环境变量 LIZYSDK_WORKER_ID 优先；
# 未设置时用本机锁文件（O_EXCL 原子占位）自动从 default 起协商，atexit 自动释放
worker = resolve_worker_id()             # -> 0
gen = IDGenerator(worker_id=worker)
ids = gen.batch(10_000)                  # 单次持锁批量生成，全部唯一且时间位单调
```

雪花位布局 `[1 保留][41 毫秒时间戳][10 worker_id][12 序列]`（默认纪元 2024-01-01 UTC）；
时钟小幅回拨自旋等待，回拨超 10ms 抛 `ClockBackwardsError`（ValueError 子类）。

### 2. 结构化日志（`lizysdk.logs`）

**基本用法**：kv 任意传、按传入顺序输出，`message` 恒为最后一段。

```python
from lizysdk import setup_logging, get_logger

setup_logging("logs", "app.log", sys_name="order-svc", level="INFO")
log = get_logger(__name__)               # 惯例传 __name__，也可自定义名字

log.info("user logged in", user_id=123, action="login")
log.error("db failed", exc_info=True)    # 异常栈自动转义为单行
```

**两种输出格式**（由 `json_format` 开关）：

```
# pipe（默认）                                # JSON（json_format=True，JSONL 紧凑输出）
INFO||2026-09-19T10:30:00||app.py:10||       {"level":"INFO","timestamp":"2026-09-19T10:30:00",
sys_name=order-svc||user_id=123||             "file":"app.py","line":10,"sys_name":"order-svc",
message=user logged in                        "user_id":"123","message":"user logged in"}
```

**`setup_logging` 参数全表**：

| 参数 | 默认 | 说明 |
|---|---|---|
| `log_dir` / `filename` | `"logs"` / `"app.log"` | 日志目录与文件名 |
| `sys_name` | `"app"` | 系统标识，注入每条日志（本地行 + 发送 body）；非空 str |
| `level` | `"INFO"` | 级别过滤（低于该级别不输出） |
| `console` | `True` | 是否同时输出到 stderr |
| `rotation` | `"size"` | `"size"` 按大小切片 / `"time"` 按时间轮转 / `"none"` |
| `max_bytes` / `backup_count` | 10MB / 5 | size 轮转参数 |
| `when` | `"midnight"` | time 轮转粒度，语义同标准库（`"s"/"m"/"h"/"d"/"midnight"`） |
| `async_writer` | `True` | 后台写入池（queue + N 线程），业务线程不阻塞 |
| `pool_size` | `1` | 后台写入线程数 |
| `json_format` | `False` | JSONL 输出 |
| `send_json` | `False` | 每条日志**打印完即发送** JSON 到远端 |
| `send_url` | `None` | 接收地址（`send_json=True` 时必填，`http(s)://`） |
| `send_timeout` | `3.0` | 单次发送超时秒 |

重复调用 `setup_logging` 幂等（自动清理旧 handler 与后台线程）。

**上下文字段（trace 必配）**：基于 `contextvars`，线程/协程自动隔离，每条日志自动附带。

```python
from lizysdk import bind_context, clear_context

bind_context(trace_id=new_trace_id(), env="prod")   # 之后的日志自动带 trace_id=... env=prod
clear_context()                                     # 清除本上下文绑定
```

**发送到远端与诊断**：

```python
setup_logging("logs", "app.log", sys_name="order-svc",
              send_json=True, send_url="http://log-server:9280/api/v1/logs")

from lizysdk import flush, send_stats
flush(timeout=5)      # 等待后台队列排空落盘/发送完毕（返回 bool）
send_stats()          # {"sent": 100, "failed": 0, "last_error": None} —— 发送失败绝不影响本地写盘
```

**解析日志行**（两种格式自动识别，转义还原）：

```python
from lizysdk import parse_line

parse_line('INFO||2026-09-19T10:30:00||app.py:10||sys_name=svc||message=hi')
# {'level': 'INFO', 'timestamp': '...', 'file': 'app.py', 'line': 10,
#  'sys_name': 'svc', 'message': 'hi'}
```

**约定**：key 须匹配 `^[A-Za-z_][A-Za-z0-9_.-]*$`；`message` 与 `sys_name` 为保留字（传入抛 ValueError）；值中的 `|`、换行自动转义；第三方库（uvicorn/httpx 等）的日志也会被统一格式捕获。

### 3. 标准化错误（`lizysdk.errors`）

**抛出与捕获**：

```python
from lizysdk import AppError, ErrorCode, NotFoundError, wrap, ensure

raise NotFoundError(params={"resource": "订单"})        # [RESOURCE_NOT_FOUND] 资源不存在: 订单
raise AppError(ErrorCode.PARAM_MISSING, params={"param": "user_id"})

try:
    ...
except AppError as e:                                   # 基类统一捕获
    print(e.code, e.http_status, e.message, e.details)
```

标准子类速查：`ParamError`(400) · `AuthError`(401) · `PermissionDeniedError`(403) ·
`NotFoundError`(404) · `ConflictError`(409) · `RateLimitError`(429) · `InternalError`(500) ·
`ServiceUnavailableError`(503) · `UpstreamTimeoutError`(504)。

**业务错误码注册**（15 个内置码之外，声明式扩展）：

```python
from lizysdk import register_code, AppError

register_code("PAY_BALANCE_NOT_ENOUGH", "余额不足: 还差 {amount} 元", 422)
raise AppError("PAY_BALANCE_NOT_ENOUGH", params={"amount": "3.50"})
# [PAY_BALANCE_NOT_ENOUGH] 余额不足: 还差 3.50 元, http_status=422
```

解析优先级：**注册表 → 内置枚举 → 未知兜底 500**；码名须匹配 `^[A-Z][A-Z0-9_]{1,63}$`；
`overwrite=True` 可覆盖内置码；配套 `unregister_code()` / `registered_codes()`。

**包装与断言**：

```python
try:
    call_redis()
except Exception as inner:
    raise wrap(inner, code=ErrorCode.SERVICE_UNAVAILABLE,
               details={"host": "db-1"}) from inner   # 保留异常链，details 记录原始异常

ensure(user.is_admin, ErrorCode.PERMISSION_DENIED, required_permission="admin")
```

**序列化**：

```python
err = NotFoundError(params={"resource": "订单"})
err.to_dict()                       # {'type','code','message','http_status','details','timestamp'}
                                    # 可直接 json.dumps；from_dict() 往返重建子类
err.to_dict(include_cause=True)     # 额外附带 cause 的类型/消息/完整 traceback
```

### 4. Web 框架适配（`lizysdk.ext`，需 `pip install lizysdk[web]`）

```python
from fastapi import FastAPI
from lizysdk.ext import install_fastapi_handler      # Flask 用 install_flask_handler

app = install_fastapi_handler(FastAPI(), include_generic=True)
# 抛 AppError 子类     -> 状态码 = http_status，body = err.to_dict()
# 抛未知异常(可选开启) -> 500 + INTERNAL_ERROR
```

---

## 完整示例

一个接好「日志 + trace + 错误 + Web 适配」的 FastAPI 服务见
[examples/web_demo.py](examples/web_demo.py)（`python examples/web_demo.py` 直接运行），
核心结构：

```python
import lizysdk as bk
from lizysdk.ext import install_fastapi_handler

bk.register_code("PAY_BALANCE_NOT_ENOUGH", "余额不足: 还差 {amount} 元", 422)
app = install_fastapi_handler(FastAPI(), include_generic=True)

@app.get("/api/orders/{order_no}")
def get_order(order_no: str):
    bk.bind_context(trace_id=bk.new_trace_id())       # 每请求独立链路
    bk.get_logger("web").info("order query", order_no=order_no)
    if order_no == "SO-404":
        raise bk.NotFoundError(params={"resource": f"订单 {order_no}"})
    if order_no == "SO-PAY":
        raise bk.AppError("PAY_BALANCE_NOT_ENOUGH", params={"amount": "3.50"})
    return {"order_no": order_no, "status": "ok", "uid": bk.new_uid()}
```

基础三件套示例：[examples/demo.py](examples/demo.py)。

## 对接日志服务端

`send_url` 指向任一 HTTP JSON 接收端即可：

- **自研 lizylog**（SQLite/PG 双存储，设计文档：[docs/log-server-design.md](docs/log-server-design.md)）
  ```python
  setup_logging(..., send_json=True, send_url="http://log-server:9280/api/v1/logs")
  ```
- **VictoriaLogs**（单二进制日志库）
  ```python
  setup_logging(..., send_json=True,
      send_url="http://logs:9428/insert/jsonline?_time_field=timestamp&_time_format=2006-01-02T15:04:05")
  ```

## FAQ

- **能不能把 `message`/`sys_name` 当业务字段传？** 不能，两者是保留字，传入抛 ValueError。
- **时间戳带时区吗？** 当前为本地时间秒级（`%Y-%m-%dT%H:%M:%S`）；对 VictoriaLogs 用
  `_time_format` 参数声明，对 lizylog 由服务端 `INCOMING_TZ` 解释。
- **位数范围？** `new_trace_id`/`new_uid` 为 8~64（含奇数）；雪花 64 位布局固定不可调；
  ULID 固定 26 字符。
- **多进程写同一个日志文件？** 不支持（单进程模型）；多进程场景建议每进程独立文件或
  直接走 `send_json` 集中收集。
- **发送端不可用会影响业务吗？** 不会。失败仅计数（`send_stats()`），本地写盘不受影响。

## 性能

v0.4.0 对热路径做了专项优化（秒级时间戳/键名/编码缓存、无转义快速路径、
SimpleQueue 写队列 + 屏障令牌 flush、缓冲写 handler、批量熵缓冲、ULID 查表
编码等），同机交错压测 **几何平均提升 +74.4%**：4 线程并发日志 **+352%**、
pipe 解析 **+577%**、ULID 生成 **+210%**、异步打点 +45%。完整数据与方法见
[benchmarks/REPORT.md](benchmarks/REPORT.md)；复现：`python benchmarks/bench.py`
与 `python benchmarks/report.py`。

## 项目结构

```
lizysdk/
├── src/lizysdk/
│   ├── ids/        # snowflake.py 雪花 · trace.py trace_id/uid · ulid.py 可排序ID · worker.py worker协商
│   ├── logs/       # formatter.py 格式与解析 · logger.py PipeLogger
│   │               # handlers.py 轮转/后台写入池/发送派发 · sender.py HTTP 发送 · context.py 上下文
│   ├── errors/     # codes.py 错误码 · registry.py 业务码注册表 · base.py AppError
│   │               # standard.py 子类 · utils.py wrap/ensure
│   └── ext/        # fastapi_adapter.py · flask_adapter.py（可选依赖组 [web]）
├── tests/          # test_ids / test_logs / test_errors / test_ext_web / test_integration
├── benchmarks/     # bench.py 压测脚本 · report.py 聚合对比 · REPORT.md 报告 · results/
├── examples/       # demo.py 基础三件套 · web_demo.py FastAPI 全家桶
└── docs/           # log-server-design.md 日志服务端设计
```

## 开发与测试

```bash
python -m pytest -v                                   # 全量测试（354 个）
python -m pytest tests/test_logs.py -v                # 单模块
python -m pytest --doctest-modules src/lizysdk/errors # 文档示例验证
python examples/web_demo.py                           # 全家桶端到端冒烟
```

贡献规范见 [AGENTS.md](AGENTS.md)（零依赖红线、契约不变量、并行开发规则）。

## 变更记录

- **0.4.0** —— 性能专项优化（同机交错压测几何平均 **+74.4%**，354 测试零回归）：
  秒级时间戳/键名校验/basename 缓存、转义与解析快速路径、写队列 SimpleQueue +
  屏障令牌 flush、异步模式缓冲写 handler（flush 语义不变）、随机 ID 线程本地
  批量熵缓冲、ULID 查表编码；JSONL 输出改为紧凑分隔符（`json.loads` 无差别）；
  新增 benchmarks/ 压测与报告
- **0.3.0** —— errors：业务错误码注册表 `register_code`/`unregister_code`/`registered_codes`、
  `to_dict(include_cause=...)`、FastAPI/Flask 适配器（`lizysdk.ext`，可选组 `[web]`）；
  ids：`new_sortable_id`（ULID 可排序 ID）、`resolve_worker_id`（env + 本机锁文件协商）
- **0.2.0** —— trace_id 默认 16 位、位数可选（8~64）；新增 `new_uid`；日志新增
  `sys_name` 标识、`json_format` JSONL 输出、`send_json`/`send_url` 打印完即发送、
  `send_stats` 诊断；`parse_line` 支持 pipe/JSON 双格式；包更名为 lizysdk
- **0.1.0** —— 首版：ids / logs / errors 三模块

License: MIT
