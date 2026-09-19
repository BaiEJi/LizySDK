# lizysdk

通用 Python 基础工具包：**唯一 ID / trace_id 生成 · 结构化日志（pipe/JSON 双格式 + 远程发送）· 标准化错误体系（含业务码注册表与 Web 框架适配）**。

- 核心纯标准库，**零第三方依赖**，业务代码可轻松接入；Web 适配器走可选依赖组 `lizysdk[web]`
- Python **3.9+**，全量类型注解（随包发布 `py.typed`）
- 线程安全（ID 生成器与日志写入均经并发验证）
- **353 个测试** + doctest 全绿
- 仓库：https://github.com/BaiEJi/LizySDK

## 安装

```bash
pip install -e .            # 本地开发安装（核心零依赖）
pip install -e .[dev]       # 附带 pytest 与 web 适配测试依赖
pip install -e .[web]       # FastAPI/Flask 适配器
```

## 快速开始

### 1. trace_id 与唯一 ID（位数可选）

```python
from lizysdk import new_trace_id, new_uid, new_id, new_prefixed_id, IDGenerator

new_trace_id()         # 'a3f0c2d4e5f60718'  默认 16 位小写 hex（secrets 随机）
new_trace_id(32)       # 位数可选：8~64（含奇数位）
new_uid()              # 通用唯一字符串 ID，默认 16 位，位数可选（8~64）
new_id()               # 7350283750400000001  雪花 64 位整数（位布局固定，位数不可调）
new_prefixed_id("ORD") # 'ORD_7350283750400000001'  业务前缀 ID

gen = IDGenerator(worker_id=3)   # 自定义 worker（0~1023），线程安全
gen.batch(10000)                 # 单次持锁批量生成

new_sortable_id()                # '01jb2xk3m8...q7' ULID 风格 26 字符，字典序即时间序
sortable_id_timestamp(uid)       # 反解该 ID 的 Unix 秒（含毫秒）
resolve_worker_id()              # 多实例部署免手工分配 worker_id：
                                 # 环境变量 LIZYSDK_WORKER_ID 优先，否则本机锁文件自动占位
```

雪花位布局：`[1 bit 保留][41 bit 毫秒时间戳][10 bit worker_id][12 bit 序列]`，
默认纪元 `2024-01-01 UTC`；小时钟回拨自旋等待，回拨超过 10ms 抛 `ClockBackwardsError`。

### 2. 结构化日志（sys_name 标识 · 双格式 · 打印完即发送）

一行日志的固定契约（`message` 恒为最后一段）：

```
LEVEL||TIMESTAMP||FILE:LINE||sys_name=xxx||k1=v1||k2=v2||message=<文本>
INFO||2026-09-19T10:30:00||app.py:42||sys_name=order-svc||user_id=123||action=login||message=user logged in
```

```python
from lizysdk import setup_logging, get_logger, bind_context, parse_line, send_stats

setup_logging(
    "logs", "app.log",
    sys_name="order-svc",       # 系统标识，注入每条日志（pipe 字段 / JSON 字段 / 发送 body）
    level="INFO",
    rotation="size", max_bytes=10*1024*1024, backup_count=5,   # 可选：size/time 轮转
    json_format=False,          # True 时文件/控制台输出 JSONL
    send_json=False,            # True 时打印完即发送 JSON 到 send_url
    send_url=None,              # 如 "http://log-collector:9200/ingest"（send_json=True 时必填）
    send_timeout=3.0,
)

log = get_logger(__name__)
log.info("user logged in", user_id=123, action="login")   # kv 任意传，按序输出

bind_context(trace_id=new_trace_id())   # contextvars 上下文字段，线程/协程隔离
log.error("db failed", exc_info=True)   # 异常栈转义进 message，仍是一行

record = parse_line(line)               # 行 → dict；自动识别 pipe 与 JSON 两种格式
send_stats()                            # {"sent": n, "failed": n, "last_error": ...} 发送诊断
```

**JSON 格式**（`json_format=True`，每行一个对象，`ensure_ascii=False`）：

```json
{"level": "INFO", "timestamp": "2026-09-19T10:30:00", "file": "app.py", "line": 42, "sys_name": "order-svc", "user_id": "123", "action": "login", "message": "user logged in"}
```

**打印完即发送**（`send_json=True`）：每条日志在写入完成后立即以 `POST application/json`
发送与 JSON 行同结构的对象（含 `sys_name` 与全部字段）；异步模式下随后台线程发送、不阻塞
业务；发送失败绝不影响本地写盘、绝不向业务抛异常，只计入 `send_stats()`。

能力一览：

| 能力 | 说明 |
|---|---|
| 轮转 | `rotation="size"`（按大小切片）/ `"time"`（按时间，`when` 同标准库）/ `"none"` |
| 线程安全 | 后台写入池（`queue.Queue` + N 个 daemon 线程），并发写入不丢行、不交错 |
| 异步 | `async_writer=True`（默认）业务线程不阻塞；`flush(timeout)` 同步落盘；atexit 优雅停机不丢日志 |
| 上下文 | `bind_context(**kv)` / `clear_context()`，基于 `contextvars` 自动附带 |
| 转义 | 值中 `\n` `\r` `|` 转义为字面量；key 须匹配 `^[A-Za-z_][A-Za-z0-9_.-]*$`；`message` 与 `sys_name` 为保留字 |

### 3. 标准化错误

```python
from lizysdk import AppError, ErrorCode, NotFoundError, wrap, ensure

raise NotFoundError(params={"resource": "订单"})
# AppError: [RESOURCE_NOT_FOUND] 资源不存在: 订单

err = AppError(ErrorCode.PARAM_TYPE_ERROR, params={"param": "age", "expected_type": "int"})
err.code          # 'PARAM_TYPE_ERROR'
err.http_status   # 400
err.to_dict()     # {'type': ..., 'code': ..., 'message': ..., 'http_status': ..., ...} 可直接 json.dumps
AppError.from_dict(err.to_dict())      # 往返重建

try:
    risky()
except Exception as inner:
    raise wrap(inner, code=ErrorCode.SERVICE_UNAVAILABLE, details={"host": "db-1"}) from inner

ensure(user.is_admin, ErrorCode.PERMISSION_DENIED, required_permission="admin")
```

内置 15 个错误码（参数/认证/授权/资源/限流/系统六组，含中文消息模板与 HTTP 状态）与
9 个标准子类：`ParamError` `AuthError` `PermissionDeniedError` `NotFoundError` `ConflictError`
`RateLimitError` `InternalError` `ServiceUnavailableError` `UpstreamTimeoutError`。

**业务错误码动态注册**（15 个通用码之外，业务码声明式扩展）：

```python
from lizysdk import register_code, AppError

register_code("PAY_BALANCE_NOT_ENOUGH", "余额不足: {amount} 元", 422)
raise AppError("PAY_BALANCE_NOT_ENOUGH", params={"amount": "3.5"})
# AppError: [PAY_BALANCE_NOT_ENOUGH] 余额不足: 3.5 元, http_status=422
```

解析优先级：**注册表 → 内置枚举 → 未知兜底 500**；`overwrite=True` 可覆盖内置码。
`err.to_dict(include_cause=True)` 可附带底层异常的类型/消息/完整 traceback。

**Web 框架适配器**（`pip install lizysdk[web]`，AppError 自动转 JSON 响应）：

```python
from fastapi import FastAPI
from lizysdk.ext import install_fastapi_handler     # Flask 同理 install_flask_handler

app = install_fastapi_handler(FastAPI(), include_generic=True)
# 抛出 AppError 子类 → 响应状态码 = http_status，body = err.to_dict()
# 未知异常（include_generic=True）→ 500 + INTERNAL_ERROR
```

### 4. 三者协同

```python
import lizysdk as bk

bk.setup_logging("logs", "app.log", sys_name="order-svc",
                 send_json=True, send_url="http://log-collector:9200/ingest")
bk.bind_context(trace_id=bk.new_trace_id())
log = bk.get_logger(__name__)

try:
    log.info("order received", order_no="SO-001", order_id=bk.new_prefixed_id("ORD"))
    ...
except bk.AppError as exc:
    log.error("order failed", code=exc.code, http_status=exc.http_status)
```

完整可运行示例见 [examples/demo.py](examples/demo.py)（`python examples/demo.py`）。

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
├── tests/          # test_ids / test_logs / test_errors / test_integration
└── examples/demo.py
```

## 开发与测试

```bash
python -m pytest -v                                   # 全量测试（225 个）
python -m pytest tests/test_logs.py -v                # 单模块
python -m pytest --doctest-modules src/lizysdk/errors # 文档示例验证
python examples/demo.py                               # 端到端冒烟
```

## 日志服务端

`send_json` 配套的自研接收端（SQLite / PostgreSQL 双存储、检索 API、保留策略）设计文档见
[docs/log-server-design.md](docs/log-server-design.md)。

## 变更记录

- **0.3.0** —— errors：业务错误码注册表 `register_code`/`unregister_code`/`registered_codes`、
  `to_dict(include_cause=...)`、FastAPI/Flask 适配器（`lizysdk.ext`，可选组 `[web]`）；
  ids：`new_sortable_id`（ULID 可排序 ID）、`resolve_worker_id`（env + 本机锁文件协商）
- **0.2.0** —— trace_id 默认 16 位、位数可选（8~64）；新增 `new_uid`；日志新增
  `sys_name` 标识、`json_format` JSONL 输出、`send_json`/`send_url` 打印完即发送、
  `send_stats` 诊断；`parse_line` 支持 pipe/JSON 双格式；包更名为 lizysdk
- **0.1.0** —— 首版：ids / logs / errors 三模块

License: MIT
