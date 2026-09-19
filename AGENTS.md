# AGENTS.md —— lizysdk 仓库协作指南（面向 AI agent 与人类贡献者）

仓库：https://github.com/BaiEJi/LizySDK（本地目录名可能仍为 `basekit`，以包名 `lizysdk` 为准）。

## 项目是什么

`lizysdk` 是一个通用 Python 基础工具包，只做三件事，边界严格：

1. **`lizysdk.ids`** —— trace_id / 通用唯一 ID（hex 字符串，**位数可选 8~64，trace_id 默认 16**）、雪花唯一 ID（64 位整数 / 业务前缀 ID）、ULID 可排序 ID（`new_sortable_id`）、worker_id 自动协商（`resolve_worker_id`）
2. **`lizysdk.logs`** —— 结构化日志（**sys_name 系统标识**、pipe/JSON 双格式、轮转、后台写入池、线程安全、contextvars 上下文、**打印完即发送 JSON 到远端**）
3. **`lizysdk.errors`** —— 标准化错误体系（错误码枚举、**业务码动态注册表**、AppError、标准子类、wrap/ensure）
4. **`lizysdk.ext`** —— Web 框架适配器（FastAPI/Flask 的 AppError 异常处理，**可选依赖组 `[web]`**）

设计参考自 [BaiEJi/LzyTools](https://github.com/BaiEJi/LzyTools) 的 `basic_tool/id_generator` 与 `basic_tool/errors`，为独立发布重新实现（不共享代码）。

## 硬性规范（改代码前必读，违反即打回）

- **零第三方运行时依赖**：只用标准库。测试仅依赖 pytest（发送测试用 `http.server` 本机起接收端，**禁止真连外网**）。
- **Python 3.9+ 兼容**：每个模块首行 `from __future__ import annotations`；禁止运行时使用 3.10+ 语法（`match`、裸 `X | Y` 传参等）。
- **全量类型注解**；公开 API 带中文 docstring（含可执行示例者优先，doctest 必须能过）。
- **标识符英文、docstring/消息中文**；错误消息模板为中文。
- **子包禁止叫 `logging`**（与标准库冲突），统一叫 `logs`。
- **分层无循环导入**：errors 内部 `codes/registry → base → standard/utils`；三个核心子包之间互不依赖（协同发生在应用层，如 `bind_context(trace_id=new_trace_id())`）；`ext` 只依赖 errors。
- **`ext` 子包铁律**：模块级**禁止** import 第三方库（fastapi/flask import 必须放在安装函数体内，未装时 `ImportError` 给中文提示 `pip install "lizysdk[web]"`）；核心包零依赖的红线不可破。
- **线程安全**：一切跨线程共享状态必须持锁或使用 `contextvars`/`queue.Queue`，禁止全局可变状态裸奔。
- **对外抛错统一 ValueError**（参数校验）；发送等 I/O 失败绝不向业务抛异常。

## 契约不变量（改动不得破坏，测试会拦）

### 日志行格式（双格式）

pipe（默认）：

```
LEVEL||TIMESTAMP||FILE:LINE||sys_name=xxx||k1=v1||k2=v2||message=<文本>
```

- 分隔符恒为 `||`；顺序恒为 级别 → 时间（本地 `%Y-%m-%dT%H:%M:%S` 秒级）→ 调用方 `basename:行号` → **`sys_name=`（恒存在）** → 业务 kv（按传入顺序）→ **`message=` 恒为最后一段**。
- JSON 格式（`json_format=True`）：每行一个对象 `{"level","timestamp","file","line","sys_name",kv...,"message"}`，`ensure_ascii=False`、UTF-8、`message` 恒存在；kv 值先转 str（与 pipe 一致）。
- 转义（pipe 的值与 message）：`\n`→字面 `\n`、`\r`→字面 `\r`、`|`→`\|`；解析侧必须用**转义感知切分**（朴素 `split("||")` 会在「值以 `|` 结尾」处误切）。
- key 必须匹配 `^[A-Za-z_][A-Za-z0-9_.-]*$`；保留字 `message` 与 `sys_name`，作为业务 kv/上下文字段传入抛 `ValueError`。
- `parse_line()` 自动识别双格式（strip 后以 `{` 开头走 JSON 分支），与格式化互逆；**旧版无 sys_name 的 pipe 行会抛 ValueError（预期行为）**。

### 发送契约（send_json）

- `setup_logging` 关键字参数：`sys_name="app"`、`json_format=False`、`send_json=False`、`send_url=None`、`send_timeout=3.0`。
- `send_json=True` 时 `send_url` 必须为 `http(s)://` 开头非空 str；每条日志**写入完成后**立即 `POST application/json`，body 与 JSON 行同构（含 sys_name）。
- `_SendHandler` 排在文件/控制台 handler 之后；body 恒由 `JsonLogFormatter` 生成（与本地 `json_format` 开关无关）。
- 异步模式经 `SendDispatcher`（FIFO 守护线程）逐条发送；同步模式直发但吞异常只计数。
- 失败（连接拒绝/超时/DNS/非 2xx）绝不影响本地写盘、绝不抛给业务；`send_stats()` 返回 `{"sent","failed","last_error"}`（锁保护计数）。
- `flush`/atexit 语义：先排空写队列、再尽力排空发送队列（受 `send_timeout` 约束）。

### 错误码解析优先级（registry 引入后）

`AppError` 对 code 的解析顺序恒为：**业务码注册表 → 内置 ErrorCode 枚举 → 未知兜底 500**。
`register_code(code, template, http_status, *, overwrite=False)`：码名须匹配 `^[A-Z][A-Z0-9_]{1,63}$`、
状态 100~599、与注册表或内置枚举重名都需 `overwrite=True`；全操作持锁。`to_dict()` 默认六键不变，
`include_cause=True` 追加 `cause`（无 cause 时为 `None`）；`from_dict` 忽略 `cause` 键。

### API 契约

顶层 `lizysdk.__init__` 聚合导出三个子包的全部公开名字（`__all__` 分组注释）。**新增/改名公开 API 的联动步骤**：子模块实现 → 子包 `__init__` 导出 → 顶层 `__init__` 导出 → `tests/test_integration.py` 断言 `__all__` 完整。

## 各子模块架构与关键决策

### ids（`src/lizysdk/ids/`）

- `trace.py`：`_random_hex(length)` 私有辅助（`token_hex(ceil(n/2))[:n]`，支持奇数位）被 `new_trace_id(length=16)` 与 `new_uid(length=16)` 共享（DRY）；length 校验：排除 bool、必须 int、`8 <= length <= 64`，否则中文 ValueError。
- `snowflake.py`：位宽常量 `1/41/10/12`（命名常量，勿写魔数）；移位 `WORKER_ID_SHIFT=12`、`TIMESTAMP_SHIFT=22`；默认纪元 `DEFAULT_EPOCH_MS = 1704067200000`（2024-01-01 UTC）。**雪花位数不可调**（位运算本质决定），docstring 已注明。
- 线程安全：`new`/`batch` 全路径持 `threading.Lock`，`batch` 单次持锁。
- 时钟回拨：≤10ms 自旋等待；>10ms 抛 `ClockBackwardsError(ValueError)`。
- **可测试接缝**：时间通过模块级 `_current_ms()`（snowflake）与 `_now_ms()`（ulid）获取，测试用 monkeypatch + 脚本化时钟做确定性验证——改实现时必须保留该接缝。
- `ulid.py`：26 字符 Crockford base32 小写（字母表 ASCII 升序 ⇒ 定长字典序==数值序）；前 10 字符 48bit 毫秒时间戳 + 后 16 字符 80bit 随机；**同毫秒内随机段 +1 递增、时钟回拨沿用上一毫秒继续递增**，全程持锁保证「生成顺序==字典序（单线程严格递增）+ 全局唯一」。
- `worker.py`：env（默认 `LIZYSDK_WORKER_ID`）显式指定优先且**完全不碰文件**；否则 `{lock_dir}/worker_{id}.json` 用 `os.open(O_CREAT|O_EXCL)` 原子占位、被占顺延、0..1023 全满抛 ValueError；陈旧回收**只按 mtime 年龄**（严禁在 Windows 用 `os.kill(pid,0)` 探活——sig=0 会触发 TerminateProcess）；atexit 释放、同进程重复调用幂等。

### logs（`src/lizysdk/logs/`）

- `PipeLogger._emit` 统一路径：校验 → `sys._getframe(2)` 取真实调用方帧（**不要**换回 `findCaller`/stacklevel，3.10/3.11 语义有差异）→ 调用方线程内合并 `{**context, **fields}` 烘焙进 `record.bk_fields`（保证经后台线程格式化不丢）。
- 已规避的标准库坑（勿回退）：① `QueueHandler.prepare` 会剥离 `exc_info`，需子类透传；② 3.9/早 3.10 的 `QueueListener._monitor` 不调 `task_done`，`flush` 依赖自持 monitor 的 `queue.join()`；③ `pool_size=1` 用 `QueueListener`，`>1` 用自实现 worker 池 + 文件写锁。
- `setup_logging` 幂等：先拆旧 handler、停后台线程再重建；atexit 钩子保证退出不丢日志。
- 已知契约自洽决策：`PipeLogger` 消息形参名为 `msg`（沿用标准库），使 `log.info("m", message="x")` 落入 `**fields` 由校验器抛 `ValueError`（若形参叫 `message` 会先抛 `TypeError`，违背契约）。
- sender.py / handlers.py 分工：`JsonSender`（urllib.request + 锁保护计数）、`SendDispatcher`（FIFO 守护线程）；`_SendHandler` 挂在 handler 链末尾。

### errors（`src/lizysdk/errors/`）

- `ErrorCode(str, Enum)`：成员由 `(码名, 中文模板, HTTP 状态)` 三元组构造，属性 `template`/`http_status`，值即码名，可直接 JSON 序列化。
- 模板渲染用 `format_map` + 安全字典：缺键保留 `{占位符}` 原样、多余键忽略，**绝不抛 KeyError**；显式 `message` 优先于模板。
- `from_dict` 依赖 `__init_subclass__` 自动维护的类型注册表还原子类，未知/缺失 `type` 回落 `AppError` 本体。
- `wrap()` 用 `setdefault` 写 `details["original_type"]`，不覆盖调用方已提供的键；`ensure()` 接受 码/实例 两种形态。
- `registry.py` 只 import `codes.py`（`base.py` 单向引用 `registry`，无循环）；`ext/` 的两个适配器返回 `app` 可链式，`include_generic=True` 时额外把未知异常 `wrap()` 成 500。

## 开发工作流

```bash
cd C:/Users/Lizy/Desktop/Code/basekit
python -m pytest -v                                    # 全量测试（当前 353 个，必须全绿）
python -m pytest tests/test_logs.py -v                 # 单模块
python -m pytest --doctest-modules src/lizysdk/errors  # docstring 示例验证
python examples/demo.py                                # 端到端冒烟
python -m pip install -e .                             # 开发安装（可省，conftest 已注入 src）
```

- 环境：Windows + Git Bash，Python 3.10（miniconda）。测试**不依赖安装**：根目录 `conftest.py` 将 `src` 注入 `sys.path`。
- 日志类测试一律用 pytest `tmp_path` 存放日志文件，禁止污染仓库；轮转测试用小 `max_bytes`/`when="s"` 真实触发。
- 并行/多人开发规则：**只改自己负责的子包目录 + 对应测试文件**；`pyproject.toml`、顶层 `__init__.py`、`conftest.py`、`README.md`、`AGENTS.md` 由集成者统一维护。
- 新增功能流程：子包内实现（含测试）→ 子包 `__init__` 导出 → 顶层 `__init__` 联动 → `test_integration.py` 补断言 → 同步 README 与本文件 → 版本号两处同步（见下）。

## 版本与发布

- 版本位于 `pyproject.toml` 与 `src/lizysdk/__init__.py` 的 `__version__`，**两处必须同步**；行为变更在 README「变更记录」补一行。
- 构建后端 setuptools，src 布局，`py.typed` 随包发布。
- Git：主分支 `main`，remote `origin = https://github.com/BaiEJi/LizySDK`。提交信息用中文、`feat:/fix:/docs:` 前缀。

## 变更记录

- **0.3.0** —— errors：业务码注册表、include_cause、ext FastAPI/Flask 适配器；ids：ULID 可排序 ID、worker_id 协商
- **0.2.0** —— trace_id 默认 16 位可选位数；新增 `new_uid`；日志 sys_name/JSONL/send_json 发送/send_stats；包更名 lizysdk
- **0.1.0** —— 首版（原包名 basekit）
