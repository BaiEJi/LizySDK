# AGENTS.md —— lizysdk 仓库协作指南（面向 AI agent 与人类贡献者）

仓库：https://github.com/BaiEJi/LizySDK（本地目录名可能仍为 `basekit`，以包名 `lizysdk` 为准）。

## 项目是什么

`lizysdk` 是一个通用 Python 基础工具包，只做三件事，边界严格：

1. **`lizysdk.ids`** —— trace_id / 通用唯一 ID（hex 字符串，**位数可选 8~64，trace_id 默认 16**）、雪花唯一 ID（64 位整数 / 业务前缀 ID）、ULID 可排序 ID（`new_sortable_id`）、worker_id 自动协商（`resolve_worker_id`）
2. **`lizysdk.logs`** —— 结构化日志（**sys_name 系统标识**、pipe/JSON 双格式、轮转、后台写入池、线程安全、contextvars 上下文、**打印完即发送 JSON 到远端**）
3. **`lizysdk.errors`** —— 标准化错误体系（错误码枚举、**业务码动态注册表**、AppError、标准子类、wrap/ensure）
4. **`lizysdk.ext`** —— Web 框架适配器（FastAPI/Flask 的 AppError 异常处理，**可选依赖组 `[web]`**）
5. **`lizysdk.pools`** —— 统一并发池（`create_pool(kind)`：thread/async/process；统一 submit/map/shutdown/stats/add_hook；重试/背压/超时/ctx 传播/八事件钩子；设计契约见 `docs/pools-design.md`）
6. **`lizysdk.shell`** —— shell 执行包装（`run`：恒 shell=False、超时终止、富错误、标准 logging 命令日志；设计契约见 `docs/shell-dist-design.md`）
7. **`lizysdk.dist`** —— 分布式原语（Redis 滑动窗口计数器 ZSET+Lua、分布式锁 SET NX PX + token + Lua；**可选组 `[redis]`，模块级懒加载**；设计契约见 `docs/shell-dist-design.md`）
8. **`lizysdk.notify`** —— 通知中心（钉钉/飞书/企微/邮件/自定义 webhook 五渠道；级别路由、静默期、频控、异步投递；零依赖；设计契约见 `docs/notify-design.md`）

设计参考自 [BaiEJi/LzyTools](https://github.com/BaiEJi/LzyTools) 的 `basic_tool/id_generator` 与 `basic_tool/errors`，为独立发布重新实现（不共享代码）。

## 硬性规范（改代码前必读，违反即打回）

- **零第三方运行时依赖**：只用标准库。测试仅依赖 pytest（发送测试用 `http.server` 本机起接收端，**禁止真连外网**）。
- **Python 3.9+ 兼容**：每个模块首行 `from __future__ import annotations`；禁止运行时使用 3.10+ 语法（`match`、裸 `X | Y` 传参等）。**多版本矩阵已实测**（3.9~3.13 全绿，见 README 支持矩阵）；新增代码/依赖后必须保持 3.9 可用，动语法或依赖时跑 `scripts/ci_matrix.sh` 验证。
- **全量类型注解**；公开 API 带中文 docstring（含可执行示例者优先，doctest 必须能过）。
- **标识符英文、docstring/消息中文**；错误消息模板为中文。
- **子包禁止叫 `logging`**（与标准库冲突），统一叫 `logs`。
- **分层无循环导入**：errors 内部 `codes/registry → base → standard/utils`；各核心子包（ids/logs/errors/pools/shell/dist）之间**互不依赖**（协同发生在应用层，如 `bind_context(trace_id=new_trace_id())`）；`ext` 只依赖 errors。**懒加载红线**：`lizysdk.dist` 模块级禁止 import redis（函数体内懒加载，未装时 ImportError 中文提示 `pip install "lizysdk[redis]"`）；`lizysdk.shell` 恒 `shell=False` 且不得暴露 shell 开关。子模块日志一律走标准 logging（被 logs 子包统一格式捕获），不得 import lizysdk.logs。
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
- `trace.py`：随机 hex 来自**线程本地批量熵缓冲**（`_ENTROPY_LOCAL`，补块一次 urandom+整体转 hex，日常一次字符串切片；勿改回进程级共享+锁——Windows 锁开销会吃掉收益）；校验慢路径抽离 `_raise_bad_length`，热路径 `type() is int` 精确匹配（天然排除 bool）。
- `ulid.py`：编码热路径 = 时间戳段按毫秒单槽缓存（`_TS_PART_CACHE`）+ 80bit 随机段 2 字符查表（`_PAIR_TABLE`，导入期用权威实现 `_encode_impl` 构建——注意表的构建必须在函数定义之后）。

### logs（`src/lizysdk/logs/`）

- `PipeLogger._emit` 统一路径：校验 → `sys._getframe(2)` 取真实调用方帧（**不要**换回 `findCaller`/stacklevel，3.10/3.11 语义有差异）→ 调用方线程内合并 `{**context, **fields}` 烘焙进 `record.bk_fields`（保证经后台线程格式化不丢）。
- 已规避的标准库坑（勿回退）：① `QueueHandler.prepare` 会剥离 `exc_info`，需子类透传（现进一步重载 `emit/handle/enqueue` 直达 `queue.put`）；② 写队列：**pool_size=1（默认）用 `queue.SimpleQueue`（C 实现）+ 自持 monitor + 屏障令牌 `_FlushBarrier` flush**——put 不持 Python 锁、免逐条 task_done 共享计数锁；**pool_size>1 必须用 `queue.Queue` + task_done/join**（多消费者下「令牌被消费 ≠ 更早记录已写完」，屏障有竞态，曾有测试抓到 3 条丢失）；③ 文件 handler 分层：异步模式用缓冲写 `_Buffered*FileHandler`（不逐条 flush，落盘由 `_Backend.flush` 显式刷流/轮转/停机承接），**同步模式必须用标准库 handler 逐条刷盘**（「调用返回即落盘」是测试固化的契约）。
- 热路径缓存契约（新增，改动勿破坏）：秒级时间戳单槽缓存（仅默认 `TIMESTAMP_FORMAT` 生效，自定义 datefmt 走标准库）、`_VALIDATED_KEYS` 合法 key 缓存（上限 4096）、`_BASENAME_CACHE`、`sys_name` 段预计算（**格式化器初始化后 sys_name 视为只读**）、上下文零拷贝 `peek_context()`（返回字典不得修改）、JSONL 紧凑分隔符输出。
- `setup_logging` 幂等：先拆旧 handler、停后台线程再重建；atexit 钩子保证退出不丢日志。
- 已知契约自洽决策：`PipeLogger` 消息形参名为 `msg`（沿用标准库），使 `log.info("m", message="x")` 落入 `**fields` 由校验器抛 `ValueError`（若形参叫 `message` 会先抛 `TypeError`，违背契约）。
- sender.py / handlers.py 分工：`JsonSender`（urllib.request + 锁保护计数）、`SendDispatcher`（FIFO 守护线程）；`_SendHandler` 挂在 handler 链末尾。

### pools（`src/lizysdk/pools/`）

- 契约唯一来源：`docs/pools-design.md`；改接口先改文档。
- 自持异常族（`PoolError/PoolClosedError/PoolRejectedError`），**禁止 import 其他 lizysdk 子包**。
- 关键决策（勿回退）：① ThreadPool 用**自管 worker 线程**而非 `ThreadPoolExecutor`（stdlib 3.9+ 线程非 daemon 且不可控，daemon 契约要求自管，docstring 已记录该偏离）；② `max_tasks_per_worker` 仅 process 池支持且走 `multiprocessing.Pool(maxtasksperchild)` 双路径（3.10 的 `ProcessPoolExecutor` 无此参数）；③ 超时语义分池诚实声明：async=`asyncio.wait_for` 真取消（统一抛内建 `TimeoutError`），thread/process=结果等待超时不可中断；④ 钩子异常一律吞掉计数（`stats()["hook_errors"]`）；⑤ 闸门/计数器全程持锁，**任何路径下用户 Future 不得悬置**（曾有计数器名前缀错误、async 取消令牌泄漏、进程池取消悬置三个真实缺陷，安全网回调是兜底，勿删）。
- 测试注意：process 套件用 session 级 fixture 复用池（Windows spawn 慢）；并发用例改完必须连跑 3 轮防抖。

### shell / dist（`src/lizysdk/shell/` · `src/lizysdk/dist/`）

- 契约唯一来源：`docs/shell-dist-design.md`；改接口先改文档。
- shell：安全红线恒 `shell=False`（str argv 走 `shlex.split`，不支持管道/重定向）；超时语义依赖 `subprocess.run(timeout=...)`（Windows TerminateProcess）；`ShellTimeoutError` 携带部分输出。
- dist 窗口计数器：ZSET+Lua 单脚本原子四步（ZADD→ZREMRANGEBYSCORE→ZCARD→PEXPIRE），窗口为 `(now-window, now]` 含边界剔除；member 唯一性靠 `{now}:{pid}:{token_hex}`；时间接缝是模块级 `_now()`（测试 monkeypatch 用，勿删）。内存 O(N)。
- dist 锁：语义对齐 redis-py Lock（`SET NX PX` + token + Lua compare-token 释放/续期；**不可重入**；`LockNotOwnedError` 释放他人锁）；阻塞轮询带随机抖动防惊群；**效率锁非正确性锁**（Kleppmann 注记勿删）。测试用 fakeredis（Lua 需 lupa），锁 TTL 过期类用例用极短 timeout + 真实 sleep（fakeredis TTL 不受 monkeypatch 时间影响）。
- dist Redis 套件（v0.8.0，契约唯一来源 `docs/redis-suite-design.md`）：六件算法全部对齐开源实现，**禁止自创语义**——RLock=Redisson RedissonLock（hash field=`{instance_uuid}:{thread_ident}` 重入计数；成功哨兵 -1；看门狗每 `watchdog_timeout/3` 续期、release 到 0 层停、`__del__` 兜底）；LeaderElector=K8s Lease（compare-holder Lua 续期；**先降级再回调**；回调内调 stop 抛 LockError 防自 join 死锁）；IdempotentKey=Stripe 两态（processing→done；fail 是 compare-holder cjson Lua，终态 done 不许拆）；ReliableQueue=redis.io 官方 pattern（LPUSH/LMOVE→processing/LREM ack/recover 全量搬回，at-least-once 明示；BLMOVE 小步循环步长 `_BLOCK_STEP`）；DelayQueue=Redisson RDelayedQueue（ZSET score=到期分 + `_MOVE_DUE_LUA` ZREM 成功才 LPUSH，并发搬运不重不漏）；Leaderboard=redis.io ZSET（**1-based 名次**是显式声明的偏差；同分 member 字典序原生保留）。时间接缝 `_now()`/`_sleep()` 勿删；冻结 `_now` 的测试只能走非阻塞入口（timeout=0）。

### notify（`src/lizysdk/notify/`）

- 契约唯一来源：`docs/notify-design.md` §2 渠道表——**payload 与签名逐字节对齐官方 API**，改渠道实现必须同步文档并跑逐字节断言。
- 签名考点（勿混）：钉钉 `hmac(key=secret, msg=f"{ts_ms}\n{secret}")`→b64→quote_plus 拼 URL；飞书 `hmac(key=f"{ts}\n{secret}", msg=b"")`→b64 进 body（ts 秒）——**两者算法产物互不相同，防串用回归钩子勿删**；企微无签名。
- 测试离线红线：HTTP 渠道一律 `transport(url, payload, headers, timeout)->(status, text)` 注入；邮件 `mailer(msg_bytes, sender, to)` 注入或 monkeypatch smtplib——**禁止真实网络**。
- 中心语义：抑制判断（静默期/频控）在**入队前**；频控 LRU 上限 1024；同步模式返回 `{渠道: bool}` 不抛发送异常；`last_error` 记最近一次失败不因成功清除。

### errors（`src/lizysdk/errors/`）

- `ErrorCode(str, Enum)`：成员由 `(码名, 中文模板, HTTP 状态)` 三元组构造，属性 `template`/`http_status`，值即码名，可直接 JSON 序列化。
- 模板渲染用 `format_map` + 安全字典：缺键保留 `{占位符}` 原样、多余键忽略，**绝不抛 KeyError**；显式 `message` 优先于模板。
- `from_dict` 依赖 `__init_subclass__` 自动维护的类型注册表还原子类，未知/缺失 `type` 回落 `AppError` 本体。
- `wrap()` 用 `setdefault` 写 `details["original_type"]`，不覆盖调用方已提供的键；`ensure()` 接受 码/实例 两种形态。
- `registry.py` 只 import `codes.py`（`base.py` 单向引用 `registry`，无循环）；`ext/` 的两个适配器返回 `app` 可链式，`include_generic=True` 时额外把未知异常 `wrap()` 成 500。

## 开发工作流

```bash
cd C:/Users/Lizy/Desktop/Code/basekit
python -m pytest -v                                    # 全量测试（当前 824 个，必须全绿）
python -m pytest tests/test_logs.py -v                 # 单模块
python -m pytest --doctest-modules src/lizysdk/errors  # docstring 示例验证
python examples/demo.py                                # 端到端冒烟
python benchmarks/bench.py --out benchmarks/results/x.json  # 性能压测
python benchmarks/report.py                            # 压测聚合对比（报告见 benchmarks/REPORT.md）
scripts/ci_matrix.sh                                   # 多版本矩阵（3.9/3.11/3.12/3.13，conda 环境驱动）
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

- **0.8.0** —— dist 扩为 Redis 能力套件：RLock 可重入锁+看门狗（Redisson）/ LeaderElector 领导选举（K8s Lease）/ IdempotentKey 幂等键（Stripe）/ ReliableQueue 可靠队列（redis.io）/ DelayQueue 延迟队列（Redisson RDelayedQueue）/ Leaderboard 排行榜（redis.io）；150 新测试（全 fakeredis 离线）；fakeredis 口径压测（benchmarks/redis_bench.py + REDIS_REPORT.md）
- **0.7.0** —— 新增 lizysdk.notify 通知中心（五渠道/路由/静默期/频控/异步重试；参考 Apprise/Grafana；docs/notify-design.md；147 离线测试）
- **0.6.0** —— 新增 lizysdk.shell（run/富错误/结构化命令日志）与 lizysdk.dist（Redis 滑动窗口计数器 + 分布式锁，可选组 [redis] 懒加载）；对比参考 sh/plumbum/limits/redis-py Lock/Redlock（docs/shell-dist-design.md）；92 新测试；多版本矩阵（3.9~3.13）随 0.5.x 建立
- **0.5.0** —— 新增 lizysdk.pools 统一并发池（设计契约 docs/pools-design.md；78 新测试；参考 concurrent.futures/pebble/anyio）
- **0.4.0** —— 性能专项：热路径缓存/快速路径、SimpleQueue+屏障令牌、缓冲写 handler（分层语义）、熵缓冲、ULID 查表；JSONL 紧凑输出；新增 benchmarks（几何平均 +74.4%，方法与数据见 benchmarks/REPORT.md）
- **0.3.0** —— errors：业务码注册表、include_cause、ext FastAPI/Flask 适配器；ids：ULID 可排序 ID、worker_id 协商
- **0.2.0** —— trace_id 默认 16 位可选位数；新增 `new_uid`；日志 sys_name/JSONL/send_json 发送/send_stats；包更名 lizysdk
- **0.1.0** —— 首版（原包名 basekit）
