# lizysdk.shell / lizysdk.dist 设计文档（shell 执行包装 · 滑动窗口计数器 · 分布式锁）

> 版本：v1（2026-09-19）· 目标版本 lizysdk 0.6.0
> 定位：shell 为零依赖核心件；窗口计数器与分布式锁属 **`lizysdk[redis]` 可选依赖组**，
> 模块级懒加载导入（未装 redis 时 `import lizysdk.dist` 不报错，使用时给中文安装提示）。

## 1. 开源参考与选型对比

### 1.1 shell 执行包装

| 参考 | 取 | 舍 |
|---|---|---|
| `subprocess.run`（stdlib） | API 语义基座：argv 列表、timeout、check、capture_output、CompletedProcess 形状 | 直接暴露裸 API——缺日志/耗时/富错误/安全拆分 |
| [sh](https://github.com/amoffat/sh) | 富错误对象（ErrorReturnCode 携带完整 stdout/stderr 与命令）、命令即函数的体验 | 动态属性魔法（`sh.git.status`）与深度包装——超出薄包装定位，且 Windows 兼容差 |
| [plumbum](https://plumbum.readthedocs.io) | 本地 shell 抽象的清晰分层 | 管道/远端 shell 能力——本模块不做管道编排 |

**决策**：`subprocess.run` 之上的**薄加固层**——永远 `shell=False`（str 参数用 `shlex.split`
安全拆分并文档说明）、超时两段终止（terminate→kill 宽限）、执行自动经**标准 logging**
输出结构化命令日志（经 `lizysdk.logs.setup_logging` 统一格式捕获，零代码依赖——与
uvicorn/httpx 等第三方日志同机制）、富错误 `ShellError`（对齐 sh 的错误完整性）。

### 1.2 滑动窗口计数器（Redis）

| 方案 | 精度 | 内存 | 边界 |
|---|---|---|---|
| 固定窗口（INCR+EXPIRE） | 低 | O(1) | 窗口交界双倍突发 |
| **滑动日志（ZSET+Lua）** | **精确** | O(N)（N=窗口内事件数） | 窗口内任何时刻恰好限速 |
| 近似滑动（双窗加权，Cloudflare） | 中 | O(1) | 实现复杂，误差 ~10% |
| 令牌桶 / GCRA（redis-cell） | 平滑 | O(1) | 语义不同（突发容量） |

**决策**：采用 [limits 库](https://github.com/alisaifee/limits) moving-window 同款 **ZSET+Lua
原子脚本**（[redis.io 官方限流教程](https://redis.io/tutorials/howtos/ratelimiting) 的标准
滑动窗口形态）：ZADD 当前时间戳（唯一 member）→ ZREMRANGEBYSCORE 清理窗口外 →
ZCARD 计数 → PEXPIRE 保底过期，四步一个 Lua 脚本保证原子性。窗口为**调用方计数的
通用滑动窗口**（不止限流：也可做「近 5 分钟错误数」这类统计），限流判定只是
`allow(key, limit)` 的薄糖。

### 1.3 分布式锁（Redis）

| 参考 | 取 | 舍 |
|---|---|---|
| [redis-py 内置 Lock](https://redis-py.readthedocs.io)（事实标准） | `SET NX PX` + 随机 token + Lua compare-and-delete 释放 + Lua 续期；不可重入；`LockNotOwnedError` 语义 | 直接转用其类——我们要自有异常族与中文错误 |
| [Redlock](https://redis.io)（antirez 多节点多数派） | 算法思想与 fencing 讨论 | 多节点多数派——v1 单实例即可；写入文档作 v2 演进 |
| [pottery](https://pottery.readthedocs.io) | 易用性封装风格 | 附加抽象层 |

**决策**：单实例锁，语义对齐 redis-py Lock：`SET key token NX PX ttl` 原子获取；释放/
续期走 Lua 校验 token（防「误删他人锁」经典事故）；**不可重入**（文档明示）；
[Kleppmann 争议](https://martin.kleppner.com)（GC 停顿下锁过期）的立场写入 docstring：
**本锁是「效率锁」不是「正确性锁」**——需要严格互斥正确性的场景请在业务侧加 fencing
token / 数据库约束。

## 2. API 契约

### 2.1 `lizysdk.shell`

```python
from lizysdk.shell import run, ShellResult, ShellError, ShellTimeoutError

res = run("git", "status", timeout=10)          # -> ShellResult(ok/returncode/stdout/stderr/elapsed_ms/argv)
res = run("ls -la | head" if False else ["ls", "-la"])   # str 参数将被 shlex.split 安全拆分（不支持管道/重定向，文档明示）
run("ping", "-n", "1", check=True)              # 非零退出码抛 ShellError（含全部上下文）
run("cat", input="hello", capture=True, env={"K": "V"}, cwd=".", encoding="utf-8")
```

- `run(argv, *, timeout=None, check=False, capture=True, input=None, env=None, cwd=None, encoding="utf-8", log=True) -> ShellResult`
- 安全红线：**恒 `shell=False`**；`env` 为**合并**语义（`{**os.environ, **env}`）
- 超时：terminate → 1s 宽限 → kill，抛 `ShellTimeoutError`（携带已捕获输出）
- 日志：`logging.getLogger("lizysdk.shell")` 输出 INFO（argv、elapsed_ms、returncode），
  失败 WARN——标准 logging 通道，被 lizysdk.logs 统一格式捕获
- `ShellResult` 冻结 dataclass + `ok` 属性；`ShellError` 属性同 Result 并有中文 `__str__`

### 2.2 `lizysdk.dist` —— SlidingWindowCounter

```python
from lizysdk.dist import SlidingWindowCounter

counter = SlidingWindowCounter(client, window=60)          # 秒窗口；client 为 redis.Redis 实例
n = counter.incr("user:1:api")        # -> 窗口内当前计数（含本次），原子
c = counter.count("user:1:api")       # 只读计数（同样原子清理后计数）
ok = counter.allow("user:1:api", 100) # 薄糖：incr 后 <= limit
counter.reset("user:1:api")           # 删除窗口键
SlidingWindowCounter.from_url("redis://localhost:6379/0", window=60)   # 懒加载构造
```

- 键结构：`{prefix}:{key}` 的 ZSET，member=`{now}:{pid}:{token_hex}`（唯一），score=now
- `window > 0` 校验；时间接缝：模块级 `_now()` 可 monkeypatch（测试冻结时间）
- 内存注记：O(N) 事件数——高频大窗口场景换固定窗口/令牌桶（文档对比表）

### 2.3 `lizysdk.dist` —— DLock

```python
from lizysdk.dist import DLock, LockError, LockTimeoutError, LockNotOwnedError

with DLock(client, "job:42", timeout=10):        # ttl 秒
    do_job()
lock = DLock(client, "job:42", timeout=10, blocking_timeout=5)
lock.acquire()          # 阻塞获取；超时抛 LockTimeoutError；acquire(blocking=False) 立即返回 bool
lock.extend(10)         # -> bool（token 校验失败/锁已失为 False）
lock.release()          # 非持有（无锁/token 不符）抛 LockNotOwnedError
lock.locked()           # -> bool（任意持有者视角）
```

- 获取：`SET {prefix}:{name} token NX PX ttl`；token 为 `secrets.token_hex(16)`
- 释放/续期：Lua compare-token 脚本（对齐 redis-py 行为）
- 阻塞获取轮询间隔 0.05~0.2s 随机抖动（防惊群）
- 不可重入；「效率锁而非正确性锁」警示进 docstring

## 3. 架构约定

- `lizysdk.shell` 零依赖、纯标准库；`lizysdk.dist` 模块级**不 import redis**（函数体内
  懒加载，ImportError 中文提示 `pip install "lizysdk[redis]"`）
- 两模块自持异常族，不依赖其他 lizysdk 子包；日志一律走标准 logging（被 logs 子包统一捕获）
- pyproject：新增 `redis = ["redis>=4.2"]` 可选组；dev 组追加 `redis>=4.2`、`fakeredis[lua]>=2.20`

## 4. 测试计划（fakeredis + lupa 驱动 Lua，全离线可复现）

- **shell**：成功/非零退出（check 两种取值）/超时终止（睡眠进程超时抛 ShellTimeoutError）/
  str argv 拆分 / input、env 合并、cwd / capture=False / 日志发出断言（caplog 捕获标准
  logging）/ ShellError 属性与中文消息 / Windows 真命令（ping/timeout 等）
- **窗口计数器**：incr 单调计数 / 窗口过期剔除（monkeypatch _now 推进时间）/ count 只读 /
  allow 边界（==limit 通过、+1 拒绝）/ 多键独立 / reset / TTL 已设置 / 并发 8 线程 incr
  计数守恒（无丢失）/ from_url 懒加载 / 参数校验（window<=0）
- **锁**：获取-释放基本流 / 互斥（第二个 Lock 超时抛 LockTimeoutError）/ 非阻塞立即
  False / token 保护（篡改他人 token 后 release 抛 LockNotOwnedError）/ TTL 过期自动
  可再获取 / extend 成功与过期失败 / 上下文管理器异常时仍释放 / locked() / 并发 4 线程
  争抢同一锁恰好串行（临界区计数无交错）
- **回归**：全量套件 + `scripts/ci_matrix.sh` 五版本全绿（各 conda 环境需重跑
  `pip install -e ".[dev]"` 拉取新 dev 依赖）

## 5. 演进

v2 候选：Redlock 多节点、固定窗口/令牌桶策略族、窗口计数器聚合快照（分组统计导出）、
shell 的流式输出迭代模式。
