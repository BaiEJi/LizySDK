# lizysdk.pools —— 统一并发池设计文档

> 版本：v1（2026-09-19）· 目标版本 lizysdk 0.5.0
> 定位：线程池 / 协程池 / 进程池的**统一入口包装**——初始化时选择类型，配置与行为语义一致，
> 内置重试、背压、钩子、统计；纯标准库，核心零依赖不变。

## 1. 目标与非目标

目标（v1）：

- [ ] 统一入口 `create_pool(kind, **config)`：`kind = "thread" | "async" | "process"`
- [ ] 统一接口：`submit / map / shutdown / stats / add_hook`，全部支持上下文管理器
- [ ] 丰富配置：workers、队列上限与拒绝策略、单任务超时、重试、worker 初始化、
  worker 回收（process）、contextvars 传播（thread/async）、命名
- [ ] 全套钩子：提交/开始/成功/失败/重试/超时/拒绝/停机 八个事件
- [ ] 运行统计 `stats()`：提交/在途/成功/失败/重试/拒绝/超时/钩子异常 计数 + 队列深度
- [ ] 充分测试：三种池 × 配置 × 钩子 × 并发正确性

非目标（v1 不做，诚实声明）：

- 池运行中动态扩缩容（anyio CapacityLimiter 支持运行时调 token，v2 演进项）
- 进程池任务超时的 worker 击杀与重启（pebble 的核心能力，涉及子进程信号协议，v2 演进项）
- 跨进程 contextvars 传播（不可 pickle，语义上也不可能）
- 分布式/跨主机池、优先级队列、动态增删 worker

## 2. 开源参考

| 来源 | 借鉴点 |
|---|---|
| `concurrent.futures`（stdlib） | API 形状：`submit/map/shutdown`、Future 语义、`min(32, cpu+4)` 线程默认值、关闭后 submit 抛 `RuntimeError` |
| `multiprocessing`（stdlib） | `maxtasksperchild` worker 回收、`initializer/initargs` worker 初始化 |
| [pebble](https://pebble.readthedocs.io) | 任务级超时 + worker 中断/重启的产品化先例；其「超时即杀 worker（连带孤儿子进程风险）」的取舍我们 v1 不跟进，只做结果等待超时并文档化 |
| [anyio CapacityLimiter](https://anyio.readthedocs.io) | 「并发上限 = 令牌」语义：async 池用 `asyncio.Semaphore(workers)` 承载；其 borrower 追踪与运行时 resize 列为演进 |
| `asyncio.Runner`（3.11+） | 「专用 loop 线程的所有权模型」：async 池自带后台事件循环线程，对外桥接为标准 Future |

## 3. 统一入口与接口

```python
from lizysdk.pools import create_pool, run_all

pool = create_pool("thread", workers=8, name="io-pool",
                   hooks={"on_error": lambda info: ...})
try:
    fut = pool.submit(fetch, url, timeout=3)      # -> concurrent.futures.Future
    result = fut.result(timeout=10)
finally:
    pool.shutdown()

with create_pool("async", workers=32) as apool:   # 上下文管理器
    fut = apool.submit(async_fetch, url)          # async def 函数
    await apool.asubmit(async_fetch, url)         # 调用方已在事件循环内时

run_all([(fetch, a), (fetch, b)], kind="thread", workers=4)   # 一次性临时池
```

`Pool` 统一协议（三种池一致）：

| 成员 | 语义 |
|---|---|
| `submit(fn, /, *args, **kwargs) -> Future` | 提交任务，返回 `concurrent.futures.Future`（async 池跨线程桥接） |
| `map(fn, *iterables, timeout=None)` | 惰性保序迭代，语义同 `concurrent.futures`（fail-fast） |
| `shutdown(wait=True, cancel_futures=True)` | 幂等停机；默认取消未开始的任务、等待在途完成 |
| `stats() -> dict` | 运行统计快照（见 §6） |
| `add_hook(event, fn)` | 追加事件监听（可多监听者，线程安全） |
| `__enter__ / __exit__` | 进入返回自身，退出 `shutdown(wait=True)` |
| `kind / name / workers` | 只读属性 |

**三种 kind 的 `fn` 契约**：thread = 普通函数；process = 可 pickle 的模块级函数；
async = `async def` 函数（或调用后返回 coroutine 的函数）。

## 4. 配置（`create_pool` 关键字参数）

| 配置 | 默认 | thread | async | process | 说明 |
|---|---|---|---|---|---|
| `workers` | 见右 | `min(32, cpu+4)` | `32` | `os.cpu_count()` | async 池语义为**最大并发任务数**（令牌，借 anyio 语义） |
| `queue_size` | `0` | ✓ | ✓ | ✓ | 在途任务上限（0=无界）；超出走 `reject_policy` |
| `reject_policy` | `"raise"` | ✓ | ✓ | ✓ | `"raise"` 抛 `PoolRejectedError`；`"block"` 阻塞等待空位（池关闭时抛） |
| `task_timeout` | `None` | 等待语义 | **真取消** | 等待语义 | async：`asyncio.wait_for` 取消任务；thread/process：结果等待超时（任务本体不可中断，诚实文档）；超时计 `timed_out` + `on_timeout` 钩子，Future 得 `TimeoutError` |
| `max_retries` | `0` | ✓ | ✓ | ✓ | 失败自动重试次数；重试在**同一 worker 内**进行（占用 worker 的是等待，文档说明） |
| `retry_backoff` | `0.0` | ✓ | ✓ | ✓ | 指数退避基秒（`base * 2^(n-1)` + 抖动），async 用 `asyncio.sleep` |
| `initializer` / `initargs` | `None` | worker 线程启动时执行 | loop 线程启动时执行一次 | 原生透传 | 初始化失败 → 池不可用，`submit` 抛错 |
| `max_tasks_per_worker` | `None` | 不适用（忽略并文档说明） | 不适用 | 原生 `maxtasksperchild` | 进程 worker 回收防内存膨胀 |
| `propagate_context` | `True` | ✓（`contextvars.copy_context().run`） | ✓（任务上下文注入） | 不适用 | trace_id 等随任务进池不丢失 |
| `name` | `"pool-{kind}"` | ✓ | ✓ | ✓ | 进 stats 与钩子载荷 |
| `thread_name_prefix` | `"lizysdk-pool"` | ✓ | ✓（loop 线程） | 不适用 | 线程命名，排查用 |
| `daemon` | `True` | ✓ | ✓ | — | 工作线程 daemon 位 |

非法配置（workers<1、未知 reject_policy、负超时等）→ `ValueError`（中文消息），
与 SDK 其余模块的校验风格一致。

## 5. 钩子

八个事件，载荷为 **TaskInfo dict**（可直接 `log.info("task done", **info)`）：

```
on_submit   {task_id, pool_name, kind, fn_name, queued_at}
on_start    + {attempt, started_at}
on_success  + {finished_at, elapsed_ms}
on_error    + {finished_at, elapsed_ms, error: str}
on_retry    + {attempt, error: str, next_delay_ms}
on_timeout  + {elapsed_ms}
on_reject   {task_id, pool_name, kind, fn_name, reason}
on_shutdown {pool_name, kind, stats: {...}}
```

契约：

- 注册：`create_pool(hooks={...})` 或 `pool.add_hook(event, fn)`；同事件多监听按注册序调用
- **钩子自身异常一律吞掉并计数**（`stats()["hook_errors"]`），绝不影响任务与池
- 钩子在**执行线程/loop 内同步调用**，要求快（慢钩子拖吞吐由调用方负责，文档写明）
- `task_id` 为池内单调计数（`t-000123` 风格），自持不依赖 ids 子包

## 6. stats 与异常族

`stats()` 键：`kind, name, workers, submitted, running, succeeded, failed,
retried, rejected, timed_out, hook_errors, queue_depth`（锁保护计数器快照）。

模块内自持异常（不跨子包依赖）：

- `PoolError(Exception)` —— 基类
- `PoolClosedError(PoolError, RuntimeError)` —— 关闭后 submit（对齐 stdlib RuntimeError 语义）
- `PoolRejectedError(PoolError, RuntimeError)` —— 队列满且 policy=raise

## 7. 架构

```
create_pool(kind, **config)
 ├── ThreadPool   —— concurrent.futures.ThreadPoolExecutor 包装：
 │                   ctx 传播包装、retry 循环、钩子埋点、计数、有界在途闸门
 ├── ProcessPool  —— ProcessPoolExecutor 包装（initializer/maxtasksperchild 透传），
 │                   同上的钩子/计数/闸门（ctx 不传播）
 └── AsyncPool    —— 专用后台事件循环线程（daemon）：
                     Semaphore(workers) 并发令牌、task_timeout 用 wait_for（真取消）、
                     submit 经 call_soon_threadsafe 桥接为 concurrent Future；
                     asubmit 返回 asyncio Future（须在 loop 内 await）
                     ctx 传播：提交时捕获调用方 contextvars，在任务开始时注入
```

关键取舍记录：

1. **thread 任务超时不可中断**是 Python 线程本质，不做「假取消」；pebble 的杀 worker
   方案列入 v2（需子进程协议，process 才可行）。
2. **async 池并发上限用 Semaphore 而非固定 worker 集**：协程非独占线程，令牌语义
   （anyio 同思路）更贴切；`workers` 命名保留为统一配置面。
3. **retry 在 worker 内 sleep**：简单且语义清晰（「任务自己重试」），代价是占用
   worker，文档明示；需要不占位的重试属调度器职责，不在池的边界内。
4. 三个池共用 base（钩子注册表、计数器、闸门、重试策略），差异收敛在执行器适配层。

## 8. 测试计划

- 工厂：三种 kind 构造与默认值、非法 kind/配置参数化校验
- thread：结果/异常传播、ctx 传播（contextvar 任务内可见）、retry+backoff+on_retry 序列、
  队列上限 raise/block 两策略、stats 一致性（并发 N 提交 → submitted==N、终态守恒）、
  shutdown 幂等 + 关闭后 submit 抛 PoolClosedError、map 保序、上下文管理器
- async：返回值桥接、真取消（task_timeout 后任务被取消且后续 await 不挂）、asubmit 在
  loop 内 await、并发令牌不超 workers（屏障计数验证）、ctx 传播、shutdown 排空
- process：CPU 密集任务正确性、initializer 生效、异常传播、maxtasksperchild 生效、
  关闭后 submit（Windows spawn 环境的真实行为）
- hooks：八事件触发顺序断言、钩子抛异常不影响任务且 hook_errors 计数、多监听者顺序
- run_all：保序、return_exceptions 两模式
- 防抖：并发用例连续跑 3 轮全绿

## 9. 演进

v2 候选：process 任务超时击杀 + worker 重启（pebble 路线）、池动态 resize
（anyio 路线）、优先级提交、metrics 直出（stats 桥接）。
