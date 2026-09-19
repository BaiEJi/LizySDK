# lizysdk.dist Redis 能力套件设计（v0.8.0）

> 版本：v0.8.0 设计稿（实现契约，子 agent 按本文档施工）
> 前置：`docs/shell-dist-design.md`（v0.6.0 已落地 SlidingWindowCounter / DLock）
> 原则：**算法全部对齐开源实现，不做自创设计**——每个组件标注出处与
> 对齐对象；语义偏差处显式声明。

## 0. 背景与范围

v0.6.0 的 dist 子包已交付两个原语（滑动窗口计数器、不可重入效率锁）。
本版本扩展为** Redis 能力套件**，P0 六件（用户已确认）：

| # | 组件 | 对齐的开源实现 |
|---|------|----------------|
| 1 | `RLock` 可重入锁 + 看门狗 | Redisson `RedissonLock`（hash 计数 + watchdog TTL/3 续期） |
| 2 | `LeaderElector` 领导选举 | Kubernetes Lease 选举语义，基于锁+心跳续期落地 |
| 3 | `IdempotentKey` 幂等键 | Stripe Idempotency Key 两态模型（processing→done） |
| 4 | `ReliableQueue` 可靠任务队列 | redis.io 官方 Reliable queue pattern（LMOVE+processing+LREM） |
| 5 | `DelayQueue` 延迟队列 | Redisson `RDelayedQueue`（ZSET 到期分 + 原子搬运到 list） |
| 6 | `Leaderboard` 排行榜 | redis.io 官方 Leaderboard pattern（ZSET 标准玩法） |

P1/P2（本版本不做，仅记录）：信号量 Semaphore（Redisson RSemaphore）、
漏斗限速（Redisson RRateLimiter，令牌桶）、Redlock 多节点多数派、
Pub/Sub 广播、延迟队列 pub/sub 即时唤醒。

## 1. 算法出处与决策（逐项）

### 1.1 RLock —— 对齐 Redisson RedissonLock

Redisson 的可重入锁核心（源码 `RedissonLock.java`，已调研确认）：

- 键是 **hash**：field = `clientId:threadId`，value = **重入计数**；
- **tryLock Lua**：`HEXISTS` 命中 → `HINCRBY +1` + `PEXPIRE`（重入）；
  未命中且键不存在 → `HSET field 1` + `PEXPIRE`（首次获取）；
  否则返回当前 `PTTL`（抢锁失败，供等待方按 TTL 排期重试）；
- **解锁 Lua**：`HINCRBY -1`；结果 <= 0 → `DEL` 整键，否则 `PEXPIRE` 续命；
- **看门狗**：未显式指定 leaseTime 时启用——默认 `lockWatchdogTimeout=30s`，
  后台线程每 `TTL/3`（10s）续期一次，持锁线程存活则锁不丢；
  显式 leaseTime → 无看门狗，到期自动失效。

lizysdk 落地语义：

- `field = f"{instance_uuid}:{threading.get_ident()}"`（对齐 clientId:threadId；
  instance_uuid 是每个 RLock 实例一个 `uuid4().hex`——两个 RLock 实例同名
  互为竞争者，与 Redisson 两个 client 互斥语义一致）；
- `RLock(client, name, *, lease_time=None, watchdog_timeout=30.0, prefix="lizy:rlock")`：
  `lease_time=None` 启用看门狗；给了 lease_time 则固定 TTL 不续期；
- `acquire(blocking=True, timeout=None)` / `release()` / `reentrant_count()`
  （当前重入层数，未持有为 0）/ `extend(lease_time)` / `force_unlock()`（DEL，管理端用）；
- 可重入性范围：**同实例同线程**；跨实例/跨线程 = 竞争者（Redisson 同语义）；
- 看门狗 = `threading.Thread(daemon=True)` + `threading.Event`，每
  `watchdog_timeout/3` 续期；`release()` 到 0 层时停看门狗；对象回收时
  `__del__` 兜底停线程（daemon 兜底进程退出）；
- 成功返回值约定：Lua 成功返回 `-1`（Redisson 返回 nil；redis-py eval 将
  nil 转为 None，用 -1 区分「成功」与「TTL=0 的极小概率竞态」）。

**与 DLock 的关系**：DLock（不可重入，token 模型）保留不动——它是
redis-py Lock 语义对齐件；RLock 是 Redisson 语义对齐件，二者并存、
docstring 互链指引选型（要重入/看门狗 → RLock；要极简效率锁 → DLock）。

### 1.2 LeaderElector —— K8s Lease 选举语义

无直接同款 Redis 开源件可抄命令级细节，但语义对齐 Kubernetes Lease
选举（业界标准）：**独占租约 + 心跳续期 + 失联自动下台**：

- `SET key holder_id NX PX lease` 抢租约；holder 周期性 Lua
  compare-holder `PEXPIRE` 续期（复用锁的校验释放思想，防误续他人）；
- 续期失败（网络分区/GC 停顿超过 lease）→ 立即降级为 standby 并回调
  `on_losing_leadership`（**先降级后回调**，杜绝「自认领导却无租约」窗口）；
- 非领导者可 `leader_id()` 查询当前领导者（跟随者发现）。

lizysdk 落地：

```python
elector = LeaderElector(client, "my-app", on_become_leader=fn1,
                        on_losing_leadership=fn2, lease=15.0)
elector.start()          # 后台竞选 + 心跳线程（deamon）
elector.is_leader        # bool
elector.leader_id()      # 当前持有者 id（可能是别的实例）
elector.stop()           # 主动下台（释放租约，触发 losing 回调）
```

- `holder_id` 默认 `f"{hostname}:{pid}:{uuid4().hex[:8]}"`，可注入；
- 心跳间隔 = `lease/3`（与看门狗同节奏，出处同 Redisson 续期比例）；
- 回调在心跳线程内同步执行（回调抛异常只记 logging.warning 不杀线程）；
- 内部实现直接复用 `.lock` 的 DLock？**不**——选举需要「续期失败即降级」
  的事件流与 holder_id 可见性，独立实现 Lua（compare-holder 续期），
  但异常族复用 `lock.py` 的 LockError 族（同子包内 import 允许）。

### 1.3 IdempotentKey —— Stripe Idempotency Key 两态模型

Stripe 的幂等键语义（业界事实标准）：**首次请求 processing 中，并发
重复请求被拒（409 语义），完成后落结果，之后的重复请求拿缓存结果**。

lizysdk 落地（键值存 JSON 信封）：

```python
idem = IdempotentKey(client)                    # prefix="lizy:idem"
with idem.guard("pay:order:123", processing_ttl=60) as g:
    result = do_pay()                           # 临界区
    g.complete(result, ttl=86400)               # 落 done + 缓存结果
# 语义：
#   别的执行者 processing 中  -> IdempotencyConflictError
#   已 done                   -> IdempotencyDoneError（.result 带缓存结果）
#   临界区抛异常未 complete    -> guard 退出时自动 fail（DEL，允许重试）
```

- `begin(key) -> bool`（SET NX EX，信封 `{"state":"processing","holder":token,"started":ts}`）；
- `complete(key, result=None, ttl=...)`（SET EX 覆写为
  `{"state":"done","result":<json>}`；result 必须可 JSON 序列化）；
- `fail(key)`（compare-holder Lua DEL——只删自己占的坑，防误删他人 processing）；
- `get(key) -> Optional[Any]`（done → 返回缓存结果；processing/不存在 → None）；
- `is_done(key) -> bool`；
- 异常族：`IdempotencyError(Exception)` 基类 +
  `IdempotencyConflictError` / `IdempotencyDoneError`（后者带 `.result` 属性）；
- guard 的 context manager 是核心 API（对齐 Stripe「自动管理键生命周期」）。

### 1.4 ReliableQueue —— redis.io 官方 Reliable queue pattern

redis.io 官方模式（已调研确认，`LMOVE` 文档 + Reliable queue 教程）：

```text
生产者:  LPUSH queue job
消费者:  LMOVE queue processing LEFT RIGHT    # 原子弹出到 processing
处理完:  LREM processing 1 job                # ack：从 processing 移除
崩溃恢复: LMOVE processing queue RIGHT LEFT   # 清扫者把残留搬回 queue
```

- at-least-once 语义：worker 处理中崩溃 → job 留在 processing →
  恢复流程搬回 queue 被重投（**可能重复，消费方需幂等**——文档明示）；
- 官方简单版恢复 = 把 processing **全部**搬回（已 ack 的早已被 LREM，
  残留即疑似失败）；不做按时间可见性超时（那需要 per-item 时间戳的
  复杂设计，列为 v2）。

lizysdk 落地：

```python
rq = ReliableQueue(client, "orders")           # prefix="lizy:rq"
rq.push(payload)                # LPUSH（payload: str；内部信封 JSON 含 uuid）
rq.push_many([p1, p2, ...])
job = rq.pop(timeout=5.0)       # LMOVE→processing（blocking 用 BLMOVE）；
                                # 返回 Job(id, payload, raw, tries) | None
rq.ack(job)                     # LREM processing 1 raw -> bool
rq.nack(job)                    # LREM + LPUSH 回队（tries+1，手动重试）
rq.recover()                    # 清扫：processing 全量搬回 queue，返回搬运数
rq.qsize() / rq.processing_size()
```

- 信封 `{"id": token_hex(8), "payload": <str>, "pushed_at": ts, "tries": 0}`：
  pop 时**解信封返回原始 payload**，raw（信封原文）挂在 `job.raw` 供
  ack/nack 精确 LREM；nack 重投时 tries+1；
- `pop(timeout=0)` 非阻塞；`timeout>0` 用 BLMOVE（fakeredis 已实测支持；
  到期返回 None）。**BLMOVE timeout=0（永久阻塞）禁止**——SDK 不提供
  无限阻塞入口；
- `recover()` 用循环 LMOVE 直到 processing 空（Lua 一次循环做也行，
  保持纯命令更贴官方文档）。

### 1.5 DelayQueue —— Redisson RDelayedQueue 同构

Redisson RDelayedQueue 结构（已调研确认）：**ZSET 存定时任务
（score = 到期时间戳）+ 后台定时器把到期项原子搬到目标 list**；
Redisson 用 pub/sub 唤醒定时器，lizysdk 简化为**消费前拉取式**
（pop 时先搬到期项再弹，无后台线程；`move_due()` 也独立暴露）。

lizysdk 落地：

```python
dq = DelayQueue(client, "reminders")           # prefix="lizy:dq"
dq.push(payload, delay=30.0)                   # ZADD score=now+delay
dq.move_due(limit=100)                         # 原子 Lua：到期项 ZREM+LPUSH ready
job = dq.pop_ready(timeout=5.0)                # 搬运 + BRPOP ready（Job 同上）
dq.cancel(job_id) -> bool                      # 未到期时取消
dq.due_size() / dq.ready_size()
```

- 搬运 Lua（原子，对齐 Redisson 的 transfer 语义）：
  `ZRANGEBYSCORE zset -inf now LIMIT n` → 逐项 `ZREM`（防其他搬运者
  竞争——ZREM 返回 1 才 `LPUSH` ready）→ 返回搬运数；
- member = 信封 JSON（含唯一 id），score = 到期秒（浮点字符串）；
- `pop_ready` 阻塞版：先 `move_due`，再 BRPOP ready（小 timeout 循环
  到 deadline；到期无货返回 None）；
- **与 ReliableQueue 组合**：`DelayQueue.pop_ready` 搬运目标就是自身的
  ready list；若需「到期后进可靠队列」，用户把 DelayQueue 挂到
  ReliableQueue 同名（push 时自己桥接）——SDK 不做自动桥接（保持
  单一职责，v2 再考虑 `delayed_reliable` 组合糖）。

### 1.6 Leaderboard —— redis.io 官方 Leaderboard pattern

ZSET 标准玩法（redis.io 数据建模教程 + 所有游戏 SDK 共识）：
`ZADD` 计分、`ZREVRANGE` 榜单、`ZREVRANK` 名次。同分按 member
字典序（Redis 原生行为，文档明示；不搞复合分——保持简单诚实）。

```python
lb = Leaderboard(client, "game:1:score")       # prefix="lizy:lb"
lb.add_score("alice", 100)                     # ZADD（覆盖语义）
lb.incr_score("alice", 50)                     # ZINCRBY
lb.rank("alice")             -> 1              # 1-based；未上榜 None
lb.top(10)                   -> [(member, score), ...]  # 降序
lb.around("alice", span=2)   -> [(member, score), ...]  # 以 alice 为中心的窗口
lb.score("alice") / lb.remove("alice") / lb.size() / lb.reset()
```

- `rank`/`top`/`around` 默认**降序**（分高在前），`ascending=True` 翻转；
- `around(member, span)`：`rank=r`（1-based）→ 取
  `[max(1, r-span), r+span]` 的窗口（对齐游戏 SDK 的「我的排名」视图；
  member 不存在 → KeyError 语义的 ValueError，中文消息）；
- 1-based 明确写进 docstring（Redisson RRank 是 0-based，这里对齐
  游戏行业惯例而非 Redisson——**显式声明的偏差**）。

## 2. API 总览与导出

`lizysdk.dist` 新增导出（`__init__.py` 由集成者统一接线，子 agent 不碰）：

```python
RLock, LeaderElector,
IdempotencyError, IdempotencyConflictError, IdempotencyDoneError, IdempotentKey,
ReliableQueue, DelayQueue, Job,
Leaderboard
```

`Job` 为 `queue.py` 内定义的 `NamedTuple("Job", [("id", str), ("payload", str), ("raw", str), ("tries", int)])`。

顶层 `lizysdk.__init__` 同步 re-export（集成者做）。

## 3. 工程红线（与既有 dist 一致，违反即打回）

1. **懒加载**：模块级禁止 `import redis`；需要处经 `_client` 或直接
   函数内导入；异常消息含 `pip install "lizysdk[redis]"`；
2. **包内自持**：不 import lizysdk 其他子包（ids/logs/errors/...）；
   dist 内互相 import 允许（如 election 复用 lock 的异常族）；
3. **py3.9+**：`from __future__ import annotations`；不用 3.10+ 语法
   （`X | Y` 类型注解、match、dataclass slots 等）；
4. **中文 docstring**：Google 风格 Args/Returns/Raises/Example/Note，
   算法出处必须写进类 docstring（如「对齐 Redisson RedissonLock」）；
5. **Lua 一律模块级常量** + 中文注释块说明 KEYS/ARGV；
6. **时间接缝**：时间取值统一走模块级 `_now()`（测试 monkeypatch 用）；
   线程 sleep 走模块级 `_sleep()` 接缝（并发测试提速）；
7. **参数校验**：构造器与方法入口全量校验，非法抛 ValueError（中文消息）；
8. **线程对象**（看门狗/心跳）：daemon=True + Event 停止 + 可重入 start
   幂等；回调异常不杀线程（logging.warning）；
9. **纯标准库** + 已有 redis 可选依赖，不引入新三方依赖。

## 4. 测试计划（fakeredis 全离线，对齐 tests/test_dist.py 风格）

| 文件 | 覆盖 |
|------|------|
| `tests/test_dist_locks.py` | RLock + LeaderElector + IdempotentKey |
| `tests/test_dist_queue.py` | ReliableQueue + DelayQueue |
| `tests/test_dist_leaderboard.py` | Leaderboard |

要点（每个组件 ≥ 15 用例，全部离线）：

- **RLock**：重入计数（同线程 acquire×3 → HINCRBY=3，release×3 归零删键）/
  跨实例互斥 / 跨线程互斥 / 看门狗续期（monkeypatch `_now` + 缩短
  watchdog，观察到 PTTL 被周期性重设）/ lease_time 显式则无看门狗 /
  extend / force_unlock / 异常族 / 参数校验；
- **LeaderElector**：单实例当选回调触发 / 心跳续期保住领导 /
  租约过期另一实例上位（is_leader 翻转 + losing 回调）/ stop 下台释放 /
  leader_id 可见 / 回调异常不死线程；
- **IdempotentKey**：guard 首次执行 / 并发第二者 Conflict / 完成后
  Done 带缓存结果 / 异常自动 fail 可重试 / compare-holder 的 fail 不误删
  他人 / get/is_done / TTL 生效；
- **ReliableQueue**：push/pop/ack 往返 / pop 无货 None（非阻塞+带超时）/
  处理中残留 → recover 搬回 / nack 重投 tries+1 / 信封解包正确 /
  并发 8 线程 push+pop 守恒（总数不丢不重，ack 后）/ LREM 精确性
  （同 payload 多份只删一份）；
- **DelayQueue**：未到期 pop 不到 / monkeypatch `_now` 推进时间后
  move_due 搬运 / pop_ready 阻塞超时 None / cancel / 过期分与就绪
  list 大小 / 并发搬运守恒（两线程同时 move_due 不重不漏）；
- **Leaderboard**：add/incr/rank（含同分字典序） / top 边界（n 超过
  size）/ around 居中与贴边 / ascending / remove/size/reset / 参数校验。

并发用例一律**可复跑**（3 轮回归防 flaky）；涉及真实等待的用例把
等待压到最小（monkeypatch `_sleep` / 极短 TTL）。

## 5. 压测计划（benchmarks/redis_bench.py + REDIS_REPORT.md）

- 环境：优先 docker 真 Redis（`docker run -p 16379:6379 redis:7`）；
  docker 不可用则 fakeredis，**报告明示「衡量的是 SDK 层开销
  （含 Lua 组包），非 Redis 网络 RTT」**；
- 场景（每场景 ≥ 3 轮取最优，报告机器噪声 ±20% 声明，沿用
  benchmarks/REPORT.md 方法论）：
  1. RLock acquire/release（浅重入 1 层 vs 深重入 5 层）；
  2. 看门狗开/关的持锁开销（稳态续期摊销）；
  3. ReliableQueue push→pop→ack 全链路（单线程串行 + 4 线程竞争）;
  4. DelayQueue push + move_due + pop_ready 批量搬运；
  5. Leaderboard add_score / top(10) / rank / around；
  6. IdempotentKey guard 全流程（begin→complete）；
  7. 对照组：DLock、SlidingWindowCounter（延续 v0.6 数据可比性）；
- 输出：`benchmarks/REDIS_REPORT.md`（表格 + 结论 + 与 DLock/SWC 的
  对照分析），数据 JSON 落 `benchmarks/results/`。

## 6. 文件与职责切分（子 agent 施工边界）

| 文件 | 归属 |
|------|------|
| `src/lizysdk/dist/reentrant_lock.py` | Agent A |
| `src/lizysdk/dist/election.py` | Agent A |
| `src/lizysdk/dist/idempotent.py` | Agent A |
| `src/lizysdk/dist/queue.py`（ReliableQueue + DelayQueue + Job） | Agent B |
| `src/lizysdk/dist/leaderboard.py` | Agent B |
| `tests/test_dist_locks.py` | Agent A |
| `tests/test_dist_queue.py` / `tests/test_dist_leaderboard.py` | Agent B |
| `dist/__init__.py` / 顶层 `__init__.py` / AGENTS.md / README / 版本号 | 集成者（禁止子 agent 碰） |

## 7. 版本与交付

- 版本 `0.7.0 → 0.8.0`（新能力批次）；
- 交付流程：设计（本文档）→ 双 agent 并行施工 → 集成接线 →
  全量回归（预期 675 + ~120 新增）→ 3 轮并发 flaky 检查 →
  5 版本矩阵（scripts/ci_matrix.sh）→ 压测与报告 → commit + push。
