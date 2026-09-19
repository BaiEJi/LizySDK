# lizysdk v0.8.0 Redis 套件压测报告

> 压测脚本 [redis_bench.py](redis_bench.py)，原始数据 [results/redis_bench.json](results/redis_bench.json)。
> 全量功能测试 **825 个全绿**（含本压测过程中抓到并修复的 1 个看门狗真 bug，见 §5）。

## 1. 环境与口径声明（重要，先读）

- **后端：fakeredis 2.38.0（进程内模拟，无网络）**——本次按计划退回
  fakeredis 口径（docker 真 Redis 环境未就绪）。因此本报告衡量的是
  **SDK 层开销**：redis-py 命令组包、Lua 脚本经 lupa 解释执行、信封
  JSON 编解码、线程启停等；**不是** Redis 服务端吞吐，也**不含**网络
  RTT。真 Redis 部署下，LAN 单次往返通常 0.2~1ms，与本报告单操作
  SDK 层开销量级相当——即真实部署中两层开销叠加，SDK 层不是短板。
- 环境：Windows 10 (19045) · Python 3.10.19 (miniconda) · 单机；本机
  跨进程性能波动可达 **±20%**（热态/省电/后台任务），所有结论只在
  量级与相对比例上有意义。
- 方法：每场景固定操作数、**3 轮独立计时取最优**（timeit 惯例，与
  benchmarks/REPORT.md 同口径）；线程类场景 4 线程真实并发。

## 2. 结果总表（ops/s，进程内 fakeredis）

| 场景 | 吞吐 | 单操作开销 |
|---|---:|---:|
| RLock 获取+释放（浅重入 1 层，看门狗模式） | 820 | ~1.22 ms |
| RLock 获取+释放（深重入 5 层，摊销） | 2,569 | ~0.39 ms |
| RLock 看门狗稳态持锁（0.3s 档续期） | — | 每持锁周期 +0.1 ms |
| ReliableQueue push→pop→ack（串行全链路） | 8,217 | ~0.12 ms |
| ReliableQueue push→pop→ack（4 线程竞争） | 6,761 | ~0.15 ms |
| DelayQueue push+move_due+pop_ready（批量） | 5,076 | ~0.20 ms |
| Leaderboard add_score（ZADD） | 8,259 | ~0.12 ms |
| Leaderboard 读混合（top10+rank+around） | 5,261 | ~0.19 ms |
| IdempotentKey begin→complete 全流程 | 13,073 | ~0.08 ms |
| DLock 获取+释放（对照，v0.6） | 2,711 | ~0.37 ms |
| SlidingWindowCounter incr（对照，v0.6） | 2,514 | ~0.40 ms |

## 3. 分析

### 3.1 RLock：看门狗线程启停主导浅循环成本

浅重入场景（820 ops/s）显著慢于深重入摊销（2,569）与 DLock 对照
（2,711），分解如下：

- 每次「acquire→release 到 0 层」循环，看门狗模式要**启停一个线程**
  （acquire 起线程、release 置停止事件），本机线程创建 ~0.3-0.8ms，
  是浅循环的主要成本；
- 深重入 5 层把一次线程启停摊到 10 个操作上，暴露出 Lua EVAL 本身
  的成本（~0.39ms/op，其中 lupa 每次解释执行 Redisson 三脚本）；
- DLock 对照（SET NX 原生命令 + 1 个 EVAL）2,711 ops/s，佐证 EVAL
  经 lupa 的解释开销是原生命令的数倍——**这是 fakeredis 口径特有的
  放大**：真 Redis 下 Lua 脚本会被服务端缓存（EVALSHA），往返数与
  原生命令相同。

**使用建议**：毫秒级以下的高频短临界区用 `DLock`（无线程、无重入）；
秒级以上的长临界区（看门狗的目标场景）用 `RLock`——此时线程启停
一次的 ~1ms 相对持锁时长可忽略（见 3.2）。

### 3.2 看门狗稳态开销：可忽略

0.3s 看门狗档（续期间隔 0.133s）持续持锁 0.5s×20 周期：看门狗开/关
的每周期开销差 **0.1ms**（11.1 vs 11.0ms，含本底 sleep 抖动）——
续期是后台异步 EVAL，不阻塞持锁线程。默认 30s 档下摊销成本再低
两个数量级。

### 3.3 队列：全链路三命令 ~0.12ms/op

串行全链路 8,217 ops/s（每「操作」= push/pop/ack 之一，含信封 JSON
编解码）。4 线程竞争反而略降（6,761）——fakeredis 进程内单实例加锁
+ GIL，并发在进程内口径没有收益；真 Redis 下连接池多路复用，多
worker 吞吐由服务端决定。DelayQueue 批量搬运（5,076 ops/s）含
`_MOVE_DUE_LUA` 逐项 ZREM+LPUSH 的 Lua 循环，每任务 ~0.2ms 属
lupa 解释口径的合理范围。

### 3.4 排行榜与幂等键

- Leaderboard 写 8,259 ops/s（单 ZADD 原生命令，无 EVAL）；读混合
  5,261 ops/s（around 需要 rank+range 两命令）；
- IdempotentKey 全流程 13,073 ops/s **最快**——happy path 是两个
  原生 SET（NX + EX），无 EVAL、无线程。

## 4. 结论

1. SDK 层单操作开销 **0.08~1.22ms**（fakeredis 进程内口径）：原生命令
   类（幂等键/排行榜写/队列）0.08~0.12ms；Lua 脚本类（RLock/SWC/
   DLock 释放路径/延迟队列搬运）0.2~0.4ms；带线程启停的看门狗浅循环
   ~1.2ms；
2. **fakeredis 口径系统性放大了 Lua 脚本成本**（每次整段解释执行），
   真 Redis 有 EVALSHA 缓存——上表 Lua 类场景在真 Redis 下的相对
   劣势会显著收窄；
3. 全部场景的绝对量级对典型业务负载（千级 QPS 的锁/队列/幂等）余量
   充足；瓶颈不在 SDK 层。

## 5. 压测抓到的真 bug（已修复 + 回归用例）

压测首轮 `rlock_watchdog` 场景崩溃暴露 `reentrant_lock.py` 的看门狗
存活判定缺陷：`_start_watchdog_if_needed` 只判 `is_alive()`——release
后旧看门狗线程尚在退出途中（停止事件已置位但未跑完）时**立即重新
acquire**，会被误判「看门狗在跑」而跳过启动新线程，新持锁无看门狗、
TTL 到期即丢锁（release 抛 `LockNotOwnedError`）。修复：存活判定
同时校验停止事件未置位；回归用例
`test_rapid_reacquire_gets_fresh_watchdog` 3 轮全绿。这正是「压测
不只是出数据，也是并发路径的照妖镜」的例证。
