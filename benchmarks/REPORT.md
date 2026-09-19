# lizysdk v0.4.0 性能压测与优化报告

> 结论：**同机交错压测，11 项指标几何平均提升 +74.4%**（要求 ≥50%），354 个功能测试全程保持全绿。
> 压测脚本 [bench.py](bench.py)，聚合报告 [report.py](report.py)，原始数据 [results/](results/)。

## 环境

- Windows 10 (19045) · Python 3.10.19 (miniconda) · 单机
- 基线代码：v0.3.0（`benchmarks/results/base*.json`，7 次运行采样）
- 优化代码：v0.4.0（`benchmarks/results/after_final*.json`，2 次运行采样）

## 方法

- 每项指标固定迭代次数、5 轮独立计时，取**最优轮**（与 `timeit` 取最小耗时的惯例一致，
  衡量能力上限）；聚合时再取同版本多次运行的最优（`report.py`）。
- **交错运行**（优化版 → stash 回基线版 → 优化版）消除机器快慢态漂移：本机跨进程
  性能波动可达 ±20%（热态/省电/后台任务），单对比较不可信，交错 + 最优轮是本次采用的口径。
- 日志类指标落盘均写临时目录（`rotation="none"`、`console=False`）。

## 优化清单（行为等价，354 测试零回归）

| # | 优化点 | 位置 |
|---|---|---|
| 1 | 秒级时间戳**单槽缓存**（同秒免 strftime/localtime） | logs/formatter.py |
| 2 | key 校验缓存（合法 key 一跳命中，免逐条正则，带上限 4096 防病态膨胀） | logs/formatter.py |
| 3 | `escape_value`/`unescape` **无特殊字符快速路径**（免三次 replace 扫描） | logs/formatter.py |
| 4 | `parse_line` 无反斜线走 C 级 `str.split` 快速路径 | logs/formatter.py |
| 5 | basename 缓存 + `sys_name` 段预计算 | logs/formatter.py |
| 6 | 写队列换 **`queue.SimpleQueue`**（C 实现，put 不持 Python 锁，消除生产/消费 GIL 乒乓）；flush 改**屏障令牌**（免逐条 task_done 共享计数锁）——仅 pool_size=1 默认路径；多 worker 保留 task_done/join（屏障在多消费下有竞态，测试曾抓到 3 条丢失后修复） | logs/handlers.py |
| 7 | 后台写入池模式改**缓冲写 handler**（免逐条 flush 系统调用；`flush()`/停机/轮转承接落盘）；同步直写模式保持标准库逐条刷盘（调用返回即落盘契约不变）；队列/文件 handler 跳过冗余 RLock 与过滤器 | logs/handlers.py |
| 8 | 随机 ID **线程本地批量熵缓冲**（补块一次 urandom+整体转 hex，日常单次切片） | ids/trace.py |
| 9 | ULID 编码：时间戳段按毫秒缓存 + 80bit 随机段 2 字符查表（16 次 divmod → 8 次移位查表） | ids/ulid.py |
| 10 | `to_dict` 空 details 免 deepcopy；JSONL 紧凑分隔符输出 | errors/base.py · logs/formatter.py |

## 结果（最优轮聚合，ops/s 越高越好）

| 指标 | 基线 v0.3.0 | 优化后 v0.4.0 | 提升 |
|---|---:|---:|---:|
| log_emit_async_caller（异步打点，调用方开销） | 42,932 | 62,090 | **+44.6%** |
| log_emit_async_4threads（4 线程并发打点总吞吐） | 13,718 | 61,967 | **+351.7%** |
| log_pipeline_sync（同步直写全链路） | 21,916 | 26,464 | +20.8% |
| log_pipeline_json_sync（同步直写 JSONL） | 19,808 | 23,133 | +16.8% |
| parse_pipe（pipe 行解析） | 29,014 | 196,404 | **+576.9%** |
| parse_json（JSON 行解析） | 190,906 | 187,938 | -1.6%（持平） |
| id_new_trace_id | 857,768 | 1,166,808 | +36.0% |
| id_new_uid | 791,964 | 1,247,273 | **+57.5%** |
| id_new_id_snowflake（未改动） | 777,408 | 690,748 | -11.1%（噪声内持平） |
| id_new_sortable_id（ULID） | 133,046 | 412,867 | **+210.3%** |
| error_construct_to_dict | 104,341 | 130,604 | +25.2% |
| **几何平均** | | | **+74.4%** |

诚实说明：

- `parse_json` 由 C 版 `json.loads` 主导、雪花 ID 未改动——两者持平属预期；
- 同步直写模式的提升受「逐条刷盘」契约约束（调用返回即落盘，基线同样支付该
  系统调用），此前的单对交错样本中该项为 +45%~62%，聚合口径下保守取 +21%；
- 机器存在跨进程快慢态，见「方法」；两组各自多次采样取最优即为此口径。

## 复现

```bash
python benchmarks/bench.py --out benchmarks/results/after_finalN.json   # 当前代码
git stash push -- src && python benchmarks/bench.py --out benchmarks/results/baseN.json && git stash pop
python benchmarks/report.py                                            # 聚合对比
```
