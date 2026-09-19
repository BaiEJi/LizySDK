# lizylog —— 日志接收与检索服务端设计文档

> 版本：v1 草案（2026-09-19）
> 定位：`lizysdk` `send_json` 的自研接收端，支持 **SQLite（默认）/ PostgreSQL（可选）** 双存储，提供上传、存储、检索能力。
> 量级假设：小规模（每天数 MB ~ 数百 MB，单机部署，1~2GB 内存即可）。

---

## 1. 背景与目标

`lizysdk` 的 `send_json=True` 会把每条日志以 `POST application/json` 发到 `send_url`，body 结构：

```json
{"level": "INFO", "timestamp": "2026-09-19T10:30:00", "file": "app.py", "line": 42,
 "sys_name": "order-svc", "user_id": "123", "trace_id": "a3f0...", "message": "user logged in"}
```

目标（v1）：

- [ ] 接收 SDK 日志（单条 + 批量端点），校验、规范化、落库，**不丢日志**（含死信兜底）
- [ ] 检索 API：时间范围 / sys_name / level / trace_id / message 全文 / 任意 kv 字段过滤
- [ ] 双存储：SQLite 零依赖开箱即用；PostgreSQL 用于正式/多写入方部署
- [ ] 保留策略（retention）自动清理
- [ ] 认证（可选静态 token）
- [ ] 优雅停机：队列排空、死信落盘

非目标（v1 不做，见 §14 演进）：

- 仪表盘 UI、告警、聚合分析（v2 可加极简查询页）
- 多机水平扩展（单进程设计；量级增长后建议直接迁移 VictoriaLogs/ClickHouse，见调研记录）
- 采样、脱敏、多租户

---

## 2. 总体架构

单进程 FastAPI（uvicorn，**workers=1**，SQLite 单写者模型要求），接收与落库解耦：

```mermaid
flowchart LR
    subgraph SDK[lizysdk 客户端]
        A["send_json 发送器<br/>POST JSON（逐条）"]
    end
    subgraph S["lizylog 服务端（单进程）"]
        B["Ingest API<br/>POST /api/v1/logs(/batch)"] --> C["校验与规范化<br/>pydantic + 级别/时间处理"]
        C --> D["queue.Queue<br/>maxsize=10000（背压）"]
        D --> E["批量写入线程（唯一写者）<br/>batch=200 条 或 500ms 触发"]
        E --> F["存储抽象层 LogStorage"]
        G["Query API<br/>GET /api/v1/logs"] --> H["查询构建器<br/>过滤下推 + 方言翻译"]
        H --> F
        I["Retention 线程<br/>每日按保留期清理"] --> F
    end
    F --> J[("SQLite（WAL）<br/>默认")]
    F --> K[("PostgreSQL<br/>可选")]
    A --> B
```

数据流要点：

1. **接收线程只做轻活**：解析 JSON → 校验 → 规范化 → 入队，微秒级返回 `202 Accepted`（日志落地是异步的）。
2. **唯一写者线程**批量落库：与 SDK 端「queue + 后台写入池」设计对称，规避 SQLite 并发写限制。
3. **队列背压**：满时丢弃最旧日志并计数（`dropped` 指标暴露在 `/api/v1/stats`），保证接收端不阻塞业务。
4. **死信兜底**：批次落库失败重试 2 次后写入 `dead_letters/` 目录（JSONL），停机时也先落盘，**绝不丢已接收日志**。

---

## 3. 技术选型

| 决策点 | 选择 | 理由 | 备选与放弃原因 |
|---|---|---|---|
| Web 框架 | **FastAPI + uvicorn** | 自动 OpenAPI 文档、pydantic 校验、生态主流 | Flask（无自动文档）；stdlib http.server（校验/路由全手写，不优雅） |
| DB 访问 | **薄存储抽象 + 双驱动**（`sqlite3` 标准库 / `psycopg` v3） | 表只有一张、SQL 简单；依赖最少（纯 SQLite 部署仅需 fastapi/uvicorn）；方言差异集中在查询构建器，可控 | SQLAlchemy Core（省去手写方言，但引入较重依赖；表变多后再考虑） |
| 数据库 | **SQLite 默认，PG 可选** | SQLite WAL 单文件零运维，小量级百万行无压力；PG 用于多写入方/容器化正式部署 | 见调研文档：量级再大两个数量级直接换 VictoriaLogs，不自研 |
| 写入模型 | **queue + 单写者线程批量** | 规避 SQLite 单写限制；吞吐远超逐条；与 SDK 端设计对称 | asyncio 队列（DB 是同步 IO，仍要线程池，绕一圈无收益） |
| 部署形态 | **单进程**（`workers=1`） | 队列与写者在进程内；SQLite 天然单机 | 多进程需引入 Redis 之类做队列，v1 无必要 |

依赖清单：`fastapi`、`uvicorn`（必选）；`psycopg[binary]`（仅 PostgreSQL 模式）。Python 3.10+。

---

## 4. 数据模型

单表 `logs`，固定列放高频过滤字段，业务 kv 整体进 JSON 列（**宽表 + JSON 混合**，免 schema 迁移）。

### 4.1 设计要点

- **`ts` 统一存 UTC**：SDK 时间戳无时区，按配置 `INCOMING_TZ`（默认 `Asia/Shanghai`）解释后转 UTC；若来串带时区（RFC3339）则直接采用。SQLite 存 ISO8601 文本 `YYYY-MM-DDTHH:MM:SS.sssZ`（**字典序即时间序**，免函数索引）；PG 用 `timestamptz`。
- **`level` 双列**：`level`（数值 10/20/30/40/50，对齐标准库，支持 `level_gte` 区间过滤）+ `level_text`（展示）。
- **`trace_id` 提升为物理列**：全链路检索是第一查询模式，必须独立索引，不能埋在 JSON 里。
- **`fields` JSON 列**：SDK 全部业务 kv 原样保留（值统一为字符串，SDK 端已如此）。PG 用 `jsonb`；SQLite 用 TEXT。
- **`received_at`**：入库时间，排查「发送延迟/时钟漂移」用。

### 4.2 DDL

PostgreSQL：

```sql
CREATE TABLE IF NOT EXISTS logs (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    level       SMALLINT    NOT NULL,
    level_text  VARCHAR(10) NOT NULL,
    sys_name    VARCHAR(64) NOT NULL,
    file        VARCHAR(255),
    line        INTEGER,
    trace_id    VARCHAR(64),
    message     TEXT        NOT NULL,
    fields      JSONB       NOT NULL DEFAULT '{}',
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_logs_ts        ON logs (ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_sys_ts    ON logs (sys_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level_ts  ON logs (level, ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_trace     ON logs (trace_id);
CREATE INDEX IF NOT EXISTS idx_logs_fields    ON logs USING GIN (fields);   -- kv 过滤
```

SQLite：

```sql
CREATE TABLE IF NOT EXISTS logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,            -- 'YYYY-MM-DDTHH:MM:SS.sssZ'
    level       INTEGER NOT NULL,
    level_text  TEXT NOT NULL,
    sys_name    TEXT NOT NULL,
    file        TEXT,
    line        INTEGER,
    trace_id    TEXT,
    message     TEXT NOT NULL,
    fields      TEXT NOT NULL DEFAULT '{}',
    received_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_logs_ts       ON logs (ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_sys_ts   ON logs (sys_name, ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_level_ts ON logs (level, ts DESC);
CREATE INDEX IF NOT EXISTS idx_logs_trace    ON logs (trace_id);
-- kv 过滤走 json_extract(fields, '$.key')；量小全表扫可接受，v2 视需要加表达式索引
```

连接参数（SQLite）：`PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout=5000;`。

### 4.3 保留策略

- `RETENTION_DAYS`（默认 30，0=永久）：Retention 线程每日 `DELETE FROM logs WHERE ts < now()-N days`；PG 随后 `VACUUM`（低峰执行），SQLite 定期 `PRAGMA incremental_vacuum`/重建。
- 死信文件 `dead_letters/YYYYMMDD.jsonl` 同样按保留期清理。

---

## 5. API 设计

统一响应结构（错误复用 `lizysdk.errors` 的码表，保持客户端/服务端一致）：

```json
// 成功
{"code": "OK", "message": "ok", "data": {...}}
// 失败（对齐 lizysdk ErrorCode）
{"code": "PARAM_INVALID", "message": "参数无效: limit", "http_status": 400, "details": {...}}
```

认证：`Authorization: Bearer <token>`，`AUTH_TOKENS` 为逗号分隔白名单；未配置则不鉴权（本地开发默认）。

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/logs` | 单条摄入（SDK 现行逐条发送直达此端点），返回 `202` |
| POST | `/api/v1/logs/batch` | 批量摄入：body 为 JSONL 文本（每行一条，Content-Type: `text/plain`）或 JSON 数组；单批上限 1000 条 |
| GET | `/api/v1/logs` | 检索（参数见下） |
| GET | `/api/v1/logs/count` | 同过滤条件仅返回总数 |
| GET | `/api/v1/stats` | 近 24h 按 sys_name/level 计数、队列深度、dropped/failed/dead_letter 计数、存储占用 |
| GET | `/api/v1/health` | 存活 + 数据库连通 |

### 5.1 检索参数（GET /api/v1/logs）

| 参数 | 类型 | 说明 |
|---|---|---|
| `start` / `end` | ISO8601 | 时间范围（闭区间）；无时区按 `INCOMING_TZ` 解释 |
| `sys_name` | str，可重复 | 多值 OR |
| `level` | str，可重复 | 精确级别，如 `ERROR` |
| `level_gte` | str | 级别下限，如 `WARNING`（≥30） |
| `trace_id` | str | 全链路查询 |
| `q` | str | message 全文：PG 用 `ILIKE '%q%'`（可演进 pg_trgm）；SQLite 用 `LIKE`，v2 升级 FTS5 `MATCH` |
| `limit` / `offset` | int | 默认 100，上限 1000；默认按 `ts DESC` |
| **其余任意参数** | str | 一律视为 **fields kv 过滤**（VictoriaLogs 风格）：`?user_id=123&region=cn` → PG `fields @> '{"user_id":"123"}'`；SQLite `json_extract(fields,'$.user_id')='123'`。保留字之外的参数名即业务 key |

响应示例：

```json
{"code": "OK", "message": "ok", "data": {
  "total": 1234,
  "items": [
    {"id": 981, "ts": "2026-09-19T02:30:00Z", "level": "ERROR", "sys_name": "order-svc",
     "file": "app.py", "line": 42, "trace_id": "a3f0...", "message": "db failed",
     "fields": {"user_id": "123", "order_no": "SO-001"}, "received_at": "2026-09-19T02:30:00Z"}
  ]
}}
```

### 5.2 校验规则（摄入）

- `level` ∈ {DEBUG, INFO, WARNING, ERROR, CRITICAL}（大小写不敏感，规范化为大写）
- `sys_name`：非空，≤64 字符
- `timestamp`：`%Y-%m-%dT%H:%M:%S`（SDK 现行）或 RFC3339 带时区；非法→ 记入死信并返回 `202`（**摄入端宽容策略**：单条坏数据不影响整批，不回报 4xx 给 SDK——SDK 发送方对 4xx 也只会吞掉计数）
- `message`/kv：一律转字符串存储（与 SDK 行为一致）

---

## 6. 写入路径（关键设计）

```
请求 → pydantic 校验 → 规范化(level大写/ts→UTC datetime/剥离保留字kv)
     → queue.put(nowait)  # 满则 pop 最旧 + dropped++，永不阻塞
     → 返回 202

写入线程（唯一写者，daemon）:
    loop:
        批触发: 攒满 BATCH_SIZE(200) 或 FLUSH_INTERVAL(500ms) 先到者
        executemany INSERT（参数化，单事务）
        失败 → 重试 2 次（指数退避）→ 仍失败整批追加 dead_letters/YYYYMMDD.jsonl + failed++
停机(atexit/SIGTERM):
    停接收 → 队列排空落库 → 剩余直写死信 → 关连接
```

- SQLite：写连接仅此线程持有（`check_same_thread=False` 亦可，但约定单写者）；读请求各自短连接或简单连接池，WAL 下读写不互斥。
- PostgreSQL：`psycopg` 连接池（`ConnectionPool`，min 1 max 5），批量用 `execute_values` 风格（psycopg3 `copy` 亦可，v1 用 executemany 足够）。
- 死信重放：`POST /api/v1/logs/batch` 本身就是重放工具（运维把死信文件直接 POST 回来）。

---

## 7. 检索实现

查询构建器把统一过滤条件编译为方言 SQL（两方言差异全部收敛在这一层）：

| 能力 | PostgreSQL | SQLite |
|---|---|---|
| 时间 | `ts BETWEEN $1 AND $2`（timestamptz） | `ts BETWEEN ? AND ?`（ISO 文本比较） |
| kv 过滤 | `fields @> %s::jsonb` | `json_extract(fields,'$.k') = ?` |
| 全文 q | `message ILIKE '%q%'` | `message LIKE '%q%'`（v2: FTS5） |
| 分页 | `ORDER BY ts DESC LIMIT/OFFSET` | 同左 |
| 计数 | `SELECT count(*)` 同过滤 | 同左 |

所有值参数化绑定，禁止字符串拼接（防注入）。`/logs/count` 与 `/logs` 共用同一构建器，避免语义漂移。

---

## 8. 配置项

环境变量（或 `config.py` dataclass 读取 `.env`，不引第三方 dotenv，直接 `os.environ`）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `LOGSDB_BACKEND` | `sqlite` | `sqlite` / `postgres` |
| `LOGSDB_SQLITE_PATH` | `data/lizylog.db` | SQLite 文件路径 |
| `LOGSDB_PG_DSN` | — | `postgresql://user:pwd@host:5432/lizylog`（backend=postgres 必填） |
| `LOGSDB_INCOMING_TZ` | `Asia/Shanghai` | 无时区时间戳的解释时区 |
| `LOGSDB_RETENTION_DAYS` | `30` | 0=永久 |
| `LOGSDB_AUTH_TOKENS` | 空 | 逗号分隔 Bearer token；空=不鉴权 |
| `LOGSDB_BATCH_SIZE` / `LOGSDB_FLUSH_MS` | `200` / `500` | 批量阈值 |
| `LOGSDB_QUEUE_MAX` | `10000` | 背压上限 |
| `LOGSDB_HOST` / `LOGSDB_PORT` | `0.0.0.0` / `9280` | 监听地址 |

SDK 侧对接：`send_url="http://server:9280/api/v1/logs"`（即装即用；配 token 时 SDK 需支持发送头，见 §13）。

---

## 9. 目录结构

仓库内独立子项目 `server/`（不并入 lizysdk 包，服务端允许第三方依赖）：

```
server/
├── pyproject.toml            # 独立依赖：fastapi/uvicorn（+psycopg 可选组）
├── src/lizylog/
│   ├── main.py               # 装配与入口（uvicorn lizylog.main:app）
│   ├── config.py             # 环境变量 → 冻结 dataclass
│   ├── schemas.py            # pydantic 摄入/响应模型
│   ├── ingest.py             # 校验、规范化、入队
│   ├── writer.py             # 批量写者线程 + 死信
│   ├── retention.py          # 保留期清理线程
│   ├── storage/
│   │   ├── base.py           # LogStorage 协议 + LogRow/SearchQuery 数据类
│   │   ├── sqlite_driver.py  # sqlite3 + WAL
│   │   └── pg_driver.py      # psycopg v3
│   ├── search.py             # 查询构建器（过滤→方言 SQL）
│   └── api.py                # FastAPI 路由 + 认证依赖
└── tests/                    # 见 §12
```

---

## 10. 部署

- **SQLite 最简**：`pip install -e server[sqlite] && uvicorn lizylog.main:app --port 9280`（单文件库 + 死信目录，整服务可随项目目录拷贝迁移）。
- **PostgreSQL**：`LOGSDB_BACKEND=postgres LOGSDB_PG_DSN=...`，其余不变。
- Docker（可选，v1 提供 Dockerfile）：单容器；SQLite 卷挂 `/app/data`。
- systemd 示例文档随代码给出。**必须单 worker**（`--workers 1` 是硬约束）。

---

## 11. 容量与性能预估

- 假设 1 万条/天 × 500B ≈ 5MB/天；30 天保留 ≈ 150MB、约 30 万行——SQLite 轻松承载（百万行内索引查询毫秒级）。
- 批量写吞吐实测预期 >5,000 行/s（WAL + executemany），远超小量级摄入峰值。
- 检索：`(sys_name, ts)`、`trace_id` 走索引毫秒级；仅 `q` 全文 + 无时间范围时走扫表，小量级可接受。

---

## 12. 测试策略

- **单元**：校验/规范化（级别、时区转换、kv 剥离）、查询构建器双方言 SQL 快照、retention 边界。
- **集成（SQLite）**：tmp_path 起库 → 摄入 → flush → 检索往返断言；批量端点、kv 过滤、分页排序；背压丢弃计数；死信写入与重放。
- **集成（PG）**：有环境则跑（`LOGSDB_PG_DSN` 指向测试库），无则标记 skip；两驱动跑同一套契约用例（参数化 fixture），保证行为一致。
- **SDK 契约**：用 `lizysdk` 真发若干条（send_json 到本地起的服务）→ 检索断言字段一一对应。
- **停机语义**：入队后立即停机，断言数据在库或死信二选一、总数守恒。

---

## 13. SDK 侧配套（可选小改动，不阻塞 v1）

| 改动 | 说明 | 必要性 |
|---|---|---|
| `send_headers: dict` 参数 | 支持服务端 token（`Authorization: Bearer ...`） | 配置鉴权时需要 |
| 批量发送缓冲 | SDK 端攒批 POST `/logs/batch`（JSONL），降低开销 | 量小可后置 |
| JSON 时间戳带时区 | RFC3339 `+08:00`，消除 `INCOMING_TZ` 依赖 | 建议，向后兼容 |

---

## 14. 演进路线

- **v1**：本文档范围（摄入/存储/检索/retention/死信/鉴权）
- **v2**：极简查询页（单文件 HTML，表格 + 过滤表单）、SQLite FTS5 全文、pg_trgm、`/stats` 简单图表、批量摄入在 SDK 端默认开启
- **量级增长**（日 GB 级或需聚合分析）：不自研扩容，按调研结论迁移 VictoriaLogs；`LogStorage`/API 语义设计已与 LogsQL 风格对齐（任意 kv 过滤、trace_id 一等公民），迁移成本低

---

## 15. 关键设计决策记录（ADR 摘要）

| # | 决策 | 理由 |
|---|---|---|
| 1 | 摄入返回 202，落库异步 | 解耦接收与写入吞吐；失败走死信，不向 SDK 报错（SDK 本就吞错） |
| 2 | 坏日志进死信而非 4xx | 单条脏数据不拖垮批次；死信可重放 |
| 3 | `ts` 统一 UTC 存储 | 双库排序/比较语义一致；SQLite 用 ISO 文本保字典序 |
| 4 | `trace_id` 物理列 + 索引 | 第一查询模式，JSON 内无法高效索引 |
| 5 | 单进程单写者 | SQLite 约束 + 队列在进程内；扩容路径是换 PG/换专用日志库，非多进程 |
| 6 | 错误结构复用 lizysdk ErrorCode | 客户端/服务端错误语义统一，工具链一致 |
| 7 | 手写薄存储层而非 SQLAlchemy | 单表场景方言差异可控，依赖最小化；表变多再评估 |
