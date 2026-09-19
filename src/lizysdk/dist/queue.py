"""lizysdk.dist.queue —— 可靠任务队列 + 延迟队列（Redis list / ZSET）。

两个队列共用同一 ``Job`` 信封模型（设计文档 §1.4 / §1.5）：

- :class:`ReliableQueue` 实现形态对齐 redis.io 官方 Reliable queue
  pattern——生产者 ``LPUSH``、消费者 ``LMOVE queue processing`` 原子弹入
  备份列表、处理完 ``LREM`` 确认、清扫者 ``LMOVE`` 搬回，at-least-once
  语义（**可能重复投递，消费方需幂等**）；
- :class:`DelayQueue` 结构对齐 Redisson ``RDelayedQueue``——ZSET 存定时
  任务（score = 到期时间戳）+ 原子 Lua 把到期项搬到 ready list；差异：
  Redisson 用 pub/sub 唤醒后台定时器，这里简化为**消费前拉取式**
  （pop 前先搬到期项，无后台线程；``move_due`` 也独立暴露）。

阻塞实现注记：设计文档指定 ``pop(timeout>0)`` 走 BLMOVE；fakeredis 的
BLMOVE 在空队列上**不阻塞**（立即返回 None），故实现为 BLMOVE 小步
循环 + ``_sleep`` 间隔（真 Redis 下 BLMOVE 本身服务端阻塞，循环只是
deadline 兜底），两种环境下语义一致。BLMOVE 的 ``timeout=0``（永久
阻塞）语义**禁用**——SDK 不提供无限阻塞入口。

本模块自持：仅标准库，模块级不 import redis、不依赖 lizysdk 其他子包；
所有方法只收已建好的 client 实例（懒加载由 ``_client`` / 集成方负责）。
"""

from __future__ import annotations

import json
import secrets
import time
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Union

__all__ = ["DelayQueue", "Job", "ReliableQueue"]

#: 阻塞弹出的小步步长（秒）——BLMOVE/BRPOP 每次只阻塞一小步，循环到
#: deadline；配合 ``_sleep`` 间隔防止 fakeredis（不真阻塞的 BLMOVE）下
#: 忙等。步长越小取货延迟越低、轮询开销越高。
_BLOCK_STEP: float = 0.05

# ---------------------------------------------------------------------------
# 时间 / sleep 接缝（测试 monkeypatch 用，设计文档 §3 红线 6）
# ---------------------------------------------------------------------------


def _now() -> float:
    """当前时间戳（秒，浮点）。

    独立成模块级函数是刻意的**时间接缝**：测试用它 monkeypatch 冻结/推进
    时间，以离线验证延迟队列的到期搬运逻辑（见 tests/test_dist_queue.py）。
    注意：冻结 ``_now`` 的测试请只用非阻塞入口（timeout=0）——阻塞入口
    的 deadline 依赖 ``_now()`` 推进。
    """
    return time.time()


def _sleep(seconds: float) -> None:
    """线程 sleep 接缝（测试可替换为 no-op 提速）。

    Args:
        seconds: 时长（秒）；<= 0 直接返回（防御 deadline 竞态下的负值，
            ``time.sleep`` 遇负数会抛 ValueError）。
    """
    if seconds > 0:
        time.sleep(seconds)


# ---------------------------------------------------------------------------
# Job 信封模型
# ---------------------------------------------------------------------------

Job = NamedTuple("Job", [("id", str), ("payload", str), ("raw", str), ("tries", int)])
Job.__doc__ = """pop 系列返回的任务信封解包结果（不可变四元组）。

Attributes:
    id: 任务唯一 id（``secrets.token_hex(8)``，16 位十六进制）——跨重投
        保持不变（nack / recover 不换 id），是 :meth:`DelayQueue.cancel`
        的定位句柄。
    payload: 用户入队的**原文**（信封解包后的 str，原样往返）。
    raw: 信封 JSON 原文——:meth:`ReliableQueue.ack` /
        :meth:`ReliableQueue.nack` 靠它对 processing 列表做精确 LREM。
    tries: 重投次数——首次投递为 0，每 nack 一次 +1（recover 原样搬回
        不累加；DelayQueue 无重投通道，恒为 0）。
"""


# ---------------------------------------------------------------------------
# 内部辅助
# ---------------------------------------------------------------------------


def _to_text(value: Any) -> Any:
    """redis 返回值统一转 str：bytes 按 utf-8 解码（兼容 decode_responses=True 的客户端），str 原样。"""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _make_envelope(payload: str) -> Dict[str, Any]:
    """构造内部信封（设计文档 §1.4）：``{"id","payload","pushed_at","tries"}``。"""
    return {
        "id": secrets.token_hex(8),
        "payload": payload,
        "pushed_at": _now(),
        "tries": 0,
    }


def _dumps(env: Dict[str, Any]) -> str:
    """信封 -> 紧凑 JSON 文本（utf-8 直存中文）。"""
    return json.dumps(env, ensure_ascii=False, separators=(",", ":"))


def _parse_job(raw: Any) -> Optional[Job]:
    """LMOVE/RPOP 返回的信封原文 -> Job（``None`` 透传为 ``None``）。

    队列内容必须由本类写入（信封 JSON）；外部塞入的非法成员会让
    ``json.loads`` 异常自然暴露——这是刻意的诚实失败（静默吞掉会破坏
    at-least-once 语义）。
    """
    if raw is None:
        return None
    text = _to_text(raw)
    env = json.loads(text)
    return Job(
        id=str(env["id"]),
        payload=str(env["payload"]),
        raw=text,
        tries=int(env["tries"]),
    )


# ---------------------------------------------------------------------------
# Lua 脚本（模块级常量）
# ---------------------------------------------------------------------------

#: 搬运脚本：把 ZSET 中到期的任务原子搬到 ready list（对齐 Redisson
#: RDelayedQueue 的 transfer 语义）。
#:
#: - KEYS[1]: 延迟 ZSET 键（``{prefix}:{name}:zset``，score = 到期时间戳）
#: - KEYS[2]: 就绪 list 键（``{prefix}:{name}:ready``）
#: - ARGV[1]: 当前时间戳（秒，浮点字符串）——score **<=** 该值即视为到期
#: - ARGV[2]: 本次搬运上限 limit（>= 1 的整数字符串）
#:
#: 逐项 ``ZREM`` 成功（返回 1）才 ``LPUSH`` ready——多搬运者并发时同一
#: 任务至多被一个搬运者入队（**不重**）；ZSET 语义保证 score <= now 的
#: 任务要么本轮被搬走、要么原样留在 ZSET 等下一轮（**不漏**）。
#: 返回实际搬运数。到期序 = score 升序，LPUSH 后 RPOP 弹出的即最早到期者。
_MOVE_DUE_LUA = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
local moved = 0
for _, member in ipairs(due) do
  if redis.call('ZREM', KEYS[1], member) == 1 then
    redis.call('LPUSH', KEYS[2], member)
    moved = moved + 1
  end
end
return moved
"""


# ---------------------------------------------------------------------------
# ReliableQueue
# ---------------------------------------------------------------------------


class ReliableQueue:
    """Redis 可靠任务队列（list + processing 备份列表，at-least-once）。

    实现形态对齐 **redis.io 官方 Reliable queue pattern**（``LMOVE`` 文档
    + Reliable queue 教程，已调研确认），纯命令组合、不用 Lua：

    .. code-block:: text

        生产者:   LPUSH queue 信封
        消费者:   LMOVE queue processing LEFT RIGHT   # 原子弹出到备份列表
        处理完:   LREM processing 1 信封              # ack：从备份列表移除
        崩溃恢复: LMOVE processing queue RIGHT LEFT   # 清扫者把残留搬回

    语义要点：

    - **at-least-once**：worker 处理中崩溃 -> 任务留在 processing ->
      :meth:`recover` 搬回重投——**可能重复，消费方需幂等**（官方文档
      明示）；恢复为简单版全量搬回（残留即疑似失败），按时间的可见性
      超时列为 v2；
    - 内部信封 ``{"id","payload","pushed_at","tries"}``：pop **解信封**
      返回原始 payload，信封原文挂在 ``job.raw`` 供 ack/nack 精确 LREM
      （同 payload 多份也只删自己那份）；nack 重投 **tries+1** 且 id 不变；
    - 实例无本地可变状态，**可跨线程共享同一实例**（互斥完全由 Redis
      命令原子性保证）。

    Args:
        client: redis 客户端实例（需支持 LPUSH/LMOVE/BLMOVE/LREM/LLEN）。
        name: 队列名（非空字符串）。
        prefix: 键前缀（默认 ``"lizy:rq"``）——实际键为
            ``{prefix}:{name}``（queue list）与
            ``{prefix}:{name}:processing``（备份列表）。

    Raises:
        ValueError: name 为空 / 非 str（中文消息）。

    Example:
        >>> from lizysdk.dist import ReliableQueue                  # doctest: +SKIP
        >>> rq = ReliableQueue(client, "orders")
        >>> rq.push("order:123")            # LPUSH，返回任务 id
        '3f2a...'
        >>> job = rq.pop(timeout=5.0)       # LMOVE -> processing；无货 None
        >>> job.payload
        'order:123'
        >>> rq.ack(job)                     # LREM：处理成功确认
        True
        >>> rq.nack(job)                    # 处理失败：LREM + 回队（tries+1）
        False
        >>> rq.recover()                    # 清扫：processing 残留搬回 queue
        0
    """

    def __init__(self, client: Any, name: str, *, prefix: str = "lizy:rq") -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"name 必须为非空字符串，当前为 {name!r}")
        self._client = client
        self._name = name
        self._prefix = prefix
        self._queue_key = f"{prefix}:{name}"
        self._processing_key = f"{prefix}:{name}:processing"

    # ------------------------------------------------------------------
    # 元信息
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """队列名（只读）。"""
        return self._name

    @property
    def queue_key(self) -> str:
        """主队列 list 的实际 Redis 键：``{prefix}:{name}``（只读）。"""
        return self._queue_key

    @property
    def processing_key(self) -> str:
        """processing 备份列表的实际 Redis 键：``{prefix}:{name}:processing``（只读）。"""
        return self._processing_key

    # ------------------------------------------------------------------
    # 生产
    # ------------------------------------------------------------------

    def push(self, payload: str) -> str:
        """入队：``LPUSH queue 信封``（对齐官方模式的生产者形态）。

        Args:
            payload: 任务原文（str；空串合法——只要求是 str）。

        Returns:
            str: 新任务的唯一 id（16 位十六进制），可留作业务侧追踪句柄。

        Raises:
            ValueError: payload 非 str（中文消息）。

        Example:
            >>> rq.push("email:send:42")     # doctest: +SKIP
            'a1b2c3d4e5f60718'
        """
        if not isinstance(payload, str):
            raise ValueError(f"payload 必须为字符串（str），当前为 {payload!r}")
        env = _make_envelope(payload)
        self._client.lpush(self._queue_key, _dumps(env))
        return env["id"]

    def push_many(self, payloads: Iterable[str]) -> List[str]:
        """批量入队：一次 ``LPUSH`` 写入多条（网络往返从 N 次收敛为 1 次）。

        Args:
            payloads: 任务原文的可迭代对象（每个元素均为 str；空可迭代
                对象为合法 no-op，返回空列表）。

        Returns:
            List[str]: 各任务的唯一 id（与输入顺序一致）。

        Raises:
            ValueError: payloads 不可迭代 / 含非 str 元素 / 传入裸 str
                （中文消息）。

        Example:
            >>> rq.push_many(["a", "b", "c"])                     # doctest: +SKIP
            ['0f1e...', '2d3c...', '4b5a...']
        """
        if isinstance(payloads, (str, bytes)):
            raise ValueError(
                f"payloads 必须为字符串的可迭代对象（如 list），当前为 {payloads!r}"
            )
        try:
            items = list(payloads)
        except TypeError:
            raise ValueError(
                f"payloads 必须为字符串的可迭代对象（如 list），当前为 {payloads!r}"
            ) from None
        for item in items:
            if not isinstance(item, str):
                raise ValueError(f"payloads 的每个元素必须为 str，当前含 {item!r}")
        if not items:
            return []
        envs = [_make_envelope(item) for item in items]
        self._client.lpush(self._queue_key, *[_dumps(env) for env in envs])
        return [env["id"] for env in envs]

    # ------------------------------------------------------------------
    # 消费 / 确认 / 重投 / 恢复
    # ------------------------------------------------------------------

    def pop(self, timeout: float = 0.0) -> Optional[Job]:
        """弹出任务：``LMOVE queue processing LEFT RIGHT``（原子，对齐官方模式）。

        Args:
            timeout: 等待上限（秒，>= 0）。``0``（默认）**非阻塞**——队列
                空立即返回 None；``> 0`` 阻塞等待至拿到任务或到期。

        Returns:
            Optional[Job]: 拿到任务返回解信封后的 :class:`Job`（payload
            为用户原文、raw 为信封原文、任务同时在 processing 备份列表
            里）；队列空（或超时无货）返回 **None**——本方法永不抛
            「队列为空」异常。

        Raises:
            ValueError: timeout < 0 / 类型非法（中文消息）。

        Note:
            阻塞实现为 **BLMOVE 小步循环**（步长 ``_BLOCK_STEP`` 秒 +
            ``_sleep`` 间隔，循环到 deadline）：真 Redis 下 BLMOVE 本身
            服务端阻塞；fakeredis 的 BLMOVE 空队列上不阻塞（立即返回
            None），小步循环保证两种环境下语义一致。**BLMOVE 的
            timeout=0（永久阻塞）语义禁用**——timeout=0 在本类恒为
            非阻塞 LMOVE。冻结 ``_now`` 的测试请只用 timeout=0 入口
            （deadline 依赖 ``_now()`` 推进）。
        """
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout < 0
        ):
            raise ValueError(f"timeout 必须为 >= 0 的秒数，当前为 {timeout!r}")
        if timeout <= 0:
            raw = self._client.lmove(self._queue_key, self._processing_key, "LEFT", "RIGHT")
            return _parse_job(raw)
        deadline = _now() + float(timeout)
        while True:
            step = min(_BLOCK_STEP, deadline - _now())
            if step <= 0:
                return None
            raw = self._client.blmove(
                self._queue_key, self._processing_key, step, src="LEFT", dest="RIGHT"
            )
            if raw is not None:
                return _parse_job(raw)
            _sleep(min(_BLOCK_STEP, deadline - _now()))

    def ack(self, job: Job) -> bool:
        """处理成功确认：``LREM processing 1 job.raw``（对齐官方模式）。

        Args:
            job: :meth:`pop` 返回的 Job（靠 ``job.raw`` 精确匹配自己的
                信封——同 payload 多份也只删这一份）。

        Returns:
            bool: 成功移除返回 True；该信封已不在 processing（已 ack 过 /
                已 nack 过 / 已被 recover 搬走）返回 False。

        Raises:
            ValueError: job 不是 Job 实例（中文消息）。
        """
        if not isinstance(job, Job):
            raise ValueError(
                f"job 必须为 Job 实例（由 pop 返回），当前为 {type(job).__name__}"
            )
        return bool(self._client.lrem(self._processing_key, 1, job.raw))

    def nack(self, job: Job) -> bool:
        """处理失败重投：``LREM processing`` + ``LPUSH queue``（tries+1）。

        重投保持任务身份不变（id / payload / pushed_at 原样），仅信封的
        ``tries`` 加一——消费方可据 ``job.tries`` 实现退避或死信策略。

        Args:
            job: :meth:`pop` 返回的 Job。

        Returns:
            bool: 成功移除并回队返回 True；该信封已不在 processing
                （已 ack / 已 nack / 已被 recover 搬走）返回 False——
                **不做任何写入**。

        Raises:
            ValueError: job 不是 Job 实例（中文消息）。
        """
        if not isinstance(job, Job):
            raise ValueError(
                f"job 必须为 Job 实例（由 pop 返回），当前为 {type(job).__name__}"
            )
        removed = self._client.lrem(self._processing_key, 1, job.raw)
        if not removed:
            return False
        env = json.loads(job.raw)
        env["tries"] = int(env.get("tries", 0)) + 1
        self._client.lpush(self._queue_key, _dumps(env))
        return True

    def recover(self) -> int:
        """崩溃恢复（清扫）：processing 残留**全量**搬回 queue。

        官方简单版恢复——循环 ``LMOVE processing queue RIGHT LEFT`` 直到
        processing 空（保持纯命令、不用 Lua，更贴官方文档；单条 LMOVE
        原子，循环只保证多轮清空）。残留即疑似失败：已 ack 的任务早已
        被 LREM，剩下的就是处理中崩溃者的 at-least-once 重投。

        Returns:
            int: 实际搬运的任务数（processing 为空时为 0）。

        Example:
            >>> rq.recover()               # doctest: +SKIP
            3
        """
        moved = 0
        while True:
            raw = self._client.lmove(self._processing_key, self._queue_key, "RIGHT", "LEFT")
            if raw is None:
                return moved
            moved += 1

    # ------------------------------------------------------------------
    # 观测
    # ------------------------------------------------------------------

    def qsize(self) -> int:
        """主队列当前长度（``LLEN``，待消费任务数）。"""
        return int(self._client.llen(self._queue_key))

    def processing_size(self) -> int:
        """processing 备份列表当前长度（已弹出未确认的任务数）。"""
        return int(self._client.llen(self._processing_key))

    def __repr__(self) -> str:
        """调试视图（不回显客户端，避免泄露连接信息）。"""
        return (
            f"{self.__class__.__name__}(name={self._name!r}, "
            f"prefix={self._prefix!r})"
        )


# ---------------------------------------------------------------------------
# DelayQueue
# ---------------------------------------------------------------------------


class DelayQueue:
    """Redis 延迟队列（ZSET 定时 + 原子 Lua 搬运到 ready list）。

    结构对齐 **Redisson ``RDelayedQueue``**（已调研确认）：ZSET 存定时
    任务（member = 信封 JSON、score = 到期时间戳），到期项被原子搬到
    目标 list。语义差异（显式声明）：Redisson 用 pub/sub 唤醒后台定时
    线程，本类简化为**消费前拉取式**——:meth:`pop_ready` 先搬到期项再
    弹、:meth:`move_due` 独立暴露给外部定时器，无后台线程（pub/sub
    即时唤醒列为 v2，见设计文档 §0 P1/P2）。

    键结构：``{prefix}:{name}:zset``（延迟 ZSET）与
    ``{prefix}:{name}:ready``（就绪 list，LPUSH 进 / RPOP 出 = FIFO，
    最早到期者先出）。

    与 ReliableQueue 的关系：本类 ready list 无 processing 备份（弹出即
    出队，无 at-least-once）；若需「到期后进可靠队列」，用户自行把
    DelayQueue 桥接到 ReliableQueue（SDK 不做自动桥接，保持单一职责）。

    实例无本地可变状态，**可跨线程共享同一实例**（搬运原子性由 Lua
    脚本保证）。

    Args:
        client: redis 客户端实例（需支持 ZADD/ZRANGEBYSCORE/EVAL/RPOP/
            BRPOP/LLEN）。
        name: 队列名（非空字符串）。
        prefix: 键前缀（默认 ``"lizy:dq"``）。

    Raises:
        ValueError: name 为空 / 非 str（中文消息）。

    Example:
        >>> from lizysdk.dist import DelayQueue                     # doctest: +SKIP
        >>> dq = DelayQueue(client, "reminders")
        >>> dq.push("sms:send:42", delay=30.0)   # 30 秒后到期，返回任务 id
        '9f8e...'
        >>> dq.move_due()                        # 到期项原子搬到 ready list
        0
        >>> job = dq.pop_ready(timeout=5.0)      # 搬运 + 弹出；无货 None
        >>> dq.cancel("9f8e...")                 # 未到期时按 id 取消
        False
        >>> dq.due_size(), dq.ready_size()
        (0, 0)
    """

    def __init__(self, client: Any, name: str, *, prefix: str = "lizy:dq") -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"name 必须为非空字符串，当前为 {name!r}")
        self._client = client
        self._name = name
        self._prefix = prefix
        self._zset_key = f"{prefix}:{name}:zset"
        self._ready_key = f"{prefix}:{name}:ready"

    # ------------------------------------------------------------------
    # 元信息
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """队列名（只读）。"""
        return self._name

    @property
    def zset_key(self) -> str:
        """延迟 ZSET 的实际 Redis 键：``{prefix}:{name}:zset``（只读）。"""
        return self._zset_key

    @property
    def ready_key(self) -> str:
        """就绪 list 的实际 Redis 键：``{prefix}:{name}:ready``（只读）。"""
        return self._ready_key

    # ------------------------------------------------------------------
    # 生产 / 搬运 / 消费 / 取消
    # ------------------------------------------------------------------

    def push(self, payload: str, delay: float) -> str:
        """入队：``ZADD zset score=now+delay 信封``（score = 到期时间戳）。

        Args:
            payload: 任务原文（str）。
            delay: 延迟时长（秒，> 0）——到期时刻为 ``_now() + delay``。

        Returns:
            str: 新任务的唯一 id（16 位十六进制）——:meth:`cancel` 的
            定位句柄，请业务侧留存。

        Raises:
            ValueError: payload 非 str / delay <= 0 或类型非法（中文消息）。

        Example:
            >>> dq.push("cleanup:tmp", delay=3600.0)               # doctest: +SKIP
            '0a1b2c3d4e5f6071'
        """
        if not isinstance(payload, str):
            raise ValueError(f"payload 必须为字符串（str），当前为 {payload!r}")
        if isinstance(delay, bool) or not isinstance(delay, (int, float)) or delay <= 0:
            raise ValueError(f"delay 必须为大于 0 的秒数，当前为 {delay!r}")
        now = _now()
        env = _make_envelope(payload)
        self._client.zadd(self._zset_key, {_dumps(env): now + float(delay)})
        return env["id"]

    def move_due(self, limit: int = 100) -> int:
        """搬运到期项：原子 Lua（``ZRANGEBYSCORE -inf now LIMIT`` → 逐项
        ``ZREM`` 成功才 ``LPUSH ready``），返回实际搬运数。

        对齐 Redisson transfer 语义：多搬运者并发调用时同一任务至多入队
        一次（不重）；score <= now 的任务要么被搬走要么留在 ZSET（不漏）。

        Args:
            limit: 单次搬运上限（>= 1 的整数；到期积压多时多次调用或
                调大本值）。

        Returns:
            int: 本次实际搬运的任务数。

        Raises:
            ValueError: limit 非 int / < 1（中文消息）。
        """
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError(f"limit 必须为 >= 1 的整数，当前为 {limit!r}")
        result = self._client.eval(
            _MOVE_DUE_LUA,
            2,
            self._zset_key,
            self._ready_key,
            str(_now()),  # ARGV[1] 当前时间戳（score <= 该值即到期）
            str(limit),   # ARGV[2] 搬运上限
        )
        return int(result)

    def pop_ready(self, timeout: float = 0.0) -> Optional[Job]:
        """弹出到期任务：先 :meth:`move_due` 搬运，再 ``RPOP ready``。

        Args:
            timeout: 等待上限（秒，>= 0）。``0``（默认）非阻塞——搬一轮
                到期项后 RPOP，无货立即返回 None；``> 0`` 阻塞：
                ``BRPOP ready`` 小步循环到 deadline，每步之间**再搬一轮
                到期项**（等待期间新到期的任务也能被取到），到期无货
                返回 None。

        Returns:
            Optional[Job]: 拿到任务返回 :class:`Job`（payload 为用户原文，
            tries 恒为 0——本类无重投通道）；无货 / 超时返回 **None**——
            本方法永不抛「队列为空」异常。

        Raises:
            ValueError: timeout < 0 / 类型非法（中文消息）。

        Note:
            冻结 ``_now`` 的测试请只用 timeout=0 入口（deadline 与到期
            判定都依赖 ``_now()`` 推进）。
        """
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or timeout < 0
        ):
            raise ValueError(f"timeout 必须为 >= 0 的秒数，当前为 {timeout!r}")
        if timeout <= 0:
            self.move_due()
            return _parse_job(self._client.rpop(self._ready_key))
        deadline = _now() + float(timeout)
        while True:
            self.move_due()
            remaining = deadline - _now()
            if remaining <= 0:
                return None
            pair = self._client.brpop(self._ready_key, min(_BLOCK_STEP, remaining))
            if pair is not None:
                return _parse_job(pair[1])
            _sleep(min(_BLOCK_STEP, max(0.0, deadline - _now())))

    def cancel(self, job: "Union[Job, str]") -> bool:
        """取消未到期任务：按任务 id 从延迟 ZSET 删除（``ZREM``）。

        Args:
            job: :meth:`push` 返回的**任务 id 字符串**，或含该 id 的
                :class:`Job` 对象（两种形态均可——id 才是定位键）。

        Returns:
            bool: 成功取消返回 True；任务不在延迟 ZSET（从未存在 / 已
            取消 / **已被搬运到 ready list**——过了可取消窗口）返回
            False。

        Raises:
            ValueError: job 既不是 Job 实例也不是非空 str（中文消息）。

        Note:
            实现说明：ZSET member 是完整信封 JSON，``ZREM`` 需要完整
            member，而本方法只拿得到 id——故先 ``ZRANGE`` 全量扫描、
            解包信封比对 id 再 ``ZREM``（**O(N) 扫描**，N 为当前未
            到期任务数；取消是低频管理操作，可接受）。扫描与 ZREM 之间
            的竞态是良性的：若搬运脚本抢先 ZREM，本方法 ZREM 返回 0 ->
            False（任务已进 ready，不可取消，语义自洽）。
        """
        if isinstance(job, Job):
            job_id = job.id
        elif isinstance(job, str) and job:
            job_id = job
        else:
            raise ValueError(f"job 必须为 Job 实例或非空 str（任务 id），当前为 {job!r}")
        for member in self._client.zrange(self._zset_key, 0, -1):
            text = _to_text(member)
            try:
                env = json.loads(text)
            except (ValueError, TypeError):
                continue
            if isinstance(env, dict) and env.get("id") == job_id:
                return bool(self._client.zrem(self._zset_key, text))
        return False

    # ------------------------------------------------------------------
    # 观测
    # ------------------------------------------------------------------

    def due_size(self) -> int:
        """延迟 ZSET 当前长度（``ZCARD``，已入队未到期任务数）。"""
        return int(self._client.zcard(self._zset_key))

    def ready_size(self) -> int:
        """就绪 list 当前长度（``LLEN``，已搬运待消费任务数）。"""
        return int(self._client.llen(self._ready_key))

    def __repr__(self) -> str:
        """调试视图（不回显客户端，避免泄露连接信息）。"""
        return (
            f"{self.__class__.__name__}(name={self._name!r}, "
            f"prefix={self._prefix!r})"
        )
