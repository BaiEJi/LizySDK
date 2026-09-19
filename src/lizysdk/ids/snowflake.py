"""雪花算法（Snowflake）唯一 ID 生成器。

64 位 ID 布局（自高位到低位）::

    |  1 bit  |      41 bit       |   10 bit   |   12 bit   |
    | 保留符号 | 毫秒时间戳偏移量  |  worker_id | 同毫秒序列 |

位宽推导：
- 41 bit 毫秒 ≈ 2^41 / 1000 / 3600 / 24 / 365 ≈ 69.7 年的可用时间窗
  （以默认纪元 2024-01-01 UTC 计算，可用至约 2093 年）；
- 10 bit worker_id 最多支持 1024 个并发发号实例；
- 12 bit 序列表示同一实例每毫秒最多发出 4096 个 ID。

核心特性：
- 线程安全：所有生成路径都在实例锁内完成，``batch`` 仅获取一次锁；
- 时钟回拨保护：小幅回拨（<= ``MAX_CLOCK_BACKWARDS_MS`` 毫秒）自旋
  等待时钟追平；大幅回拨抛 :class:`ClockBackwardsError`；
- 同毫秒序列耗尽时自旋等待下一毫秒，保证 ID 不重复且整体递增。

与位数可选的 :func:`lizysdk.ids.new_trace_id` / :func:`new_uid`
不同，雪花 ID 的 64 位总长由位运算布局的本质决定（1+41+10+12 各段
联合编码时间戳 / 节点 / 序列），位数不可调节。

仅依赖标准库（threading / time）。
"""

from __future__ import annotations

import threading
import time

__all__ = [
    "ClockBackwardsError",
    "DEFAULT_EPOCH_MS",
    "IDGenerator",
    "MAX_SEQUENCE",
    "MAX_WORKER_ID",
    "new_id",
    "new_prefixed_id",
]

# ---------------------------------------------------------------------------
# 位布局常量（四种位宽之和必须为 64）
# ---------------------------------------------------------------------------
#: 最高位保留（符号位），恒为 0，保证生成的 ID 为正整数
SIGN_BITS: int = 1
#: 时间戳偏移量位数：2^41 毫秒 ≈ 69.7 年
TIMESTAMP_BITS: int = 41
#: 工作节点位数：2^10 = 1024 个实例
WORKER_ID_BITS: int = 10
#: 同毫秒内序列位数：2^12 = 每毫秒 4096 个
SEQUENCE_BITS: int = 12

#: worker_id 允许的最大值（2^10 - 1 = 1023）
MAX_WORKER_ID: int = (1 << WORKER_ID_BITS) - 1
#: 同毫秒内序列的最大值（2^12 - 1 = 4095）
MAX_SEQUENCE: int = (1 << SEQUENCE_BITS) - 1
#: worker_id 在 ID 中的左移位数（worker_id 的低位是 sequence）
WORKER_ID_SHIFT: int = SEQUENCE_BITS
#: 时间戳在 ID 中的左移位数（时间戳的低位是 worker_id + sequence）
TIMESTAMP_SHIFT: int = WORKER_ID_BITS + SEQUENCE_BITS

#: 默认纪元：2024-01-01T00:00:00.000Z 的 Unix 毫秒时间戳
DEFAULT_EPOCH_MS: int = 1704067200000

#: 时钟回拨容忍上限（毫秒）：回拨不超过该值时自旋等待，超过则抛异常
MAX_CLOCK_BACKWARDS_MS: int = 10
#: 自旋等待时钟推进时的轮询间隔（秒），避免忙等空转
SPIN_WAIT_SECONDS: float = 0.001


def _current_ms() -> int:
    """返回当前 Unix 时间戳（毫秒）。

    独立成模块级函数是刻意留出的测试接缝：monkeypatch 本函数即可
    模拟时钟回拨 / 停滞，无需改动生成器内部逻辑。
    """
    return int(time.time() * 1000)


def _spin_until(target_ms: int) -> int:
    """自旋等待时钟追平 ``target_ms``（返回值 >= ``target_ms``）。

    用于小幅时钟回拨：等待系统时钟重新追上最近一次发号时间。

    Args:
        target_ms: 需要追平的毫秒时间戳。

    Returns:
        int: 最新读取到的、不早于 ``target_ms`` 的时间戳。
    """
    while True:
        now = _current_ms()
        if now >= target_ms:
            return now
        time.sleep(SPIN_WAIT_SECONDS)


def _spin_past(target_ms: int) -> int:
    """自旋等待时钟越过 ``target_ms``（返回值 > ``target_ms``）。

    用于同毫秒序列耗尽：必须等到下一个新的毫秒才可继续发号。

    Args:
        target_ms: 需要越过的毫秒时间戳。

    Returns:
        int: 最新读取到的、严格晚于 ``target_ms`` 的时间戳。
    """
    while True:
        now = _current_ms()
        if now > target_ms:
            return now
        time.sleep(SPIN_WAIT_SECONDS)


class ClockBackwardsError(ValueError):
    """系统时钟大幅回拨（超过 ``MAX_CLOCK_BACKWARDS_MS`` 毫秒）时抛出。

    继承自 :class:`ValueError`：调用方既可以精确捕获本异常，也可以
    按普通参数/状态错误宽松捕获 ``ValueError``。

    拒绝继续发号而不是静默等待，是为了避免长时间阻塞调用线程，
    同时防止在时钟源异常时发出重复 ID。
    """


class IDGenerator:
    """线程安全的雪花唯一 ID 生成器。

    Args:
        worker_id: 工作节点 ID，取值范围 ``[0, 1023]``，同一业务部署
            内必须唯一；超出范围抛 :class:`ValueError`。
        epoch: 纪元毫秒时间戳。ID 中存储的是 ``当前时间 - epoch``，
            默认 :data:`DEFAULT_EPOCH_MS`（2024-01-01 UTC）。

    Raises:
        ValueError: ``worker_id`` 不在 ``[0, 1023]`` 内，或参数类型非法。

    时钟回拨策略（见模块 docstring）：
        - 回拨幅度 <= 10 毫秒：自旋等待时钟追平后继续发号；
        - 回拨幅度 > 10 毫秒：抛 :class:`ClockBackwardsError`
          （``ValueError`` 子类）。

    Example:
        >>> gen = IDGenerator(worker_id=1)
        >>> 0 < gen.new() < (1 << 63)
        True
        >>> ids = gen.batch(2)
        >>> len(ids)
        2
        >>> ids[0] < ids[1]
        True
    """

    def __init__(self, worker_id: int = 0, epoch: int = DEFAULT_EPOCH_MS) -> None:
        if not isinstance(worker_id, int):
            raise ValueError(
                f"worker_id 必须为 int，当前类型为 {type(worker_id).__name__}"
            )
        if not 0 <= worker_id <= MAX_WORKER_ID:
            raise ValueError(
                f"worker_id 必须在 [0, {MAX_WORKER_ID}] 内，当前为 {worker_id}"
            )
        if not isinstance(epoch, int):
            raise ValueError(
                f"epoch 必须为 int，当前类型为 {type(epoch).__name__}"
            )
        self._worker_id = worker_id
        self._epoch = epoch
        self._lock = threading.Lock()
        # 最近一次发号所在的毫秒时间戳；-1 表示尚未发过号，
        # 保证首次调用必然落入“新毫秒”分支（序列从 0 开始）。
        self._last_ts: int = -1
        self._seq: int = 0

    def new(self) -> int:
        """生成一个唯一 ID（线程安全）。

        Returns:
            int: 64 位正整数，同一实例内严格递增；同一毫秒内依赖
            12 bit 序列区分，序列耗尽自动等待下一毫秒。

        Raises:
            ClockBackwardsError: 系统时钟回拨超过容忍上限。
            ValueError: ``epoch`` 晚于当前系统时间导致时间戳偏移为负。

        Example:
            >>> gen = IDGenerator(worker_id=0)
            >>> gen.new() != gen.new()
            True
        """
        with self._lock:
            return self._generate_locked()

    def batch(self, count: int) -> list[int]:
        """一次性批量生成 ``count`` 个唯一 ID。

        整个批量过程只获取一次锁，比循环调用 :meth:`new` 更高效，
        且结果保持严格递增。

        Args:
            count: 需要生成的数量，必须为正整数。

        Returns:
            list[int]: 长度为 ``count`` 的 ID 列表，无重复且递增。

        Raises:
            ValueError: ``count`` 不是正整数（<= 0 或非 int）。

        Example:
            >>> ids = IDGenerator().batch(3)
            >>> len(ids)
            3
            >>> len(set(ids))
            3
        """
        if not isinstance(count, int) or count <= 0:
            raise ValueError(f"count 必须为正整数，当前为 {count!r}")
        with self._lock:
            return [self._generate_locked() for _ in range(count)]

    def _generate_locked(self) -> int:
        """发号核心逻辑（调用方必须已持有 ``self._lock``）。"""
        now = _current_ms()

        # --- 时钟回拨保护 ---
        if now < self._last_ts:
            backwards = self._last_ts - now
            if backwards > MAX_CLOCK_BACKWARDS_MS:
                raise ClockBackwardsError(
                    f"系统时钟回拨 {backwards} ms，超过容忍上限 "
                    f"{MAX_CLOCK_BACKWARDS_MS} ms，拒绝发号"
                )
            # 小幅回拨：自旋等待时钟追平最近一次发号时间
            now = _spin_until(self._last_ts)

        # --- 计算 / 推进序列号 ---
        if now > self._last_ts:
            # 进入新的毫秒，序列归零
            self._seq = 0
        else:
            # 与上次发号同一毫秒，序列递增
            self._seq += 1
            if self._seq > MAX_SEQUENCE:
                # 本毫秒 4096 个号已用尽，等待下一毫秒再归零
                now = _spin_past(self._last_ts)
                self._seq = 0
        self._last_ts = now

        delta = now - self._epoch
        if delta < 0:
            raise ValueError(
                f"epoch({self._epoch}) 晚于当前系统时间({now})，"
                f"时间戳偏移为负，无法生成合法 ID"
            )
        return (
            (delta << TIMESTAMP_SHIFT)
            | (self._worker_id << WORKER_ID_SHIFT)
            | self._seq
        )


#: 模块级默认生成器实例：模块函数 new_id / new_prefixed_id 委托于它
_default = IDGenerator()


def new_id() -> int:
    """生成一个雪花唯一 ID（模块级默认生成器的快捷方式）。

    等价于 ``_default.new()``；默认实例 ``worker_id=0``、纪元为
    :data:`DEFAULT_EPOCH_MS`，进程内所有调用线程共享同一发号器，
    结果全局唯一且严格递增。

    Returns:
        int: 64 位正整数唯一 ID（``0 < id < 2^63``）。

    Example:
        >>> 0 < new_id() < (1 << 63)
        True
    """
    return _default.new()


def new_prefixed_id(prefix: str) -> str:
    """生成带业务前缀的唯一 ID，形如 ``"ORD_731234567890123456"``。

    在雪花 ID 之前拼接 ``prefix`` 与下划线，便于日志 / 存储 /
    消息队列中直观辨识业务归属；唯一性由数值部分保证。

    Args:
        prefix: 业务前缀，必须为非空字符串（建议大写字母，如
            ``"ORD"``、``"PAY"``）。

    Returns:
        str: 形如 ``"<prefix>_<snowflake>"`` 的字符串。

    Raises:
        ValueError: ``prefix`` 不是 ``str``，或为空字符串。

    Example:
        >>> pid = new_prefixed_id("ORD")
        >>> pid.startswith("ORD_")
        True
        >>> int(pid.split("_", 1)[1]) > 0
        True
    """
    if not isinstance(prefix, str) or not prefix:
        raise ValueError(f"prefix 必须为非空 str，当前为 {prefix!r}")
    return f"{prefix}_{_default.new()}"
