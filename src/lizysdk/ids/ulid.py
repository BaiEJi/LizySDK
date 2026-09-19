"""ULID 风格可排序字符串 ID：``new_sortable_id`` / ``sortable_id_timestamp``。

ULID（Universally Unique Lexicographically Sortable Identifier）以 26 个
Crockford base32 字符编码 128 bit 信息，兼顾「全局唯一」与「按生成时间
可排序」两类需求，适合做数据库主键、对象存储 key、消息去重键等需要
按时间范围扫描的场景。

布局（自高位到低位，共 128 bit = 26 字符 x 5 bit）::

    |   前 10 字符：48 bit 毫秒 Unix 时间戳   |   后 16 字符：80 bit 随机   |
    |       10 x 5 = 50 bit >= 48 bit        |      16 x 5 = 80 bit       |

位宽推导：
- 48 bit 毫秒 Unix 时间戳 ≈ 2^48 ms ≈ 8925 年，以 1970 纪元可用至约
  公元 10889 年，无需自定义 epoch；
- 10 字符 x 5 bit = 50 bit 容量覆盖 48 bit 时间戳（最高位字符只能取
  ``0`` / ``1``，与 ULID 标准一致）；
- 80 bit 随机熵由 16 字符 x 5 bit 恰好编码满，无填充浪费。

字符集为 Crockford base32 小写字母表 ``0123456789abcdefghjkmnpqrstvwxyz``
（排除 ``i`` / ``l`` / ``o`` / ``u`` 四个易混淆字符），且该字母表本身按
ASCII 升序排列，因此「定长字符串的字典序」严格等价于「128 bit 数值的
数值序」——这是字符串可直接排序比较的前提。

单调性保证（同进程内单调不减）：
- 跨毫秒：随机段用 :mod:`secrets` 重新取值，时间戳段必然增大，故新 ID
  的字典序必然大于此前所有 ID；
- 同一毫秒：随机段不再重新随机，而是改为「上一次随机值 + 1」的递增
  计数器，保证同一毫秒内生成的 ID 依然严格递增（生成顺序 == 字典序）；
- 时钟小幅回拨（读数 <= 上一毫秒）时沿用上一毫秒的时间戳继续递增，
  单调性不被破坏；
- 全部状态更新都在模块锁内完成：多线程调用不要求全局按调用次序有序，
  但保证全局唯一（每次调用的「时间戳 + 随机段」组合互不相同）。

仅依赖标准库（secrets / threading / time），Python 3.9+。
"""

from __future__ import annotations

import secrets
import threading
import time

__all__ = ["new_sortable_id", "sortable_id_timestamp"]

# ---------------------------------------------------------------------------
# 布局常量（26 字符 = 10 时间戳字符 + 16 随机字符）
# ---------------------------------------------------------------------------
#: Crockford base32 小写字母表：32 个字符 = 2^5，排除 i / l / o / u
CROCKFORD_ALPHABET: str = "0123456789abcdefghjkmnpqrstvwxyz"

#: 每个字符携带的位数：32 个符号 = 5 bit
BITS_PER_CHAR: int = 5

#: 时间戳段位数：48 bit 毫秒 Unix 时间戳 ≈ 8925 年（见模块 docstring 推导）
TIMESTAMP_BITS: int = 48
#: 随机段位数：80 bit 随机熵
RANDOMNESS_BITS: int = 80

#: 时间戳段字符数：ceil(48 / 5) = 10（50 bit 容量 >= 48 bit，首位字符仅取 0/1）
TIMESTAMP_CHARS: int = (TIMESTAMP_BITS + BITS_PER_CHAR - 1) // BITS_PER_CHAR
#: 随机段字符数：80 / 5 = 16（恰好编码满 80 bit，无填充位）
RANDOMNESS_CHARS: int = RANDOMNESS_BITS // BITS_PER_CHAR
#: 完整 ULID 长度：10 + 16 = 26
ULID_LENGTH: int = TIMESTAMP_CHARS + RANDOMNESS_CHARS

#: 时间戳段可编码的最大毫秒值：2^48 - 1（约公元 10889 年）
MAX_TIMESTAMP: int = (1 << TIMESTAMP_BITS) - 1
#: 随机段最大值：2^80 - 1
MAX_RANDOMNESS: int = (1 << RANDOMNESS_BITS) - 1

#: 字符 -> 数值 的反查表（sortable_id_timestamp 解码用）
_CHAR_INDEX: dict[str, int] = {ch: i for i, ch in enumerate(CROCKFORD_ALPHABET)}

#: 时间戳段字符串的单槽缓存 ``(毫秒, 编码文本)``：同一毫秒内所有 ID 的
#: 前 10 字符完全相同，写侧原子替换，读侧竞态只会多编码一次。
_TS_PART_CACHE: list = [None]


def _now_ms() -> int:
    """返回当前 Unix 时间戳（毫秒）。

    独立成模块级函数是刻意留出的测试接缝：monkeypatch 本函数即可冻结 /
    脚本化时间推进，验证同毫秒计数器与跨毫秒重新随机的语义，无需改动
    生成逻辑本身（与 :func:`lizysdk.ids.snowflake._current_ms` 同一套路）。
    """
    return int(time.time() * 1000)


def _encode(value: int, length: int) -> str:
    """把非负整数编码为 ``length`` 位 Crockford base32 字符串（大端、零填充）。

    Args:
        value: 待编码的非负整数，必须小于 ``2 ** (length * 5)``。
        length: 目标字符位数。

    Returns:
        str: 长度精确为 ``length`` 的字符串，仅含
        :data:`CROCKFORD_ALPHABET` 中的字符。

    Raises:
        ValueError: ``value`` 为负或超出 ``length`` 个字符的编码容量。
    """
    return _encode_impl(value, length)


def _encode_impl(value: int, length: int) -> str:
    """:func:`_encode` 的权威实现（divmod 循环，同时用于构建查表）。"""
    if value < 0 or value >= (1 << (length * BITS_PER_CHAR)):
        raise ValueError(
            f"value 必须在 [0, {1 << (length * BITS_PER_CHAR) - 1}] 内，"
            f"当前为 {value}，无法编码为 {length} 个字符"
        )
    chars = [""] * length
    for i in range(length - 1, -1, -1):
        value, rem = divmod(value, len(CROCKFORD_ALPHABET))
        chars[i] = CROCKFORD_ALPHABET[rem]
    return "".join(chars)


#: 2 字符组合查表（10 bit -> 2 字符）：热路径编码由 16 次 divmod 循环
#: 降为 8 次移位 + 查表（表在导入期用权威实现 :func:`_encode_impl` 构建）
_PAIR_TABLE: "tuple[str, ...]" = tuple(
    _encode_impl(i, 2) for i in range(1 << (2 * BITS_PER_CHAR))
)


def _encode_randomness(value: int) -> str:
    """80 bit 随机段 -> 16 字符（热路径：8 次移位 + 2 字符查表）。"""
    table = _PAIR_TABLE
    return "".join(
        (
            table[(value >> 70) & 0x3FF],
            table[(value >> 60) & 0x3FF],
            table[(value >> 50) & 0x3FF],
            table[(value >> 40) & 0x3FF],
            table[(value >> 30) & 0x3FF],
            table[(value >> 20) & 0x3FF],
            table[(value >> 10) & 0x3FF],
            table[value & 0x3FF],
        )
    )


def _timestamp_part(ts_ms: int) -> str:
    """时间戳段（前 10 字符），按毫秒单槽缓存。"""
    cached = _TS_PART_CACHE[0]
    if cached is not None and cached[0] == ts_ms:
        return cached[1]
    text = _encode_impl(ts_ms, TIMESTAMP_CHARS)
    _TS_PART_CACHE[0] = (ts_ms, text)
    return text


# ---------------------------------------------------------------------------
# 模块级发号状态（全部读写都在 _LOCK 内，保证线程安全）
# ---------------------------------------------------------------------------
_state_lock = threading.Lock()
#: 最近一次发号所用的毫秒时间戳；-1 表示尚未发过号，
#: 保证首次调用必然落入「新毫秒」分支（重新随机）。
_last_ms: int = -1
#: 最近一次发号的随机段数值；同一毫秒内在其基础上 +1 递增。
_last_randomness: int = -1


def new_sortable_id() -> str:
    """生成一个 ULID 风格可排序字符串 ID（26 字符，小写 Crockford base32）。

    结果形如 ``"01fdh4t2v7tqkcb8f9xzm5xyab"``：前 10 字符编码生成时刻的
    48 bit 毫秒 Unix 时间戳，后 16 字符编码 80 bit 随机段。

    单调性：同进程内生成的 ID 单调不减且实际严格递增——同一毫秒内随机段
    变为递增计数器（持锁保证线程安全），跨毫秒重新随机，因此「生成顺序
    == 字典序」；多线程并发调用时不保证全局按调用次序有序，但保证全局
    唯一。随机源为 :mod:`secrets`（操作系统级密码学安全随机）。

    Returns:
        str: 长度 26 的小写字符串，可用 :func:`sortable_id_timestamp`
        反解出 Unix 秒时间戳。

    Raises:
        ValueError: 系统时钟超过 48 bit 毫秒时间戳可表示范围（约公元
            10889 年之后，实践中几乎不可能触发）。

    Example:
        >>> uid = new_sortable_id()
        >>> len(uid)
        26
        >>> set(uid) <= set("0123456789abcdefghjkmnpqrstvwxyz")
        True
        >>> first, second = new_sortable_id(), new_sortable_id()
        >>> first < second  # 字典序 == 生成顺序
        True
    """
    with _state_lock:
        global _last_ms, _last_randomness
        now = _now_ms()
        if now > _last_ms:
            # 进入新的毫秒：随机段重新随机（时间戳段必然增大）
            ts_ms = now
            randomness = secrets.randbits(RANDOMNESS_BITS)
        else:
            # 同一毫秒（或时钟小幅回拨）：沿用上一毫秒时间戳，
            # 随机段 +1 递增，保证字典序严格递增
            ts_ms = _last_ms
            randomness = _last_randomness + 1
            if randomness > MAX_RANDOMNESS:
                # 80 bit 随机段在上一毫秒内用尽（理论边界，概率约
                # 2^-80）：时间戳虚拟推进 1ms 并重新随机，单调性不破坏
                ts_ms = _last_ms + 1
                randomness = secrets.randbits(RANDOMNESS_BITS)
        if ts_ms > MAX_TIMESTAMP:
            raise ValueError(
                f"当前毫秒时间戳 {ts_ms} 超出 48 bit 可编码上限 "
                f"{MAX_TIMESTAMP}，无法生成合法 ULID"
            )
        _last_ms = ts_ms
        _last_randomness = randomness
        return _timestamp_part(ts_ms) + _encode_randomness(randomness)


def sortable_id_timestamp(uid: str) -> float:
    """从 ULID 字符串反解出 Unix 秒时间戳（含毫秒精度）。

    仅解码前 10 个时间戳字符；但会校验全部 26 个字符的合法性，
    非法输入一律抛 :class:`ValueError`（而不是返回无意义结果）。

    Args:
        uid: :func:`new_sortable_id` 生成的 26 位字符串。

    Returns:
        float: Unix 秒时间戳（毫秒精度，即 ``毫秒值 / 1000``），
        可直接与 :func:`time.time` 的结果比较。

    Raises:
        ValueError: ``uid`` 不是 str、长度不等于 26，或含有 Crockford
        base32 字母表之外的字符（包括大写字母与 ``i`` / ``l`` /
        ``o`` / ``u``）。

    Example:
        >>> import time
        >>> abs(sortable_id_timestamp(new_sortable_id()) - time.time()) < 2
        True
        >>> sortable_id_timestamp("0" * 26)
        0.0
        >>> sortable_id_timestamp("01arz3ndektsv4gfftd8g5hxv9") > 0
        True
        >>> sortable_id_timestamp("i" * 26)
        Traceback (most recent call last):
            ...
        ValueError: uid 第 0 个字符 'i' 非法，合法字符集为 Crockford base32 ...
    """
    if not isinstance(uid, str):
        raise ValueError(
            f"uid 必须为 {ULID_LENGTH} 位 Crockford base32 字符串，"
            f"当前类型为 {type(uid).__name__}"
        )
    if len(uid) != ULID_LENGTH:
        raise ValueError(
            f"uid 长度必须为 {ULID_LENGTH}，当前为 {len(uid)}：{uid!r}"
        )
    ms_value = 0
    for pos, ch in enumerate(uid):
        idx = _CHAR_INDEX.get(ch)
        if idx is None:
            raise ValueError(
                f"uid 第 {pos} 个字符 {ch!r} 非法，合法字符集为 "
                f"Crockford base32（{CROCKFORD_ALPHABET}）：{uid!r}"
            )
        if pos < TIMESTAMP_CHARS:
            ms_value = ms_value * len(CROCKFORD_ALPHABET) + idx
    return ms_value / 1000.0
