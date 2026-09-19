"""lizysdk.ids 测试：随机 hex ID（trace_id / uid）/ 雪花 ID / 并发安全 / 时钟回拨保护。

覆盖范围：
1. trace_id 与 uid：默认 16 位小写 hex（不再是 32 位）、位数可选
   （8/16/24/32/64 及奇数位均精确到指定长度、字符集正确）、
   各长度 1 万次与默认长度 10 万次无重复、非法 length（越界 /
   str / float / bool）抛 ValueError 且消息说明合法范围、
   同参数下 trace_id 与 uid 生成值互不相同；
2. 雪花 new_id：64 位区间、单线程 10 万次无重复、连续调用不等、
   batch(10000) 无重复且时间戳位单调不减；
3. 并发：8 线程（含 batch 线程）共享默认实例无碰撞；
   不同 worker_id 的两个实例并发生成亦无碰撞；
4. worker_id 校验：越界抛 ValueError，边界值 0/1023 合法；
5. new_prefixed_id：格式与非空 str 校验；
6. batch：count<=0 抛 ValueError，batch(N) 长度为 N；
7. 时钟回拨：通过 monkeypatch snowflake._current_ms 接缝模拟回拨，
   小幅回拨自旋等待、大幅回拨抛 ClockBackwardsError；
8. 默认实例与自建实例互不干扰；
9. ULID 可排序 ID（new_sortable_id / sortable_id_timestamp）：长度 26、
   Crockford base32 小写字符集、10 万次唯一、单线程 10 万次严格单调
   （排序后与原序列一致）、8 线程 x 1 万次全唯一、时间戳反解偏差 < 2s、
   非法 uid（长度错 / 含 i o l u / 非 str）抛 ValueError、冻结毫秒的
   同毫秒计数器语义（monkeypatch ulid._now_ms 接缝）、跨毫秒边界单调；
10. worker_id 自动协商（resolve_worker_id）：环境变量优先（合法值含
    边界 0/1023 直接返回且零文件探测、非法值抛 ValueError 含变量名、
    自定义变量名）、锁文件占位（内容含当前 pid 与 ISO8601 created）、
    被占顺延、陈旧回收（mtime 改旧）与新锁不回收、全满抛 ValueError、
    同进程重复调用幂等、atexit 释放函数删文件且可重复协商、锁目录
    不可写抛中文 OSError、default 参数非法抛 ValueError。
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime
from typing import Callable, List

import pytest

import lizysdk.ids
from lizysdk.ids import (
    ClockBackwardsError,
    IDGenerator,
    new_id,
    new_prefixed_id,
    new_sortable_id,
    new_trace_id,
    new_uid,
    resolve_worker_id,
    sortable_id_timestamp,
)
from lizysdk.ids import snowflake
from lizysdk.ids import ulid, worker

# ---------------------------------------------------------------------------
# 测试辅助
# ---------------------------------------------------------------------------

_HEX_CHARS = frozenset("0123456789abcdef")
_PREFIXED_RE = re.compile(r"^(?P<prefix>[A-Z]+)_(?P<num>\d+)$")

#: 需求规定的合法参数化位数
_VALID_LENGTHS = [8, 16, 24, 32, 64]
#: 奇数位：覆盖“生成偶数位 hex 再截断”路径的精确长度
_ODD_LENGTHS = [9, 17, 63]
#: 非法 length：越界
_BAD_RANGE_LENGTHS = [7, 65, 0, -1]
#: 非法 length：类型错误（str / float / bool，bool 是 int 子类须排除）
_BAD_TYPE_LENGTHS = ["16", 16.0, True]

#: trace_id 与 uid 共用同一套规格，参数化到两个工厂上
_ID_FACTORIES: List[Callable[..., str]] = [new_trace_id, new_uid]


def _worker_of(snowflake_id: int) -> int:
    """从雪花 ID 中解出 worker_id 段。"""
    return (snowflake_id >> snowflake.WORKER_ID_SHIFT) & snowflake.MAX_WORKER_ID


def _ts_of(snowflake_id: int) -> int:
    """从雪花 ID 中解出毫秒时间戳偏移段。"""
    return snowflake_id >> snowflake.TIMESTAMP_SHIFT


class ScriptedClock:
    """按脚本依次返回毫秒时间戳；脚本耗尽后固定返回最后一个值。

    用于 monkeypatch snowflake._current_ms，精确模拟时钟回拨 / 停滞 /
    推进，避免依赖真实系统时钟导致测试不确定。
    """

    def __init__(self, values: List[int]) -> None:
        assert values, "脚本时钟至少需要一个值"
        self._values = list(values)
        self._i = 0

    def __call__(self) -> int:
        value = self._values[min(self._i, len(self._values) - 1)]
        self._i += 1
        return value


def _run_concurrently(worker_fn: Callable[[int], List[int]], n_threads: int) -> List[List[int]]:
    """并发执行 worker_fn（带 barrier 对齐起跑），返回各线程结果列表。"""
    barrier = threading.Barrier(n_threads)
    results: List[List[int]] = []

    def _run(idx: int) -> None:
        barrier.wait(timeout=30)
        results.append(worker_fn(idx))

    threads = [
        threading.Thread(target=_run, args=(i,)) for i in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert len(results) == n_threads, "存在未完成的线程，并发测试失败"
    return results


# ---------------------------------------------------------------------------
# 1. trace_id / uid：位数可选的随机 hex ID
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("factory", _ID_FACTORIES, ids=["trace_id", "uid"])
def test_random_id_default_length_16(factory: Callable[..., str]) -> None:
    """默认参数应生成 16 位（不再是 32 位）小写十六进制字符串。"""
    for _ in range(100):
        value = factory()
        assert len(value) == 16
        assert value == value.lower()
        assert set(value) <= _HEX_CHARS, f"非法字符集: {value}"


@pytest.mark.parametrize("length", _VALID_LENGTHS + _ODD_LENGTHS)
@pytest.mark.parametrize("factory", _ID_FACTORIES, ids=["trace_id", "uid"])
def test_random_id_exact_length_and_charset(
    factory: Callable[..., str], length: int
) -> None:
    """任意合法位数（8~64，含奇数位）：长度精确、全小写 hex 字符集。"""
    for _ in range(200):
        value = factory(length)
        assert len(value) == length
        assert value == value.lower()
        assert set(value) <= _HEX_CHARS, f"非法字符集: {value}"


def _unique_samples(length: int) -> int:
    """按位数返回可做「严格唯一」断言的采样量。

    生日悖论下 n 次采样碰撞概率 P ≈ n² / (2·16^length)；位数越短空间越小，
    必须等比缩水采样量，否则测试会以约 1% 的概率随机失败（8 位 × 1 万次
    在 32bit 空间的碰撞概率 ≈ 1.16%）。以下取值均保证 P < 1e-6。
    """
    if length >= 16:
        return 10_000
    if length >= 12:
        return 3_000
    if length >= 10:
        return 500
    return 60  # 8~9 位（32~36bit）：60 次碰撞概率约 4e-7


@pytest.mark.parametrize("length", _VALID_LENGTHS)
@pytest.mark.parametrize("factory", _ID_FACTORIES, ids=["trace_id", "uid"])
def test_random_id_unique_per_length_scaled(
    factory: Callable[..., str], length: int
) -> None:
    """同一长度连续生成不应重复（采样量按位数熵缩放）。"""
    total = _unique_samples(length)
    values = {factory(length) for _ in range(total)}
    assert len(values) == total


@pytest.mark.parametrize("factory", _ID_FACTORIES, ids=["trace_id", "uid"])
def test_random_id_unique_100k_default_length(factory: Callable[..., str]) -> None:
    """默认长度（16）连续 10 万次生成不应出现重复。"""
    total = 100_000
    values = {factory() for _ in range(total)}
    assert len(values) == total


@pytest.mark.parametrize("bad", _BAD_RANGE_LENGTHS)
@pytest.mark.parametrize("factory", _ID_FACTORIES, ids=["trace_id", "uid"])
def test_random_id_length_out_of_range_raises(
    factory: Callable[..., str], bad: int
) -> None:
    """位数越界（7 / 65 / 0 / -1）应抛 ValueError，且错误消息说明合法范围。"""
    with pytest.raises(ValueError, match=r"\[8, 64\]"):
        factory(bad)


@pytest.mark.parametrize("bad", _BAD_TYPE_LENGTHS)
@pytest.mark.parametrize("factory", _ID_FACTORIES, ids=["trace_id", "uid"])
def test_random_id_length_wrong_type_raises(
    factory: Callable[..., str], bad: object
) -> None:
    """类型非法（"16" / 16.0 / True）应抛 ValueError，且错误消息说明合法范围。"""
    with pytest.raises(ValueError, match=r"\[8, 64\]"):
        factory(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize("length", _VALID_LENGTHS)
def test_new_uid_differs_from_new_trace_id(length: int) -> None:
    """同参数下 uid 与 trace_id 的生成值互不相同（语义隔离，值不共享）。"""
    for _ in range(100):
        assert new_trace_id(length) != new_uid(length)


def test_new_trace_id_two_calls_differ() -> None:
    """不同调用应返回不同结果。"""
    assert new_trace_id() != new_trace_id()


# ---------------------------------------------------------------------------
# 2. 雪花 new_id：区间 / 唯一性 / 有序性
# ---------------------------------------------------------------------------


def test_new_id_in_64bit_range() -> None:
    """new_id 应为正的 63 位整数（符号位恒为 0）。"""
    for _ in range(1000):
        sid = new_id()
        assert 0 < sid < (1 << 63)


def test_new_id_unique_100k_single_thread() -> None:
    """单线程连续 10 万次 new_id 不应重复。"""
    total = 100_000
    ids = [new_id() for _ in range(total)]
    assert len(set(ids)) == total


def test_new_id_consecutive_calls_differ() -> None:
    """同一默认生成器连续两次调用应严格递增。"""
    first = new_id()
    second = new_id()
    assert first != second
    assert second > first


def test_batch_10k_unique_and_timestamp_monotonic() -> None:
    """batch(10000) 应无重复，且时间戳位单调不减。"""
    ids = IDGenerator(worker_id=9).batch(10_000)
    assert len(ids) == 10_000
    assert len(set(ids)) == 10_000
    ts = [_ts_of(i) for i in ids]
    assert all(ts[k] <= ts[k + 1] for k in range(len(ts) - 1))
    # 10000 > 2 * 4096，必然跨越至少 3 个不同的毫秒
    assert len(set(ts)) >= 3


# ---------------------------------------------------------------------------
# 3. 并发安全
# ---------------------------------------------------------------------------


def test_concurrent_new_id_no_collision() -> None:
    """8 线程（6 线程逐个 new_id + 2 线程走 batch）共享默认实例，汇总无碰撞。"""
    n_plain_threads = 6
    n_batch_threads = 2
    per_thread = 12_000  # 每线程至少 1 万次

    def work(idx: int) -> List[int]:
        if idx < n_plain_threads:
            return [new_id() for _ in range(per_thread)]
        batch_times = per_thread // 100
        return [sid for _ in range(batch_times) for sid in snowflake._default.batch(100)]

    results = _run_concurrently(work, n_plain_threads + n_batch_threads)
    merged = [sid for chunk in results for sid in chunk]
    total = (n_plain_threads + n_batch_threads) * per_thread
    assert len(merged) == total
    assert len(set(merged)) == total


def test_two_generators_different_worker_id_concurrent_no_collision() -> None:
    """不同 worker_id 的两个实例并发发号，汇总亦无碰撞且 worker 位正确。"""
    gen_a = IDGenerator(worker_id=3)
    gen_b = IDGenerator(worker_id=1023)
    n_threads_per_gen = 2
    per_thread = 10_000

    def work(idx: int) -> List[int]:
        gen = gen_a if idx < n_threads_per_gen else gen_b
        return [gen.new() for _ in range(per_thread)]

    results = _run_concurrently(work, n_threads_per_gen * 2)
    merged = [i for chunk in results for i in chunk]
    total = n_threads_per_gen * 2 * per_thread
    assert len(merged) == total
    assert len(set(merged)) == total
    assert all(_worker_of(i) in (3, 1023) for i in merged)


# ---------------------------------------------------------------------------
# 4. worker_id 校验
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [-1, 1024, 999_999])
def test_worker_id_out_of_range_raises(bad: int) -> None:
    """worker_id 超出 [0, 1023] 应抛 ValueError。"""
    with pytest.raises(ValueError):
        IDGenerator(worker_id=bad)


@pytest.mark.parametrize("ok", [0, 1, 1023])
def test_worker_id_boundary_accepted(ok: int) -> None:
    """边界值 0 / 1023 应合法可用。"""
    sid = IDGenerator(worker_id=ok).new()
    assert 0 < sid < (1 << 63)
    assert _worker_of(sid) == ok


# ---------------------------------------------------------------------------
# 5. new_prefixed_id
# ---------------------------------------------------------------------------


def test_new_prefixed_id_format() -> None:
    """前缀 ID 应形如 PREFIX_<数字>，且同一前缀连续生成不重复。"""
    pid = new_prefixed_id("ORD")
    match = _PREFIXED_RE.fullmatch(pid)
    assert match is not None, f"格式非法: {pid}"
    assert match.group("prefix") == "ORD"
    assert 0 < int(match.group("num")) < (1 << 63)
    assert new_prefixed_id("ORD") != pid


@pytest.mark.parametrize("bad", ["", None, 123, 1.5, b"ORD", ["ORD"]])
def test_new_prefixed_id_invalid_prefix_raises(bad: object) -> None:
    """prefix 为空或非 str 应抛 ValueError。"""
    with pytest.raises(ValueError):
        new_prefixed_id(bad)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 6. batch count 校验
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad", [0, -1, -100])
def test_batch_invalid_count_raises(bad: int) -> None:
    """count <= 0 应抛 ValueError。"""
    with pytest.raises(ValueError):
        IDGenerator().batch(bad)


def test_batch_returns_requested_count() -> None:
    """batch(37) 应返回长度为 37 的无重复列表。"""
    ids = IDGenerator().batch(37)
    assert len(ids) == 37
    assert len(set(ids)) == 37


# ---------------------------------------------------------------------------
# 7. 时钟回拨（通过 monkeypatch snowflake._current_ms 模拟）
# ---------------------------------------------------------------------------


def test_clock_backwards_within_tolerance_spins(monkeypatch: pytest.MonkeyPatch) -> None:
    """小幅回拨（<= 容忍上限）应自旋等待时钟追平后继续发号。"""
    gen = IDGenerator(worker_id=2, epoch=0)
    t0 = 10_000
    # 读数序列：t0（首次发号）→ t0-3（回拨 3ms，触发自旋）→ t0（追平）
    clock = ScriptedClock([t0, t0 - 3, t0])
    monkeypatch.setattr(snowflake, "_current_ms", clock)

    first = gen.new()
    second = gen.new()

    assert first != second
    # 自旋后时钟追平回 t0：两次发号落在同一毫秒，第二次序列号更大
    assert _ts_of(first) == _ts_of(second) == t0
    assert second == first + 1


def test_clock_backwards_beyond_tolerance_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """大幅回拨（> 容忍上限）应抛 ClockBackwardsError。"""
    gen = IDGenerator(worker_id=2, epoch=0)
    t0 = 20_000
    backwards = snowflake.MAX_CLOCK_BACKWARDS_MS + 1
    clock = ScriptedClock([t0, t0 - backwards])
    monkeypatch.setattr(snowflake, "_current_ms", clock)

    assert 0 < gen.new()  # 首次发号正常
    with pytest.raises(ClockBackwardsError):
        gen.new()
    # ClockBackwardsError 是 ValueError 子类，允许宽松捕获
    with pytest.raises(ValueError):
        gen.new()


def test_sequence_overflow_waits_next_millisecond(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """同一毫秒内序列耗尽（4096 个）后应等待下一毫秒再发号。"""
    gen = IDGenerator(worker_id=1, epoch=0)
    t0 = 5_000
    # 前 4100 次读数停在 t0（一毫秒最多 4096 个号），之后推进到 t0+1
    clock = ScriptedClock([t0] * 4100 + [t0 + 1])
    monkeypatch.setattr(snowflake, "_current_ms", clock)

    ids = gen.batch(4097)

    assert len(ids) == 4097
    assert len(set(ids)) == 4097
    ts_counts: dict = {}
    for sid in ids:
        ts_counts[_ts_of(sid)] = ts_counts.get(_ts_of(sid), 0) + 1
    assert set(ts_counts) == {t0, t0 + 1}
    assert ts_counts[t0] == 4096  # t0 毫秒内发满 4096 个
    assert ts_counts[t0 + 1] == 1  # 第 4097 个必须等到 t0+1 毫秒


def test_bit_layout_exact(monkeypatch: pytest.MonkeyPatch) -> None:
    """冻结位布局：[41bit 时间戳][10bit worker][12bit 序列]。"""
    gen = IDGenerator(worker_id=5, epoch=0)
    t0 = 1_000_000
    monkeypatch.setattr(snowflake, "_current_ms", lambda: t0)

    first = gen.new()
    second = gen.new()

    assert first == (t0 << snowflake.TIMESTAMP_SHIFT) | (5 << snowflake.WORKER_ID_SHIFT) | 0
    assert second == (t0 << snowflake.TIMESTAMP_SHIFT) | (5 << snowflake.WORKER_ID_SHIFT) | 1


# ---------------------------------------------------------------------------
# 8. 默认实例与自建实例互不干扰
# ---------------------------------------------------------------------------


def test_default_and_custom_instances_independent() -> None:
    """自建实例发号不影响默认实例；二者 ID 通过 worker 位区分且互不碰撞。"""
    custom = IDGenerator(worker_id=512)

    before = [new_id() for _ in range(50)]
    custom_ids = custom.batch(50)
    after = [new_id() for _ in range(50)]

    assert all(_worker_of(i) == 0 for i in before + after)  # 默认实例 worker_id=0
    assert all(_worker_of(i) == 512 for i in custom_ids)
    merged = before + custom_ids + after
    assert len(set(merged)) == 150
    # 自建实例的批量发号不打断默认实例的严格递增
    assert max(after) > max(before)


# ---------------------------------------------------------------------------
# 附：公开 API 面 / 常量自洽
# ---------------------------------------------------------------------------


def test_public_api_surface() -> None:
    """__init__ 必须按契约导出全部公开 API。"""
    required = {
        "new_trace_id",
        "new_uid",
        "new_id",
        "new_prefixed_id",
        "IDGenerator",
        "ClockBackwardsError",
    }
    for name in required:
        assert hasattr(lizysdk.ids, name), f"缺少导出: {name}"
        assert name in lizysdk.ids.__all__


def test_bit_width_constants() -> None:
    """位宽常量之和应为 64，派生常量取值正确。"""
    total = (
        snowflake.SIGN_BITS
        + snowflake.TIMESTAMP_BITS
        + snowflake.WORKER_ID_BITS
        + snowflake.SEQUENCE_BITS
    )
    assert total == 64
    assert snowflake.MAX_WORKER_ID == 1023
    assert snowflake.MAX_SEQUENCE == 4095
    assert snowflake.DEFAULT_EPOCH_MS == 1704067200000  # 2024-01-01T00:00:00Z


def test_public_api_surface_new_modules() -> None:
    """新增两个子模块的 API 必须按契约从 lizysdk.ids 导出。"""
    required = {
        "new_sortable_id",
        "sortable_id_timestamp",
        "resolve_worker_id",
    }
    for name in required:
        assert hasattr(lizysdk.ids, name), f"缺少导出: {name}"
        assert name in lizysdk.ids.__all__


# ---------------------------------------------------------------------------
# 9. ULID 可排序字符串 ID（new_sortable_id / sortable_id_timestamp）
# ---------------------------------------------------------------------------

_CROCKFORD_CHARS = frozenset(ulid.CROCKFORD_ALPHABET)


def test_ulid_length_26_and_charset() -> None:
    """每个 ID 长度恒为 26，且仅含 Crockford base32 小写字符。"""
    for _ in range(1000):
        uid = new_sortable_id()
        assert len(uid) == 26
        assert uid == uid.lower()
        assert set(uid) <= _CROCKFORD_CHARS, f"非法字符集: {uid}"


def test_ulid_unique_100k() -> None:
    """连续 10 万次生成不应出现重复。"""
    total = 100_000
    values = {new_sortable_id() for _ in range(total)}
    assert len(values) == total


def test_ulid_single_thread_strictly_monotonic_100k() -> None:
    """单线程 10 万次：排序后与原序列完全一致（生成顺序 == 字典序）。"""
    ids = [new_sortable_id() for _ in range(100_000)]
    assert sorted(ids) == ids
    assert all(ids[k] < ids[k + 1] for k in range(len(ids) - 1))


def test_ulid_8threads_10k_all_unique() -> None:
    """8 线程 x 1 万次并发生成，汇总 8 万个 ID 全唯一。"""

    def work(_idx: int) -> List[str]:
        return [new_sortable_id() for _ in range(10_000)]

    results = _run_concurrently(work, 8)
    merged = [uid for chunk in results for uid in chunk]
    assert len(merged) == 80_000
    assert len(set(merged)) == 80_000


def test_ulid_timestamp_close_to_now() -> None:
    """反解出的 Unix 秒与 time.time() 偏差小于 2 秒。"""
    uid = new_sortable_id()
    assert abs(sortable_id_timestamp(uid) - time.time()) < 2


@pytest.mark.parametrize(
    "bad",
    [
        "",  # 空串
        "0" * 25,  # 长度不足
        "0" * 27,  # 长度超长
        "i" + "0" * 25,  # 非法字符 i（Crockford 排除）
        "0" * 13 + "l" + "0" * 12,  # 非法字符 l
        "0" * 25 + "o",  # 非法字符 o
        "0" * 10 + "u" + "0" * 15,  # 非法字符 u
        "01arz3ndektsv4gfftd8g5hxv" + "I",  # 大写同样非法
        123,  # 非 str
        None,  # 非 str
        b"0" * 26,  # 非 str
    ],
)
def test_ulid_timestamp_invalid_uid_raises(bad: object) -> None:
    """非法 uid（长度错 / 含 i l o u / 非 str）应抛 ValueError。"""
    with pytest.raises(ValueError):
        sortable_id_timestamp(bad)  # type: ignore[arg-type]


def test_ulid_timestamp_decodes_known_values() -> None:
    """已知样本反解：全零对应纪元 0，7zzzzzzzzz 对应 48bit 上界。"""
    assert sortable_id_timestamp("0" * 26) == 0.0
    # '7' + 'z'*9 恰为 2^48 - 1（8 * 32^9 - 1），随机段不影响时间戳
    assert sortable_id_timestamp("7" + "z" * 9 + "0" * 16) == (2**48 - 1) / 1000
    # ULID 标准文档示例（转小写）：时间戳应为 2016 年附近的合法过去时间
    ts = sortable_id_timestamp("01arz3ndektsv4gfftd8g5hxv9")
    assert 1_400_000_000 < ts < time.time() + 5


def test_ulid_layout_constants() -> None:
    """布局常量自洽：26 字符、字符表 32 个且升序、位宽推导正确。"""
    assert ulid.ULID_LENGTH == 26
    assert ulid.TIMESTAMP_CHARS + ulid.RANDOMNESS_CHARS == 26
    assert ulid.TIMESTAMP_CHARS * ulid.BITS_PER_CHAR >= ulid.TIMESTAMP_BITS
    assert ulid.RANDOMNESS_CHARS * ulid.BITS_PER_CHAR == ulid.RANDOMNESS_BITS
    assert len(ulid.CROCKFORD_ALPHABET) == 32
    # 排除 i / l / o / u
    assert set("ilou") & set(ulid.CROCKFORD_ALPHABET) == set()
    # 字母表本身按 ASCII 升序：字典序 == 数值序的前提
    assert sorted(ulid.CROCKFORD_ALPHABET) == list(ulid.CROCKFORD_ALPHABET)


def _freeze_ulid_clock(monkeypatch: pytest.MonkeyPatch, ms_values: List[int]) -> None:
    """冻结 / 脚本化 ulid 时间接缝并重置发号状态（teardown 自动还原）。"""
    monkeypatch.setattr(ulid, "_now_ms", ScriptedClock(ms_values))
    monkeypatch.setattr(ulid, "_last_ms", -1)
    monkeypatch.setattr(ulid, "_last_randomness", -1)


def test_ulid_same_ms_counter_semantics(monkeypatch: pytest.MonkeyPatch) -> None:
    """冻结毫秒：同毫秒内前缀相同、后缀（随机段）严格递增、全唯一。"""
    frozen_ms = 10_000_000_000_000  # 任意合法 48bit 毫秒值
    _freeze_ulid_clock(monkeypatch, [frozen_ms])

    n = 500
    ids = [new_sortable_id() for _ in range(n)]

    prefixes = {uid[:10] for uid in ids}
    suffixes = [uid[10:] for uid in ids]
    assert len(prefixes) == 1  # 同一毫秒：时间戳前缀完全相同
    assert all(suffixes[k] < suffixes[k + 1] for k in range(n - 1))  # 严格递增
    assert suffixes == sorted(suffixes)
    assert len(set(ids)) == n  # 全唯一
    # 时间戳前缀反解回冻结值
    assert sortable_id_timestamp(ids[0]) == frozen_ms / 1000


def test_ulid_monotonic_across_ms_boundary(monkeypatch: pytest.MonkeyPatch) -> None:
    """跨毫秒：前缀切换为新毫秒且整体仍严格递增、同毫秒段内递增。"""
    t0 = 10_000_000_000_000
    _freeze_ulid_clock(monkeypatch, [t0, t0, t0, t0 + 1, t0 + 1, t0 + 1])

    ids = [new_sortable_id() for _ in range(6)]

    assert ids == sorted(ids)  # 整体严格递增
    assert len({uid[:10] for uid in ids}) == 2  # 恰好两个毫秒前缀
    assert ids[2][:10] < ids[3][:10]  # 跨毫秒后前缀变大
    # 各毫秒段内部后缀递增
    assert ids[0][10:] < ids[1][10:] < ids[2][10:]
    assert ids[3][10:] < ids[4][10:] < ids[5][10:]


# ---------------------------------------------------------------------------
# 10. worker_id 自动协商（resolve_worker_id）
# ---------------------------------------------------------------------------


@pytest.fixture
def clean_worker_state(monkeypatch: pytest.MonkeyPatch):
    """worker 协商测试的隔离夹具：清空进程内占用状态 + 移除默认环境变量。

    前后各释放一次（幂等），保证每个用例从「未协商」状态起步、结束后
    不把锁文件状态泄漏给后续用例。
    """
    worker._release_lock()
    monkeypatch.delenv("LIZYSDK_WORKER_ID", raising=False)
    yield
    worker._release_lock()


@pytest.mark.parametrize("value", ["0", "7", "1023", " 42 "])
def test_worker_env_var_valid_returns_directly(
    tmp_path, clean_worker_state, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """环境变量为合法值（含边界 0/1023）直接返回，且不做任何文件探测。"""
    monkeypatch.setenv("LIZYSDK_WORKER_ID", value)
    lock_dir = tmp_path / "locks"
    assert resolve_worker_id(lock_dir=lock_dir) == int(value)
    # 显式指定时不做文件探测：锁目录根本不会被创建
    assert not lock_dir.exists()
    assert list(tmp_path.iterdir()) == []


def test_worker_env_var_skips_probing_even_if_lock_dir_broken(
    tmp_path, clean_worker_state, monkeypatch: pytest.MonkeyPatch
) -> None:
    """env 指定时连「不可用的 lock_dir」也不会被触碰。"""
    monkeypatch.setenv("LIZYSDK_WORKER_ID", "5")
    blocker = tmp_path / "blocker"
    blocker.write_text("not a dir", encoding="utf-8")
    assert resolve_worker_id(lock_dir=blocker / "locks") == 5


@pytest.mark.parametrize("value", ["abc", "12.5", "-1", "1024", "0x1", "  ", ""])
def test_worker_env_var_invalid_raises(
    tmp_path, clean_worker_state, monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    """环境变量非数字 / 越界应抛 ValueError，且消息含变量名。"""
    monkeypatch.setenv("LIZYSDK_WORKER_ID", value)
    with pytest.raises(ValueError, match="LIZYSDK_WORKER_ID"):
        resolve_worker_id(lock_dir=tmp_path / "locks")


def test_worker_custom_env_var_name(
    tmp_path, clean_worker_state, monkeypatch: pytest.MonkeyPatch
) -> None:
    """env_var 可自定义：只读指定变量，默认变量即使为垃圾也不受影响。"""
    monkeypatch.setenv("MY_TEST_WID", "9")
    monkeypatch.setenv("LIZYSDK_WORKER_ID", "not-a-number")
    assert (
        resolve_worker_id(env_var="MY_TEST_WID", lock_dir=tmp_path / "locks") == 9
    )


def test_worker_claims_from_default_with_pid_created(
    tmp_path, clean_worker_state
) -> None:
    """无 env 时从 default 起占位成功，锁文件内容含当前 pid 与 ISO8601。"""
    lock_dir = tmp_path / "locks"
    assert resolve_worker_id(lock_dir=lock_dir) == 0

    path = lock_dir / "worker_0.json"
    assert path.exists()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["pid"] == os.getpid()
    created = datetime.fromisoformat(data["created"])  # 合法 ISO8601
    assert created.tzinfo is not None
    assert abs(created.timestamp() - time.time()) < 60


def test_worker_occupied_advances_to_next(tmp_path, clean_worker_state) -> None:
    """起始 id 已被占用时应顺延到下一个空闲 id。"""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    (lock_dir / "worker_0.json").write_text('{"pid": 1}', encoding="utf-8")
    (lock_dir / "worker_1.json").write_text('{"pid": 1}', encoding="utf-8")

    assert resolve_worker_id(lock_dir=lock_dir) == 2
    assert (lock_dir / "worker_2.json").exists()
    assert json.loads((lock_dir / "worker_2.json").read_text())["pid"] == os.getpid()


def test_worker_custom_default_start(tmp_path, clean_worker_state) -> None:
    """default=3 且 worker_3 被占时应占 worker_4。"""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    (lock_dir / "worker_3.json").write_text('{"pid": 1}', encoding="utf-8")

    assert resolve_worker_id(default=3, lock_dir=lock_dir) == 4


def test_worker_stale_lock_reclaimed(tmp_path, clean_worker_state) -> None:
    """mtime 超过阈值的陈旧锁应被回收重占（删除后写入了本进程 pid）。"""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    stale = lock_dir / "worker_0.json"
    stale.write_text('{"pid": 123}', encoding="utf-8")
    old = time.time() - 7200  # 2 小时前
    os.utime(stale, (old, old))

    assert resolve_worker_id(lock_dir=lock_dir, stale_after_seconds=3600) == 0
    assert json.loads(stale.read_text())["pid"] == os.getpid()


def test_worker_fresh_lock_not_reclaimed(tmp_path, clean_worker_state) -> None:
    """mtime 较新的锁不是陈旧锁：不回收，顺延到下一个 id。"""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    fresh = lock_dir / "worker_0.json"
    fresh.write_text('{"pid": 123}', encoding="utf-8")
    recent = time.time() - 60  # 1 分钟前，远未到 1 小时阈值
    os.utime(fresh, (recent, recent))

    assert resolve_worker_id(lock_dir=lock_dir, stale_after_seconds=3600) == 1
    assert json.loads(fresh.read_text())["pid"] == 123  # 原锁未被破坏


def test_worker_all_slots_full_raises(tmp_path, clean_worker_state) -> None:
    """0..1023 全部被占时应抛 ValueError（中文消息说明探测范围）。"""
    lock_dir = tmp_path / "locks"
    lock_dir.mkdir()
    for i in range(1024):
        (lock_dir / f"worker_{i}.json").write_text('{"pid": 1}', encoding="utf-8")

    with pytest.raises(ValueError, match=r"槽位已全部被占用.*\[0, 1023\]"):
        resolve_worker_id(lock_dir=lock_dir)


def test_worker_repeated_calls_idempotent(tmp_path, clean_worker_state) -> None:
    """同进程重复调用幂等：返回同一 id，锁文件数量不增加。"""
    lock_dir = tmp_path / "locks"
    first = resolve_worker_id(lock_dir=lock_dir)
    assert resolve_worker_id(lock_dir=lock_dir) == first
    # 已占用槽位时 default 参数被忽略，仍返回已占用的 id
    assert resolve_worker_id(default=first + 1, lock_dir=lock_dir) == first

    files = sorted(p.name for p in lock_dir.iterdir())
    assert files == [f"worker_{first}.json"]  # 不泄漏多个锁文件


def test_worker_release_lock_removes_file_and_allows_renegotiate(
    tmp_path, clean_worker_state
) -> None:
    """显式调用内部释放函数（atexit 注册的同款）验证：删文件、幂等、可再协商。"""
    lock_dir = tmp_path / "locks"
    wid = resolve_worker_id(lock_dir=lock_dir)
    path = lock_dir / f"worker_{wid}.json"
    assert path.exists()

    worker._release_lock()
    assert not path.exists()
    worker._release_lock()  # 幂等：重复释放不抛错

    # 释放后可再次协商（重新占位同一目录）
    assert resolve_worker_id(lock_dir=lock_dir) == wid


def test_worker_unwritable_lock_dir_raises_clear_error(
    tmp_path, clean_worker_state
) -> None:
    """lock_dir 不可写（路径被普通文件占据）应抛中文 OSError，而非静默。"""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a dir", encoding="utf-8")
    with pytest.raises(OSError, match="锁目录"):
        resolve_worker_id(lock_dir=blocker)


@pytest.mark.parametrize("bad", [-1, 1024, 999_999, "3", 1.5, True])
def test_worker_invalid_default_raises(
    tmp_path, clean_worker_state, bad: object
) -> None:
    """default 非法（越界 / 非 int / bool）应抛 ValueError。"""
    with pytest.raises(ValueError):
        resolve_worker_id(default=bad, lock_dir=tmp_path / "locks")  # type: ignore[arg-type]
