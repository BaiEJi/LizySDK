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
8. 默认实例与自建实例互不干扰。
"""

from __future__ import annotations

import re
import threading
from typing import Callable, List

import pytest

import lizysdk.ids
from lizysdk.ids import (
    ClockBackwardsError,
    IDGenerator,
    new_id,
    new_prefixed_id,
    new_trace_id,
    new_uid,
)
from lizysdk.ids import snowflake

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


@pytest.mark.parametrize("length", _VALID_LENGTHS)
@pytest.mark.parametrize("factory", _ID_FACTORIES, ids=["trace_id", "uid"])
def test_random_id_unique_10k_per_length(
    factory: Callable[..., str], length: int
) -> None:
    """同一长度连续 1 万次生成不应出现重复。"""
    total = 10_000
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
