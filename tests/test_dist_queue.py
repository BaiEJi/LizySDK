"""lizysdk.dist.queue 测试：可靠任务队列 + 延迟队列（fakeredis 全离线）。

覆盖范围（设计文档 §1.4 / §1.5 / §4 对应行全落地）：
1. ReliableQueue：push/pop/ack 往返（信封解包——payload 原文往返 /
   raw 即 processing 里存的信封原文 / id 16 位 hex / tries 0）/
   pop 无货 None（非阻塞 + 带超时且确实等待满）/ 阻塞 pop 取到迟到
   入队任务 / push_many 批量与空批量 no-op / ack 后 processing 清空、
   重复 ack False / LREM 精确性（同 payload 三份只删 ack 的那份，
   剩余 id 互不相同）/ nack 重投 tries+1 且 id 不变、对旧 raw 再
   nack False / recover 把 processing 残留全量搬回（信封原样、可再
   消费、空时 0）/ qsize 与 processing_size / 键结构（默认与自定义
   prefix）/ 8 线程并发 push+pop+ack 守恒（400 条不丢不重、ack 全
   成功、两列表归零）/ 参数校验（name / payload / push_many /
   timeout / job 类型）/ repr；
2. DelayQueue：未到期 pop_ready 取不到（冻结 ``_now``）/ 时间推进后
   move_due 搬运（score = 冻结 now + delay 落在 ZSET）/ 恰好到期
   边界（score == now 即到期）/ move_due 的 limit 分批 / 到期序 FIFO
   （最早到期者先弹出）/ pop_ready 超时返回 None（确实等待满）/
   阻塞 pop_ready 取到稍后到期任务 / cancel 按任务 id、按 Job 对象、
   搬运后不可取消、未知 id False / 同 payload 多份 id 唯一（ZSET
   member 互不覆盖）/ 双线程并发 move_due 不重不漏（40 条搬运守恒、
   弹出集合恰等）/ due_size 与 ready_size / 键结构 / 参数校验
   （delay / limit / timeout / cancel / payload / name）/ repr；
   并发用例可复跑（3 轮回归验证无 flaky）。
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, List

import fakeredis
import pytest

from lizysdk.dist import queue as dist_queue
from lizysdk.dist.queue import DelayQueue, Job, ReliableQueue


# ---------------------------------------------------------------------------
# 通用 fixture 与辅助
# ---------------------------------------------------------------------------


@pytest.fixture()
def client() -> Any:
    """每个测试一个全新的 FakeStrictRedis（支持 EVAL / LMOVE / BLMOVE / 线程安全）。"""
    return fakeredis.FakeStrictRedis()


def run_threads(count: int, target: Any) -> List[threading.Thread]:
    """启动 count 个线程执行 target() 并 join，返回线程列表。"""
    threads = [threading.Thread(target=target, daemon=True) for _ in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    return threads


# ---------------------------------------------------------------------------
# Job 信封模型
# ---------------------------------------------------------------------------


class TestJob:
    """Job NamedTuple 的结构与字段语义。"""

    def test_structure(self) -> None:
        """四字段 str/str/str/int，实例化与解包可用。"""
        job = Job(id="abc123", payload="p", raw='{"id":"abc123"}', tries=2)
        assert Job._fields == ("id", "payload", "raw", "tries")
        assert job.id == "abc123"
        assert job.payload == "p"
        assert job.raw == '{"id":"abc123"}'
        assert job.tries == 2
        assert isinstance(job, tuple)

    def test_immutable(self) -> None:
        """NamedTuple 不可变：赋值抛 AttributeError。"""
        job = Job(id="a", payload="p", raw="r", tries=0)
        with pytest.raises(AttributeError):
            job.tries = 5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# ReliableQueue
# ---------------------------------------------------------------------------


class TestReliableQueue:
    """往返 / 信封 / ack 精确性 / nack / recover / 并发守恒与参数校验。"""

    def test_push_pop_ack_roundtrip(self, client: Any) -> None:
        """基本流：push -> qsize 1 -> pop（信封解包）-> ack -> processing 清空。"""
        rq = ReliableQueue(client, "orders")
        job_id = rq.push("order:中文:123")
        assert isinstance(job_id, str) and len(job_id) == 16
        assert rq.qsize() == 1
        assert rq.processing_size() == 0

        job = rq.pop()
        assert isinstance(job, Job)
        assert job.id == job_id                 # id 即 push 返回值
        assert job.payload == "order:中文:123"  # 用户原文往返
        assert job.tries == 0
        assert rq.qsize() == 0
        assert rq.processing_size() == 1        # 弹出即在备份列表

        assert rq.ack(job) is True
        assert rq.processing_size() == 0
        assert rq.ack(job) is False             # 重复 ack：信封已不在

    def test_envelope_raw_stored_in_processing(self, client: Any) -> None:
        """信封解包：job.raw 就是 processing 里存的那条 JSON 原文。"""
        rq = ReliableQueue(client, "env")
        rq.push("hello")
        job = rq.pop()
        stored = client.lindex("lizy:rq:env:processing", 0)
        assert stored is not None
        assert stored.decode("utf-8") == job.raw
        env = json.loads(job.raw)
        assert set(env) == {"id", "payload", "pushed_at", "tries"}
        assert env["id"] == job.id
        assert env["payload"] == "hello"
        assert env["tries"] == 0

    def test_pop_empty_returns_none_nonblocking(self, client: Any) -> None:
        """非阻塞 pop 空队列：立即返回 None（不抛「队列为空」）。"""
        rq = ReliableQueue(client, "empty")
        start = time.monotonic()
        assert rq.pop() is None
        assert time.monotonic() - start < 0.5

    def test_pop_empty_returns_none_with_timeout(self, client: Any) -> None:
        """带超时 pop 空队列：等满 timeout 后返回 None（不永久阻塞）。"""
        rq = ReliableQueue(client, "empty2")
        start = time.monotonic()
        assert rq.pop(timeout=0.15) is None
        elapsed = time.monotonic() - start
        assert 0.1 <= elapsed <= 3.0            # 确实等待、且如期返回

    def test_pop_timeout_picks_up_late_push(self, client: Any) -> None:
        """阻塞 pop：另一线程稍后 push，pop 在超时窗内取到该任务。"""
        rq = ReliableQueue(client, "late")
        producer = threading.Thread(
            target=lambda: (time.sleep(0.15), rq.push("late-arrival")),
            daemon=True,
        )
        producer.start()
        start = time.monotonic()
        job = rq.pop(timeout=3.0)
        elapsed = time.monotonic() - start
        assert job is not None
        assert job.payload == "late-arrival"
        assert elapsed >= 0.1                   # 确实等待过
        assert rq.ack(job) is True
        producer.join()

    def test_push_many_batch(self, client: Any) -> None:
        """push_many 批量：一次入队多条，id 互不相同；空列表 no-op。"""
        rq = ReliableQueue(client, "batch")
        ids = rq.push_many(["a", "b", "c"])
        assert len(ids) == 3
        assert len(set(ids)) == 3               # id 唯一
        assert rq.qsize() == 3
        assert rq.push_many([]) == []
        assert rq.qsize() == 3                  # 空批量不动队列
        popped_ids = set()
        payloads = []
        while True:
            job = rq.pop()
            if job is None:
                break
            popped_ids.add(job.id)
            payloads.append(job.payload)
        assert popped_ids == set(ids)
        assert sorted(payloads) == ["a", "b", "c"]

    def test_ack_precision_same_payload(self, client: Any) -> None:
        """LREM 精确性：同 payload 三份，ack 只删自己那份信封。"""
        rq = ReliableQueue(client, "dup")
        rq.push_many(["same", "same", "same"])
        jobs = [rq.pop(), rq.pop(), rq.pop()]
        assert all(job.payload == "same" for job in jobs)
        assert len({job.id for job in jobs}) == 3

        victim = jobs[1]
        assert rq.ack(victim) is True
        assert rq.processing_size() == 2        # 恰好删一份
        assert rq.qsize() == 0                  # 队列不受影响

        assert rq.recover() == 2                # 剩余两份搬回队（信封原样）
        assert rq.processing_size() == 0
        back = set()
        while True:
            job = rq.pop()
            if job is None:
                break
            back.add(job.id)
        assert back == {jobs[0].id, jobs[2].id}  # 已 ack 者不再出现
        assert victim.id not in back

    def test_nack_requeues_with_tries_plus_one(self, client: Any) -> None:
        """nack：LREM + 回队，tries+1 且 id / payload 不变。"""
        rq = ReliableQueue(client, "retry")
        rq.push("retry-me")
        job = rq.pop()
        assert rq.nack(job) is True
        assert rq.qsize() == 1
        assert rq.processing_size() == 0

        again = rq.pop()
        assert again is not None
        assert again.id == job.id               # 同一任务身份
        assert again.payload == "retry-me"
        assert again.tries == job.tries + 1 == 1
        assert json.loads(again.raw)["tries"] == 1

        assert rq.nack(job) is False            # 旧 raw 已不在 processing
        assert rq.qsize() == 0                  # 失败 nack 不写入（again 已弹出）

    def test_ack_on_acked_returns_false(self, client: Any) -> None:
        """ack 已确认过的任务：LREM 0 -> False，队列状态不变。"""
        rq = ReliableQueue(client, "acked")
        rq.push("x")
        job = rq.pop()
        assert rq.ack(job) is True
        rq.push("y")                            # 队列里有别的任务
        assert rq.ack(job) is False
        assert rq.qsize() == 1
        assert rq.processing_size() == 0

    def test_recover_moves_processing_back(self, client: Any) -> None:
        """recover：processing 残留全量搬回 queue，信封原样可再消费。"""
        rq = ReliableQueue(client, "recover")
        rq.push_many(["a", "b", "c"])
        first = rq.pop()
        second = rq.pop()                       # 两份滞留 processing（模拟崩溃）
        assert rq.processing_size() == 2
        assert rq.qsize() == 1

        assert rq.recover() == 2
        assert rq.qsize() == 3                  # 队列剩余 1 + 搬回 2
        assert rq.processing_size() == 0
        assert rq.recover() == 0                # 再清扫：无残留

        ids = set()
        while True:
            job = rq.pop()
            if job is None:
                break
            ids.add(job.id)
            assert job.tries == 0               # 原样搬回不累加 tries
        assert len(ids) == 3                    # 队列原有 1 + 搬回 2，全量弹出
        assert first.id in ids and second.id in ids   # 残留两份 id 不变

    def test_recover_empty_returns_zero(self, client: Any) -> None:
        """空 processing 的 recover：返回 0（安全 no-op）。"""
        rq = ReliableQueue(client, "recover2")
        assert rq.recover() == 0
        rq.push("only")
        assert rq.recover() == 0                # 只在 queue，不在 processing

    def test_key_layout_default_and_custom_prefix(self, client: Any) -> None:
        """键结构：{prefix}:{name} 与 {prefix}:{name}:processing。"""
        rq = ReliableQueue(client, "orders")
        assert rq.queue_key == "lizy:rq:orders"
        assert rq.processing_key == "lizy:rq:orders:processing"
        rq.push("x")
        assert client.exists("lizy:rq:orders") == 1
        rq.pop()
        assert client.exists("lizy:rq:orders:processing") == 1

        custom = ReliableQueue(client, "o2", prefix="app:q")
        assert custom.queue_key == "app:q:o2"
        custom.push("y")
        assert client.exists("app:q:o2") == 1

    def test_concurrent_push_pop_ack_conservation(self, client: Any) -> None:
        """8 线程并发 push_many + pop + ack：400 条不丢不重、ack 全成功。"""
        rq = ReliableQueue(client, "shared")
        n_threads = 8
        per_thread = 50
        barrier = threading.Barrier(n_threads)
        popped: List[str] = []
        guard = threading.Lock()
        acked = {"count": 0}
        errors: List[BaseException] = []

        def worker(idx: int) -> None:
            try:
                rq.push_many([f"t{idx}-p{k}" for k in range(per_thread)])
                barrier.wait()                  # 全部入队完毕才开始消费
                while True:
                    job = rq.pop()
                    if job is None:
                        break
                    ok = rq.ack(job)
                    with guard:
                        popped.append(job.payload)
                        acked["count"] += int(ok)
            except BaseException as exc:  # noqa: BLE001 测试需要捕获一切
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(i,), daemon=True)
            for i in range(n_threads)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert errors == []
        assert len(popped) == n_threads * per_thread          # 不丢
        assert len(set(popped)) == n_threads * per_thread     # 不重
        expected = {
            f"t{i}-p{k}" for i in range(n_threads) for k in range(per_thread)
        }
        assert set(popped) == expected
        assert acked["count"] == n_threads * per_thread       # ack 全成功
        assert rq.qsize() == 0
        assert rq.processing_size() == 0

    def test_validation_constructor(self, client: Any) -> None:
        """构造校验：name 非空 str。"""
        for bad in ("", None, 123, b"q"):
            with pytest.raises(ValueError, match="name"):
                ReliableQueue(client, bad)  # type: ignore[arg-type]

    def test_validation_push(self, client: Any) -> None:
        """push 校验：payload 必须为 str。"""
        rq = ReliableQueue(client, "v")
        for bad in (123, b"bytes", None, 1.5):
            with pytest.raises(ValueError, match="payload"):
                rq.push(bad)  # type: ignore[arg-type]

    def test_validation_push_many(self, client: Any) -> None:
        """push_many 校验：拒绝裸 str / 不可迭代 / 非 str 元素。"""
        rq = ReliableQueue(client, "v2")
        with pytest.raises(ValueError, match="payloads"):
            rq.push_many("abc")  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="payloads"):
            rq.push_many(5)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="payloads"):
            rq.push_many(["ok", 42])  # type: ignore[list-item]
        assert rq.qsize() == 0                   # 校验失败不留半批数据

    def test_validation_pop_timeout(self, client: Any) -> None:
        """pop 校验：timeout >= 0 且为数值。"""
        rq = ReliableQueue(client, "v3")
        for bad in (-0.1, -1, "1", True):
            with pytest.raises(ValueError, match="timeout"):
                rq.pop(bad)  # type: ignore[arg-type]

    def test_validation_ack_nack_job_type(self, client: Any) -> None:
        """ack / nack 校验：必须是 Job 实例。"""
        rq = ReliableQueue(client, "v4")
        for bad in ("raw-string", None, 42, {"id": "x"}):
            with pytest.raises(ValueError, match="Job"):
                rq.ack(bad)  # type: ignore[arg-type]
            with pytest.raises(ValueError, match="Job"):
                rq.nack(bad)  # type: ignore[arg-type]

    def test_repr_and_properties(self, client: Any) -> None:
        """repr 不回显客户端；name / queue_key / processing_key 可读。"""
        rq = ReliableQueue(client, "meta", prefix="p")
        assert rq.name == "meta"
        assert rq.queue_key == "p:meta"
        assert rq.processing_key == "p:meta:processing"
        text = repr(rq)
        assert "meta" in text and "p" in text
        assert "FakeStrictRedis" not in text


# ---------------------------------------------------------------------------
# DelayQueue
# ---------------------------------------------------------------------------


class TestDelayQueue:
    """到期语义 / 搬运原子性 / cancel / 阻塞弹出与参数校验。"""

    def test_not_due_invisible(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        """未到期：move_due 搬不动、pop_ready 取不到、大小计数正确。"""
        clock = {"now": 500_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "rem")
        dq.push("future-task", delay=30.0)
        assert dq.due_size() == 1
        assert dq.ready_size() == 0
        assert dq.move_due() == 0               # 未到期搬运数为 0
        assert dq.pop_ready() is None           # 非阻塞取不到
        assert dq.due_size() == 1               # 任务仍在延迟 ZSET

    def test_push_writes_score_now_plus_delay(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """push 落 ZSET：member 是信封 JSON、score = 冻结 now + delay。"""
        clock = {"now": 500_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "score")
        dq.push("payload-x", delay=30.0)
        rows = client.zrange(dq.zset_key, 0, -1, withscores=True)
        assert len(rows) == 1
        env = json.loads(rows[0][0].decode("utf-8"))
        assert env["payload"] == "payload-x"
        assert rows[0][1] == 500_030.0          # 500_000 + 30

    def test_advance_clock_then_move_due(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """冻结 _now 推进时间：到期项被 move_due 搬到 ready，可弹出。"""
        clock = {"now": 1_000_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "adv")
        dq.push("task-a", delay=10.0)
        dq.push("task-b", delay=10.0)
        clock["now"] += 5.0
        assert dq.move_due() == 0               # 还差 5 秒
        clock["now"] += 5.1
        assert dq.move_due() == 2               # 两份齐到期
        assert dq.due_size() == 0
        assert dq.ready_size() == 2
        payloads = {dq.pop_ready().payload, dq.pop_ready().payload}
        assert payloads == {"task-a", "task-b"}
        assert dq.pop_ready() is None

    def test_due_boundary_exact(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """边界：score == now（恰好到期）即视为到期可搬运。"""
        clock = {"now": 2_000_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "edge")
        dq.push("boundary", delay=30.0)         # score = 2_000_030.0
        clock["now"] = 2_000_029.999            # 差一点点
        assert dq.move_due() == 0
        clock["now"] = 2_000_030.0              # 恰好到达（直接赋值避开浮点累加）
        assert dq.move_due() == 1               # <= now 含边界
        assert dq.pop_ready().payload == "boundary"

    def test_move_due_limit_batches(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """move_due(limit)：分批搬运，剩余留在 ZSET。"""
        clock = {"now": 3_000_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "limit")
        for i in range(5):
            dq.push(f"m{i}", delay=1.0)
        clock["now"] += 10.0                    # 全部到期
        assert dq.move_due(limit=2) == 2
        assert dq.due_size() == 3
        assert dq.ready_size() == 2
        assert dq.move_due(limit=2) == 2
        assert dq.move_due(limit=2) == 1
        assert dq.due_size() == 0
        assert dq.ready_size() == 5

    def test_pop_ready_fifo_earliest_due_first(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """到期序 FIFO：最早到期的任务先弹出。"""
        clock = {"now": 4_000_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "fifo")
        dq.push("late-item", delay=5.0)
        dq.push("early-item", delay=1.0)
        clock["now"] += 10.0                    # 两份都到期
        dq.move_due()
        assert dq.pop_ready().payload == "early-item"
        assert dq.pop_ready().payload == "late-item"

    def test_pop_ready_timeout_returns_none(self, client: Any) -> None:
        """pop_ready 超时：无到期任务时等满 timeout 返回 None。"""
        dq = DelayQueue(client, "none")
        start = time.monotonic()
        assert dq.pop_ready(timeout=0.15) is None
        elapsed = time.monotonic() - start
        assert 0.1 <= elapsed <= 3.0            # 确实等待、且如期返回
        assert dq.due_size() == 0 and dq.ready_size() == 0

    def test_pop_ready_timeout_picks_up_later_due(self, client: Any) -> None:
        """阻塞 pop_ready：稍后才入队且很快到期的任务也能在窗口内取到。"""
        dq = DelayQueue(client, "later")

        def producer() -> None:
            time.sleep(0.1)
            dq.push("due-soon", delay=0.05)     # 0.15s 时刻到期

        thread = threading.Thread(target=producer, daemon=True)
        thread.start()
        job = dq.pop_ready(timeout=3.0)         # 循环内每步都重新 move_due
        assert job is not None
        assert job.payload == "due-soon"
        assert dq.due_size() == 0 and dq.ready_size() == 0
        thread.join()

    def test_cancel_by_id(self, client: Any, monkeypatch: pytest.MonkeyPatch) -> None:
        """cancel：按 push 返回的任务 id 取消未到期任务。"""
        clock = {"now": 500_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "cancel")
        job_id = dq.push("to-be-cancelled", delay=30.0)
        assert dq.cancel(job_id) is True
        assert dq.due_size() == 0
        assert dq.cancel(job_id) is False       # 重复取消：已不在
        clock["now"] += 100.0
        assert dq.move_due() == 0               # 已取消，无可搬运
        assert dq.pop_ready() is None

    def test_cancel_by_job_object(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cancel：接受含目标 id 的 Job 对象（id 是定位键）。"""
        clock = {"now": 500_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "cancel2")
        job_id = dq.push("cancel-via-job", delay=10.0)
        handle = Job(id=job_id, payload="cancel-via-job", raw="", tries=0)
        assert dq.cancel(handle) is True
        assert dq.due_size() == 0

    def test_cancel_after_moved_returns_false(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cancel 窗口：已搬运到 ready list 的任务不可取消。"""
        clock = {"now": 500_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "cancel3")
        job_id = dq.push("already-moved", delay=1.0)
        clock["now"] += 10.0
        assert dq.move_due() == 1               # 已进 ready
        assert dq.cancel(job_id) is False       # 过了可取消窗口
        assert dq.ready_size() == 1             # ready 不受影响

    def test_cancel_unknown_id_returns_false(self, client: Any) -> None:
        """cancel 未知 id：False（空 ZSET 扫描直接落空）。"""
        dq = DelayQueue(client, "cancel4")
        assert dq.cancel("deadbeefdeadbeef") is False

    def test_same_payload_distinct_ids(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """同 payload 多份：id 唯一，ZSET member 互不覆盖。"""
        clock = {"now": 500_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "same")
        id1 = dq.push("dup", delay=5.0)
        id2 = dq.push("dup", delay=5.0)
        assert id1 != id2
        assert dq.due_size() == 2               # 两份都在
        clock["now"] += 10.0
        assert dq.move_due() == 2
        payloads = [dq.pop_ready().payload, dq.pop_ready().payload]
        assert payloads == ["dup", "dup"]

    def test_concurrent_move_due_no_dup_no_loss(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """双线程并发 move_due：40 条恰好搬运一次（不重不漏）。"""
        clock = {"now": 1_000_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "fanout")
        payloads = [f"item-{i}" for i in range(40)]
        for payload in payloads:
            dq.push(payload, delay=10.0)
        clock["now"] += 20.0                    # 全部到期
        barrier = threading.Barrier(2)
        moved: List[int] = []
        guard = threading.Lock()
        errors: List[BaseException] = []

        def mover() -> None:
            try:
                barrier.wait()
                local = 0
                for _ in range(500):            # 竞争轮次远大于所需
                    got = dq.move_due(limit=3)
                    local += got
                    if got == 0 and dq.due_size() == 0:
                        break
                with guard:
                    moved.append(local)
            except BaseException as exc:  # noqa: BLE001 测试需要捕获一切
                errors.append(exc)

        run_threads(2, mover)
        assert errors == []
        assert sum(moved) == 40                 # 恰好各搬运一次
        assert dq.due_size() == 0
        assert dq.ready_size() == 40
        got_payloads = set()
        while True:
            job = dq.pop_ready()
            if job is None:
                break
            got_payloads.add(job.payload)
        assert got_payloads == set(payloads)    # 弹出集合恰等：不重不漏

    def test_job_shape_from_pop(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """pop_ready 返回的 Job：id 16 位 hex、tries 0、raw 可解析。"""
        clock = {"now": 500_000.0}
        monkeypatch.setattr(dist_queue, "_now", lambda: clock["now"])
        dq = DelayQueue(client, "shape")
        job_id = dq.push("shape-中文", delay=1.0)
        clock["now"] += 2.0
        job = dq.pop_ready()
        assert isinstance(job, Job)
        assert job.id == job_id
        assert len(job.id) == 16
        assert all(c in "0123456789abcdef" for c in job.id)
        assert job.payload == "shape-中文"
        assert job.tries == 0
        env = json.loads(job.raw)
        assert env["id"] == job.id and env["payload"] == job.payload

    def test_key_layout_default_and_custom_prefix(
        self, client: Any, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """键结构：{prefix}:{name}:zset 与 {prefix}:{name}:ready。"""
        monkeypatch.setattr(dist_queue, "_now", lambda: 100.0)
        dq = DelayQueue(client, "rem")
        assert dq.zset_key == "lizy:dq:rem:zset"
        assert dq.ready_key == "lizy:dq:rem:ready"
        dq.push("x", delay=5.0)
        assert client.exists("lizy:dq:rem:zset") == 1
        monkeypatch.setattr(dist_queue, "_now", lambda: 200.0)   # 推进到到期后
        assert dq.move_due() == 1
        assert client.exists("lizy:dq:rem:ready") == 1

        custom = DelayQueue(client, "r2", prefix="app:dq")
        assert custom.zset_key == "app:dq:r2:zset"
        assert custom.ready_key == "app:dq:r2:ready"

    def test_validation_constructor(self, client: Any) -> None:
        """构造校验：name 非空 str。"""
        for bad in ("", None, 7, b"dq"):
            with pytest.raises(ValueError, match="name"):
                DelayQueue(client, bad)  # type: ignore[arg-type]

    def test_validation_delay(self, client: Any) -> None:
        """push 校验：delay > 0 且为数值；payload 为 str。"""
        dq = DelayQueue(client, "v")
        for bad in (0, -1, -0.5, "5", None, True):
            with pytest.raises(ValueError, match="delay"):
                dq.push("p", bad)  # type: ignore[arg-type]
        for bad in (123, b"bytes", None):
            with pytest.raises(ValueError, match="payload"):
                dq.push(bad, delay=1.0)  # type: ignore[arg-type]
        assert dq.due_size() == 0                 # 校验失败不落 ZSET

    def test_validation_move_due_limit(self, client: Any) -> None:
        """move_due 校验：limit >= 1 的整数。"""
        dq = DelayQueue(client, "v2")
        for bad in (0, -1, 1.5, "3", True):
            with pytest.raises(ValueError, match="limit"):
                dq.move_due(bad)  # type: ignore[arg-type]

    def test_validation_pop_ready_timeout(self, client: Any) -> None:
        """pop_ready 校验：timeout >= 0 且为数值。"""
        dq = DelayQueue(client, "v3")
        for bad in (-0.1, -5, "1", True):
            with pytest.raises(ValueError, match="timeout"):
                dq.pop_ready(bad)  # type: ignore[arg-type]

    def test_validation_cancel(self, client: Any) -> None:
        """cancel 校验：Job 实例或非空 str。"""
        dq = DelayQueue(client, "v4")
        for bad in (None, 42, "", 3.5, b"id"):
            with pytest.raises(ValueError, match="job"):
                dq.cancel(bad)  # type: ignore[arg-type]

    def test_repr_and_properties(self, client: Any) -> None:
        """repr 不回显客户端；name / zset_key / ready_key 可读。"""
        dq = DelayQueue(client, "meta", prefix="p")
        assert dq.name == "meta"
        assert dq.zset_key == "p:meta:zset"
        assert dq.ready_key == "p:meta:ready"
        text = repr(dq)
        assert "meta" in text
        assert "FakeStrictRedis" not in text
