"""lizysdk.dist.leaderboard 测试：排行榜（fakeredis 全离线）。

覆盖范围（设计文档 §1.6 / §4 对应行全落地）：
add_score 覆盖语义与中文 member / incr_score 增减与缺失成员从 0 起 /
score 在榜与未上榜 / rank 1-based（降序默认 + ascending 翻转 + 未上榜
None）/ 同分平局 member 字典序（降序视图倒字典序、升序视图正字典序，
Redis 原生行为）/ top 基本榜单与 ascending / top 边界（n 超过 size、
空榜）/ around 居中窗口 / around 贴边（榜首与榜尾自动收缩）/ span=0
仅自己 / around ascending / around 不在榜抛中文 ValueError（消息含
成员名）/ remove 与重复 remove / size / reset 幂等 / 参数校验
（name / member / score 与 NaN / amount / n / span）/ 键结构与 repr。
"""

from __future__ import annotations

import math
from typing import Any, List, Tuple

import fakeredis
import pytest

from lizysdk.dist.leaderboard import Leaderboard


@pytest.fixture()
def client() -> Any:
    """每个测试一个全新的 FakeStrictRedis。"""
    return fakeredis.FakeStrictRedis()


def seed(lb: Leaderboard, entries: List[Tuple[str, float]]) -> None:
    """按 (member, score) 列表依次 add_score。"""
    for member, score in entries:
        lb.add_score(member, score)


# ---------------------------------------------------------------------------
# 写路径：add / incr / remove / reset
# ---------------------------------------------------------------------------


class TestLeaderboardWrites:
    """计分、加分、移除与清空。"""

    def test_add_score_overwrites(self, client: Any) -> None:
        """add_score 是覆盖语义：再设置直接替换旧分。"""
        lb = Leaderboard(client, "g")
        assert lb.add_score("alice", 100) is None
        assert lb.score("alice") == 100.0
        lb.add_score("alice", 80)               # 覆盖，不累加
        assert lb.score("alice") == 80.0
        assert lb.size() == 1

    def test_add_score_negative_and_float(self, client: Any) -> None:
        """负分与浮点分均合法。"""
        lb = Leaderboard(client, "g")
        lb.add_score("minus", -5)
        lb.add_score("frac", 1.25)
        assert lb.score("minus") == -5.0
        assert lb.score("frac") == 1.25

    def test_incr_score_accumulates(self, client: Any) -> None:
        """incr_score：返回最新分数，正负增量均累加。"""
        lb = Leaderboard(client, "g")
        lb.add_score("alice", 100)
        assert lb.incr_score("alice", 50) == 150.0
        assert lb.incr_score("alice", -30) == 120.0
        assert lb.score("alice") == 120.0

    def test_incr_score_missing_member_from_zero(self, client: Any) -> None:
        """incr_score 未上榜成员：从 0 起加（ZINCRBY 语义）。"""
        lb = Leaderboard(client, "g")
        assert lb.incr_score("newbie", 5) == 5.0
        assert lb.score("newbie") == 5.0

    def test_remove(self, client: Any) -> None:
        """remove：在榜 True、榜空后重复 False。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 1), ("b", 2)])
        assert lb.remove("a") is True
        assert lb.size() == 1
        assert lb.rank("a") is None
        assert lb.remove("a") is False           # 已不在榜
        assert lb.remove("ghost") is False       # 从未在榜

    def test_reset(self, client: Any) -> None:
        """reset 清空整榜，幂等可重复。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 1), ("b", 2), ("c", 3)])
        assert lb.size() == 3
        lb.reset()
        assert lb.size() == 0
        assert lb.top(10) == []
        assert lb.rank("a") is None
        assert lb.score("a") is None
        lb.reset()                               # 空榜重复 reset 安全
        assert lb.size() == 0


# ---------------------------------------------------------------------------
# 读路径：score / rank / top / around
# ---------------------------------------------------------------------------


class TestLeaderboardReads:
    """名次、榜单与名次窗口（含同分平局与边界）。"""

    def test_score_missing_returns_none(self, client: Any) -> None:
        """score 未上榜：None。"""
        lb = Leaderboard(client, "g")
        assert lb.score("ghost") is None

    def test_rank_one_based_descending(self, client: Any) -> None:
        """rank 默认降序 1-based：分高者名次 1。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 30), ("c", 20)])
        assert lb.rank("b") == 1
        assert lb.rank("c") == 2
        assert lb.rank("a") == 3
        assert lb.rank("ghost") is None         # 未上榜 None

    def test_rank_ascending_flips(self, client: Any) -> None:
        """rank ascending=True：分低者名次 1。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 30), ("c", 20)])
        assert lb.rank("a", ascending=True) == 1
        assert lb.rank("c", ascending=True) == 2
        assert lb.rank("b", ascending=True) == 3

    def test_rank_tie_lexicographic_descending(self, client: Any) -> None:
        """同分平局（降序视图）：member 倒字典序——bob 在 alice 前。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("alice", 100), ("bob", 100), ("carol", 90)])
        assert lb.rank("bob") == 1              # 同分，bob 字典序大者靠前
        assert lb.rank("alice") == 2
        assert lb.rank("carol") == 3

    def test_rank_tie_lexicographic_ascending(self, client: Any) -> None:
        """同分平局（升序视图）：member 正字典序——alice 在 bob 前。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("alice", 100), ("bob", 100), ("carol", 90)])
        assert lb.rank("carol", ascending=True) == 1    # 分最低
        assert lb.rank("alice", ascending=True) == 2    # 同分正字典序
        assert lb.rank("bob", ascending=True) == 3

    def test_top_descending(self, client: Any) -> None:
        """top 默认降序，元素为 (member, float score)。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 30), ("c", 20)])
        rows = lb.top(3)
        assert rows == [("b", 30.0), ("c", 20.0), ("a", 10.0)]
        assert all(isinstance(score, float) for _, score in rows)

    def test_top_ascending(self, client: Any) -> None:
        """top ascending=True：分低在前。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 30), ("c", 20)])
        assert lb.top(3, ascending=True) == [
            ("a", 10.0),
            ("c", 20.0),
            ("b", 30.0),
        ]

    def test_top_tie_order(self, client: Any) -> None:
        """top 同分平局：降序视图倒字典序。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("alice", 100), ("bob", 100), ("carol", 90)])
        assert lb.top(3) == [("bob", 100.0), ("alice", 100.0), ("carol", 90.0)]

    def test_top_n_exceeds_size(self, client: Any) -> None:
        """top 边界：n 超过榜单大小返回全部。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 1), ("b", 2)])
        assert lb.top(10) == [("b", 2.0), ("a", 1.0)]

    def test_top_empty_board(self, client: Any) -> None:
        """top 空榜：空列表。"""
        lb = Leaderboard(client, "g")
        assert lb.top(5) == []
        assert lb.size() == 0

    def test_around_center(self, client: Any) -> None:
        """around 居中：以 member 为中心的 2*span+1 窗口，member 恰在正中。"""
        lb = Leaderboard(client, "g")
        seed(
            lb,
            [("a", 10), ("b", 20), ("c", 30), ("d", 40), ("e", 50), ("f", 60), ("g2", 70)],
        )
        # 降序名次：g2=1 f=2 e=3 d=4 c=5 b=6 a=7；d 的 span=2 窗口 = 第 2..6 名
        assert lb.around("d", span=2) == [
            ("f", 60.0),
            ("e", 50.0),
            ("d", 40.0),
            ("c", 30.0),
            ("b", 20.0),
        ]

    def test_around_top_edge(self, client: Any) -> None:
        """around 贴边：榜首窗口自动收缩到 [1, 1+span]。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 20), ("c", 30)])
        assert lb.around("c", span=2) == [("c", 30.0), ("b", 20.0), ("a", 10.0)]

    def test_around_bottom_edge(self, client: Any) -> None:
        """around 贴边：榜尾窗口自动收缩到 [r-span, r]。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 20), ("c", 30)])
        assert lb.around("a", span=2) == [("c", 30.0), ("b", 20.0), ("a", 10.0)]

    def test_around_span_zero(self, client: Any) -> None:
        """around span=0：窗口只剩 member 自己。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 20), ("c", 30)])
        assert lb.around("b", span=0) == [("b", 20.0)]

    def test_around_default_span_is_two(self, client: Any) -> None:
        """around 默认 span=2（5 条窗口）。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 20), ("c", 30), ("d", 40), ("e", 50)])
        assert lb.around("c") == [
            ("e", 50.0),
            ("d", 40.0),
            ("c", 30.0),
            ("b", 20.0),
            ("a", 10.0),
        ]

    def test_around_ascending(self, client: Any) -> None:
        """around ascending=True：升序名次窗口（分低在前）。"""
        lb = Leaderboard(client, "g")
        seed(lb, [("a", 10), ("b", 20), ("c", 30), ("d", 40), ("e", 50)])
        # 升序名次：a=1 b=2 c=3 d=4 e=5；c 的 span=1 窗口 = 第 2..4 名
        assert lb.around("c", span=1, ascending=True) == [
            ("b", 20.0),
            ("c", 30.0),
            ("d", 40.0),
        ]

    def test_around_missing_member_raises(self, client: Any) -> None:
        """around 不在榜：中文 ValueError，消息含成员名与榜名。"""
        lb = Leaderboard(client, "game:1")
        seed(lb, [("a", 10)])
        with pytest.raises(ValueError) as excinfo:
            lb.around("ghost", span=2)
        message = str(excinfo.value)
        assert "ghost" in message
        assert "game:1" in message
        assert "不在排行榜" in message

    def test_size_tracks_members(self, client: Any) -> None:
        """size 随 add / remove 增减。"""
        lb = Leaderboard(client, "g")
        assert lb.size() == 0
        lb.add_score("a", 1)
        lb.add_score("b", 2)
        assert lb.size() == 2
        lb.remove("a")
        assert lb.size() == 1


# ---------------------------------------------------------------------------
# 键结构 / 参数校验 / repr
# ---------------------------------------------------------------------------


class TestLeaderboardMeta:
    """键结构、只读属性、repr 与全量参数校验。"""

    def test_key_layout_and_properties(self, client: Any) -> None:
        """键为 {prefix}:{name}；name / key 只读属性。"""
        lb = Leaderboard(client, "game:1:score")
        assert lb.name == "game:1:score"
        assert lb.key == "lizy:lb:game:1:score"
        lb.add_score("alice", 1)
        assert client.exists("lizy:lb:game:1:score") == 1

        custom = Leaderboard(client, "s2", prefix="app:lb")
        assert custom.key == "app:lb:s2"
        custom.add_score("bob", 2)
        assert client.exists("app:lb:s2") == 1

    def test_two_boards_independent(self, client: Any) -> None:
        """两个 Leaderboard 实例（不同 name）互不影响。"""
        first = Leaderboard(client, "one")
        second = Leaderboard(client, "two")
        first.add_score("a", 100)
        second.add_score("a", 1)
        assert first.rank("a") == 1
        assert second.rank("a") == 1
        assert first.score("a") == 100.0
        assert second.score("a") == 1.0

    def test_repr(self, client: Any) -> None:
        """repr 含 name 与 key，不回显客户端。"""
        lb = Leaderboard(client, "game:1", prefix="p")
        text = repr(lb)
        assert "game:1" in text
        assert "p:game:1" in text
        assert "FakeStrictRedis" not in text

    def test_validation_constructor(self, client: Any) -> None:
        """构造校验：name 非空 str。"""
        for bad in ("", None, 7, b"lb"):
            with pytest.raises(ValueError, match="name"):
                Leaderboard(client, bad)  # type: ignore[arg-type]

    def test_validation_member(self, client: Any) -> None:
        """member 校验：所有入口统一拒绝空串 / 非 str。"""
        lb = Leaderboard(client, "g")
        for method in (
            lambda m: lb.add_score(m, 1),
            lambda m: lb.incr_score(m, 1),
            lambda m: lb.score(m),
            lambda m: lb.rank(m),
            lambda m: lb.remove(m),
        ):
            for bad in ("", None, 42, b"m"):
                with pytest.raises(ValueError, match="member"):
                    method(bad)  # type: ignore[arg-type]
        for bad in ("", None, 42, b"m"):
            with pytest.raises(ValueError, match="member"):
                lb.around(bad, span=1)  # type: ignore[arg-type]

    def test_validation_score(self, client: Any) -> None:
        """score 校验：数值（排除 bool），拒绝 NaN。"""
        lb = Leaderboard(client, "g")
        for bad in ("100", None, True, [1]):
            with pytest.raises(ValueError, match="score"):
                lb.add_score("a", bad)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="score"):
            lb.add_score("a", math.nan)
        assert lb.size() == 0                       # 校验失败不落榜

    def test_validation_incr_amount(self, client: Any) -> None:
        """incr_score 的 amount 校验：数值（排除 bool），拒绝 NaN。"""
        lb = Leaderboard(client, "g")
        for bad in ("5", None, True):
            with pytest.raises(ValueError, match="amount"):
                lb.incr_score("a", bad)  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="amount"):
            lb.incr_score("a", math.nan)

    def test_validation_top_n(self, client: Any) -> None:
        """top 校验：n >= 1 的整数（排除 bool / 浮点 / str）。"""
        lb = Leaderboard(client, "g")
        for bad in (0, -1, 1.5, "10", True, None):
            with pytest.raises(ValueError, match="n "):
                lb.top(bad)  # type: ignore[arg-type]

    def test_validation_around_span(self, client: Any) -> None:
        """around 校验：span >= 0 的整数（排除 bool / 浮点 / str）。"""
        lb = Leaderboard(client, "g")
        lb.add_score("a", 1)
        for bad in (-1, -0.5, 1.5, "2", True, None):
            with pytest.raises(ValueError, match="span"):
                lb.around("a", span=bad)  # type: ignore[arg-type]
