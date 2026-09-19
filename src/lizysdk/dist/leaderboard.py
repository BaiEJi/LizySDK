"""lizysdk.dist.leaderboard —— Redis 排行榜（ZSET 标准玩法）。

实现形态对齐 **redis.io 官方 Leaderboard pattern**（数据建模教程 + 游戏
SDK 共识）：``ZADD`` 计分、``ZREVRANGE`` 榜单、``ZREVRANK`` 名次。同分
按 member 字典序（Redis 原生行为，显式保留——不搞复合分，保持简单诚实）。

本模块自持：仅标准库，模块级不 import redis、不依赖 lizysdk 其他子包；
所有方法只收已建好的 client 实例（懒加载由 ``_client`` / 集成方负责）。
"""

from __future__ import annotations

import math
from typing import Any, List, Optional, Tuple

__all__ = ["Leaderboard"]


def _to_text(value: Any) -> Any:
    """redis 返回的 member 统一转 str：bytes 按 utf-8 解码（兼容 decode_responses=True），str 原样。"""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value


def _check_member(member: Any) -> None:
    """member 参数校验：非空 str，否则抛中文 ValueError。"""
    if not isinstance(member, str) or not member:
        raise ValueError(f"member 必须为非空字符串，当前为 {member!r}")


def _check_number(name: str, value: Any) -> None:
    """数值参数校验：int/float（排除 bool）、非 NaN，否则抛中文 ValueError。"""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} 必须为数值（int/float），当前为 {value!r}")
    if isinstance(value, float) and math.isnan(value):
        raise ValueError(f"{name} 必须为有限数值，当前为 {value!r}")


class Leaderboard:
    """Redis 排行榜（单一 ZSET：计分 / 名次 / 榜单 / 名次窗口）。

    实现形态对齐 **redis.io 官方 Leaderboard pattern**（ZSET 标准玩法）：
    ``ZADD`` 计分、``ZINCRBY`` 加分、``ZREVRANGE`` 榜单、``ZREVRANK``
    名次。两个显式声明的语义决策（设计文档 §1.6）：

    - **名次 1-based**（第 1 名 = 榜首）——对齐游戏行业惯例而非
      Redisson ``RRank`` 的 0-based（显式声明的偏差）；
    - **同分按 member 字典序**（Redis 原生行为，不搞复合分）：底层
      ZSET 按 (score, member) 排序，因此**降序视图**（默认）下同分
      member 呈**倒字典序**（如 bob 在 alice 前）、升序视图下呈正
      字典序。

    实例无本地可变状态，**可跨线程共享同一实例**。

    Args:
        client: redis 客户端实例（需支持 ZADD/ZINCRBY/ZRANK/ZREVRANK/
            ZRANGE/ZREVRANGE/ZSCORE/ZREM/ZCARD/DEL）。
        name: 排行榜名（非空字符串；实际键为 ``{prefix}:{name}``）。
        prefix: 键前缀（默认 ``"lizy:lb"``）。

    Raises:
        ValueError: name 为空 / 非 str（中文消息）。

    Example:
        >>> from lizysdk.dist import Leaderboard                    # doctest: +SKIP
        >>> lb = Leaderboard(client, "game:1:score")
        >>> lb.add_score("alice", 100)         # ZADD（覆盖语义）
        >>> lb.incr_score("alice", 50)         # ZINCRBY -> 新分数
        150.0
        >>> lb.rank("alice")                   # 1-based；未上榜 None
        1
        >>> lb.top(10)                         # [(member, score), ...] 降序
        [('alice', 150.0)]
        >>> lb.around("alice", span=2)         # 以 alice 为中心的名次窗口
        [('alice', 150.0)]
    """

    def __init__(self, client: Any, name: str, *, prefix: str = "lizy:lb") -> None:
        if not isinstance(name, str) or not name:
            raise ValueError(f"name 必须为非空字符串，当前为 {name!r}")
        self._client = client
        self._name = name
        self._prefix = prefix
        self._key = f"{prefix}:{name}"

    # ------------------------------------------------------------------
    # 元信息
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """排行榜名（只读）。"""
        return self._name

    @property
    def key(self) -> str:
        """实际 Redis 键：``{prefix}:{name}``（只读）。"""
        return self._key

    # ------------------------------------------------------------------
    # 写路径
    # ------------------------------------------------------------------

    def add_score(self, member: str, score: float) -> None:
        """设置分数：``ZADD``（**覆盖语义**——已有分数被直接替换，非累加）。

        Args:
            member: 成员（非空字符串）。
            score: 分数（int/float，可为负；NaN 拒绝）。

        Raises:
            ValueError: member 非空 str / score 非数值或 NaN（中文消息）。

        Example:
            >>> lb.add_score("bob", 200)       # doctest: +SKIP
            >>> lb.score("bob")
            200.0
        """
        _check_member(member)
        _check_number("score", score)
        self._client.zadd(self._key, {member: score})

    def incr_score(self, member: str, amount: float = 1.0) -> float:
        """增减分数：``ZINCRBY``（成员不存在时从 0 起加，负数即减分）。

        Args:
            member: 成员（非空字符串）。
            amount: 增量（int/float，可为负；NaN 拒绝）。

        Returns:
            float: 增减后的最新分数。

        Raises:
            ValueError: member 非空 str / amount 非数值或 NaN（中文消息）。

        Example:
            >>> lb.incr_score("alice", 50)     # doctest: +SKIP
            150.0
            >>> lb.incr_score("alice", -30)    # 减分
            120.0
        """
        _check_member(member)
        _check_number("amount", amount)
        return float(self._client.zincrby(self._key, amount, member))

    def remove(self, member: str) -> bool:
        """移除成员（``ZREM``）。

        Args:
            member: 成员（非空字符串）。

        Returns:
            bool: 成员存在且被移除返回 True；不在榜上返回 False。

        Raises:
            ValueError: member 非空 str（中文消息）。
        """
        _check_member(member)
        return bool(self._client.zrem(self._key, member))

    def reset(self) -> None:
        """清空整个排行榜（``DEL`` 键；空榜重复调用安全）。

        Example:
            >>> lb.reset()                     # doctest: +SKIP
            >>> lb.size()
            0
        """
        self._client.delete(self._key)

    # ------------------------------------------------------------------
    # 读路径
    # ------------------------------------------------------------------

    def score(self, member: str) -> Optional[float]:
        """查分数（``ZSCORE``）。

        Args:
            member: 成员（非空字符串）。

        Returns:
            Optional[float]: 在榜返回分数（float）；未上榜返回 None。

        Raises:
            ValueError: member 非空 str（中文消息）。
        """
        _check_member(member)
        value = self._client.zscore(self._key, member)
        return None if value is None else float(value)

    def rank(self, member: str, *, ascending: bool = False) -> Optional[int]:
        """查名次（**1-based**；默认降序——分高者名次小）。

        Args:
            member: 成员（非空字符串）。
            ascending: ``False``（默认）降序（榜首名次 1，``ZREVRANK+1``）；
                ``True`` 升序翻转（``ZRANK+1``）。

        Returns:
            Optional[int]: 在榜返回 1-based 名次（int）；未上榜返回 None。

        Raises:
            ValueError: member 非空 str（中文消息）。

        Note:
            同分平局：按 member 字典序（Redis 原生）——降序视图下同分
            member 呈倒字典序（字典序大者名次靠前），升序视图反之。
        """
        _check_member(member)
        if ascending:
            index = self._client.zrank(self._key, member)
        else:
            index = self._client.zrevrank(self._key, member)
        return None if index is None else int(index) + 1

    def top(self, n: int, *, ascending: bool = False) -> List[Tuple[str, float]]:
        """榜单前 n 名（默认降序——分高在前）。

        Args:
            n: 取前几名（>= 1 的整数；超过榜单大小时返回全部）。
            ascending: ``False``（默认）降序（``ZREVRANGE``）；
                ``True`` 升序（``ZRANGE``）。

        Returns:
            List[Tuple[str, float]]: ``(member, score)`` 列表，按名次
            排列；空榜返回空列表。同分平局按 member 字典序（降序视图
            为倒字典序，Redis 原生行为）。

        Raises:
            ValueError: n 非 int / < 1（中文消息）。
        """
        if isinstance(n, bool) or not isinstance(n, int) or n < 1:
            raise ValueError(f"n 必须为 >= 1 的整数，当前为 {n!r}")
        if ascending:
            rows = self._client.zrange(self._key, 0, n - 1, withscores=True)
        else:
            rows = self._client.zrevrange(self._key, 0, n - 1, withscores=True)
        return [(_to_text(member), float(score)) for member, score in rows]

    def around(self, member: str, span: int = 2, *, ascending: bool = False) -> List[Tuple[str, float]]:
        """以某成员为中心的名次窗口（「我的排名」视图，对齐游戏 SDK 惯例）。

        窗口计算：设 member 的 1-based 名次为 r，取名次区间
        ``[max(1, r - span), r + span]``（贴边自动收缩，最多
        ``2 * span + 1`` 条；``span=0`` 即只看自己）。

        Args:
            member: 成员（非空字符串，**必须在榜**）。
            span: 向上下各取的名次数（>= 0 的整数；0 表示仅该成员）。
            ascending: ``False``（默认）降序窗口；``True`` 升序。

        Returns:
            List[Tuple[str, float]]: 窗口内 ``(member, score)`` 列表，
            按所选顺序排列（member 恰在正中，贴边时靠边）。

        Raises:
            ValueError: span 非 int / < 0，或 member 非空 str，或
                **member 不在榜上**（KeyError 语义的中文 ValueError，
                消息含成员名——与 dict 缺键同类：查一个不存在的中心
                没有唯一合理解）。

        Example:
            >>> lb.around("alice", span=2)     # doctest: +SKIP
            [('carol', 300.0), ('bob', 200.0), ('alice', 150.0),
             ('dave', 100.0), ('erin', 50.0)]
        """
        if isinstance(span, bool) or not isinstance(span, int) or span < 0:
            raise ValueError(f"span 必须为 >= 0 的整数，当前为 {span!r}")
        _check_member(member)
        position = self.rank(member, ascending=ascending)
        if position is None:
            raise ValueError(f"成员 {member!r} 不在排行榜 {self._name!r} 上，无法定位其名次窗口")
        low = max(1, position - span)
        high = position + span
        if ascending:
            rows = self._client.zrange(self._key, low - 1, high - 1, withscores=True)
        else:
            rows = self._client.zrevrange(self._key, low - 1, high - 1, withscores=True)
        return [(_to_text(m), float(s)) for m, s in rows]

    # ------------------------------------------------------------------
    # 观测
    # ------------------------------------------------------------------

    def size(self) -> int:
        """榜单成员数（``ZCARD``）。"""
        return int(self._client.zcard(self._key))

    def __repr__(self) -> str:
        """调试视图（不回显客户端，避免泄露连接信息）。"""
        return (
            f"{self.__class__.__name__}(name={self._name!r}, "
            f"key={self._key!r})"
        )
