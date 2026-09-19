"""lizysdk.dist —— Redis 幂等键 IdempotentKey（Stripe 两态模型对齐件）。

实现形态（设计文档 §1.3 决策）：对齐 Stripe Idempotency Key 语义——
**首次请求 processing 中，并发重复请求被拒（409 语义），完成后落结果，
之后的重复请求拿缓存结果**：

- 键值是 JSON 信封：processing 态 ``{"state":"processing","holder":token,
  "started":ts}``；done 态 ``{"state":"done","result":<json>}``；
- :meth:`begin` = ``SET NX EX``（抢占 processing）；:meth:`complete` =
  ``SET EX`` 覆写为 done + 缓存结果；:meth:`fail` = compare-holder Lua
  ``DEL``（只删自己占的坑，防误删他人 processing）；
- :meth:`guard` 上下文管理器是核心 API（对齐 Stripe「自动管理键生命
  周期」）：进入时抢占，抢占失败按状态抛
  :class:`IdempotencyConflictError` / :class:`IdempotencyDoneError`，
  临界区抛异常退出时自动 fail（DEL，允许重试）。

本模块自持：仅标准库；模块级不 import redis、不依赖 lizysdk 其他子包。
"""

from __future__ import annotations

import json
import math
import secrets
import threading
import time
from types import TracebackType
from typing import Any, Dict, Optional

__all__ = [
    "IdempotencyError",
    "IdempotencyConflictError",
    "IdempotencyDoneError",
    "IdempotentKey",
]

# ---------------------------------------------------------------------------
# 异常族（模块内自持）
# ---------------------------------------------------------------------------


class IdempotencyError(Exception):
    """幂等键相关错误的基类。"""


class IdempotencyConflictError(IdempotencyError):
    """并发冲突：键正被其他执行者 processing 中（Stripe 409 语义）。"""


class IdempotencyDoneError(IdempotencyError):
    """键已完成：重复请求应直接取缓存结果，不要重复执行。

    Attributes:
        result: 已缓存的执行结果（complete 时落库的原始 JSON 反序列化值）。
    """

    def __init__(self, message: str, *, result: Any = None) -> None:
        super().__init__(message)
        self.result = result


# ---------------------------------------------------------------------------
# Lua 脚本（模块级常量）
# ---------------------------------------------------------------------------

#: 失败清除脚本：compare-holder 后 DEL（只删自己占的坑）。
#:
#: - KEYS[1]: 幂等键（``{prefix}:{key}``，string 结构，值 = JSON 信封）
#: - ARGV[1]: 本执行者的 holder token（begin 时生成）
#:
#: 只有信封仍是「state == processing 且 holder == 自己的 token」才 DEL；
#: 已 done（终态不许拆）、holder 是他人（processing 过期被接管）、键不
#: 存在（已清除）一律返回 0——**绝不误删他人占的坑**（复用 DLock
#: compare-token 校验释放的思想）。cjson 解析失败（值被外部篡改成非法
#: JSON）也返回 0，交给 processing TTL 自然过期兜底。
_IDEM_FAIL_LUA = """
local value = redis.call('GET', KEYS[1])
if not value then
  return 0
end
local ok, envelope = pcall(cjson.decode, value)
if not ok or type(envelope) ~= 'table' then
  return 0
end
if envelope['state'] == 'processing' and envelope['holder'] == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


def _now() -> float:
    """当前时间戳（秒，浮点）。

    独立成模块级函数是刻意的**时间接缝**：信封里的 ``started`` 时间戳
    取自它，测试用它 monkeypatch 冻结时间做确定性断言。
    """
    return time.time()


def _dump_envelope(envelope: Dict[str, Any]) -> str:
    """信封 dict -> 紧凑 JSON 字符串（separators 固定，Lua 侧按结构解析）。"""
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)


def _load_envelope(raw: Any) -> Optional[Dict[str, Any]]:
    """Redis GET 原值 -> 信封 dict；缺失/非法 JSON/非 dict 一律 None。"""
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    try:
        value = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return value if isinstance(value, dict) else None


def _ttl_seconds(ttl: float) -> int:
    """秒 -> EX 需要的整秒（向上取整，最小 1）。"""
    return max(1, math.ceil(float(ttl)))


class _IdempotencyGuard:
    """guard 返回的上下文管理器句柄（内部类，经 :meth:`IdempotentKey.guard` 获取）。

    生命周期：``__enter__`` 抢占 processing（失败按状态抛 Conflict/Done），
    临界区内调 :meth:`complete` 落 done + 结果；抛异常退出时 ``__exit__``
    自动 :meth:`fail`（DEL，允许后续重试）。**正常退出但未 complete**
    时键停留在 processing 态直至 TTL 过期（对齐 Stripe「忘记落结果则
    键悬挂至过期」的行为，务必在临界区内 complete）。
    """

    def __init__(
        self,
        idem: "IdempotentKey",
        key: str,
        processing_ttl: float,
    ) -> None:
        self._idem = idem
        self._key = key
        self._processing_ttl = processing_ttl
        self._entered = False
        self._completed = False

    @property
    def key(self) -> str:
        """业务 key（自动加 ``{prefix}:`` 前缀前的原值，只读）。"""
        return self._key

    def complete(self, result: Any = None, *, ttl: float = 86400.0) -> None:
        """落 done + 缓存结果（透传 :meth:`IdempotentKey.complete`）。"""
        self._idem.complete(self._key, result, ttl=ttl)
        self._completed = True

    def fail(self) -> bool:
        """手动失败清除（透传 :meth:`IdempotentKey.fail`）。"""
        return self._idem.fail(self._key)

    def __enter__(self) -> "_IdempotencyGuard":
        """抢占 processing；失败按状态抛 Conflict / Done（带缓存结果）。"""
        if not self._idem.begin(self._key, processing_ttl=self._processing_ttl):
            envelope = self._idem._read_envelope(self._key)
            if envelope is None:
                # SET NX 失败但 GET 不到：processing 键恰在两命令间过期——
                # 重试一次抢占，仍失败再按状态分流
                if self._idem.begin(
                    self._key, processing_ttl=self._processing_ttl
                ):
                    self._entered = True
                    return self
                envelope = self._idem._read_envelope(self._key)
            if envelope is not None and envelope.get("state") == "done":
                raise IdempotencyDoneError(
                    f"幂等键 {self._key!r} 已完成，请直接使用缓存结果"
                    f"（result 已挂在异常的 .result 属性上）",
                    result=envelope.get("result"),
                )
            raise IdempotencyConflictError(
                f"幂等键 {self._key!r} 正在被其他执行者处理中（processing），"
                f"拒绝并发重复执行"
            )
        self._entered = True
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """异常退出且未 complete → 自动 fail（DEL 允许重试）；异常照常传播。"""
        if exc_type is not None and not self._completed:
            self._idem.fail(self._key)
        self._entered = False

    def __repr__(self) -> str:
        """调试视图（不含 holder token）。"""
        return (
            f"{self.__class__.__name__}(key={self._key!r}, "
            f"active={self._entered!r})"
        )


class IdempotentKey:
    """Redis 幂等键（Stripe 两态模型：processing → done）。

    状态机（键值 JSON 信封，``{prefix}:{key}``）：

    .. code-block:: text

        (不存在) --begin(SET NX EX)--> processing --complete(SET EX)--> done
           ^                              |
           +-------- fail(compare-holder DEL，仅自己可拆) <+(异常退出)

    - **并发互斥**：processing 期间其他执行者 begin 失败（Conflict）；
    - **结果复用**：done 之后所有重复请求拿缓存结果（Done 带 ``.result``）；
    - **失败可重试**：临界区异常退出自动 fail（只删自己占的坑），下次
      请求重新开始。

    实例线程安全（内部锁保护 holder token 表）；跨实例/跨进程共用同一
    Redis 键即天然协调（token 是每次 begin 现生成的）。

    Args:
        client: redis 客户端实例（需支持 SET/GET/EVAL）。
        prefix: 键前缀（非空字符串，默认 ``"lizy:idem"``）。

    Raises:
        ValueError: prefix 为空 / 各方法 key 或 TTL 参数非法（中文消息）。

    Example:
        >>> from lizysdk.dist import IdempotentKey                         # doctest: +SKIP
        >>> idem = IdempotentKey(client)
        >>> with idem.guard("pay:order:123", processing_ttl=60) as g:
        ...     result = do_pay()                 # 临界区（至多一个执行者）
        ...     g.complete(result, ttl=86400)     # 落 done + 缓存结果
        >>> # 语义：
        >>> #   别的执行者 processing 中  -> IdempotencyConflictError
        >>> #   已 done                   -> IdempotencyDoneError（.result 带缓存）
        >>> #   临界区抛异常未 complete    -> guard 退出时自动 fail，允许重试
    """

    def __init__(self, client: Any, *, prefix: str = "lizy:idem") -> None:
        if not isinstance(prefix, str) or not prefix:
            raise ValueError(f"prefix 必须为非空字符串，当前为 {prefix!r}")
        self._client = client
        self._prefix = prefix
        #: 本实例各 key 的 holder token（begin 抢占成功时登记，fail 用）
        self._tokens: Dict[str, str] = {}
        self._tokens_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 元信息
    # ------------------------------------------------------------------

    @property
    def prefix(self) -> str:
        """键前缀（只读）。"""
        return self._prefix

    def _full_key(self, key: str) -> str:
        """业务 key -> 实际 Redis 键：``{prefix}:{key}``。"""
        return f"{self._prefix}:{key}"

    def _read_envelope(self, key: str) -> Optional[Dict[str, Any]]:
        """读键上的信封（缺失/非法为 None）。"""
        return _load_envelope(self._client.get(self._full_key(key)))

    # ------------------------------------------------------------------
    # 两态转移
    # ------------------------------------------------------------------

    def begin(self, key: str, *, processing_ttl: float = 60.0) -> bool:
        """抢占 processing：``SET {key} {信封} NX EX {processing_ttl}``。

        Args:
            key: 业务 key（自动加 ``{prefix}:`` 前缀）。
            processing_ttl: processing 态的兜底 TTL（秒，> 0）——执行者
                崩溃没来得及 fail/complete 时，键靠它自然过期解锁。

        Returns:
            bool: 抢占成功 True（信封 holder 为本次生成的 token，已登记
            到本实例）；键已存在（processing/done）返回 False。

        Raises:
            ValueError: key 为空 / processing_ttl <= 0 或类型非法（中文消息）。
        """
        self._validate_key(key)
        self._validate_ttl(processing_ttl, "processing_ttl")
        token = secrets.token_hex(8)
        envelope = {"state": "processing", "holder": token, "started": _now()}
        created = bool(
            self._client.set(
                self._full_key(key),
                _dump_envelope(envelope),
                nx=True,
                ex=_ttl_seconds(processing_ttl),
            )
        )
        if created:
            with self._tokens_lock:
                self._tokens[key] = token
        return created

    def complete(self, key: str, result: Any = None, *, ttl: float = 86400.0) -> None:
        """落 done + 缓存结果：``SET {key} {信封} EX {ttl}`` 覆写。

        Args:
            key: 业务 key。
            result: 要缓存的结果（必须可 JSON 序列化；dict/list/str/数字/
                None/bool 均可）。
            ttl: done 态缓存时长（秒，> 0，默认 86400 即 24 小时）。

        Raises:
            ValueError: key 为空 / ttl <= 0 / result 不可 JSON 序列化
                （中文消息）。

        Note:
            按 Stripe 模型 done 是终态：即使 processing 已过期被他人接管，
            complete 仍会覆写（重复执行方落的结果以后写者为准）。
        """
        self._validate_key(key)
        self._validate_ttl(ttl, "ttl")
        try:
            payload = _dump_envelope({"state": "done", "result": result})
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"result 必须可 JSON 序列化（当前类型 {type(result).__name__} "
                f"不支持）：{exc}"
            ) from exc
        self._client.set(self._full_key(key), payload, ex=_ttl_seconds(ttl))
        with self._tokens_lock:
            self._tokens.pop(key, None)

    def fail(self, key: str) -> bool:
        """失败清除：compare-holder Lua DEL（只删自己占的坑）。

        Args:
            key: 业务 key。

        Returns:
            bool: 清除成功 True；本实例未占用该键（从未 begin/已清除）、
            键已 done（终态不许拆）、或 holder 是他人（processing 过期被
            接管）均返回 False 且**不做任何删除**。

        Raises:
            ValueError: key 为空（中文消息）。
        """
        self._validate_key(key)
        with self._tokens_lock:
            token = self._tokens.get(key)
            if token is not None:
                self._tokens.pop(key, None)
        if token is None:
            return False
        return bool(
            self._client.eval(_IDEM_FAIL_LUA, 1, self._full_key(key), token)
        )

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get(self, key: str) -> Optional[Any]:
        """取缓存结果（仅 done 态有值）。

        Args:
            key: 业务 key。

        Returns:
            Optional[Any]: done 态返回缓存结果（注意 ``complete(key, None)``
            的合法结果就是 None，需配合 :meth:`is_done` 消歧）；processing
            中或键不存在返回 None。

        Raises:
            ValueError: key 为空（中文消息）。
        """
        self._validate_key(key)
        envelope = self._read_envelope(key)
        if envelope is not None and envelope.get("state") == "done":
            return envelope.get("result")
        return None

    def is_done(self, key: str) -> bool:
        """键是否已 done（终态判定，与 :meth:`get` 配合消歧 None 结果）。

        Args:
            key: 业务 key。

        Returns:
            bool: done 为 True；processing 中 / 键不存在为 False。

        Raises:
            ValueError: key 为空（中文消息）。
        """
        self._validate_key(key)
        envelope = self._read_envelope(key)
        return envelope is not None and envelope.get("state") == "done"

    # ------------------------------------------------------------------
    # guard 上下文管理器（核心 API）
    # ------------------------------------------------------------------

    def guard(self, key: str, *, processing_ttl: float = 60.0) -> _IdempotencyGuard:
        """幂等执行守卫：进入抢占 processing，异常退出自动 fail。

        Args:
            key: 业务 key。
            processing_ttl: processing 态兜底 TTL（秒，> 0，默认 60）。

        Returns:
            _IdempotencyGuard: 上下文管理器，``as g`` 拿句柄，临界区内
            ``g.complete(result, ttl=...)`` 落结果。

        Raises:
            ValueError: key 为空 / processing_ttl 非法（中文消息）。
            IdempotencyConflictError: 进入时键正被其他执行者 processing。
            IdempotencyDoneError: 进入时键已 done（``.result`` 带缓存结果）。

        Example:
            >>> with idem.guard("pay:order:123") as g:                    # doctest: +SKIP
            ...     result = do_pay()
            ...     g.complete(result, ttl=86400)
        """
        self._validate_key(key)
        self._validate_ttl(processing_ttl, "processing_ttl")
        return _IdempotencyGuard(self, key, processing_ttl)

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    @staticmethod
    def _validate_key(key: str) -> None:
        """key 必须为非空字符串。"""
        if not isinstance(key, str) or not key:
            raise ValueError(f"key 必须为非空字符串，当前为 {key!r}")

    @staticmethod
    def _validate_ttl(ttl: float, name: str) -> None:
        """TTL 参数必须为大于 0 的秒数。"""
        if (
            isinstance(ttl, bool)
            or not isinstance(ttl, (int, float))
            or ttl <= 0
        ):
            raise ValueError(f"{name} 必须为大于 0 的秒数，当前为 {ttl!r}")

    def __repr__(self) -> str:
        """调试视图（不回显客户端与 holder token）。"""
        return f"{self.__class__.__name__}(prefix={self._prefix!r})"
