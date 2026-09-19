r"""lizysdk.notify 通知中心：路由 / 静默期 / 频控 / 异步分发 / 统计。

设计契约 ``docs/notify-design.md`` §3/§4 的语义细则逐条落实：

- **级别**：``info | warning | error | critical``（与 logging 对齐），
  非法级别抛 :class:`ValueError`（中文）；
- **路由**：``notify(channels=None)`` 优先显式 ``channels``，其次按
  ``route(level, ...)`` 注册表解析，该级别无路由则发**全部渠道**；
  ``notify_to`` 指定渠道直发；
- **静默期**：本地时间 ``HH:MM`` 窗口（支持跨午夜，如 ``"22:00"`` ~
  ``"08:00"``），``except_levels`` 之外的级别被整体抑制（含异步入队），
  计入 ``quiet_suppressed``；
- **频控**：同 ``title`` 在窗口内第二次及以后被抑制（``except_levels``
  豁免），计入 ``cooldown_suppressed``；窗口内记录 LRU 上限 1024
  （防内存泄漏）；
- **异步模式**（默认）：单守护分发线程 + :class:`queue.Queue`，逐条按
  渠道扇出，**单渠道失败隔离**；失败重试 ``retry`` 次指数退避
  （``retry_backoff`` 基秒：``backoff * 1, * 2, * 4 ...``）；``atexit``
  钩子保底 flush + 停机；
- **同步模式**（``async_send=False``）：内联发送，返回
  ``{渠道名: 成功与否}``，**不抛发送异常**（失败进 stats 与
  ``last_error``）；参数非法仍抛 ValueError。

工程约定：静默期 / 频控判断在**入队前**（异步模式下也立即抑制）；
stats 计数全程持锁；时间统一走模块级
:func:`lizysdk.notify.channels._now` 接缝（测试 monkeypatch 冻结用）。
"""

from __future__ import annotations

import atexit
import itertools
import logging
import queue
import re
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .channels import _check_str, _now
from .exceptions import NotifyError

__all__ = ["NotifyCenter"]

#: 子包统一日志通道（对 lizysdk.logs 零 import 依赖，由根 handler 统一捕获）
logger = logging.getLogger("lizysdk.notify")

#: 合法级别（与 logging 对齐；顺序即严重度）
_LEVELS: Tuple[str, ...] = ("info", "warning", "error", "critical")
#: 频控窗口内同标题记录的 LRU 上限（防内存泄漏，设计文档 §3）
_COOLDOWN_LRU_LIMIT: int = 1024
#: flush 的轮询间隔（秒）
_FLUSH_POLL_INTERVAL: float = 0.005
#: 停机时等待分发线程退出的超时（尽力而为）
_SHUTDOWN_JOIN_TIMEOUT: float = 10.0
#: atexit 保底排空的超时
_ATEXIT_FLUSH_TIMEOUT: float = 10.0
#: 分发线程退出的哨兵对象（入队尾部，保证 FIFO 排空前不退出）
_SHUTDOWN = object()
#: 分发线程编号（命名唯一）
_SERIAL = itertools.count(1)
#: ``HH:MM`` 严格格式
_HHMM_RE = re.compile(r"^(\d{2}):(\d{2})$")


def _check_level(level: Any) -> str:
    """校验 ``level`` 为合法级别，返回原值。"""
    if not isinstance(level, str) or level not in _LEVELS:
        raise ValueError(f"level 必须为 {'/'.join(_LEVELS)} 之一，当前为 {level!r}")
    return level


def _validate_levels(value: Any, param: str) -> Tuple[str, ...]:
    """校验 except_levels 形参：级别序列（拒绝裸字符串），返回元组。"""
    if isinstance(value, str):
        raise ValueError(
            f"{param} 不能是单个字符串，请传级别列表/元组（如 ('critical',)），当前为 {value!r}"
        )
    try:
        levels = tuple(value)
    except TypeError:
        raise ValueError(
            f"{param} 必须为级别序列，当前类型为 {type(value).__name__}：{value!r}"
        ) from None
    for lv in levels:
        if not isinstance(lv, str) or lv not in _LEVELS:
            raise ValueError(
                f"{param} 含非法级别 {lv!r}，合法级别：{'/'.join(_LEVELS)}"
            )
    return levels


def _parse_hhmm(value: Any, name: str) -> int:
    """把 ``HH:MM`` 解析为当日分钟数（00:00 -> 0），非法抛 ValueError。"""
    if not isinstance(value, str):
        raise ValueError(f"{name} 必须为 HH:MM 格式的字符串，当前为 {value!r}")
    m = _HHMM_RE.match(value)
    if not m:
        raise ValueError(f"{name} 必须为 HH:MM 格式（如 \"22:00\"），当前为 {value!r}")
    hour, minute = int(m.group(1)), int(m.group(2))
    if hour > 23 or minute > 59:
        raise ValueError(f"{name} 非法时间：{value!r}（小时 00~23、分钟 00~59）")
    return hour * 60 + minute


class NotifyCenter:
    r"""多渠道通知中心：路由 / 静默期 / 频控 / 异步投递与重试 / 统计。

    渠道按**鸭子类型**注册——任何实现了 ``send(title, body, level)``
    的对象都可以（内置五渠道见 :mod:`lizysdk.notify.channels`）。

    Args:
        async_send: True（默认）异步投递——单守护分发线程 +
            :class:`queue.Queue`，``notify`` 入队即返回；False 同步内联
            发送，返回 ``{渠道名: 成功与否}`` 且不抛发送异常。
        retry: 单渠道失败后的重试次数（总尝试 = 1 + retry）。
        retry_backoff: 指数退避基秒（第 n 次重试前睡 ``backoff * 2**(n-1)``）。

    Raises:
        ValueError: 构造参数非法。

    Example:
        >>> from lizysdk.notify import NotifyCenter, WebhookChannel
        >>> center = NotifyCenter(async_send=False)
        >>> center.add_channel("hook", WebhookChannel("https://h.example.com/x",
        ...     transport=lambda u, p, h, t: (200, "ok")))
        >>> center.route("error", channels=["hook"])
        >>> center.notify("构建失败", "单测挂了", level="error")
        {'hook': True}
        >>> center.stats()["sent"]
        1
        >>> center.shutdown()   # 幂等，同步模式同样可调用
    """

    def __init__(
        self,
        *,
        async_send: bool = True,
        retry: int = 2,
        retry_backoff: float = 1.0,
    ) -> None:
        if not isinstance(async_send, bool):
            raise ValueError(f"async_send 必须为 bool，当前为 {async_send!r}")
        if isinstance(retry, bool) or not isinstance(retry, int):
            raise ValueError(f"retry 必须为非负整数，当前为 {retry!r}")
        if retry < 0:
            raise ValueError(f"retry 必须为非负整数，当前为 {retry}")
        if isinstance(retry_backoff, bool) or not isinstance(retry_backoff, (int, float)):
            raise ValueError(f"retry_backoff 必须为非负数秒，当前为 {retry_backoff!r}")
        if retry_backoff < 0:
            raise ValueError(f"retry_backoff 必须为非负数秒，当前为 {retry_backoff}")
        self._async_send = async_send
        self._retry = int(retry)
        self._retry_backoff = float(retry_backoff)
        # ---- 以下状态全部由 self._lock 保护 ----
        self._channels: Dict[str, Any] = {}
        self._routes: Dict[str, Tuple[str, ...]] = {}
        # 静默期：(start 分钟, end 分钟, except_levels)；None 未启用
        self._quiet: Optional[Tuple[int, int, Tuple[str, ...]]] = None
        # 频控：窗口秒数（None 未启用）与豁免级别
        self._cooldown_window: Optional[float] = None
        self._cooldown_except: Tuple[str, ...] = ()
        # 频控窗口内已发送标题 -> 派发时刻（LRU，上限 1024）
        self._cooldown_seen: "OrderedDict[str, float]" = OrderedDict()
        # 计数器（stats 的前五键 + last_error）
        self._counters: Dict[str, int] = {
            "sent": 0,
            "failed": 0,
            "retried": 0,
            "quiet_suppressed": 0,
            "cooldown_suppressed": 0,
        }
        self._last_error: Optional[str] = None
        self._channel_counters: Dict[str, Dict[str, int]] = {}
        self._lock = threading.Lock()
        # ---- 异步基础设施（worker 懒启动）----
        self._queue: "queue.Queue[Any]" = queue.Queue()
        self._worker: Optional[threading.Thread] = None
        self._closed = False
        if self._async_send:
            # 进程退出保底：排空在途通知（尽力）+ 停机
            atexit.register(self._atexit_flush)

    # ------------------------------------------------------------------ #
    # 注册与配置
    # ------------------------------------------------------------------ #

    def add_channel(self, name: str, channel: Any) -> None:
        """注册一个渠道（鸭子类型：须实现 ``send(title, body, level)``）。

        Args:
            name: 渠道名（非空 str，路由 / notify_to / stats 的键）。
            channel: 渠道对象（内置五渠道或自定义）。

        Raises:
            ValueError: name 为空 / 重复注册 / channel 无 send 方法。

        Example:
            >>> from lizysdk.notify import NotifyCenter, WeComChannel
            >>> center = NotifyCenter(async_send=False)
            >>> center.add_channel("wecom", WeComChannel("https://qyapi.weixin.qq.com/x",
            ...     transport=lambda u, p, h, t: (200, '{"errcode": 0}')))
        """
        _check_str(name, "name")
        if not callable(getattr(channel, "send", None)):
            raise ValueError(
                f"channel 必须实现 send(title, body, level) 方法（可调用），"
                f"当前类型为 {type(channel).__name__}：{channel!r}"
            )
        with self._lock:
            if name in self._channels:
                raise ValueError(f"渠道名 {name!r} 已注册，请勿重复添加")
            self._channels[name] = channel
            self._channel_counters[name] = {"sent": 0, "failed": 0}

    def route(self, level: str, *, channels: Sequence[str]) -> None:
        """注册级别路由：该级别的通知发往指定渠道集合。

        同级别重复调用覆盖旧路由；``notify`` 显式 ``channels`` 参数
        优先于路由；该级别无路由则发全部渠道。

        Args:
            level: 级别（``info|warning|error|critical``）。
            channels: 已注册渠道名列表/元组（非空）。

        Raises:
            ValueError: 级别非法 / channels 形状非法 / 含未注册渠道名。

        Example:
            >>> from lizysdk.notify import NotifyCenter
            >>> center = NotifyCenter(async_send=False)
            >>> center.route("error", channels=["ops", "mail"])   # doctest: +SKIP
        """
        _check_level(level)
        names = self._validate_channel_names(channels, "channels")
        with self._lock:
            for name in names:
                if name not in self._channels:
                    raise ValueError(
                        f"渠道 {name!r} 未注册，已注册渠道：{sorted(self._channels)}"
                    )
            self._routes[level] = tuple(names)

    def set_quiet_hours(
        self,
        start: str,
        end: str,
        *,
        except_levels: Sequence[str] = (),
    ) -> None:
        """设置静默期（本地时间 ``HH:MM`` 窗口，支持跨午夜）。

        窗口语义为**左闭右开**``[start, end)``；``start > end`` 视为跨
        午夜（如 ``"22:00"`` ~ ``"08:00"`` 表示 22:00 起至次日 08:00 前）。
        静默期内 ``except_levels`` 之外的级别整体抑制（发送动作整个
        跳过，含异步入队），计入 ``quiet_suppressed``。

        Args:
            start: 起始 ``HH:MM``（含）。
            end: 结束 ``HH:MM``（不含）。
            except_levels: 豁免级别（如 ``("critical",)``）。

        Raises:
            ValueError: 时间格式非法 / 起止相同 / 豁免级别非法。

        Example:
            >>> from lizysdk.notify import NotifyCenter
            >>> center = NotifyCenter(async_send=False)
            >>> center.set_quiet_hours("22:00", "08:00", except_levels=("critical",))
        """
        start_min = _parse_hhmm(start, "start")
        end_min = _parse_hhmm(end, "end")
        if start_min == end_min:
            raise ValueError(f"静默期起止时间不能相同（当前均为 {start!r}）")
        excepts = _validate_levels(except_levels, "except_levels")
        with self._lock:
            self._quiet = (start_min, end_min, excepts)

    def set_cooldown(self, window: float, *, except_levels: Sequence[str] = ()) -> None:
        """设置同标题频控：窗口内同 ``title`` 只投递第一次。

        判定基于**派发时刻**（不区分成败）；窗口内记录 LRU 上限 1024
        防内存泄漏。``except_levels`` 豁免的级别不受频控约束。

        Args:
            window: 窗口秒数（正数）。
            except_levels: 豁免级别（如 ``("critical",)``）。

        Raises:
            ValueError: window 非正数 / 豁免级别非法。

        Example:
            >>> from lizysdk.notify import NotifyCenter
            >>> center = NotifyCenter(async_send=False)
            >>> center.set_cooldown(600, except_levels=("critical",))
        """
        if isinstance(window, bool) or not isinstance(window, (int, float)):
            raise ValueError(
                f"window 必须为正数秒（int/float），"
                f"当前类型为 {type(window).__name__}：{window!r}"
            )
        if window <= 0:
            raise ValueError(f"window 必须为正数秒，当前为 {window}")
        excepts = _validate_levels(except_levels, "except_levels")
        with self._lock:
            self._cooldown_window = float(window)
            self._cooldown_except = excepts

    # ------------------------------------------------------------------ #
    # 发送入口
    # ------------------------------------------------------------------ #

    def notify(
        self,
        title: str,
        body: str,
        level: str = "info",
        *,
        channels: Optional[Sequence[str]] = None,
    ) -> Optional[Dict[str, bool]]:
        """发送一条通知（按路由扇出到多渠道）。

        渠道解析优先级：显式 ``channels`` > ``route(level, ...)`` 注册表
        > 全部已注册渠道。发送前依次做静默期 / 频控判定（在入队前，
        异步模式下也立即抑制）。

        Args:
            title: 通知标题（非空 str；频控按它去重）。
            body: 通知正文（str，允许空串）。
            level: 级别（``info|warning|error|critical``）。
            channels: 显式指定渠道名列表（覆盖路由）。

        Returns:
            Optional[Dict[str, bool]]: 异步模式恒 None（入队即返回）；
            同步模式返回 ``{渠道名: 是否送达}``（被静默期 / 频控抑制时
            返回空 dict）。

        Raises:
            ValueError: 参数非法（title/body/level/channels）。
            NotifyError: 通知中心已停机。

        Example:
            >>> from lizysdk.notify import NotifyCenter, DingTalkChannel
            >>> center = NotifyCenter(async_send=False)
            >>> center.add_channel("ops", DingTalkChannel("https://oapi.dingtalk.com/x",
            ...     transport=lambda u, p, h, t: (200, '{"errcode": 0}')))
            >>> center.notify("订单异常", "SO-001 扣减失败", level="error")
            {'ops': True}
        """
        targets = self._prepare(title, body, level, channels_param=channels, forced=None)
        return self._dispatch(targets, title, body, level)

    def notify_to(
        self,
        channel: str,
        title: str,
        body: str,
        level: str = "info",
    ) -> Optional[Dict[str, bool]]:
        """直发指定渠道（路由被忽略；静默期 / 频控仍生效）。

        Args:
            channel: 已注册渠道名。
            title: 通知标题（非空 str）。
            body: 通知正文（str）。
            level: 级别。

        Returns:
            Optional[Dict[str, bool]]: 同 :meth:`notify`。

        Raises:
            ValueError: 参数非法或渠道未注册。
            NotifyError: 通知中心已停机。

        Example:
            >>> from lizysdk.notify import NotifyCenter, FeishuChannel
            >>> center = NotifyCenter(async_send=False)
            >>> center.add_channel("fs", FeishuChannel("https://open.feishu.cn/x",
            ...     transport=lambda u, p, h, t: (200, '{"code": 0}')))
            >>> center.notify_to("fs", "标题", "内容")
            {'fs': True}
        """
        targets = self._prepare(title, body, level, channels_param=None, forced=channel)
        return self._dispatch(targets, title, body, level)

    # ------------------------------------------------------------------ #
    # 生命周期与统计
    # ------------------------------------------------------------------ #

    def flush(self, timeout: float = 10.0) -> bool:
        """排空在途通知（等待队列与正在分发的条目全部完成）。

        Args:
            timeout: 最长等待秒数。

        Returns:
            bool: True 已排空（或本就为空 / 同步模式无在途）；False 超时
            仍未排空（尽力而为，不抛异常）。

        Raises:
            ValueError: timeout 非正数。

        Example:
            >>> from lizysdk.notify import NotifyCenter
            >>> center = NotifyCenter(async_send=False)
            >>> center.flush()
            True
        """
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError(f"timeout 必须为正数秒，当前为 {timeout!r}")
        if not self._async_send:
            return True
        deadline = _now() + float(timeout)
        while True:
            # unfinished_tasks 在 item 入队时 +1、task_done 时 -1，
            # 为 0 即「队列空且在途条目已处理完」（等价 join 的语义）
            if self._queue.unfinished_tasks == 0:
                return True
            if _now() >= deadline:
                return False
            time.sleep(_FLUSH_POLL_INTERVAL)

    def shutdown(self) -> None:
        """停机：拒绝新通知，分发线程在排空前已入队条目后退出（幂等）。

        与 :meth:`flush` 的分工：flush 等待排空但不停机；shutdown 投递
        哨兵让 worker 排空队列后退出（等待上限 10 秒，尽力而为）。
        异步模式的 ``atexit`` 钩子会自动先 flush 再调用本方法。
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
        worker = self._worker
        if worker is not None and worker is not threading.current_thread() and worker.is_alive():
            self._queue.put(_SHUTDOWN)
            worker.join(timeout=_SHUTDOWN_JOIN_TIMEOUT)

    def stats(self) -> Dict[str, Any]:
        """返回统计快照（持锁复制，调用方可安全修改）。

        Returns:
            Dict[str, Any]: 键为 ``sent`` / ``failed`` / ``retried`` /
            ``quiet_suppressed`` / ``cooldown_suppressed`` /
            ``last_error``（最近一次失败描述，None 表示无）/
            ``channels``（``{渠道名: {"sent", "failed"}}``，含零计数渠道）。

        Note:
            ``last_error`` 记录**最近一次失败**（含重试中途的失败），
            不因后续成功而清除。

        Example:
            >>> from lizysdk.notify import NotifyCenter, WebhookChannel
            >>> center = NotifyCenter(async_send=False)
            >>> center.add_channel("hook", WebhookChannel("https://h.example.com/x",
            ...     transport=lambda u, p, h, t: (200, "ok")))
            >>> center.notify("标题", "内容")
            {'hook': True}
            >>> center.stats()["channels"]["hook"]
            {'sent': 1, 'failed': 0}
        """
        with self._lock:
            snapshot: Dict[str, Any] = dict(self._counters)
            snapshot["last_error"] = self._last_error
            snapshot["channels"] = {
                name: dict(counter) for name, counter in self._channel_counters.items()
            }
        return snapshot

    # ------------------------------------------------------------------ #
    # 内部实现
    # ------------------------------------------------------------------ #

    @staticmethod
    def _validate_channel_names(value: Any, param: str) -> List[str]:
        """校验渠道名序列形状（非空、元素非空 str；注册性由调用方查）。"""
        if isinstance(value, str):
            raise ValueError(f"{param} 不能是单个字符串，请传渠道名列表/元组，当前为 {value!r}")
        if not isinstance(value, (list, tuple)):
            raise ValueError(
                f"{param} 必须为渠道名列表/元组，"
                f"当前类型为 {type(value).__name__}：{value!r}"
            )
        names = list(value)
        if not names:
            raise ValueError(f"{param} 不能为空：至少提供一个已注册渠道名")
        for name in names:
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"{param} 中的渠道名必须为非空 str，当前为 {name!r}")
        return names

    def _prepare(
        self,
        title: str,
        body: str,
        level: str,
        channels_param: Optional[Sequence[str]],
        forced: Optional[str],
    ) -> Optional[List[str]]:
        """校验参数、判定静默期 / 频控（入队前）、解析目标渠道。

        Returns:
            Optional[List[str]]: 目标渠道名列表；None 表示被抑制。
        """
        _check_str(title, "title")
        if not isinstance(body, str):
            raise ValueError(
                f"body 必须为 str（允许空串），当前类型为 {type(body).__name__}：{body!r}"
            )
        _check_level(level)
        explicit: Optional[List[str]] = None
        if forced is not None:
            _check_str(forced, "channel")
            explicit = [forced]
        elif channels_param is not None:
            explicit = self._validate_channel_names(channels_param, "channels")
        with self._lock:
            if self._closed:
                raise NotifyError("通知中心已停机，不再接受新通知")
            if explicit is not None:
                for name in explicit:
                    if name not in self._channels:
                        raise ValueError(
                            f"渠道 {name!r} 未注册，已注册渠道：{sorted(self._channels)}"
                        )
            now = _now()
            # 1) 静默期（抑制则整个发送动作跳过，不记频控）
            if self._quiet is not None and level not in self._quiet[2]:
                if self._in_quiet_window_locked(now):
                    self._counters["quiet_suppressed"] += 1
                    logger.info("通知被静默期抑制：title=%r level=%s", title, level)
                    return None
            # 2) 频控（同 title 窗口内第二次及以后抑制；LRU 上限）
            if self._cooldown_window is not None and level not in self._cooldown_except:
                last = self._cooldown_seen.get(title)
                if last is not None and now - last < self._cooldown_window:
                    self._counters["cooldown_suppressed"] += 1
                    logger.info("通知被频控抑制：title=%r level=%s", title, level)
                    return None
                self._cooldown_seen[title] = now
                self._cooldown_seen.move_to_end(title)
                if len(self._cooldown_seen) > _COOLDOWN_LRU_LIMIT:
                    self._cooldown_seen.popitem(last=False)
            # 3) 解析目标：显式 > 路由 > 全渠道
            if explicit is not None:
                return list(explicit)
            routed = self._routes.get(level)
            if routed is not None:
                return list(routed)
            return list(self._channels.keys())

    def _in_quiet_window_locked(self, now: float) -> bool:
        """判定 ``now`` 是否落在静默期窗口内（调用方持锁且已启用静默期）。

        窗口左闭右开；``start > end`` 为跨午夜（如 22:00~08:00 表示
        ``[22:00, 24:00) ∪ [00:00, 08:00)``）。
        """
        start, end = self._quiet[0], self._quiet[1]  # type: ignore[index]
        lt = time.localtime(now)
        minutes = lt.tm_hour * 60 + lt.tm_min
        if start > end:
            return minutes >= start or minutes < end
        return start <= minutes < end

    def _dispatch(
        self,
        targets: Optional[List[str]],
        title: str,
        body: str,
        level: str,
    ) -> Optional[Dict[str, bool]]:
        """把已解析的通知投递出去：同步内联 / 异步入队。"""
        if targets is None or not targets:
            return {} if not self._async_send else None
        if not self._async_send:
            results: Dict[str, bool] = {}
            for name in targets:
                results[name] = self._send_with_retry(name, title, body, level)
            return results
        with self._lock:
            # _prepare 与此处之间可能已停机：入队前再查一次
            if self._closed:
                raise NotifyError("通知中心已停机，不再接受新通知")
            self._ensure_worker_locked()
            self._queue.put((title, body, level, tuple(targets)))
        return None

    def _ensure_worker_locked(self) -> None:
        """确保分发线程存活（懒启动；调用方持锁）。"""
        if self._worker is None or not self._worker.is_alive():
            self._worker = threading.Thread(
                target=self._worker_loop,
                name=f"lizysdk-notify-{next(_SERIAL)}",
                daemon=True,
            )
            self._worker.start()

    def _worker_loop(self) -> None:
        """分发线程主循环：逐条按渠道扇出，哨兵退出。"""
        while True:
            item = self._queue.get()
            try:
                if item is _SHUTDOWN:
                    break
                title, body, level, names = item
                for name in names:
                    try:
                        self._send_with_retry(name, title, body, level)
                    except Exception:  # 双保险：_send_with_retry 理论上不抛
                        logger.exception("渠道 %s 分发出现意外异常（已隔离）", name)
            except Exception:  # 双保险：保证循环永不被单条毒丸杀死
                logger.exception("通知分发条目处理异常（该条已跳过）")
            finally:
                self._queue.task_done()

    def _send_with_retry(
        self,
        name: str,
        title: str,
        body: str,
        level: str,
    ) -> bool:
        """向单个渠道发送，失败按配置指数退避重试；返回是否最终成功。

        **单渠道失败隔离**：本方法绝不向外抛异常（所有异常计入 stats
        与 ``last_error``），确保不影响同批其他渠道与后续通知。
        """
        channel = self._channels.get(name)
        if channel is None:  # 防御：理论上注册后不会消失
            with self._lock:
                self._counters["failed"] += 1
                self._channel_counters.setdefault(name, {"sent": 0, "failed": 0})
                self._channel_counters[name]["failed"] += 1
                self._last_error = f"[{name}] 渠道未注册（分发时已不存在）"
            return False
        attempts = self._retry + 1
        for attempt in range(attempts):
            if attempt > 0:
                delay = self._retry_backoff * (2 ** (attempt - 1))
                if delay > 0:
                    time.sleep(delay)
                with self._lock:
                    self._counters["retried"] += 1
            try:
                channel.send(title, body, level)
            except Exception as exc:  # ChannelError 及任意异常均隔离
                with self._lock:
                    self._last_error = f"[{name}] {exc}"
                logger.warning(
                    "channel=%s title=%r attempt=%d/%d 发送失败：%s",
                    name,
                    title,
                    attempt + 1,
                    attempts,
                    exc,
                )
            else:
                with self._lock:
                    self._counters["sent"] += 1
                    self._channel_counters[name]["sent"] += 1
                return True
        with self._lock:
            self._counters["failed"] += 1
            self._channel_counters[name]["failed"] += 1
        return False

    def _atexit_flush(self) -> None:
        """atexit 钩子：排空在途通知（尽力）+ 幂等停机；绝不抛异常。"""
        try:
            self.flush(timeout=_ATEXIT_FLUSH_TIMEOUT)
            self.shutdown()
        except Exception:  # 进程退出路径绝不抛
            logger.exception("atexit 排空通知中心失败（已忽略）")
