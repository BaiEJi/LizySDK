"""lizysdk.pools 异常族。

模块内**自持**（不 import lizysdk 其他子包，子包互不依赖红线）：

- :class:`PoolError` —— 池相关错误基类
- :class:`PoolClosedError` —— 池已关闭后再 ``submit``（混入
  :class:`RuntimeError`，对齐标准库 ``Executor`` 「关闭后提交抛
  RuntimeError」的语义）
- :class:`PoolRejectedError` —— 有界队列满且 ``reject_policy="raise"``
  时的同步拒绝（同样混入 :class:`RuntimeError`）

Example:
    >>> from lizysdk.pools import PoolClosedError, PoolError
    >>> issubclass(PoolClosedError, PoolError)
    True
    >>> issubclass(PoolClosedError, RuntimeError)
    True
"""

from __future__ import annotations

__all__ = ["PoolClosedError", "PoolError", "PoolRejectedError"]


class PoolError(Exception):
    """池相关错误的基类。

    设计文档 §6：模块内自持异常，不跨子包依赖。
    """


class PoolClosedError(PoolError, RuntimeError):
    """池已关闭后再次 :meth:`~lizysdk.pools.Pool.submit` 抛出。

    混入 :class:`RuntimeError` 是刻意的：标准库
    ``concurrent.futures.Executor`` 关闭后提交抛 ``RuntimeError``，
    依赖该语义的既有代码无需修改即可捕获。
    """


class PoolRejectedError(PoolError, RuntimeError):
    """有界在途闸门（``queue_size > 0``）已满且拒绝策略为 ``"raise"``。

    抛出发生在 ``submit()`` 调用现场（同步拒绝），同时计
    ``stats()["rejected"]`` 并触发 ``on_reject`` 钩子。
    """
