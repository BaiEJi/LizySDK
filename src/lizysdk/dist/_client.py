"""lizysdk.dist —— redis 客户端懒加载工厂。

懒加载红线（设计文档 §3）：**整个 ``lizysdk.dist`` 包在模块级一律不 import
redis**——``import lizysdk.dist`` 在未安装 redis 的环境必须成功；所有需要
redis 的路径都收敛到本模块，在**函数体内**导入，未安装时抛出带中文安装
提示的 :class:`ImportError`。

安装提示统一为（``lizysdk[redis]`` 可选依赖组）::

    pip install "lizysdk[redis]"

本模块自持：仅标准库 typing，不 import lizysdk 其他子包，也不在模块级
import redis。
"""

from __future__ import annotations

from typing import Any

__all__ = ["INSTALL_HINT", "client_from_url", "load_redis"]

#: 统一安装提示文案（ImportError 消息与文档引用同一常量）
INSTALL_HINT = 'pip install "lizysdk[redis]"'


def load_redis() -> Any:
    """在函数体内导入 redis 并返回模块对象（懒加载的唯一入口）。

    Returns:
        Any: ``redis`` 模块对象。注解用 ``Any`` 而非模块类型，正是因为
            不能在模块级（含注解求值处）引入 redis。

    Raises:
        ImportError: 当前环境未安装 redis。消息为中文并携带安装提示
            ``pip install "lizysdk[redis]"``，原始异常挂到 ``__cause__``。

    Example:
        >>> from lizysdk.dist._client import load_redis
        >>> load_redis().__name__   # 已安装 redis 的环境
        'redis'
        >>> import sys; _ = sys.modules.pop('redis', None)   # doctest: +SKIP
    """
    try:
        import redis  # noqa: PLC0415 懒加载红线：仅允许函数体内导入
    except ImportError as exc:
        raise ImportError(
            "lizysdk.dist 需要 redis 客户端，当前环境未安装。"
            f"请先执行：{INSTALL_HINT}"
        ) from exc
    return redis


def client_from_url(url: str, **kwargs: Any) -> Any:
    """懒加载构造 redis 客户端：``redis.Redis.from_url(url, **kwargs)``。

    仅供 :meth:`lizysdk.dist.SlidingWindowCounter.from_url` 等惰性入口使用；
    已有客户端实例的调用方请直接把实例传进构造器。构造**不会立即建连**
    （``from_url`` 语义：首个命令发出时才连接）。

    Args:
        url: 连接串，如 ``"redis://localhost:6379/0"``。
        **kwargs: 原样透传 ``redis.Redis.from_url``（如
            ``decode_responses=True``、``socket_timeout=1``）。

    Returns:
        Any: ``redis.Redis`` 客户端实例（未建连）。

    Raises:
        ImportError: 未安装 redis（中文提示，见 :func:`load_redis`）。
        ValueError: url 非法（由 redis-py 抛出）。

    Example:
        >>> from lizysdk.dist._client import client_from_url
        >>> client_from_url("redis://localhost:6379/0")   # doctest: +SKIP
        Redis<ConnectionPool<Connection<host=localhost,port=6379,db=0>>>
    """
    redis = load_redis()
    return redis.Redis.from_url(url, **kwargs)
