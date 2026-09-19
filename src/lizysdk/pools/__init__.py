"""lizysdk.pools —— 统一并发池（线程池 / 协程池 / 进程池）。

初始化时选择类型，配置与行为语义一致；内置重试、背压（在途闸门）、
钩子、统计。纯标准库，核心零依赖不变（不依赖 lizysdk 其他子包）。

公开 API（均可从 ``lizysdk.pools`` 直接导入）：

- :func:`create_pool` —— 统一工厂：``kind = "thread" | "async" | "process"``
- :func:`run_all` —— 一次性临时池批量执行（保序返回）
- :class:`ThreadPool` / :class:`AsyncPool` / :class:`ProcessPool` —— 三种池
- :class:`Pool` —— 统一抽象基类（不可直接实例化）
- :class:`PoolError` / :class:`PoolClosedError` / :class:`PoolRejectedError`
  —— 自持异常族

快速上手::

    from lizysdk.pools import create_pool, run_all

    # 线程池：submit / map / stats / hooks / 上下文管理器
    with create_pool("thread", workers=8, name="io-pool",
                     hooks={"on_error": lambda info: ...}) as pool:
        fut = pool.submit(pow, 2, 10)      # -> concurrent.futures.Future
        pool.stats()                       # -> 运行统计快照

    # 协程池：async def 函数 + 真取消超时
    with create_pool("async", workers=32) as apool:
        fut = apool.submit(async_fetch, url)     # 任意线程
        await apool.asubmit(async_fetch, url)    # 已在事件循环内时

    # 进程池：可 pickle 的模块级函数 + worker 回收
    create_pool("process", max_tasks_per_worker=100)

    run_all([(fetch, (url1,), {}), (fetch, (url2,), {})], kind="thread")

三种池 ``fn`` 契约：thread = 普通函数；process = 可 pickle 的模块级函数；
async = ``async def`` 函数（或调用后返回 coroutine 的函数）。
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, List, Optional, Tuple, Union

from .async_pool import AsyncPool
from .base import Pool
from .exceptions import PoolClosedError, PoolError, PoolRejectedError
from .process_pool import ProcessPool
from .thread_pool import ThreadPool

__all__ = [
    "AsyncPool",
    "Pool",
    "PoolClosedError",
    "PoolError",
    "PoolRejectedError",
    "ProcessPool",
    "ThreadPool",
    "create_pool",
    "run_all",
]

#: 合法的池类型
_POOL_KINDS: Tuple[str, ...] = ("thread", "async", "process")

#: 单个任务的形态：可调用对象 / (fn, args) / (fn, args, kwargs)
_TaskSpec = Union[Callable[..., Any], Tuple[Any, ...]]


def create_pool(
    kind: str,
    *,
    name: Optional[str] = None,
    workers: Optional[int] = None,
    queue_size: int = 0,
    reject_policy: str = "raise",
    task_timeout: Optional[float] = None,
    max_retries: int = 0,
    retry_backoff: float = 0.0,
    initializer: Optional[Callable[..., Any]] = None,
    initargs: Tuple[Any, ...] = (),
    max_tasks_per_worker: Optional[int] = None,
    propagate_context: bool = True,
    thread_name_prefix: str = "lizysdk-pool",
    daemon: bool = True,
    hooks: Optional[dict] = None,
) -> Pool:
    """创建统一并发池（三种 kind 的配置面与行为语义一致）。

    Args:
        kind: ``"thread" | "async" | "process"``（非法值抛中文 ValueError）。
        name: 池名（默认 ``"pool-{kind}"``，进 stats 与钩子载荷）。
        workers: worker 数。thread 默认 ``min(32, cpu+4)``；async 默认
            ``32``（语义为**最大并发任务数**——令牌）；process 默认
            ``os.cpu_count()``。
        queue_size: 在途任务上限（0=无界）。>0 时超出部分走
            ``reject_policy``（在途 = 已接受 - 未终态，含正在执行的任务）。
        reject_policy: ``"raise"``（默认，抛 :class:`PoolRejectedError`）
            或 ``"block"``（阻塞等待空位；池关闭时抛 :class:`PoolClosedError`）。
        task_timeout: 单任务超时秒数。async 池为**真取消**
            （``asyncio.wait_for``）；thread/process 池为**等待语义**
            （Future 立即得 :class:`TimeoutError`，任务本体不中断——
            诚实声明见各池 docstring）。
        max_retries: 失败自动重试次数（thread 在 worker 内退避等待；
            process 在父进程侧重投、不保证同 worker）。
        retry_backoff: 指数退避基秒（``base * 2^(n-1)`` + 最多 10% 抖动）。
        initializer/initargs: worker 初始化（thread=每线程、async=loop
            线程一次、process=原生透传）；失败则池不可用，``submit`` 抛错。
        max_tasks_per_worker: **仅 process 池生效**（worker 回收，
            ``maxtasksperchild`` 语义）；thread/async 忽略（docstring 说明）。
        propagate_context: contextvars 传播开关（thread=提交时 copy_context
            + 任务内 ``ctx.run``；async=任务协程开始处注入；process 不适用，
            忽略——不可 pickle）。
        thread_name_prefix: 线程命名前缀（默认 ``"lizysdk-pool"``）。
        daemon: 工作线程 / loop 线程 daemon 位（默认 True；process 的
            子进程不受影响）。
        hooks: 初始钩子表，如 ``{"on_error": 回调}`` 或
            ``{"on_success": [回调1, 回调2]}``（八事件见 :class:`Pool`）。

    Returns:
        Pool: 对应类型的池实例（统一接口见 :class:`Pool`）。

    Raises:
        ValueError: kind 非法或任一配置不合法（中文消息）。

    Example:
        >>> from lizysdk.pools import create_pool
        >>> with create_pool("thread", workers=2) as pool:
        ...     pool.submit(pow, 2, 3).result()
        8
        >>> create_pool("greenlet")
        Traceback (most recent call last):
            ...
        ValueError: kind 必须为 'thread' / 'async' / 'process' 之一，当前为 'greenlet'
    """
    if kind == "thread":
        pool_cls: type = ThreadPool
    elif kind == "async":
        pool_cls = AsyncPool
    elif kind == "process":
        pool_cls = ProcessPool
    else:
        raise ValueError(
            f"kind 必须为 {' / '.join(repr(k) for k in _POOL_KINDS)} 之一，"
            f"当前为 {kind!r}"
        )
    return pool_cls(
        name=name,
        workers=workers,
        queue_size=queue_size,
        reject_policy=reject_policy,
        task_timeout=task_timeout,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        initializer=initializer,
        initargs=initargs,
        max_tasks_per_worker=max_tasks_per_worker,
        propagate_context=propagate_context,
        thread_name_prefix=thread_name_prefix,
        daemon=daemon,
        hooks=hooks,
    )


def _normalize_task(item: _TaskSpec, index: int) -> Tuple[Callable[..., Any], Tuple[Any, ...], dict]:
    """把单个任务归一化为 (fn, args, kwargs)；形态非法抛中文 ValueError。"""
    label = f"tasks[{index}]"
    if callable(item):
        return item, (), {}
    if isinstance(item, tuple):
        if len(item) == 2:
            fn, args = item
            kwargs: dict = {}
        elif len(item) == 3:
            fn, args, kwargs = item
        else:
            raise ValueError(
                f"{label} 元组长度必须为 2（fn, args）或 3（fn, args, kwargs），"
                f"当前长度为 {len(item)}"
            )
        if not callable(fn):
            raise ValueError(f"{label} 的 fn 必须可调用，当前为 {fn!r}")
        if not isinstance(args, (tuple, list)):
            raise ValueError(f"{label} 的 args 必须为 tuple/list，当前为 {args!r}")
        if not isinstance(kwargs, dict):
            raise ValueError(f"{label} 的 kwargs 必须为 dict，当前为 {kwargs!r}")
        return fn, tuple(args), dict(kwargs)
    raise ValueError(
        f"{label} 必须是可调用对象、(fn, args) 二元组或 (fn, args, kwargs) 三元组，"
        f"当前为 {item!r}"
    )


def run_all(
    tasks: Iterable[_TaskSpec],
    *,
    kind: str = "thread",
    return_exceptions: bool = False,
    workers: Optional[int] = None,
    **config: Any,
) -> List[Any]:
    """用一次性临时池批量执行任务，**保序**返回全部结果。

    Args:
        tasks: 任务序列，元素为 ``fn``、``(fn, args)`` 或
            ``(fn, args, kwargs)``（fn 契约随 ``kind``，见 :func:`create_pool`）。
        kind: 池类型（默认 ``"thread"``，同 :func:`create_pool`）。
        return_exceptions: ``True`` 时异常对象进入结果列表对应位置；
            ``False``（默认）时首个异常直接向调用方抛出（其余排队任务
            在池销毁时被取消）。
        workers: worker 数（``None`` 用该 kind 的默认值）。
        **config: 其余配置原样透传 :func:`create_pool`（如 ``hooks=``）。

    Returns:
        List[Any]: 与 ``tasks`` 同序的结果列表（或异常对象列表）。

    Raises:
        ValueError: kind / 任务形态 / 配置非法。

    Example:
        >>> from lizysdk.pools import run_all
        >>> run_all([(pow, (2, 3)), (pow, (2, 4))], workers=2)
        [8, 16]
        >>> run_all([(pow, (2, 'x',))], return_exceptions=True)[0].__class__.__name__
        'TypeError'
    """
    normalized = [_normalize_task(item, index) for index, item in enumerate(tasks)]
    with create_pool(kind, workers=workers, **config) as pool:
        futures = [pool.submit(fn, *args, **kwargs) for fn, args, kwargs in normalized]
        results: List[Any] = []
        for future in futures:
            if return_exceptions:
                try:
                    results.append(future.result())
                except Exception as exc:  # noqa: BLE001 按约定收进结果列表
                    results.append(exc)
            else:
                results.append(future.result())
        return results
