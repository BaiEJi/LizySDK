"""worker_id 自动协商：基于本机锁文件占位的实例编号分配。

多实例部署（同一台机器起多个进程 / 容器）共享雪花算法的 10 bit
worker_id 空间（0 ~ 1023）时，人工逐实例分配既繁琐又容易冲突。本模块
提供 :func:`resolve_worker_id`，按以下优先级自动确定本进程的
worker_id：

1. **环境变量优先**：``LIZYSDK_WORKER_ID``（可自定义变量名）已设置时
   直接采用其整数值（校验 ``0 <= id <= 1023``），完全不触碰锁文件；
2. **锁文件占位协商**：从 ``default`` 起逐个探测 ``{lock_dir}/
   worker_{id}.json``，用 ``os.open(..., O_CREAT | O_EXCL | O_WRONLY)``
   原子创建占位（Windows / Linux 均为标准库语义，天然支持多进程互斥），
   被占用则 ``id + 1`` 顺延，超过 ``MAX_WORKER_ID`` 仍无空位则抛
   :class:`ValueError`；
3. **陈旧回收**：已存在锁文件的 mtime 距今超过 ``stale_after_seconds``
   （默认 86400 秒 = 1 天）视为残留（持有进程已死亡 / 机器异常重启），
   删除后重新占位；删除与创建之间的竞态由 ``O_EXCL`` 兜底——删除失败
   或被他人抢先则跳过该 id 继续探测。

刻意**只用文件 mtime 年龄判定陈旧**，不做「持有进程是否存活」探测：
在 Windows 上 ``os.kill(pid, 0)`` 会真实触发 ``TerminateProcess``，
属于危险操作，严禁使用；且 pid 复用会让探活本身不可靠。

生命周期：
- 成功占位后通过 :mod:`atexit` 注册释放（删除自己的锁文件），进程正常
  退出即自动归还槽位；异常退出留下的锁文件靠上述陈旧回收兜底；
- 同一进程重复调用 :func:`resolve_worker_id` 幂等：直接返回已占用的
  id，不再创建新的锁文件。

锁文件内容为 JSON：``{"pid": <进程号>, "created": <ISO8601 UTC 时间>}``，
便于运维排查「谁占了这个号」。仅依赖标准库，Python 3.9+。
"""

from __future__ import annotations

import atexit
import json
import os
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

from .snowflake import MAX_WORKER_ID

__all__ = ["resolve_worker_id"]

#: 默认锁目录：系统临时目录下的 ``lizysdk_workers``
DEFAULT_LOCK_DIR: Path = Path(tempfile.gettempdir()) / "lizysdk_workers"

#: 锁文件名模板：``worker_{id}.json``
_LOCK_FILE_TEMPLATE: str = "worker_{worker_id}.json"


class _WorkerLock:
    """本进程成功占用的 worker 槽位（进程内单例，见 ``_state``）。"""

    __slots__ = ("worker_id", "path")

    def __init__(self, worker_id: int, path: Path) -> None:
        self.worker_id = worker_id
        self.path = path


#: 进程内已占用槽位；None 表示尚未协商过
_state: _WorkerLock | None = None
#: 保护 _state 与协商过程的锁（保证同进程多线程调用也只占一个槽位）
_state_lock = threading.Lock()
#: atexit 释放函数是否已注册（避免重复注册）
_atexit_registered = False


def _silent_unlink(path: Path) -> None:
    """尽力删除锁文件，任何 OSError 都忽略。

    释放路径运行在进程退出（atexit）阶段，不允许抛错中断退出流程；
    删除失败（文件已不在 / 权限问题）时残留文件由陈旧回收兜底。
    """
    try:
        path.unlink()
    except OSError:
        pass


def _release_lock() -> None:
    """释放本进程占用的 worker 槽位（删除自己的锁文件）。

    幂等：状态已清空时是空操作，可安全重复调用。由 :mod:`atexit` 在
    进程退出时自动调用；测试亦可通过显式调用验证释放行为。
    """
    global _state
    with _state_lock:
        state = _state
        _state = None
    if state is not None:
        _silent_unlink(state.path)


def _claim_slot(
    directory: Path, start: int, stale_after_seconds: float
) -> _WorkerLock:
    """在 ``directory`` 下从 ``start`` 起原子占位一个 worker 槽位。

    目录不存在则自动创建；创建 / 写入失败（权限、路径被普通文件占据等）
    抛带中文说明的 :class:`OSError`，绝不静默换成「错误的 id」。

    Args:
        directory: 锁目录（已存在或可创建）。
        start: 起始探测 id（含），向上顺延至 ``MAX_WORKER_ID``（含）。
        stale_after_seconds: 锁文件 mtime 距今超过该秒数视为陈旧可回收。

    Returns:
        _WorkerLock: 成功占用的槽位（锁文件已写入 pid 与创建时间）。

    Raises:
        OSError: 锁目录无法创建，或锁文件无法创建 / 写入。
        ValueError: ``[start, MAX_WORKER_ID]`` 范围内全部槽位被占用。
    """
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise OSError(
            f"无法创建 worker_id 锁目录 {directory}，自动协商失败：{exc}"
        ) from exc

    payload = json.dumps(
        {"pid": os.getpid(), "created": datetime.now(timezone.utc).isoformat()}
    ).encode("utf-8")
    now = time.time()

    for worker_id in range(start, MAX_WORKER_ID + 1):
        path = directory / _LOCK_FILE_TEMPLATE.format(worker_id=worker_id)

        # --- 判断槽位是否可尝试占位（空闲 / 陈旧） ---
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError:
            pass  # 槽位空闲，直接进入占位
        except OSError:
            continue  # stat 失败（权限等）：保守视为被占用，跳过
        else:
            if now - mtime <= stale_after_seconds:
                continue  # 活跃占用，顺延到下一个 id
            # 陈旧锁：先删除再占位；删除失败（竞态 / 权限）则跳过该 id
            try:
                path.unlink()
            except FileNotFoundError:
                pass  # 已被其他进程回收，继续尝试占位
            except OSError:
                continue

        # --- 原子占位：O_CREAT | O_EXCL 保证多进程下只有一个赢家 ---
        try:
            fd = os.open(
                str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644
            )
        except FileExistsError:
            continue  # 与其他进程竞争失败，顺延
        except OSError as exc:
            raise OSError(
                f"无法在锁目录 {directory} 中创建锁文件 {path.name}，"
                f"自动协商失败：{exc}"
            ) from exc
        try:
            os.write(fd, payload)
        except OSError as exc:
            os.close(fd)
            _silent_unlink(path)  # 回收写坏的空壳文件，避免白白占位
            raise OSError(
                f"无法写入锁文件 {path}，自动协商失败：{exc}"
            ) from exc
        os.close(fd)
        return _WorkerLock(worker_id, path)

    raise ValueError(
        f"锁目录 {directory} 中 worker_id 槽位已全部被占用"
        f"（已探测 [{start}, {MAX_WORKER_ID}] 全部无空位），"
        f"无法自动协商 worker_id；请清理陈旧锁文件，"
        f"或通过环境变量显式指定 worker_id"
    )


def resolve_worker_id(
    default: int = 0,
    *,
    env_var: str = "LIZYSDK_WORKER_ID",
    lock_dir: str | Path | None = None,
    stale_after_seconds: float = 86400.0,
) -> int:
    """解析本进程的雪花 worker_id：环境变量优先，否则锁文件自动协商。

    语义（详见模块 docstring）：

    1. ``env_var`` 已设置（存在于环境变量中即视为设置，即使值为空串）
       时转 int 并校验 ``0 <= id <= MAX_WORKER_ID``，非法抛
       :class:`ValueError`（消息含变量名）；**显式指定时不做任何文件
       探测**，连锁目录都不会创建；
    2. 否则从 ``default`` 起在 ``{lock_dir}/worker_{id}.json`` 上原子
       占位（``O_CREAT | O_EXCL``），被占则顺延，全满抛
       :class:`ValueError`；
    3. mtime 距今超过 ``stale_after_seconds`` 的锁文件视为陈旧，删除后
       重占（竞态由 ``O_EXCL`` 与「删失败即跳过」兜底）；
    4. 成功占位后注册 atexit 释放；同进程重复调用幂等返回已占用的 id，
       不会泄漏多个锁文件。

    Args:
        default: 未显式指定时的起始探测 id，须在
            ``[0, MAX_WORKER_ID]`` 内。
        env_var: 优先读取的环境变量名。
        lock_dir: 锁目录；``None`` 时用系统临时目录下的
            ``lizysdk_workers``，不存在则自动创建。
        stale_after_seconds: 锁文件视为陈旧的 mtime 年龄阈值（秒）。

    Returns:
        int: 本进程可用的 worker_id，``0 <= id <= 1023``。

    Raises:
        ValueError: 环境变量值不是整数 / 越界（消息含变量名）；``default``
            非法；或锁文件槽位全部被占满。
        OSError: 锁目录无法创建 / 锁文件无法写入（消息为中文说明，
            绝不静默降级成错误的 id）。

    Example:
        >>> import os, tempfile
        >>> from pathlib import Path
        >>> os.environ.pop("LIZYSDK_WORKER_ID", None) is None or True
        True
        >>> directory = Path(tempfile.mkdtemp()) / "locks"
        >>> worker_id = resolve_worker_id(lock_dir=directory)
        >>> 0 <= worker_id <= 1023
        True
        >>> (directory / f"worker_{worker_id}.json").exists()
        True
        >>> resolve_worker_id(lock_dir=directory) == worker_id  # 幂等
        True
    """
    # --- 1. 环境变量优先：显式指定时不做文件探测 ---
    raw = os.environ.get(env_var)
    if raw is not None:
        try:
            worker_id = int(raw)
        except ValueError as exc:
            raise ValueError(
                f"环境变量 {env_var}={raw!r} 不是合法整数，"
                f"无法作为 worker_id"
            ) from exc
        if not 0 <= worker_id <= MAX_WORKER_ID:
            raise ValueError(
                f"环境变量 {env_var}={worker_id} 超出合法范围 "
                f"[0, {MAX_WORKER_ID}]，无法作为 worker_id"
            )
        return worker_id

    # --- 参数校验（bool 是 int 子类，须先排除） ---
    if isinstance(default, bool) or not isinstance(default, int):
        raise ValueError(
            f"default 必须为 int（bool 除外），当前类型为 "
            f"{type(default).__name__}"
        )
    if not 0 <= default <= MAX_WORKER_ID:
        raise ValueError(
            f"default 必须在 [0, {MAX_WORKER_ID}] 内，当前为 {default}"
        )

    directory = Path(lock_dir) if lock_dir is not None else DEFAULT_LOCK_DIR

    # --- 2/3/4. 锁文件协商（幂等：已占用槽位直接返回） ---
    global _state, _atexit_registered
    with _state_lock:
        if _state is not None:
            return _state.worker_id
        claimed = _claim_slot(directory, default, stale_after_seconds)
        _state = claimed
        if not _atexit_registered:
            atexit.register(_release_lock)
            _atexit_registered = True
        return claimed.worker_id
