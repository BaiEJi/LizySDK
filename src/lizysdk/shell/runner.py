"""shell 命令执行薄加固层：:func:`run` / :class:`ShellResult` / 富错误族。

在 :func:`subprocess.run` 语义基座之上做四件事（设计文档
``docs/shell-dist-design.md`` §1.1/§2.1）：

1. **安全拆分与安全红线**：``argv`` 允许传 str，内部经
   :func:`shlex.split` 拆分为参数列表——因此**天然不支持** shell
   管道（``|``）、重定向（``>`` ``<``）、命令替换与通配符展开，
   这些符号只会作为字面参数传给子进程；``shell=`` 形参被刻意从
   API 中移除，底层**恒 ``shell=False``**，杜绝命令注入面。
2. **结构化执行日志**：每次执行经 ``logging.getLogger("lizysdk.shell")``
   输出 kv 形式的 INFO（``argv`` / ``returncode`` / ``elapsed_ms``），
   非零退出与超时输出 WARN——只走标准 logging 通道（对 lizysdk.logs
   **零 import 依赖**），由根 handler 统一格式捕获，与 uvicorn/httpx
   等第三方日志同机制。
3. **富错误**：``check=True`` 且非零退出抛 :class:`ShellError`、超时抛
   :class:`ShellTimeoutError`，均携带完整 argv / returncode / stdout /
   stderr / elapsed_ms 上下文与中文 ``__str__``（对齐 sh 库
   ErrorReturnCode 的错误完整性）。
4. **便捷结果**：:class:`ShellResult` 冻结 dataclass + ``ok`` 属性，
   耗时（毫秒）内置。

安全说明（务必阅读）：

- 本模块**永不**经由 shell 解释命令，任何元字符（``;`` ``&&`` ``|``
  ``>`` ``$(`` 等）都不会被展开，只会原样传给子进程；
- Windows 下 str 形式的 ``argv`` 经 posix 语义的 :func:`shlex.split`
  拆分，反斜杠会被当作转义符吞掉——含反斜杠的 Windows 路径请改传
  列表形式，或先把路径转为正斜杠（如 ``C:/path/python.exe``）；
- ``env`` 为**合并**语义（``{**os.environ, **env}``），调用方只需给增量。
"""

from __future__ import annotations

import logging
import os
import shlex
import subprocess
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

__all__ = ["ShellError", "ShellResult", "ShellTimeoutError", "run"]

#: 结构化执行日志通道：与 lizysdk 其他子包一致按 ``lizysdk.<子包>`` 命名，
#: 由根 handler 统一捕获（本模块对 lizysdk.logs 零 import 依赖）
logger = logging.getLogger("lizysdk.shell")

#: ``__str__`` 中 stderr / stdout 摘要的最大长度（空白折叠后按字符截断）
_BRIEF_LIMIT: int = 160


def _summarize(text: str, limit: int = _BRIEF_LIMIT) -> str:
    """折叠空白并截断到 ``limit`` 个字符，作为错误消息里的输出摘要。

    Args:
        text: 原始输出片段（可能含换行 / 大量空白）。
        limit: 摘要最大长度，超出部分以 ``…`` 结尾。

    Returns:
        str: 适合单行展示的摘要。
    """
    collapsed = " ".join(text.split())
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 1] + "…"


def _coerce_output(chunk: Union[None, str, bytes], encoding: str) -> Optional[str]:
    """把子进程输出统一为 str：bytes 按 ``encoding`` 解码（errors="replace"）。

    覆盖三类来源：文本模式下的 str、二进制模式 / 超时异常里携带的
    bytes、以及未捕获（``capture=False``）时的 None——分别对应返回
    解码结果、原样 str、None。

    Args:
        chunk: 子进程输出的原始片段。
        encoding: bytes 解码使用的编码名。

    Returns:
        Optional[str]: 解码后的输出；输入为 None 时返回 None。
    """
    if isinstance(chunk, bytes):
        return chunk.decode(encoding, errors="replace")
    return chunk


def _normalize_argv(argv: Union[str, Sequence[str]]) -> Tuple[str, ...]:
    """校验并把 ``argv`` 归一化为参数元组（str 形式经 shlex.split 拆分）。

    Args:
        argv: 命令行参数，str（将按 posix 语义拆分，不展开任何 shell
            元字符）或 str 序列（list/tuple）。

    Returns:
        Tuple[str, ...]: 逐元素的参数元组。

    Raises:
        ValueError: ``argv`` 为空 str / 空白 str（shlex 拆分后无 token）
            或空序列；str 含无法解析的引号。
        TypeError: ``argv`` 类型不是 str 也不是序列，或序列元素含非 str。
    """
    if isinstance(argv, str):
        try:
            tokens: List[str] = shlex.split(argv)
        except ValueError as exc:
            raise ValueError(
                f"argv 字符串包含无法解析的引号（shlex.split 失败：{exc}）：{argv!r}"
            ) from exc
    elif isinstance(argv, Sequence):
        tokens = list(argv)
    else:
        raise TypeError(
            f"argv 必须为 str 或 str 序列（list/tuple），"
            f"当前类型为 {type(argv).__name__}：{argv!r}"
        )
    for item in tokens:
        if not isinstance(item, str):
            raise TypeError(
                f"argv 的每个元素必须为 str，"
                f"发现 {type(item).__name__} 类型元素：{item!r}"
            )
    if not tokens:
        raise ValueError(
            "argv 不能为空：至少需要包含可执行命令名"
            "（str 形式经 shlex.split 拆分后没有任何参数）"
        )
    return tuple(tokens)


def _check_timeout(timeout: Optional[float]) -> None:
    """校验 ``timeout`` 形参：None 或正数秒（bool 除外）。"""
    if timeout is None:
        return
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
        raise TypeError(
            f"timeout 必须为正数秒（int/float），"
            f"当前类型为 {type(timeout).__name__}：{timeout!r}"
        )
    if timeout <= 0:
        raise ValueError(f"timeout 必须为正数秒，当前为 {timeout}")


@dataclass(frozen=True)
class ShellResult:
    """一次 shell 命令的执行结果（不可变值对象）。

    Attributes:
        argv: 实际执行的参数元组（str 形式已拆分、列表形式已转元组）。
        returncode: 子进程退出码（``subprocess`` 约定：被信号终止时为负，
            Windows 下超时被终止由 :class:`ShellTimeoutError` 表达而非退出码）。
        stdout: 标准输出文本；``capture=False`` 时为 None（未捕获）。
        stderr: 标准错误文本；``capture=False`` 时为 None（未捕获）。
        elapsed_ms: 执行耗时（毫秒，``time.perf_counter`` 差值）。
        ok: 只读属性，等价于 ``returncode == 0``。

    Note:
        冻结 dataclass（``frozen=True``）：结果是不可变快照，任何字段
        赋值都会抛 :class:`dataclasses.FrozenInstanceError`，可安全跨
        线程 / 跨日志传递。

    Example:
        >>> import sys
        >>> from lizysdk.shell import run
        >>> res = run([sys.executable, "-c", "print('hi')"])
        >>> res.ok
        True
        >>> res.stdout.strip()
        'hi'
    """

    argv: Tuple[str, ...]
    returncode: int
    stdout: Optional[str]
    stderr: Optional[str]
    elapsed_ms: float

    @property
    def ok(self) -> bool:
        """命令是否成功（退出码为 0）。"""
        return self.returncode == 0


class ShellError(Exception):
    """shell 命令执行失败的富错误（对齐 sh 库 ErrorReturnCode 的上下文完整性）。

    由 :func:`run` 在 ``check=True`` 且非零退出时抛出；超时场景抛其子类
    :class:`ShellTimeoutError`。除异常消息外，完整保留执行上下文供
    调用方程序化处理（无需解析消息文本）。

    Attributes:
        argv: 实际执行的参数元组。
        returncode: 退出码；超时场景（子类）下进程被强制终止、退出码
            不可知，为 None。
        stdout: 已捕获的标准输出（未捕获或无输出时为 None / 空串）。
        stderr: 已捕获的标准错误。
        elapsed_ms: 到失败 / 超时为止的耗时（毫秒）。

    Example:
        >>> from lizysdk.shell import ShellError
        >>> exc = ShellError(["git", "status"], returncode=128,
        ...                  stdout="", stderr="fatal: not a git repository",
        ...                  elapsed_ms=12.3)
        >>> "退出码 128" in str(exc)
        True
    """

    def __init__(
        self,
        argv: Sequence[str],
        *,
        returncode: Optional[int] = None,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        elapsed_ms: float = 0.0,
    ) -> None:
        self.argv = tuple(argv)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.elapsed_ms = elapsed_ms
        super().__init__(self._describe())

    @property
    def _cmdline(self) -> str:
        """以空格连接的命令行展示形式（仅供消息展示，非重新执行依据）。"""
        return " ".join(self.argv)

    def _describe(self) -> str:
        """组装中文错误消息：命令、退出码、stderr 摘要、耗时。"""
        parts = [f"命令执行失败（退出码 {self.returncode}）：{self._cmdline}"]
        if self.stderr:
            parts.append(f"stderr 摘要：{_summarize(self.stderr)}")
        parts.append(f"耗时 {self.elapsed_ms:.1f}ms")
        return "；".join(parts)

    def __str__(self) -> str:
        """中文错误描述（命令 / 退出码 / stderr 摘要 / 耗时）。"""
        return self.args[0] if self.args else self._describe()


class ShellTimeoutError(ShellError):
    """shell 命令执行超时（子进程已被强制终止）的富错误。

    :class:`ShellError` 的子类：``returncode`` 恒为 None（进程被终止、
    退出码不可知），额外携带 ``timeout``（触发的超时上限，秒）与超时
    前已捕获的部分输出。

    终止语义：超时由 :func:`subprocess.run` 的 ``timeout`` 机制处理，
    其内部在超时后立即 kill 子进程并收割（Windows 为
    ``TerminateProcess`` 立即终止；POSIX 为 ``SIGKILL``），随后从
    ``TimeoutExpired`` 上携带已累积的输出构造本异常。

    Attributes:
        timeout: 触发超时的上限（秒），即调用方传入的 ``timeout``。
        argv / stdout / stderr / elapsed_ms: 见 :class:`ShellError`。

    Example:
        >>> from lizysdk.shell import ShellTimeoutError
        >>> exc = ShellTimeoutError(["sleep", "5"], timeout=0.5,
        ...                         stdout="", stderr="", elapsed_ms=520.0)
        >>> "超时" in str(exc) and "0.5" in str(exc)
        True
    """

    def __init__(
        self,
        argv: Sequence[str],
        *,
        timeout: float,
        stdout: Optional[str] = None,
        stderr: Optional[str] = None,
        elapsed_ms: float = 0.0,
    ) -> None:
        self.timeout = timeout
        super().__init__(
            argv,
            returncode=None,
            stdout=stdout,
            stderr=stderr,
            elapsed_ms=elapsed_ms,
        )

    def _describe(self) -> str:
        """组装中文错误消息：命令、超时上限、已捕获输出摘要、耗时。"""
        parts = [
            f"命令执行超时（上限 {self.timeout}s，子进程已强制终止）：{self._cmdline}"
        ]
        if self.stdout:
            parts.append(f"已捕获 stdout：{_summarize(self.stdout)}")
        if self.stderr:
            parts.append(f"已捕获 stderr：{_summarize(self.stderr)}")
        parts.append(f"耗时 {self.elapsed_ms:.1f}ms")
        return "；".join(parts)


def run(
    argv: Union[str, Sequence[str]],
    *,
    timeout: Optional[float] = None,
    check: bool = False,
    capture: bool = True,
    input: Optional[Union[str, bytes]] = None,
    env: Optional[Mapping[str, str]] = None,
    cwd: Optional[Union[str, "os.PathLike[str]"]] = None,
    encoding: str = "utf-8",
    log: bool = True,
) -> ShellResult:
    """执行一条 shell 命令并返回 :class:`ShellResult`（恒 ``shell=False``）。

    :func:`subprocess.run` 之上的薄加固层：str ``argv`` 安全拆分、
    超时转 :class:`ShellTimeoutError`、``check=True`` 非零退出转
    :class:`ShellError`、自动输出结构化执行日志。**安全红线**：底层
    恒 ``shell=False`` 且本 API 不暴露 ``shell`` 开关——str 形式的
    ``argv`` 经 :func:`shlex.split` 拆分，管道（``|``）、重定向
    （``>`` ``<``）、命令替换、通配符展开等 shell 元字符**一律不被
    解释**，只会作为字面参数传给子进程。

    Args:
        argv: 命令与参数。str 形式经 posix 语义的 :func:`shlex.split`
            拆分（注意：Windows 反斜杠路径会被当作转义符吞掉，含反斜杠
            的路径请传列表形式或改用正斜杠）；list/tuple 形式要求每个
            元素都是 str。空 str / 空白 str / 空序列抛 ValueError。
        timeout: 超时上限（秒，正数）。超时后子进程被立即强制终止
            （Windows 为 TerminateProcess，POSIX 为 SIGKILL），抛
            :class:`ShellTimeoutError`（携带已捕获的部分输出）。
        check: 为 True 且退出码非 0 时抛 :class:`ShellError`；为
            False 时非零退出仅体现在 ``ShellResult.ok`` 为 False。
        capture: 为 True（默认）捕获 stdout / stderr 为 str（按
            ``encoding`` 解码、``errors="replace"`` 容错）；为 False
            继承调用方终端，结果中两字段为 None。
        input: 传给子进程 stdin 的内容（str 或 bytes）。str 走文本
            管道；bytes 走二进制管道（输出再按 ``encoding`` 解码）。
        env: 环境变量**增量**：与 ``os.environ`` 合并（
            ``{**os.environ, **env}``），同名键以 ``env`` 为准；
            None 表示直接继承当前进程环境。
        cwd: 子进程工作目录，None 表示继承当前目录。
        encoding: 输入 / 输出的文本编码，默认 "utf-8"。
        log: 为 True（默认）时经 ``lizysdk.shell`` logger 输出执行
            日志：成功 INFO、非零退出与超时 WARN，消息为 kv 形式
            （``argv=`` / ``returncode=`` / ``elapsed_ms=`` 等），便于
            根 handler 做结构化捕获。

    Returns:
        ShellResult: 冻结结果对象（argv / returncode / stdout / stderr /
        elapsed_ms / ok）。

    Raises:
        ValueError: ``argv`` 为空（str / 空白 / 空序列）或含无法解析的
            引号；``timeout`` 非 正数。
        TypeError: ``argv`` / 其元素 / ``env`` / ``timeout`` 类型非法。
        ShellTimeoutError: 超时（无论 ``check`` 取值）。
        ShellError: ``check=True`` 且退出码非 0。
        FileNotFoundError / PermissionError: 可执行文件不存在 / 不可
            执行时由 :mod:`subprocess` 原样透传（属环境错误，不属于
            命令执行失败，不包装为 ShellError）。

    Note:
        不支持（也不打算支持）shell 管道 / 重定向 / 通配符——需要管道
        编排请用 :mod:`subprocess` 的 Popen 组合或 plumbum 类库；本
        模块定位是安全、可观测的单命令执行。

    Example:
        >>> import sys
        >>> from lizysdk.shell import run
        >>> res = run([sys.executable, "-c", "print('hi')"])
        >>> res.ok, res.returncode, res.stdout.strip()
        (True, 0, 'hi')
        >>> run(f'"{sys.executable.replace(chr(92), chr(47))}" -c pass')  # doctest: +SKIP
        >>> run(["definitely-not-exists"], check=False).ok  # doctest: +SKIP
        Traceback (most recent call last):
            ...
        FileNotFoundError: ...
    """
    argv_norm = _normalize_argv(argv)
    _check_timeout(timeout)

    if env is None:
        merged_env: Optional[Dict[str, str]] = None
    elif isinstance(env, Mapping):
        # 合并语义：os.environ 打底，调用方增量覆盖同名键
        merged_env = {**os.environ, **env}
    else:
        raise TypeError(
            f"env 必须为 str 到 str 的映射（Mapping），"
            f"当前类型为 {type(env).__name__}：{env!r}"
        )

    # bytes 的 input 必须走二进制管道（文本模式无法写入 bytes），
    # 此时输出为 bytes、由本层按 encoding 手动解码；其余情况交给
    # subprocess 文本模式统一解码（errors="replace" 容错）
    if isinstance(input, bytes):
        text_kwargs: Dict[str, Any] = {}
    else:
        text_kwargs = {"encoding": encoding, "errors": "replace"}

    start = time.perf_counter()
    try:
        completed = subprocess.run(
            argv_norm,
            shell=False,  # 安全红线：恒 False，本 API 不提供 shell 开关
            input=input,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            timeout=timeout,
            check=False,  # 富错误由本层统一抛 ShellError，不用 CalledProcessError
            env=merged_env,
            cwd=cwd,
            **text_kwargs,
        )
    except subprocess.TimeoutExpired as exc:
        # subprocess.run 的超时路径已 kill 子进程并收割（Windows 上还会
        # 把已累积输出回填到异常对象），这里只做富错误转译
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        partial_stdout = _coerce_output(exc.stdout, encoding)
        partial_stderr = _coerce_output(exc.stderr, encoding)
        if log:
            logger.warning(
                "argv=%r timeout=%.3f elapsed_ms=%.1f",
                list(argv_norm),
                float(timeout),
                elapsed_ms,
            )
        raise ShellTimeoutError(
            argv_norm,
            timeout=float(timeout),
            stdout=partial_stdout,
            stderr=partial_stderr,
            elapsed_ms=elapsed_ms,
        ) from exc

    elapsed_ms = (time.perf_counter() - start) * 1000.0
    stdout = _coerce_output(completed.stdout, encoding)
    stderr = _coerce_output(completed.stderr, encoding)

    if log:
        if completed.returncode == 0:
            logger.info(
                "argv=%r returncode=%d elapsed_ms=%.1f",
                list(argv_norm),
                completed.returncode,
                elapsed_ms,
            )
        else:
            logger.warning(
                "argv=%r returncode=%d elapsed_ms=%.1f",
                list(argv_norm),
                completed.returncode,
                elapsed_ms,
            )

    if check and completed.returncode != 0:
        raise ShellError(
            argv_norm,
            returncode=completed.returncode,
            stdout=stdout,
            stderr=stderr,
            elapsed_ms=elapsed_ms,
        )
    return ShellResult(
        argv=argv_norm,
        returncode=completed.returncode,
        stdout=stdout,
        stderr=stderr,
        elapsed_ms=elapsed_ms,
    )
