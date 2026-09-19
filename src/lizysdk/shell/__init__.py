"""lizysdk.shell —— shell 执行包装子模块（零依赖、纯标准库）。

公开 API（均可从 ``lizysdk.shell`` 直接导入）：

- :func:`run` —— :func:`subprocess.run` 之上的薄加固层：恒
  ``shell=False``（安全红线，API 不暴露 shell 开关）、str 形式
  argv 经 ``shlex.split`` 安全拆分（不支持管道 / 重定向 / 通配符
  展开）、超时转 :class:`ShellTimeoutError`、``check=True`` 非零
  退出转 :class:`ShellError`、自动输出结构化执行日志
  （``lizysdk.shell`` logger，INFO 成功 / WARN 失败，kv 形式）
- :class:`ShellResult` —— 冻结 dataclass 执行结果
  （argv / returncode / stdout / stderr / elapsed_ms + ``ok`` 属性）
- :class:`ShellError` —— 非零退出的富错误（携带完整执行上下文，
  中文 ``__str__`` 含命令 / 退出码 / stderr 摘要）
- :class:`ShellTimeoutError` —— 超时富错误（:class:`ShellError`
  子类，额外携带 ``timeout`` 上限与已捕获的部分输出）

本子包对其他 lizysdk 子包零 import 依赖（日志只走标准 logging 通道，
由 lizysdk.logs 的根 handler 统一格式捕获）；实现见
:mod:`lizysdk.shell.runner`。
"""

from __future__ import annotations

from .runner import ShellError, ShellResult, ShellTimeoutError, run

__all__ = [
    "ShellError",
    "ShellResult",
    "ShellTimeoutError",
    "run",
]
