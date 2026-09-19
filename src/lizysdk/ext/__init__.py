"""lizysdk.ext —— 可选框架适配器子包（核心包零依赖不变）。

提供 :mod:`lizysdk.errors` 标准化错误体系与常见 Web 框架之间的粘合：

- :func:`install_fastapi_handler` —— FastAPI 的 AppError 异常处理器
- :func:`install_flask_handler`   —— Flask 的 AppError 异常处理器

零依赖红线（本 ``__init__`` 只 import 标准库与 :mod:`lizysdk.errors`）：

- ``import lizysdk.ext`` 不会连带安装 / 导入 fastapi、flask；
- 各适配器模块的第三方 import 全部放在安装函数**函数体内**：未装第三方库时
  ``import lizysdk.ext``（以及导入适配器子模块）本身不报错，仅在调用安装
  函数时抛 ``ImportError``，并附中文安装提示（``pip install "lizysdk[web]"``）。

使用示例::

    from lizysdk.ext import install_fastapi_handler

    app = install_fastapi_handler(fastapi_app)
"""

from __future__ import annotations

from .fastapi_adapter import install_fastapi_handler
from .flask_adapter import install_flask_handler

__all__ = ["install_fastapi_handler", "install_flask_handler"]
