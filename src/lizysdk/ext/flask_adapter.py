"""Flask 适配器：把 ``AppError`` 统一转换为标准化 JSON HTTP 响应。

用法::

    from flask import Flask
    from lizysdk.ext.flask_adapter import install_flask_handler

    app = install_flask_handler(Flask(__name__))

    @app.route("/orders/<order_id>")
    def get_order(order_id):
        raise NotFoundError(params={"resource": order_id})

请求 ``/orders/no-such`` 时返回 ``404``，响应体与
``NotFoundError(params={"resource": "no-such"}).to_dict()`` 完全一致。

设计要点：

- 第三方 import（``flask``）放在 :func:`install_flask_handler` 函数体内，
  未安装 Flask 时导入本模块不报错，调用安装函数才抛 ``ImportError``
  （中文提示 ``pip install lizysdk[web]``）；
- 经 ``app.register_error_handler(AppError, handler)`` 注册，子类异常按
  MRO 查找同样被捕获：返回 ``(jsonify(err.to_dict()), err.http_status)``；
- ``include_generic=True`` 时额外为 ``Exception`` 注册兜底处理器：
  经 :func:`~lizysdk.errors.wrap` 包装后按 ``INTERNAL_ERROR`` / 500 返回。
"""

from __future__ import annotations

from typing import Any

from ..errors import AppError, wrap

__all__ = ["install_flask_handler"]

_INSTALL_HINT = (
    "Flask 未安装：web 适配器是 lizysdk 的可选依赖，"
    '请先安装：pip install "lizysdk[web]"'
)


def install_flask_handler(app: Any, *, include_generic: bool = False) -> Any:
    """为 Flask 应用注册 ``AppError`` 标准化异常处理器。

    示例：
        >>> from flask import Flask
        >>> from lizysdk.ext.flask_adapter import install_flask_handler
        >>> app = Flask(__name__)
        >>> install_flask_handler(app) is app  # 返回 app 本身，可链式
        True

    注册内容：

    - ``app.register_error_handler(AppError, handler)``：返回
      ``(jsonify(err.to_dict()), err.http_status)``；
    - ``include_generic=True`` 时再注册 ``Exception`` 处理器：
      ``wrap(exc)`` 包装后按 ``INTERNAL_ERROR`` / 500 返回
      （响应体含 ``details.original`` 等包装信息）。

    :param app: ``flask.Flask`` 应用实例
    :param include_generic: 是否额外注册未知异常的 500 兜底处理器
    :return: 传入的 ``app`` 本身（可链式书写）
    :raises ImportError: 未安装 Flask 时抛出，消息附安装命令
    """
    try:
        from flask import jsonify
    except ImportError as exc:  # pragma: no cover - 取决于环境是否安装 flask
        raise ImportError(_INSTALL_HINT) from exc

    def _handle_app_error(err: AppError) -> Any:
        """AppError 专属处理器：状态码与响应体均取自异常自身。"""
        return jsonify(err.to_dict()), err.http_status

    def _handle_generic(err: Exception) -> Any:
        """未知异常兜底：wrap 包装后按 INTERNAL_ERROR / 500 返回。"""
        wrapped = wrap(err)
        return jsonify(wrapped.to_dict()), wrapped.http_status

    app.register_error_handler(AppError, _handle_app_error)
    if include_generic:
        app.register_error_handler(Exception, _handle_generic)
    return app
