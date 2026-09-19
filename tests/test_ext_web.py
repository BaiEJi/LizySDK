"""lizysdk.ext 框架适配器（FastAPI / Flask）的单元测试。

未安装 web 可选依赖的环境自动整模块跳过（importorskip）；
本环境（fastapi / flask / httpx 已装）覆盖：

- AppError（含子类）抛出 -> 响应状态码 = http_status，JSON body 与
  to_dict() 逐键一致；
- include_generic=True 时未知异常 -> 500 且 code = INTERNAL_ERROR；
- 正常路由不受适配器影响；
- install_* 返回 app 本身（可链式）；
- ext 子包与适配器模块的懒加载契约（模块级不引入第三方）。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("flask")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from flask import Flask  # noqa: E402

from lizysdk.errors import AppError, NotFoundError, ParamError, wrap  # noqa: E402
from lizysdk.ext.fastapi_adapter import install_fastapi_handler  # noqa: E402
from lizysdk.ext.flask_adapter import install_flask_handler  # noqa: E402


def assert_body_matches_to_dict(body: dict, expected: dict) -> None:
    """逐键断言响应 JSON 与 err.to_dict() 完全一致。

    timestamp 由异常实例生成时刻决定，此处只要求是非空字符串。
    """
    assert set(body) == set(expected)
    for key, value in expected.items():
        if key == "timestamp":
            assert isinstance(body[key], str) and body[key]
        else:
            assert body[key] == value, f"键 {key!r} 不一致: {body[key]!r} != {value!r}"


# ---------------------------------------------------------------------------
# 1. 懒加载契约
# ---------------------------------------------------------------------------


def test_ext_import_does_not_pull_third_party() -> None:
    """import lizysdk.ext 不引入 fastapi / flask（核心零依赖不变）。"""
    src = Path(__file__).resolve().parents[1] / "src"
    code = (
        "import sys; import lizysdk.ext; "
        "assert 'fastapi' not in sys.modules, 'fastapi 被连带导入'; "
        "assert 'flask' not in sys.modules, 'flask 被连带导入'; "
        "import lizysdk.ext.fastapi_adapter, lizysdk.ext.flask_adapter; "
        "print('lazy-ok')"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        env={**os.environ, "PYTHONPATH": str(src)},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "lazy-ok" in result.stdout


def test_install_fastapi_without_fastapi_raises_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """未装 fastapi 时调用安装函数：ImportError 且含中文安装提示。"""
    # 同时阻断 fastapi 与已缓存的子模块，模拟「未安装 fastapi」的环境
    monkeypatch.setitem(sys.modules, "fastapi", None)
    monkeypatch.setitem(sys.modules, "fastapi.responses", None)
    with pytest.raises(ImportError, match=r"pip install .lizysdk\[web\]."):
        install_fastapi_handler(FastAPI())


def test_install_flask_without_flask_raises_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    """未装 flask 时调用安装函数：ImportError 且含中文安装提示。"""
    monkeypatch.setitem(sys.modules, "flask", None)  # 阻断 import flask
    with pytest.raises(ImportError, match=r"pip install .lizysdk\[web\]."):
        install_flask_handler(Flask(__name__))


# ---------------------------------------------------------------------------
# 2. FastAPI 适配器
# ---------------------------------------------------------------------------


def test_fastapi_app_error_response() -> None:
    """AppError 子类抛出：状态码 = http_status，body 与 to_dict 逐键一致。"""
    app = FastAPI()
    assert install_fastapi_handler(app) is app

    err = NotFoundError(params={"resource": "order-1"}, details={"trace_id": "t-1"})

    @app.get("/missing")
    def missing() -> None:
        raise err

    client = TestClient(app)
    resp = client.get("/missing")
    assert resp.status_code == 404
    assert resp.headers["content-type"].startswith("application/json")
    assert_body_matches_to_dict(resp.json(), err.to_dict())


def test_fastapi_base_app_error_and_param_error() -> None:
    """AppError 本体与另一子类同样被捕获（MRO 查找）。"""
    app = install_fastapi_handler(FastAPI())

    @app.get("/bad-param")
    def bad_param() -> None:
        raise ParamError(params={"param": "page"})

    @app.get("/plain")
    def plain() -> None:
        raise AppError("SOME_CUSTOM_CODE", "自定义消息", details={"k": "v"})

    client = TestClient(app)
    resp = client.get("/bad-param")
    assert resp.status_code == 400
    assert resp.json()["code"] == "PARAM_INVALID"
    assert resp.json()["message"] == "参数无效: page"

    resp2 = client.get("/plain")
    assert resp2.status_code == 500
    assert resp2.json()["code"] == "SOME_CUSTOM_CODE"
    assert resp2.json()["message"] == "自定义消息"
    assert resp2.json()["details"] == {"k": "v"}


def test_fastapi_include_generic_unknown_exception() -> None:
    """include_generic=True：未知异常 -> 500 且 code = INTERNAL_ERROR。"""
    app = install_fastapi_handler(FastAPI(), include_generic=True)

    @app.get("/boom")
    def boom() -> None:
        raise RuntimeError("unexpected")

    # Starlette 兜底处理器返回响应后仍会向服务端重抛原异常，测试需关闭
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/boom")
    assert resp.status_code == 500
    body = resp.json()
    assert body["code"] == "INTERNAL_ERROR"
    assert body["http_status"] == 500
    assert body["details"]["original"] == "unexpected"
    assert body["details"]["original_type"] == "RuntimeError"
    assert_body_matches_to_dict(body, wrap(RuntimeError("unexpected")).to_dict())


def test_fastapi_generic_disabled_default() -> None:
    """include_generic 缺省 False：未注册 Exception 兜底（客户端侧直接抛出）。"""
    app = install_fastapi_handler(FastAPI())

    @app.get("/boom")
    def boom() -> None:
        raise RuntimeError("no handler")

    client = TestClient(app)
    with pytest.raises(RuntimeError, match="no handler"):
        client.get("/boom")


def test_fastapi_normal_route_unaffected() -> None:
    """正常路由不受适配器影响：200 且内容原样返回。"""
    app = install_fastapi_handler(FastAPI(), include_generic=True)

    @app.get("/ok")
    def ok() -> dict:
        return {"status": "ok", "items": [1, 2, 3]}

    client = TestClient(app)
    resp = client.get("/ok")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "items": [1, 2, 3]}


def test_fastapi_returns_app_chainable() -> None:
    """install_fastapi_handler 返回传入的 app 本身（可链式）。"""
    app = FastAPI()
    assert install_fastapi_handler(app) is app

    @app.get("/chain")
    def chain() -> str:
        return "chain"

    client = TestClient(install_fastapi_handler(app))
    assert client.get("/chain").status_code == 200


# ---------------------------------------------------------------------------
# 3. Flask 适配器
# ---------------------------------------------------------------------------


def test_flask_app_error_response() -> None:
    """AppError 子类抛出：状态码 = http_status，body 与 to_dict 逐键一致。"""
    app = Flask(__name__)
    assert install_flask_handler(app) is app

    err = NotFoundError(params={"resource": "order-1"}, details={"trace_id": "t-1"})

    @app.route("/missing")
    def missing() -> None:
        raise err

    client = app.test_client()
    resp = client.get("/missing")
    assert resp.status_code == 404
    assert resp.content_type.startswith("application/json")
    assert_body_matches_to_dict(resp.get_json(), err.to_dict())


def test_flask_base_app_error_and_param_error() -> None:
    """AppError 本体与另一子类同样被捕获（MRO 查找）。"""
    app = install_flask_handler(Flask(__name__))

    @app.route("/bad-param")
    def bad_param() -> None:
        raise ParamError(params={"param": "page"})

    @app.route("/plain")
    def plain() -> None:
        raise AppError("SOME_CUSTOM_CODE", "自定义消息", details={"k": "v"})

    client = app.test_client()
    resp = client.get("/bad-param")
    assert resp.status_code == 400
    assert resp.get_json()["code"] == "PARAM_INVALID"
    assert resp.get_json()["message"] == "参数无效: page"

    resp2 = client.get("/plain")
    assert resp2.status_code == 500
    assert resp2.get_json()["code"] == "SOME_CUSTOM_CODE"
    assert resp2.get_json()["message"] == "自定义消息"
    assert resp2.get_json()["details"] == {"k": "v"}


def test_flask_include_generic_unknown_exception() -> None:
    """include_generic=True：未知异常 -> 500 且 code = INTERNAL_ERROR。"""
    app = install_flask_handler(Flask(__name__), include_generic=True)

    @app.route("/boom")
    def boom() -> None:
        raise RuntimeError("unexpected")

    client = app.test_client()
    resp = client.get("/boom")
    assert resp.status_code == 500
    body = resp.get_json()
    assert body["code"] == "INTERNAL_ERROR"
    assert body["http_status"] == 500
    assert body["details"]["original"] == "unexpected"
    assert body["details"]["original_type"] == "RuntimeError"


def test_flask_generic_disabled_default() -> None:
    """include_generic 缺省 False：未注册 Exception 兜底（testing 模式下直接抛出）。"""
    app = install_flask_handler(Flask(__name__))
    app.testing = True  # 非 testing 下 Flask 会吞掉异常并返回 500 HTML

    @app.route("/boom")
    def boom() -> None:
        raise RuntimeError("no handler")

    with pytest.raises(RuntimeError, match="no handler"):
        app.test_client().get("/boom")


def test_flask_normal_route_unaffected() -> None:
    """正常路由不受适配器影响：200 且内容原样返回。"""
    app = install_flask_handler(Flask(__name__), include_generic=True)

    @app.route("/ok")
    def ok() -> dict:
        return {"status": "ok", "items": [1, 2, 3]}

    client = app.test_client()
    resp = client.get("/ok")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok", "items": [1, 2, 3]}


def test_flask_returns_app_chainable() -> None:
    """install_flask_handler 返回传入的 app 本身（可链式）。"""
    app = Flask(__name__)
    assert install_flask_handler(app) is app

    @app.route("/chain")
    def chain() -> str:
        return "chain"

    assert install_flask_handler(app).test_client().get("/chain").status_code == 200
