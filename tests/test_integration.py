"""跨子模块集成测试：顶层导出自洽 + trace_id 注入日志 + 错误对象落盘全链路。"""

from __future__ import annotations

import time

import lizysdk as bk


def test_top_level_exports_cover_three_submodules() -> None:
    """顶层 __all__ 的每个名字都可导入、可调用/可实例化，且与子模块同源。"""
    assert bk.__version__
    for name in bk.__all__:
        assert hasattr(bk, name), f"顶层缺失导出: {name}"

    assert bk.new_trace_id() != bk.new_trace_id()
    assert isinstance(bk.new_id(), int)
    assert bk.new_prefixed_id("ORD").startswith("ORD_")
    assert issubclass(bk.NotFoundError, bk.AppError)
    assert isinstance(bk.ErrorCode.RESOURCE_NOT_FOUND, str)


def test_trace_flow_logging_and_errors(tmp_path) -> None:
    """trace_id 经 bind_context 进入每行日志；AppError 属性作为 kv 落盘。"""
    log_dir = tmp_path / "logs"
    bk.setup_logging(log_dir, "app.log", level="DEBUG", console=False, async_writer=False)
    log = bk.get_logger("integration")

    trace_id = bk.new_trace_id()
    bk.bind_context(trace_id=trace_id)

    log.info("user logged in", user_id=123, action="login")

    captured: bk.AppError | None = None
    try:
        raise bk.NotFoundError(params={"resource": "订单"})
    except bk.AppError as exc:
        captured = exc
        log.error("request failed", code=exc.code, resource="订单")
    bk.flush(5)

    lines = (log_dir / "app.log").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2

    first = bk.parse_line(lines[0])
    assert first["level"] == "INFO"
    assert first["sys_name"] == "app"
    assert first["trace_id"] == trace_id
    assert first["user_id"] == "123"
    assert first["action"] == "login"
    assert first["message"] == "user logged in"
    assert first["file"] == "test_integration.py"

    second = bk.parse_line(lines[1])
    assert second["level"] == "ERROR"
    assert "RESOURCE_NOT_FOUND" in second["code"]
    assert second["message"] == "request failed"

    assert captured is not None
    assert captured.to_dict()["code"] == "RESOURCE_NOT_FOUND"
    bk.clear_context()


def test_json_format_output_and_sys_name(tmp_path) -> None:
    """json_format=True 时输出 JSONL，sys_name 注入，parse_line 双格式可解析。"""
    log_dir = tmp_path / "logs"
    bk.setup_logging(
        log_dir, "app.log", console=False, async_writer=False,
        sys_name="order-svc", json_format=True,
    )
    bk.get_logger("integration").info("hello json", req_id=bk.new_uid())

    line = (log_dir / "app.log").read_text(encoding="utf-8").splitlines()[0]
    parsed = bk.parse_line(line)
    assert parsed["sys_name"] == "order-svc"
    assert parsed["level"] == "INFO"
    assert parsed["file"] == "test_integration.py"
    assert parsed["req_id"]
    assert parsed["message"] == "hello json"


def test_v03_capabilities_wired(monkeypatch) -> None:
    """0.3.0 新能力贯通：ULID 可排序、worker_id env 协商、业务码注册表生效。"""
    sid1, sid2 = bk.new_sortable_id(), bk.new_sortable_id()
    assert len(sid1) == 26 and sid2 > sid1
    assert abs(bk.sortable_id_timestamp(sid1) - time.time()) < 5

    monkeypatch.setenv("LIZYSDK_TEST_WORKER_ID", "7")
    assert bk.resolve_worker_id(env_var="LIZYSDK_TEST_WORKER_ID") == 7

    code = bk.register_code("IT_DEMO_CODE", "演示业务错误: {name}", 422)
    try:
        err = bk.AppError(code, params={"name": "订单"})
        assert err.http_status == 422
        assert err.message == "演示业务错误: 订单"
        assert code in bk.registered_codes()
    finally:
        bk.unregister_code(code)
    assert code not in bk.registered_codes()


def test_pools_context_flows_into_logs(tmp_path) -> None:
    """pools × logs 协同：bind_context 的 trace_id 经线程池传播进任务内日志。"""
    log_dir = tmp_path / "logs"
    bk.setup_logging(log_dir, "app.log", console=False, async_writer=False)
    trace_id = bk.new_trace_id()
    bk.bind_context(trace_id=trace_id)

    def task() -> str:
        log = bk.get_logger("task")
        log.info("running in worker", job="sync")
        return bk.new_uid(8)

    with bk.create_pool("thread", workers=2, name="it-pool") as pool:
        uid = pool.submit(task).result(timeout=10)
    bk.clear_context()

    line = (log_dir / "app.log").read_text(encoding="utf-8").splitlines()[0]
    parsed = bk.parse_line(line)
    assert parsed["trace_id"] == trace_id
    assert parsed["job"] == "sync"
    assert len(uid) == 8


def test_run_all_top_level() -> None:
    """顶层 run_all：保序执行与结果返回。"""
    out = bk.run_all(
        [(str.upper, ("a",), {}), (str.lower, ("B",), {})],
        kind="thread", workers=2,
    )
    assert out == ["A", "b"]


def test_shell_and_dist_wired() -> None:
    """0.6.0 新能力贯通：shell 执行 + 窗口计数 + 分布式锁（fakeredis 离线）。"""
    import sys as _sys

    res = bk.run([_sys.executable, "-c", "print('ok')"])
    assert res.ok and res.stdout.strip() == "ok"

    import fakeredis

    client = fakeredis.FakeStrictRedis()
    counter = bk.SlidingWindowCounter(client, window=60)
    assert counter.incr("it") == 1
    assert counter.allow("it", 5) is True

    with bk.DLock(client, "it-lock", timeout=2):
        other = bk.DLock(client, "it-lock", timeout=2)
        assert other.acquire(blocking=False) is False
    assert bk.DLock(client, "it-lock", timeout=2).acquire(blocking=False) is True


def test_notify_center_wired() -> None:
    """0.7.0 通知中心贯通：路由 + 异步送达 + 统计（transport 注入，离线）。"""
    sent: list[tuple[str, dict]] = []

    def fake_transport(url, payload, headers, timeout):
        sent.append((url, payload))
        return 200, '{"errcode":0}'

    center = bk.NotifyCenter()
    center.add_channel("ops", bk.WebhookChannel("http://x/hook", transport=fake_transport))
    center.route("error", channels=["ops"])
    center.notify("订单异常", "SO-001 扣减失败", level="error")
    assert center.flush(timeout=5) is True

    assert len(sent) == 1
    assert sent[0][1]["title"] == "订单异常"
    assert sent[0][1]["level"] == "error"
    stats = center.stats()
    assert stats["sent"] == 1 and stats["channels"]["ops"]["sent"] == 1
    center.shutdown()


def test_error_wrapped_into_log_with_trace(tmp_path) -> None:
    """wrap() 包装底层异常后，异常信息可结构化进入日志，trace_id 全程一致。"""
    log_dir = tmp_path / "logs"
    bk.setup_logging(log_dir, "app.log", console=False, async_writer=False)

    trace_id = bk.new_trace_id()
    bk.bind_context(trace_id=trace_id)

    try:
        try:
            raise ValueError("connection refused")
        except ValueError as inner:
            raise bk.wrap(inner, code=bk.ErrorCode.SERVICE_UNAVAILABLE, details={"host": "db-1"}) from inner
    except bk.AppError as app_err:
        bk.get_logger("integration").error(
            "upstream failed", error_code=app_err.code, host=app_err.details["host"]
        )
    bk.flush(5)

    line = (log_dir / "app.log").read_text(encoding="utf-8").splitlines()[0]
    parsed = bk.parse_line(line)
    assert parsed["trace_id"] == trace_id
    assert "SERVICE_UNAVAILABLE" in parsed["error_code"]
    assert parsed["host"] == "db-1"
    bk.clear_context()
