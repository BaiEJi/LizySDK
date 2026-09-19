"""lizysdk.logs（pipe-logfmt / JSONL 结构化日志）测试。

覆盖：格式精确断言（含 sys_name）/ kv 语义 / 转义与 parse_line 双格式往返 /
级别过滤与 console / size 轮转 / time 轮转 / 并发完整性 / 异步语义 /
contextvars 上下文 / setup 幂等 / 参数校验 / JSON 输出格式 / 远端发送
（本机 http.server 接收线程，不连外网）/ 发送失败容错等契约场景。
日志目录一律使用 pytest tmp_path。
"""

from __future__ import annotations

import json
import logging
import re
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from lizysdk.logs import (
    PipeLogger,
    bind_context,
    clear_context,
    flush,
    get_logger,
    parse_line,
    send_stats,
    setup_logging,
)

THIS_FILE = Path(__file__).name
TS_RE = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


# ---------------------------------------------------------------- helpers


def _setup(tmp_path: Path, **kwargs) -> None:
    kwargs.setdefault("console", False)
    setup_logging(log_dir=tmp_path / "logs", **kwargs)


def _read_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    # 用 split("\n") 而非 splitlines()：避免 \u2028 等罕见分行符干扰
    return [line for line in path.read_text(encoding="utf-8").split("\n") if line]


def _read(tmp_path: Path, filename: str = "app.log") -> list[str]:
    return _read_file(tmp_path / "logs" / filename)


def _src(lineno: int) -> str:
    """读取测试源码第 lineno 行，用于校验 file:line 指向真实调用处。"""
    return Path(__file__).read_text(encoding="utf-8").split("\n")[lineno - 1]


# 本机 JSON 收集服务（不连外网）：记录 (body, content_type)，可配置响应码。
_LOCK = threading.Lock()


class _SinkHandler(BaseHTTPRequestHandler):
    """收集 POST body 与 Content-Type 的接收 handler。"""

    #: 每个用例开始前重置（pytest 单进程串行执行，无竞争问题）。
    received: list = []
    status_to_return: int = 200

    def do_POST(self) -> None:  # noqa: N802 - http.server 约定命名
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8")
        with _LOCK:
            type(self).received.append((body, self.headers.get("Content-Type")))
        self.send_response(type(self).status_to_return)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # 静音访问日志
        pass


def _start_sink(status: int = 200) -> tuple[ThreadingHTTPServer, str]:
    """在本机随机端口起接收服务，返回 (server, url)。用后须 shutdown。"""
    _SinkHandler.received = []
    _SinkHandler.status_to_return = status
    server = ThreadingHTTPServer(("127.0.0.1", 0), _SinkHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}/collect"
    return server, url


def _closed_port_url() -> str:
    """绑定随即关闭一个随机端口：得到一个（几乎必然）连接被拒绝的地址。"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{s.getsockname()[1]}/nowhere"


# ------------------------------------------------- 1. 格式精确断言


LEVEL_CASES = [
    ("debug", "DEBUG"),
    ("info", "INFO"),
    ("warning", "WARNING"),
    ("error", "ERROR"),
    ("critical", "CRITICAL"),
]


@pytest.mark.parametrize("method,level_name", LEVEL_CASES)
def test_format_exact_per_level(tmp_path, method, level_name):
    """各级别输出行与期望完全一致：级别/时间/file:line/kv 顺序/message 恒最后。"""
    _setup(tmp_path, level="DEBUG")
    log = get_logger("fmt")
    getattr(log, method)("user logged in", user_id=123, action="login")
    assert flush(timeout=5) is True

    lines = _read(tmp_path)
    assert len(lines) == 1
    raw = lines[0]
    parsed = parse_line(raw)

    assert parsed["level"] == level_name
    assert TS_RE.fullmatch(parsed["timestamp"])
    assert parsed["file"] == THIS_FILE
    assert parsed["sys_name"] == "app"  # 默认系统标识
    # file:line 必须指向本测试文件中真实调用 log.<method> 的那一行
    src = _src(parsed["line"])
    assert '("user logged in", user_id=123, action="login")' in src
    assert parsed["user_id"] == "123"
    assert parsed["action"] == "login"
    assert parsed["message"] == "user logged in"
    # 整行逐字符一致（时间取自该行并已被上面的正则约束）
    assert raw == (
        f"{level_name}||{parsed['timestamp']}||{THIS_FILE}:{parsed['line']}||"
        f"sys_name=app||user_id=123||action=login||message=user logged in"
    )
    # 字段顺序：级别 || 时间 || 位置 || sys_name（恒在 FILE:LINE 之后第一段）
    # || kv（按传入顺序）|| message 恒最后
    pos = raw.index(f"{THIS_FILE}:{parsed['line']}")
    assert (
        pos < raw.index("sys_name=app")
        < raw.index("user_id=")
        < raw.index("action=")
        < raw.index("message=")
    )
    assert raw.count("||") == 6


# --------------------------------------------------------- 2. kv 语义


def test_kv_value_types_auto_str(tmp_path):
    """bool/None/int/float/str 自动 str()。"""
    _setup(tmp_path)
    log = get_logger("kv")
    log.info(
        "m", b=True, n=None, i=42, f=3.5, s="txt", empty=""
    )
    assert flush(timeout=5)
    p = parse_line(_read(tmp_path)[0])
    assert p["b"] == "True"
    assert p["n"] == "None"
    assert p["i"] == "42"
    assert p["f"] == "3.5"
    assert p["s"] == "txt"
    assert p["empty"] == ""


def test_invalid_kv_keys_raise(tmp_path):
    """非法 key 与保留字 message 均抛 ValueError（含被级别过滤的调用）。"""
    _setup(tmp_path)
    log = get_logger("bad")
    with pytest.raises(ValueError):
        log.info("m", **{"2bad": 1})
    with pytest.raises(ValueError):
        log.info("m", **{"has space": 1})
    with pytest.raises(ValueError):
        log.info("m", **{"also-bad!": 1})
    with pytest.raises(ValueError):
        log.info("m", message="oops")
    with pytest.raises(ValueError):
        log.info(message="oops")  # message 是保留字：不允许作为消息 kwarg
    with pytest.raises(ValueError):
        bind_context(**{"ctx bad": 1})
    with pytest.raises(ValueError):
        bind_context(message="oops")
    # 校验发生在级别过滤之前：DEBUG 低于默认 INFO 级别同样抛错
    with pytest.raises(ValueError):
        log.debug("m", **{"9x": 1})


# --------------------------------------------- 3. 转义与 parse_line 往返


def test_escape_roundtrip(tmp_path):
    """值含 ||、=、换行、回车、竖线、中文、空格 的 round-trip。"""
    _setup(tmp_path)
    log = get_logger("esc")
    payload = {
        "v_pipe2": "a||b",
        "v_eq": "x=y=z",
        "v_nl": "line1\nline2",
        "v_cr": "c1\rc2",
        "v_pipe": "p|q",
        "v_cn": "中文 值",
        "v_sp": "  padded  ",
    }
    log.info("msg with || and = and 中文", **payload)
    assert flush(timeout=5)
    lines = _read(tmp_path)
    assert len(lines) == 1  # 转义后恒为单行
    raw = lines[0]
    assert "\n" not in raw and "\r" not in raw
    p = parse_line(raw)
    for key, value in payload.items():
        assert p[key] == value
    assert p["message"] == "msg with || and = and 中文"


def test_exception_stack_escaped_single_line(tmp_path):
    """多行异常栈经转义后仍是单行，parse_line 可还原。"""
    _setup(tmp_path)
    log = get_logger("exc")
    try:
        raise ValueError("boom")
    except ValueError:
        log.error("db failed", code=500, exc_info=True)
    assert flush(timeout=5)
    lines = _read(tmp_path)
    assert len(lines) == 1  # 栈内有真实换行，落盘后仍只占一个物理行
    p = parse_line(lines[0])
    assert p["code"] == "500"
    assert p["message"].startswith("db failed\n")
    assert "Traceback (most recent call last)" in p["message"]
    assert "ValueError: boom" in p["message"]


def test_parse_line_invalid_inputs():
    """parse_line 对畸形输入抛 ValueError / TypeError（双格式）。"""
    with pytest.raises(ValueError):
        parse_line("garbage")
    with pytest.raises(ValueError):
        parse_line("INFO||t||f:1||k=v")  # 缺 sys_name 与 message 末段
    with pytest.raises(ValueError):
        parse_line("INFO||t||f:1||k=v||notamessage")  # 缺 sys_name 段
    with pytest.raises(ValueError):
        parse_line("INFO||t||f:1||sys_name=app||k=v")  # 缺 message 末段
    with pytest.raises(ValueError):
        parse_line("VERBOSE||t||f:1||sys_name=app||message=x")  # 非法级别
    with pytest.raises(ValueError):
        parse_line("INFO||t||noline||sys_name=app||message=x")  # 位置段无行号
    with pytest.raises(ValueError):
        # 旧格式（FILE:LINE 后直接业务 kv，无 sys_name）：不再合法
        parse_line("INFO||t||f:1||k=v||message=x")
    with pytest.raises(ValueError):
        parse_line("INFO||t||f:1||sys_name=||message=x")  # sys_name 为空
    with pytest.raises(ValueError):
        # sys_name 段后重复出现 sys_name
        parse_line("INFO||t||f:1||sys_name=app||sys_name=x||message=m")
    # JSON 分支的畸形输入
    with pytest.raises(ValueError):
        parse_line("{not a json")  # 非法 JSON
    with pytest.raises(ValueError):
        parse_line('{"level": "INFO"}')  # 缺固定字段
    with pytest.raises(ValueError):
        parse_line("[1, 2]")  # 非 { 开头 -> 走 pipe 分支 -> 畸形
    with pytest.raises(TypeError):
        parse_line(b"bytes not allowed")  # type: ignore[arg-type]


# ------------------------------------------- 4. 级别过滤与 console 开关


def test_level_filter(tmp_path):
    """level=WARNING 时 debug/info 不写文件。"""
    _setup(tmp_path, level="WARNING")
    log = get_logger("lvl")
    log.debug("d")
    log.info("i")
    log.warning("w")
    log.error("e")
    log.critical("c")
    assert flush(timeout=5)
    levels = [parse_line(line)["level"] for line in _read(tmp_path)]
    assert levels == ["WARNING", "ERROR", "CRITICAL"]


def test_console_switch(tmp_path, capsys):
    """console=False 时 stderr 无输出；console=True 时同步输出 pipe 行。"""
    _setup(tmp_path, console=False)
    log = get_logger("con1")
    log.warning("quiet please")
    assert flush(timeout=5)
    assert capsys.readouterr().err == ""
    assert len(_read(tmp_path)) == 1

    _setup(tmp_path, console=True)
    log = get_logger("con2")
    log.warning("noise please")
    assert flush(timeout=5)
    err = capsys.readouterr().err
    assert "message=noise please" in err
    parsed = [parse_line(line) for line in err.split("\n") if line]
    assert any(
        p["message"] == "noise please" and p["file"] == THIS_FILE
        for p in parsed
    )


# -------------------------------------------------------- 5. size 轮转


def test_size_rotation_backup_limit_and_conservation(tmp_path):
    """max_bytes 很小时产生 backup 文件、backup_count 生效、总行数守恒。"""
    _setup(tmp_path, rotation="size", max_bytes=560, backup_count=2)
    log = get_logger("rot")
    logs_dir = tmp_path / "logs"

    total = 9  # 每段约容纳 3 行（行长约 170 字符），共 3 段、恰好 2 次轮转
    for i in range(total):
        log.info("m%03d" % i, seq=i, pad="x" * 100)
    assert flush(timeout=10)

    names = {p.name for p in logs_dir.iterdir()}
    backups = {n for n in names if re.fullmatch(r"app\.log\.\d+", n)}
    assert "app.log" in names
    assert backups == {"app.log.1", "app.log.2"}  # 出现 backup 且受上限约束

    all_lines = []
    for name in sorted(backups | {"app.log"}):
        all_lines.extend(_read_file(logs_dir / name))
    assert len(all_lines) == total  # 内容不丢
    seqs = sorted(int(parse_line(line)["seq"]) for line in all_lines)
    assert seqs == list(range(total))  # 内容不乱且每行可 parse_line

    # 继续写更多，也不会突破 backup_count=2（不出现 app.log.3）
    for i in range(total, total + 9):
        log.info("m%03d" % i, seq=i, pad="x" * 100)
    assert flush(timeout=10)
    names = {p.name for p in logs_dir.iterdir()}
    assert "app.log.3" not in names
    assert (
        len({n for n in names if re.fullmatch(r"app\.log\.\d+", n)}) <= 2
    )


# -------------------------------------------------------- 6. time 轮转


def test_time_rotation_crossing_boundary(tmp_path):
    """when="s"（1 秒一轮）跨轮转点后再写，断言新文件产生。"""
    _setup(tmp_path, rotation="time", when="s", backup_count=5)
    log = get_logger("trot")
    log.info("before rollover", seq=1)
    assert flush(timeout=5)

    time.sleep(1.3)  # 跨过轮转点
    log.info("after rollover", seq=2)
    assert flush(timeout=5)

    logs_dir = tmp_path / "logs"
    files = sorted(p.name for p in logs_dir.iterdir())
    assert "app.log" in files
    backups = [n for n in files if n.startswith("app.log.")]
    assert len(backups) >= 1  # 产生带时间戳后缀的历史文件

    all_lines = []
    for name in files:
        all_lines.extend(_read_file(logs_dir / name))
    assert len(all_lines) == 2
    seqs = sorted(int(parse_line(line)["seq"]) for line in all_lines)
    assert seqs == [1, 2]


# ------------------------------------------------------ 7/8. 并发与异步


def test_concurrency_integrity_default_pool(tmp_path):
    """8 线程 x 500 条（async_writer=True，单 worker）：不丢行、行行可解析。"""
    _setup(tmp_path, rotation="none")
    log = get_logger("conc")
    n_threads, n_msgs = 8, 500

    def worker(tid: int) -> None:
        for seq in range(n_msgs):
            log.info("hello from %d-%d", tid, seq, tid=tid, seq=seq)

    threads = [
        threading.Thread(target=worker, args=(t,)) for t in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert flush(timeout=30) is True

    lines = _read(tmp_path)
    assert len(lines) == n_threads * n_msgs
    seen = set()
    for line in lines:  # 每行都能 parse：无交错/截断/半行
        p = parse_line(line)
        seen.add((int(p["tid"]), int(p["seq"])))
    assert len(seen) == n_threads * n_msgs
    # 单 worker FIFO：顺序也应严格保持
    assert [parse_line(l)["message"] for l in lines[:3]] == [
        "hello from 0-0",
        "hello from 0-1",
        "hello from 0-2",
    ]


def test_concurrency_integrity_multiworker_pool(tmp_path):
    """pool_size=4 多 worker：行完整、总行数守恒、行行可解析。"""
    _setup(tmp_path, rotation="none", pool_size=4)
    log = get_logger("conc4")
    n_threads, n_msgs = 4, 300

    def worker(tid: int) -> None:
        for seq in range(n_msgs):
            log.info("w %d %d", tid, seq, tid=tid, seq=seq)

    threads = [
        threading.Thread(target=worker, args=(t,)) for t in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert flush(timeout=30) is True

    lines = _read(tmp_path)
    assert len(lines) == n_threads * n_msgs
    seen = set()
    for line in lines:
        p = parse_line(line)
        seen.add((int(p["tid"]), int(p["seq"])))
    assert len(seen) == n_threads * n_msgs


def test_flush_returns_true_and_persisted(tmp_path):
    """异步写后 flush(timeout=5) 返回 True 且内容已落盘。"""
    _setup(tmp_path)
    log = get_logger("fl")
    for i in range(50):
        log.info("line %d", i)
    assert flush(timeout=5) is True
    lines = _read(tmp_path)
    assert len(lines) == 50
    assert [parse_line(l)["message"] for l in lines] == [
        f"line {i}" for i in range(50)
    ]


def test_sync_writer_complete(tmp_path):
    """async_writer=False 同步直写：log 返回即已在盘上，同样完整。"""
    _setup(tmp_path, async_writer=False)
    log = get_logger("sync")
    log.info("immediate")
    lines = _read(tmp_path)  # 不调用 flush，直接读
    assert len(lines) == 1
    assert parse_line(lines[0])["message"] == "immediate"


# ----------------------------------------------------- 9. contextvars


def test_context_bind_and_clear(tmp_path):
    """bind_context 后每行自动附带；调用侧同名覆盖；clear 后不再附带。"""
    _setup(tmp_path)
    log = get_logger("ctx")
    bind_context(trace_id="abc")
    log.info("with ctx", seq=1)
    bind_context(k="ctx")
    log.info("override", k="call")
    clear_context()
    log.info("no ctx", seq=3)
    assert flush(timeout=5)

    raws = _read(tmp_path)
    p0, p1, p2 = (parse_line(r) for r in raws)
    assert p0["trace_id"] == "abc"
    assert raws[0].index("trace_id=") < raws[0].index("seq=")  # 上下文在前
    assert p1["k"] == "call"  # 调用侧字段覆盖上下文字段
    assert "trace_id" not in p2 and "k" not in p2


def test_context_thread_isolation(tmp_path):
    """子线程内 bind 不影响主线程。"""
    _setup(tmp_path)
    log = get_logger("ctx2")
    bind_context(trace_id="main")

    def child() -> None:
        bind_context(child_field="1")
        log.info("from child", marker="child")

    t = threading.Thread(target=child)
    t.start()
    t.join()
    log.info("from main", marker="main")
    assert flush(timeout=5)

    by_marker = {parse_line(r)["marker"]: parse_line(r) for r in _read(tmp_path)}
    assert by_marker["child"]["child_field"] == "1"
    assert "child_field" not in by_marker["main"]  # 主线程看不到子线程的 bind
    assert by_marker["main"]["trace_id"] == "main"
    clear_context()


# ------------------------------------------------------- 10. 幂等


def test_setup_idempotent_no_duplicate(tmp_path):
    """连续两次 setup_logging 不崩溃、旧 handler 被清理（无重复输出）。"""
    _setup(tmp_path)
    log = get_logger("idem")
    log.info("first")
    assert flush(timeout=5)

    _setup(tmp_path)  # 第二次：先清理旧 handler / 后台线程再重建
    log.info("second")
    assert flush(timeout=5)

    lines = _read(tmp_path)
    assert [parse_line(l)["message"] for l in lines] == ["first", "second"]
    root_handlers = logging.getLogger().handlers
    assert len(root_handlers) == 1  # 只剩一个入口 handler


# ------------------------------------------------- 11. 参数校验


@pytest.mark.parametrize(
    "kwargs",
    [
        {"rotation": "daily"},
        {"level": "VERBOSE"},
        {"level": "trace"},
        {"pool_size": 0},
        {"pool_size": -3},
    ],
)
def test_setup_logging_argument_validation(tmp_path, kwargs):
    """非法 rotation / level / pool_size<=0 抛 ValueError。"""
    with pytest.raises(ValueError):
        setup_logging(log_dir=tmp_path / "logs", console=False, **kwargs)


# ------------------------------------------------- 补充契约场景


def test_get_logger_identity_and_types():
    """get_logger 返回 PipeLogger，同名同实例。"""
    log = get_logger("typed")
    assert isinstance(log, PipeLogger)
    assert isinstance(log, logging.Logger)
    assert get_logger("typed") is log
    assert get_logger("typed.other") is not log
    assert get_logger() is get_logger("app")


def test_rebuild_legacy_plain_logger(tmp_path):
    """name 已是普通 Logger 时 get_logger 能替换为 PipeLogger。"""
    logging.setLoggerClass(logging.Logger)
    plain = logging.getLogger("legacy.mod")
    assert not isinstance(plain, PipeLogger)

    _setup(tmp_path)
    log = get_logger("legacy.mod")
    assert isinstance(log, PipeLogger)
    log.warning("rebuilt ok")
    assert flush(timeout=5)
    p = parse_line(_read(tmp_path)[0])
    assert p["message"] == "rebuilt ok"
    assert p["file"] == THIS_FILE
    assert 'log.warning("rebuilt ok")' in _src(p["line"])


def test_percent_style_args(tmp_path):
    """message 支持 % 风格惰性格式化。"""
    _setup(tmp_path)
    log = get_logger("pct")
    log.info("hello %s-%d", "a", 7)
    assert flush(timeout=5)
    assert parse_line(_read(tmp_path)[0])["message"] == "hello a-7"


def test_log_method_and_exception_helper(tmp_path):
    """log.log(level, ...) 与 exception() 辅助方法。"""
    _setup(tmp_path, level="DEBUG")
    log = get_logger("helpers")
    log.log(logging.WARNING, "via log", k=1)
    try:
        1 / 0
    except ZeroDivisionError:
        log.exception("calc failed")
    assert flush(timeout=5)

    lines = _read(tmp_path)
    p1, p2 = (parse_line(l) for l in lines)
    assert p1["level"] == "WARNING" and p1["k"] == "1"
    assert p1["message"] == "via log"
    assert p2["level"] == "ERROR"
    assert "ZeroDivisionError: division by zero" in p2["message"]


def test_exc_info_true_outside_except(tmp_path):
    """exc_info=True 但无活动异常时不崩溃、不追加栈。"""
    _setup(tmp_path)
    log = get_logger("noexc")
    log.error("no stack here", exc_info=True)
    assert flush(timeout=5)
    assert parse_line(_read(tmp_path)[0])["message"] == "no stack here"


# ------------------------------------------------ 12. sys_name 系统标识


def test_sys_name_custom_in_pipe_and_json(tmp_path):
    """自定义 sys_name 注入 pipe 与 JSON 两种输出，位置正确。"""
    _setup(tmp_path, sys_name="order-svc")
    log = get_logger("sysname")
    log.info("hello", k=1)
    assert flush(timeout=5)
    raw = _read(tmp_path)[0]
    p = parse_line(raw)
    assert p["sys_name"] == "order-svc"
    # pipe 格式位置：FILE:LINE 之后第一段、业务 kv 之前
    assert (
        raw.index(f"{THIS_FILE}:")
        < raw.index("sys_name=order-svc")
        < raw.index("k=1")
        < raw.index("message=")
    )

    _setup(tmp_path, sys_name="order-svc", json_format=True)
    log = get_logger("sysname2")
    log.info("hello", k=1)
    assert flush(timeout=5)
    lines = _read(tmp_path)
    assert len(lines) == 2  # 第一次 setup 的 pipe 行 + 本次 JSON 行（追加）
    obj = json.loads(lines[-1])
    assert obj["sys_name"] == "order-svc"
    assert obj["k"] == "1"
    assert obj["message"] == "hello"


def test_sys_name_with_pipe_char_roundtrip(tmp_path):
    """sys_name 取值含竖线/等号也能转义往返。"""
    _setup(tmp_path, sys_name="a|b=c")
    log = get_logger("sysname3")
    log.info("m")
    assert flush(timeout=5)
    p = parse_line(_read(tmp_path)[0])
    assert p["sys_name"] == "a|b=c"


def test_sys_name_validation(tmp_path):
    """sys_name 非空 str 校验：空串 / None / 非法类型抛 ValueError。"""
    for bad in ["", None, 123, b"app", 3.14]:
        with pytest.raises(ValueError):
            setup_logging(log_dir=tmp_path / "logs", console=False, sys_name=bad)


def test_sys_name_reserved_as_kv(tmp_path):
    """sys_name 是保留字段：作为业务 kv / 上下文字段传入抛 ValueError。"""
    _setup(tmp_path)
    log = get_logger("sysname4")
    with pytest.raises(ValueError):
        log.info("m", sys_name="x")
    with pytest.raises(ValueError):
        bind_context(sys_name="x")


# ------------------------------------------------ 13. JSON 输出格式


def test_json_format_line_structure(tmp_path):
    """JSONL：每行可 loads；字段完整、顺序正确；中文不转义。"""
    _setup(tmp_path, json_format=True, level="DEBUG")
    log = get_logger("json1")
    log.info("中文 message", user_id=123, flag=True)
    assert flush(timeout=5)
    lines = _read(tmp_path)
    assert len(lines) == 1
    raw = lines[0]

    obj = json.loads(raw)
    assert obj["level"] == "INFO"
    assert TS_RE.fullmatch(obj["timestamp"])
    assert obj["file"] == THIS_FILE
    assert isinstance(obj["line"], int) and obj["line"] > 0
    assert '("中文 message", user_id=123, flag=True)' in _src(obj["line"])
    assert obj["sys_name"] == "app"
    assert obj["user_id"] == "123"  # kv 值仍先转 str（与 pipe 一致）
    assert obj["flag"] == "True"
    assert obj["message"] == "中文 message"

    keys = list(obj.keys())
    assert keys[:5] == ["level", "timestamp", "file", "line", "sys_name"]
    assert keys.index("user_id") < keys.index("flag")  # kv 原序
    assert keys[-1] == "message"  # message 恒在最后
    assert "中文 message" in raw and "\\u" not in raw  # ensure_ascii=False
    assert parse_line(raw) == obj  # parse_line 的 JSON 分支 round-trip


def test_json_format_message_always_present_and_empty_ok(tmp_path):
    """JSON 行 message 恒存在（空消息时为空串）。"""
    _setup(tmp_path, json_format=True)
    log = get_logger("json2")
    log.info("")
    log.info("real")
    assert flush(timeout=5)
    objs = [json.loads(line) for line in _read(tmp_path)]
    assert all("message" in obj for obj in objs)
    assert objs[0]["message"] == ""
    assert objs[1]["message"] == "real"


def test_json_format_exc_info_single_line(tmp_path):
    """JSON 格式下多行异常栈仍是单个物理行，message 含完整栈文本。"""
    _setup(tmp_path, json_format=True)
    log = get_logger("json3")
    try:
        raise ValueError("boom")
    except ValueError:
        log.error("db failed", code=500, exc_info=True)
    assert flush(timeout=5)
    lines = _read(tmp_path)
    assert len(lines) == 1  # 换行由 json.dumps 编码，恒单行
    obj = json.loads(lines[0])
    assert obj["code"] == "500"
    assert obj["message"].startswith("db failed\n")
    assert "Traceback (most recent call last)" in obj["message"]
    assert "ValueError: boom" in obj["message"]


def test_json_format_console_output(tmp_path, capsys):
    """json_format=True 时控制台同样输出 JSONL。"""
    _setup(tmp_path, json_format=True, console=True)
    log = get_logger("json4")
    log.warning("to stderr", k="v")
    assert flush(timeout=5)
    err_lines = [line for line in capsys.readouterr().err.split("\n") if line]
    assert len(err_lines) == 1
    obj = json.loads(err_lines[0])
    assert obj["message"] == "to stderr"
    assert obj["k"] == "v"
    assert obj["sys_name"] == "app"


def test_json_format_rotation_still_works(tmp_path):
    """JSONL 与 size 轮转兼容：backup 受上限约束、内容守恒可 loads。"""
    # 每行约 234 字节：max_bytes=780 时每段恰好容纳 3 行，8 行 -> 2 次轮转
    _setup(tmp_path, json_format=True, rotation="size", max_bytes=780, backup_count=2)
    log = get_logger("json5")
    total = 8
    for i in range(total):
        log.info("m%03d", i, seq=i, pad="x" * 80)
    assert flush(timeout=10)
    logs_dir = tmp_path / "logs"
    names = {p.name for p in logs_dir.iterdir()}
    backups = {n for n in names if re.fullmatch(r"app\.log\.\d+", n)}
    assert backups == {"app.log.1", "app.log.2"}  # 恰好 2 次轮转且受上限约束
    all_lines = []
    for name in sorted(backups | {"app.log"}):
        all_lines.extend(_read_file(logs_dir / name))
    assert len(all_lines) == total  # 内容不丢
    seqs = sorted(json.loads(line)["seq"] for line in all_lines)
    assert seqs == [str(i) for i in range(total)]  # 每行可 loads 且内容不乱


# ------------------------------------------------ 14. 远端发送（本机 sink）


def test_send_json_async_mode(tmp_path):
    """异步发送：flush 后收到的 JSON 与本地行逐条一致、条数相等。"""
    server, url = _start_sink()
    try:
        _setup(tmp_path, json_format=True, send_json=True, send_url=url, send_timeout=5.0)
        log = get_logger("send1")
        for i in range(5):
            log.info("msg %d", i, seq=i)
        assert flush(timeout=10) is True

        local_lines = _read(tmp_path)
        assert len(local_lines) == 5
        with _LOCK:
            received = list(_SinkHandler.received)
        assert len(received) == 5  # 条数相等
        bodies = [body for body, _ in received]
        assert bodies == local_lines  # 与本地行内容逐字符一致（单发送线程 FIFO）
        assert {ct for _, ct in received} == {"application/json"}
        stats = send_stats()
        assert stats["sent"] == 5
        assert stats["failed"] == 0
        assert stats["last_error"] is None
    finally:
        server.shutdown()
        server.server_close()


def test_send_json_sync_mode(tmp_path):
    """同步发送（async_writer=False）：log 返回即已送达，且不向调用方抛错。"""
    server, url = _start_sink()
    try:
        _setup(
            tmp_path,
            json_format=True,
            async_writer=False,
            send_json=True,
            send_url=url,
            send_timeout=5.0,
        )
        log = get_logger("send2")
        for i in range(3):
            log.info("sync %d", i, seq=i)  # 每条同步发送完成后才返回

        local_lines = _read(tmp_path)
        assert len(local_lines) == 3
        with _LOCK:
            received = list(_SinkHandler.received)
        assert len(received) == 3  # 无需 flush：写完即同步送达
        assert [body for body, _ in received] == local_lines
        assert send_stats()["sent"] == 3
    finally:
        server.shutdown()
        server.server_close()


def test_send_json_while_local_pipe_format(tmp_path):
    """本地 pipe 格式 + 远端 JSON：body 与本地行 parse 后同结构。"""
    server, url = _start_sink()
    try:
        _setup(tmp_path, send_json=True, send_url=url, send_timeout=5.0)
        log = get_logger("send3")
        log.info("hello", a=1, b="中文")
        assert flush(timeout=10) is True

        local_lines = _read(tmp_path)
        assert len(local_lines) == 1
        assert local_lines[0].startswith("INFO||")  # 本地仍是 pipe-logfmt
        with _LOCK:
            received = list(_SinkHandler.received)
        assert len(received) == 1
        body, content_type = received[0]
        assert content_type == "application/json"
        obj = json.loads(body)
        assert obj == parse_line(local_lines[0])  # 双格式同结构（值均为 str）
        assert obj["sys_name"] == "app"
        assert obj["message"] == "hello"
    finally:
        server.shutdown()
        server.server_close()


def test_send_stats_zero_when_not_sending(tmp_path):
    """未启用 send_json（默认参数）时 send_stats 返回全零快照。"""
    _setup(tmp_path)
    log = get_logger("send4")
    log.info("m")
    assert flush(timeout=5)
    assert send_stats() == {"sent": 0, "failed": 0, "last_error": None}


# ------------------------------------------------ 15. 发送失败容错


def test_send_failure_async_never_raises_and_counts(tmp_path):
    """异步模式发送失败：业务写日志全程无异常，failed 计数、本地不受影响。"""
    url = _closed_port_url()  # 指向已关闭端口：连接被拒绝
    _setup(tmp_path, send_json=True, send_url=url, send_timeout=1.0)
    log = get_logger("sendfail1")
    for i in range(3):
        log.info("still fine %d", i)  # 全程不抛
    assert flush(timeout=10) is True

    stats = send_stats()
    assert stats["sent"] == 0
    assert stats["failed"] == 3
    assert stats["last_error"]  # 记录了失败原因
    # 本地文件不受发送失败影响
    assert [parse_line(line)["message"] for line in _read(tmp_path)] == [
        "still fine 0",
        "still fine 1",
        "still fine 2",
    ]


def test_send_failure_sync_never_raises(tmp_path):
    """同步模式发送失败：log 调用不抛异常、failed 计数、文件完好。"""
    url = _closed_port_url()
    _setup(
        tmp_path,
        async_writer=False,
        send_json=True,
        send_url=url,
        send_timeout=0.5,
    )
    log = get_logger("sendfail2")
    log.info("ok")
    log.warning("ok too")
    stats = send_stats()
    assert stats["failed"] == 2
    assert stats["sent"] == 0
    lines = _read(tmp_path)
    assert [parse_line(line)["message"] for line in lines] == ["ok", "ok too"]


def test_send_non_2xx_counts_failed(tmp_path):
    """远端返回 500：计入 failed，本地写盘不受影响。"""
    server, url = _start_sink(status=500)
    try:
        _setup(tmp_path, send_json=True, send_url=url, send_timeout=2.0)
        log = get_logger("sendfail3")
        log.info("m")
        assert flush(timeout=10) is True

        with _LOCK:
            received = list(_SinkHandler.received)
        assert len(received) == 1  # 服务端确实收到了 body
        stats = send_stats()
        assert stats["failed"] == 1
        assert stats["sent"] == 0
        assert "500" in stats["last_error"]
        assert len(_read(tmp_path)) == 1  # 本地不受影响
    finally:
        server.shutdown()
        server.server_close()


# ------------------------------------------------ 16. 发送参数校验


@pytest.mark.parametrize(
    "bad_url",
    [None, "", "ftp://host/x", "collector:9000", "http:/oops", 123],
)
def test_send_url_validation(tmp_path, bad_url):
    """send_json=True 且 send_url 非法（None/空/非 http(s)）-> ValueError。"""
    with pytest.raises(ValueError):
        setup_logging(
            log_dir=tmp_path / "logs",
            console=False,
            send_json=True,
            send_url=bad_url,
        )


@pytest.mark.parametrize("bad_timeout", [0, -1.5, "3", None])
def test_send_timeout_validation(tmp_path, bad_timeout):
    """send_timeout 非正数 / 非数值 -> ValueError。"""
    with pytest.raises(ValueError):
        setup_logging(
            log_dir=tmp_path / "logs",
            console=False,
            send_json=True,
            send_url="http://127.0.0.1:1/x",
            send_timeout=bad_timeout,
        )


def test_send_json_false_tolerates_any_url(tmp_path):
    """send_json=False（默认）时不校验 send_url，也不发送。"""
    _setup(tmp_path, send_url="not a url at all")  # 无 ValueError
    log = get_logger("send5")
    log.info("quiet")
    assert flush(timeout=5)
    assert send_stats()["sent"] == 0
    assert len(_read(tmp_path)) == 1


# ------------------------------------------------ 17. parse_line 双格式


def test_parse_line_json_branch_roundtrip():
    """JSON 行：loads 还原同结构 dict；容忍首尾空白与换行。"""
    obj = {
        "level": "INFO",
        "timestamp": "2026-09-19T10:00:00",
        "file": "a.py",
        "line": 7,
        "sys_name": "svc",
        "k": "v",
        "message": "hi",
    }
    line = json.dumps(obj, ensure_ascii=False)
    assert parse_line(line) == obj
    assert parse_line("  " + line + "  \n") == obj  # strip 容忍空白


def test_parse_line_pipe_branch_with_sys_name():
    """pipe 行（含 sys_name 与转义）按新契约解析。"""
    d = parse_line(
        "ERROR||2026-09-19T23:59:59||app.py:42||sys_name=order-svc||"
        "v=a\\|b||message=多行\\n文本"
    )
    assert d["level"] == "ERROR"
    assert d["timestamp"] == "2026-09-19T23:59:59"
    assert d["file"] == "app.py"
    assert d["line"] == 42
    assert d["sys_name"] == "order-svc"
    assert d["v"] == "a|b"  # 转义还原
    assert d["message"] == "多行\n文本"


def test_parse_line_cross_format_consistency(tmp_path):
    """同一 setup 下逐条对比：pipe 行与 JSON 行 parse 出的同构 dict 一致。"""
    _setup(tmp_path, sys_name="both")
    log = get_logger("cross1")
    log.info("same record", a=1, b="中文|值")
    assert flush(timeout=5)
    pipe_parsed = parse_line(_read(tmp_path)[0])

    _setup(tmp_path, sys_name="both", json_format=True)
    log = get_logger("cross2")
    log.info("same record", a=1, b="中文|值")
    assert flush(timeout=5)
    json_parsed = parse_line(_read(tmp_path)[-1])  # 文件追加：取最新一行

    for key in ("level", "sys_name", "file", "a", "b", "message"):
        assert pipe_parsed[key] == json_parsed[key]
    assert isinstance(pipe_parsed["line"], int) is isinstance(json_parsed["line"], int)
