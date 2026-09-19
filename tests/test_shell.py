"""lizysdk.shell 测试：run 薄加固包装 / ShellResult 冻结 / 富错误族 / 日志与超时。

覆盖范围（命令一律用 ``sys.executable -c`` 保证跨 Windows 平台真实可用）：
1. 成功执行：ok / returncode / stdout / stderr / elapsed_ms / argv 全字段
   （stdout 精确到换行；stdout 与 stderr 同时捕获；``check=True`` 且
   成功不抛错）；
2. 非零退出：``check=False`` 返回 ``ok=False``；``check=True`` 抛
   ShellError 且 argv / returncode / stderr / elapsed_ms 属性完整、
   中文 ``__str__`` 含命令 / 退出码 / stderr 摘要，长 stderr 截断；
3. argv 校验：str 经 shlex.split 拆分（带引号的可执行路径）、空串 /
   空白串 / 空列表 / 空元组抛 ValueError（中文）、未闭合引号抛
   ValueError、非 str 类型与元素非 str 抛 TypeError；
4. input：str 与 bytes 两种透传（stdin 回显）；env 为合并语义
   （新变量注入 + 既有变量仍在 + 同名键以 env 为准 + 非 Mapping 抛
   TypeError）；cwd 生效（临时目录 + 子进程打印 os.getcwd()）；
5. capture=False：stdout / stderr 均为 None 且退出码正常；
6. timeout：睡眠子进程超时抛 ShellTimeoutError 且整段耗时 < 3s
   （证明 Windows 下子进程被及时终止）、ShellTimeoutError 为
   ShellError 子类、携带 timeout / argv / 已捕获的部分输出、
   中文 ``__str__`` 含超时上限；timeout 非正数 / 非数抛 ValueError /
   TypeError；
7. 日志：caplog 捕获 "lizysdk.shell" 的 INFO（argv= / returncode= /
   elapsed_ms= kv 形式）、非零退出 WARN（check 两种取值都 WARN）、
   超时 WARN（含 timeout=）、``log=False`` 完全静默；
8. ShellResult 冻结：任意字段赋值抛 dataclasses.FrozenInstanceError、
   ``dataclasses.is_dataclass`` 且 frozen 帧参数为真；
9. 解码：子进程按 PYTHONIOENCODING=utf-8 输出中文，run 按 utf-8
   正确解码；公开 API 导出与异常族继承关系。
"""

from __future__ import annotations

import dataclasses
import logging
import os
import sys
import time

import pytest

import lizysdk.shell as shell_pkg
from lizysdk.shell import ShellError, ShellResult, ShellTimeoutError, run

#: 子进程一律复用当前解释器（跨平台、无需依赖外部命令是否存在）
EXE = sys.executable
#: str 形式 argv 用的正斜杠可执行路径（shlex 的 posix 语义不吞正斜杠）
EXE_FWD = EXE.replace("\\", "/")
#: 非零退出（退出码 3）+ stderr 输出的固定子进程代码
FAIL_CODE = "import sys; sys.stderr.write('boom'); sys.exit(3)"
#: 睡眠 5 秒的子进程代码（配合小 timeout 验证超时终止）
SLEEP_CODE = "import time; time.sleep(5)"


class TestRunSuccess:
    """成功路径：字段完整性。"""

    def test_success_all_fields(self):
        res = run([EXE, "-c", "print('hi')"])
        assert isinstance(res, ShellResult)
        assert res.ok is True
        assert res.returncode == 0
        # subprocess 文本模式读取时启用 universal newlines，跨平台恒为 "\n"
        assert res.stdout == "hi\n"
        assert res.stderr == ""
        assert res.argv == (EXE, "-c", "print('hi')")
        assert isinstance(res.argv, tuple)
        assert isinstance(res.elapsed_ms, float)
        assert 0.0 <= res.elapsed_ms < 60_000.0

    def test_stdout_and_stderr_both_captured(self):
        code = "import sys; sys.stdout.write('OUT'); sys.stderr.write('ERR')"
        res = run([EXE, "-c", code])
        assert res.returncode == 0
        assert res.stdout == "OUT"
        assert res.stderr == "ERR"

    def test_check_true_with_success_does_not_raise(self):
        res = run([EXE, "-c", "pass"], check=True)
        assert res.ok is True

    def test_tuple_argv_accepted(self):
        res = run((EXE, "-c", "pass"))
        assert res.ok is True
        assert res.argv == (EXE, "-c", "pass")


class TestNonZeroExit:
    """非零退出：check 两种取值。"""

    def test_check_false_returns_not_ok(self):
        res = run([EXE, "-c", FAIL_CODE])
        assert res.ok is False
        assert res.returncode == 3
        assert "boom" in res.stderr

    def test_check_true_raises_shell_error_with_context(self):
        with pytest.raises(ShellError) as ei:
            run([EXE, "-c", FAIL_CODE], check=True)
        exc = ei.value
        assert exc.returncode == 3
        assert exc.argv == (EXE, "-c", FAIL_CODE)
        assert "boom" in exc.stderr
        assert isinstance(exc.elapsed_ms, float) and exc.elapsed_ms >= 0.0
        assert not isinstance(exc, ShellTimeoutError)
        # 中文 __str__：命令、退出码、stderr 摘要三要素
        text = str(exc)
        assert "退出码 3" in text
        assert EXE in text
        assert "boom" in text

    def test_shell_error_str_truncates_long_stderr(self):
        exc = ShellError(["a", "b"], returncode=9, stdout="", stderr="E" * 500, elapsed_ms=1.0)
        text = str(exc)
        assert "退出码 9" in text
        assert "…" in text
        assert len(text) < 500


class TestArgvValidation:
    """argv 归一化：str 拆分与非法取值。"""

    def test_str_argv_split_with_quoted_path(self):
        res = run(f'"{EXE_FWD}" -c "print(42)"')
        assert res.returncode == 0
        assert res.stdout.strip() == "42"
        assert res.argv == (EXE_FWD, "-c", "print(42)")

    def test_str_argv_may_contain_spaces_in_argument(self):
        res = run(f'"{EXE_FWD}" -c "import sys; sys.stdout.write(\'a b c\')"')
        assert res.stdout == "a b c"
        assert res.argv[2] == "import sys; sys.stdout.write('a b c')"

    @pytest.mark.parametrize("bad", ["", "   ", "\t\n "], ids=["empty", "spaces", "tabs"])
    def test_blank_str_argv_raises_value_error(self, bad):
        with pytest.raises(ValueError, match="argv"):
            run(bad)

    @pytest.mark.parametrize("bad", [[], ()], ids=["empty-list", "empty-tuple"])
    def test_empty_sequence_argv_raises_value_error(self, bad):
        with pytest.raises(ValueError, match="argv"):
            run(bad)

    def test_unclosed_quote_raises_value_error(self):
        with pytest.raises(ValueError, match="引号"):
            run(f'"{EXE_FWD} -c pass')

    @pytest.mark.parametrize("bad", [123, None, 3.14], ids=["int", "none", "float"])
    def test_non_str_non_sequence_argv_raises_type_error(self, bad):
        with pytest.raises(TypeError, match="argv"):
            run(bad)

    @pytest.mark.parametrize("bad", [[EXE, "-c", 42], [b"ls"], (EXE, None)], ids=["int-elem", "bytes", "none-elem"])
    def test_non_str_element_raises_type_error(self, bad):
        with pytest.raises(TypeError, match="str"):
            run(bad)


class TestInputEnvCwd:
    """input 透传 / env 合并 / cwd 生效。"""

    def test_input_str_echoed(self):
        code = "import sys; sys.stdout.write(sys.stdin.read())"
        res = run([EXE, "-c", code], input="hello shell")
        assert res.returncode == 0
        assert res.stdout == "hello shell"

    def test_input_bytes_echoed(self):
        code = "import sys; sys.stdout.buffer.write(sys.stdin.buffer.read())"
        res = run([EXE, "-c", code], input=b"raw-bytes")
        assert res.returncode == 0
        assert res.stdout == "raw-bytes"

    def test_env_merge_keeps_existing_vars(self):
        code = (
            "import os, sys; "
            "sys.stdout.write(os.environ.get('LIZYSDK_SHELL_NEW', '?') "
            "+ '|' + os.environ.get('PATH', 'MISSING'))"
        )
        res = run([EXE, "-c", code], env={"LIZYSDK_SHELL_NEW": "42"})
        assert res.returncode == 0
        assert res.stdout.startswith("42|")
        assert res.stdout != "42|MISSING"  # os.environ 的既有变量仍在

    def test_env_merge_override_wins(self, monkeypatch):
        monkeypatch.setenv("LIZYSDK_SHELL_OLD", "old")
        code = "import os, sys; sys.stdout.write(os.environ.get('LIZYSDK_SHELL_OLD', '?'))"
        res = run([EXE, "-c", code], env={"LIZYSDK_SHELL_OLD": "new"})
        assert res.stdout == "new"

    def test_env_non_mapping_raises_type_error(self):
        with pytest.raises(TypeError, match="env"):
            run([EXE, "-c", "pass"], env=123)

    def test_cwd_takes_effect(self, tmp_path):
        res = run([EXE, "-c", "import os; print(os.getcwd())"], cwd=tmp_path)
        assert res.returncode == 0
        assert os.path.normcase(res.stdout.strip()) == os.path.normcase(str(tmp_path))


class TestCaptureAndEncoding:
    """capture=False 语义与文本解码。"""

    def test_capture_false_yields_none_outputs(self):
        res = run([EXE, "-c", "pass"], capture=False)
        assert res.returncode == 0
        assert res.ok is True
        assert res.stdout is None
        assert res.stderr is None

    def test_utf8_decoding_of_non_ascii_output(self):
        code = "import sys; sys.stdout.write('你好，lizysdk')"
        # 子进程强制以 utf-8 写管道，run 默认按 utf-8 解码，两端应精确一致
        res = run([EXE, "-c", code], env={"PYTHONIOENCODING": "utf-8"})
        assert res.returncode == 0
        assert res.stdout == "你好，lizysdk"


class TestTimeout:
    """超时：终止时效性、富错误族与部分输出。"""

    def test_timeout_raises_and_process_killed_promptly(self):
        t0 = time.perf_counter()
        with pytest.raises(ShellTimeoutError) as ei:
            run([EXE, "-c", SLEEP_CODE], timeout=0.5)
        # 整段耗时 < 3s：证明子进程在超时后被及时终止（Windows TerminateProcess）
        assert time.perf_counter() - t0 < 3.0
        exc = ei.value
        assert isinstance(exc, ShellError)  # ShellTimeoutError 是 ShellError 子类
        assert exc.timeout == 0.5
        assert exc.returncode is None
        assert exc.argv == (EXE, "-c", SLEEP_CODE)
        assert isinstance(exc.elapsed_ms, float) and exc.elapsed_ms >= 0.0
        text = str(exc)
        assert "超时" in text
        assert "0.5" in text
        assert EXE in text

    def test_timeout_carries_partial_output(self):
        code = "import sys; print('partial'); sys.stdout.flush(); " + SLEEP_CODE
        with pytest.raises(ShellTimeoutError) as ei:
            run([EXE, "-c", code], timeout=1.0)
        assert "partial" in (ei.value.stdout or "")

    def test_timeout_raised_regardless_of_check(self):
        with pytest.raises(ShellTimeoutError):
            run([EXE, "-c", SLEEP_CODE], timeout=0.3, check=False)

    @pytest.mark.parametrize("bad", [0, -1, -0.5], ids=["zero", "negative", "negative-float"])
    def test_timeout_must_be_positive(self, bad):
        with pytest.raises(ValueError, match="timeout"):
            run([EXE, "-c", "pass"], timeout=bad)

    @pytest.mark.parametrize("bad", ["1", "abc"], ids=["str-numeric", "str"])
    def test_timeout_type_error(self, bad):
        with pytest.raises(TypeError, match="timeout"):
            run([EXE, "-c", "pass"], timeout=bad)


class TestLogging:
    """标准 logging 通道：INFO / WARN / 静默。"""

    @staticmethod
    def _records(caplog, level):
        return [
            r
            for r in caplog.records
            if r.name == "lizysdk.shell" and r.levelno == level
        ]

    def test_success_logs_info_kv(self, caplog):
        with caplog.at_level(logging.INFO, logger="lizysdk.shell"):
            run([EXE, "-c", "print('logged')"])
        infos = self._records(caplog, logging.INFO)
        assert len(infos) == 1
        msg = infos[0].getMessage()
        assert "argv=" in msg
        assert "returncode=0" in msg
        assert "elapsed_ms=" in msg

    def test_failure_with_check_true_logs_warn(self, caplog):
        with caplog.at_level(logging.INFO, logger="lizysdk.shell"):
            with pytest.raises(ShellError):
                run([EXE, "-c", FAIL_CODE], check=True)
        warns = self._records(caplog, logging.WARNING)
        assert len(warns) == 1
        assert "returncode=3" in warns[0].getMessage()

    def test_failure_without_check_still_logs_warn(self, caplog):
        with caplog.at_level(logging.INFO, logger="lizysdk.shell"):
            run([EXE, "-c", FAIL_CODE], check=False)
        assert len(self._records(caplog, logging.WARNING)) == 1

    def test_timeout_logs_warn(self, caplog):
        with caplog.at_level(logging.INFO, logger="lizysdk.shell"):
            with pytest.raises(ShellTimeoutError):
                run([EXE, "-c", SLEEP_CODE], timeout=0.3)
        warns = self._records(caplog, logging.WARNING)
        assert len(warns) == 1
        assert "timeout=" in warns[0].getMessage()

    def test_log_false_is_silent(self, caplog):
        with caplog.at_level(logging.INFO, logger="lizysdk.shell"):
            run([EXE, "-c", "pass"], log=False)
            run([EXE, "-c", FAIL_CODE], log=False)
        assert not [r for r in caplog.records if r.name == "lizysdk.shell"]


class TestResultImmutabilityAndApi:
    """冻结语义与公开 API 形状。"""

    def test_result_frozen_assignment_raises(self):
        res = run([EXE, "-c", "pass"])
        with pytest.raises(dataclasses.FrozenInstanceError):
            res.returncode = 1
        with pytest.raises(dataclasses.FrozenInstanceError):
            res.stdout = "x"
        assert res.returncode == 0  # 赋值未生效
        assert res.stdout == ""

    def test_result_is_frozen_dataclass(self):
        res = run([EXE, "-c", "pass"])
        assert dataclasses.is_dataclass(res)
        assert res.__dataclass_params__.frozen

    def test_public_api_exports(self):
        assert set(shell_pkg.__all__) == {"run", "ShellResult", "ShellError", "ShellTimeoutError"}
        for name in shell_pkg.__all__:
            assert hasattr(shell_pkg, name)
        assert callable(run)
        assert issubclass(ShellTimeoutError, ShellError)
        assert issubclass(ShellError, Exception)
