r"""pipe-logfmt / JSONL 双格式化与解析（输出契约的核心实现）。

**pipe 格式**（默认）：一行日志由固定分隔符 ``||``（双竖线）按下述顺序拼成，
``sys_name`` 恒为 ``FILE:LINE`` 之后、业务 kv 之前的第一个字段，
``message=...`` 恒为最后一段::

    LEVEL||TIMESTAMP||FILE:LINE||sys_name=xxx||k1=v1||k2=v2||message=<文本>

示例::

    INFO||2026-09-19T10:30:00||app.py:42||sys_name=app||user_id=123||action=login||message=user logged in

**JSON 格式**（``json_format=True``）：每行一个 JSON 对象（JSONL），字段顺序为
固定头 + 业务 kv（按调用传入顺序）+ ``message`` 恒存在::

    {"level": "INFO", "timestamp": "2026-09-19T10:30:00", "file": "app.py", "line": 42, "sys_name": "app", "user_id": "123", "action": "login", "message": "user logged in"}

两种格式共用字段语义：

- ``level``      —— DEBUG / INFO / WARNING / ERROR / CRITICAL；
- ``timestamp``  —— ISO8601 本地时间，秒级（``%Y-%m-%dT%H:%M:%S``）；
- ``file``/``line`` —— 调用方文件 basename 与真实调用行号（如 ``app.py:42``）；
- ``sys_name``   —— 系统标识（:func:`setup_logging` 注入，非空 str，恒存在）；
- 业务 kv 对     —— 按调用传入顺序排列，值先转 ``str``；
- ``message``    —— pipe 格式恒为最后一段，JSON 格式恒存在。

转义规则（仅 pipe 格式需要，值与 message 均适用，:func:`parse_line` 负责
还原，二者互逆）：

- 真实换行 ``\n`` -> 字面 ``\n``（反斜线 + n）
- 真实回车 ``\r`` -> 字面 ``\r``
- 竖线 ``|``     -> ``\|``

JSON 格式不依赖转义：单行合法性由 ``json.dumps(..., ensure_ascii=False)``
保证（值中的换行/竖线/中文均按 JSON 规则原样编码）。

.. note::
    pipe 格式中反斜线本身不参与转义（遵循契约）。因此取值中「字面的反斜线
    + n」与「真实换行」在转义后不可区分，写入侧应避免这种病态取值。
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any

__all__ = [
    "SEP",
    "TIMESTAMP_FORMAT",
    "KEY_PATTERN",
    "LEVELS",
    "RESERVED_KEYS",
    "SYS_NAME_KEY",
    "DEFAULT_SYS_NAME",
    "JSON_REQUIRED_KEYS",
    "FIELDS_ATTR",
    "validate_sys_name",
    "escape_value",
    "unescape",
    "validate_fields",
    "parse_line",
    "PipeLogFormatter",
    "JsonLogFormatter",
]

#: 段分隔符：双竖线。
SEP = "||"

#: 时间格式：ISO8601 本地时间，秒级。
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

#: 业务 kv 的 key 合法模式。
KEY_PATTERN = r"^[A-Za-z_][A-Za-z0-9_.-]*$"

#: 合法日志级别。
LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")

#: 系统标识的固定字段名。
SYS_NAME_KEY = "sys_name"

#: 系统标识默认值（setup_logging 未显式指定时使用）。
DEFAULT_SYS_NAME = "app"

#: JSON 行必含的固定字段（parse_line 的 JSON 分支按此校验完整性）。
JSON_REQUIRED_KEYS = ("level", "timestamp", "file", "line", "sys_name", "message")

#: 保留 key：不允许作为业务 kv 传入（会与固定字段冲突）。
RESERVED_KEYS = frozenset({"message", SYS_NAME_KEY})

#: LogRecord 上承载业务 kv 的属性名（由 :class:`~lizysdk.logs.PipeLogger` 注入）。
FIELDS_ATTR = "bk_fields"

_KEY_RE = re.compile(KEY_PATTERN)
_UNESCAPE_MAP = {"n": "\n", "r": "\r", "|": "|"}

#: 秒级时间戳的单槽缓存：``(整数秒, 已格式化文本)``。
#: 时间格式为秒级精度，同一秒内的所有日志行时间戳文本完全相同；
#: 单槽在 GIL 下原子替换，读侧偶发未命中只会多算一次，无正确性问题。
_TS_CACHE: list = [None]

#: 已验证合法的业务 key 缓存（热路径免正则）。key 来自代码字面量，实际有界；
#: 设上限防御病态场景（如动态拼接 key），超限后退回逐次正则。
_VALIDATED_KEYS: set = set()
_VALIDATED_KEYS_MAX = 4096

#: pathname -> basename 缓存（源文件数量天然有界）。
_BASENAME_CACHE: dict = {}
_BASENAME_CACHE_MAX = 8192


def _cached_basename(path: str) -> str:
    """带缓存的 ``os.path.basename``（热路径每条日志调用一次）。"""
    cache = _BASENAME_CACHE
    try:
        return cache[path]
    except KeyError:
        base = os.path.basename(path)
        if len(cache) < _BASENAME_CACHE_MAX:
            cache[path] = base
        return base


def validate_sys_name(sys_name: Any) -> str:
    """校验系统标识：必须是**非空 str**。

    Args:
        sys_name: 待校验的取值。

    Returns:
        原样返回合法值。

    Raises:
        ValueError: 不是 ``str``，或是空串。

    示例::

        >>> validate_sys_name("order-svc")
        'order-svc'
    """
    if not isinstance(sys_name, str) or not sys_name:
        raise ValueError(f"sys_name 必须为非空 str，得到 {sys_name!r}")
    return sys_name


def _to_text(value: Any) -> str:
    """把任意取值转为 ``str``（bool/None/int/float/str 均适用）。"""
    return value if isinstance(value, str) else str(value)


def escape_value(value: Any) -> str:
    """把任意取值转为安全的单行文本（pipe 格式专用）。

    非 ``str`` 取值先做 ``str()``（bool/None/int/float/str 均适用），
    再按契约转义换行、回车与竖线。

    示例::

        >>> escape_value("a|b\nc")
        'a\\|b\\nc'
        >>> escape_value(123)
        '123'
    """
    text = _to_text(value)
    # 快速路径：绝大多数取值不含任何需转义字符，三次 replace 全程扫描可省
    if "|" in text or "\n" in text or "\r" in text:
        return (
            text.replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("|", "\\|")
        )
    return text


def unescape(text: str) -> str:
    """还原 :func:`escape_value` 的转义（``\\n``/``\\r``/``\\|`` -> 真实字符）。

    非转义序列的反斜线原样保留。
    """
    if "\\" not in text:  # 快速路径：无反斜线则不可能存在转义序列
        return text
    out: list[str] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch == "\\" and i + 1 < n and text[i + 1] in _UNESCAPE_MAP:
            out.append(_UNESCAPE_MAP[text[i + 1]])
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def validate_fields(fields: dict[str, Any]) -> None:
    """校验业务 kv：key 必须匹配 :data:`KEY_PATTERN`，且不得撞保留字。

    Raises:
        ValueError: key 非法或使用了保留字（``message`` / ``sys_name``）。
    """
    validated = _VALIDATED_KEYS
    for key in fields:
        if key in validated:  # 热路径：重复 key 免正则（一跳命中）
            continue
        if not isinstance(key, str) or not _KEY_RE.match(key):
            raise ValueError(
                f"非法日志字段名 {key!r}：须匹配正则 {KEY_PATTERN}"
            )
        if key in RESERVED_KEYS:
            raise ValueError(
                f"{key} 为保留字段（固定输出字段），不允许作为业务 kv 传入"
            )
        if len(validated) < _VALIDATED_KEYS_MAX:
            validated.add(key)


def _split_escaped(line: str) -> list[str]:
    """按「未转义的 ``||``」切分一行，段内的转义序列原样保留。

    与朴素的 ``str.split("||")`` 不同，本函数能正确处理段内被转义的
    竖线（``\\|``），例如 ``k=a\\||b`` 中不会产生误切分。
    """
    parts: list[str] = []
    buf: list[str] = []
    i, n = 0, len(line)
    while i < n:
        ch = line[i]
        if ch == "\\" and i + 1 < n and line[i + 1] in _UNESCAPE_MAP:
            buf.append(line[i : i + 2])
            i += 2
        elif ch == "|" and line.startswith(SEP, i):
            parts.append("".join(buf))
            buf = []
            i += 2
        else:
            buf.append(ch)
            i += 1
    parts.append("".join(buf))
    return parts


def parse_line(line: str) -> dict[str, Any]:
    """把一行日志还原为字典，与格式化互逆（round-trip），支持双格式。

    格式自动判别：行 ``strip()`` 后以 ``{`` 开头视为 **JSON 行**
    （``json.loads`` 还原为同结构 dict）；否则按 **pipe-logfmt** 解析
    （含 ``sys_name`` 固定字段与既有转义还原）。

    Args:
        line: 单行日志文本（允许带一个行尾换行符与首尾空白）。

    Returns:
        形如 ``{"level", "timestamp", "file", "line", "sys_name", *业务kv,
        "message"}`` 的字典。pipe 分支的 ``line`` 为 ``int``，业务 kv 与
        ``message`` 均已做转义还原；JSON 分支保持 ``json.loads`` 的原生
        结构（固定头之外本就全是 ``str`` 值，与 pipe 分支一致）。

    Raises:
        ValueError: 行结构不完整、级别非法、缺少 ``sys_name=`` /
            ``message=`` 段、JSON 非法或缺固定字段等。
        TypeError: ``line`` 不是 str。

    示例::

        >>> d = parse_line(
        ...     "INFO||2026-09-19T10:30:00||app.py:42||sys_name=app||"
        ...     "user_id=123||message=user logged in"
        ... )
        >>> d["file"], d["line"], d["sys_name"], d["user_id"], d["message"]
        ('app.py', 42, 'app', '123', 'user logged in')
        >>> d = parse_line('{"level": "INFO", "timestamp": "t", "file": "a.py",'
        ...               ' "line": 1, "sys_name": "app", "message": "hi"}')
        >>> d["sys_name"], d["line"], d["message"]
        ('app', 1, 'hi')
    """
    if not isinstance(line, str):
        raise TypeError(f"line 必须为 str，得到 {type(line).__name__}")
    raw = line
    if raw.endswith("\r\n"):
        raw = raw[:-2]
    elif raw.endswith("\n"):
        raw = raw[:-1]

    # ---- JSON 分支：strip 后以 { 开头 ------------------------------------------------
    stripped = raw.strip()
    if stripped.startswith("{"):
        try:
            obj = json.loads(stripped)
        except ValueError as exc:  # json.JSONDecodeError 是 ValueError 子类
            raise ValueError(f"非法 JSON 日志行: {line!r}") from exc
        if not isinstance(obj, dict):
            raise ValueError(f"JSON 日志行必须是一个对象: {line!r}")
        missing = [key for key in JSON_REQUIRED_KEYS if key not in obj]
        if missing:
            raise ValueError(
                f"JSON 日志行缺少固定字段 {missing}: {line!r}"
            )
        return obj

    # ---- pipe 分支 ------------------------------------------------------------------
    # 快速路径：行内无反斜线则不可能存在转义序列，C 级 str.split 与
    # 转义感知切分语义完全一致；含转义的行（少数）走完整扫描。
    parts = raw.split(SEP) if "\\" not in raw else _split_escaped(raw)
    if len(parts) < 5:
        raise ValueError(
            f"非法日志行（至少需要 级别/时间/位置/sys_name/message 五段）: {line!r}"
        )

    level = parts[0]
    if level not in LEVELS:
        raise ValueError(f"非法日志级别: {level!r}，可选: {LEVELS}")

    file_part, colon, lineno_part = parts[2].rpartition(":")
    if not colon or not file_part or not lineno_part.isdigit():
        raise ValueError(f"非法调用位置段（应为 FILE:LINE）: {parts[2]!r}")

    kv_parts = parts[3:]
    if not kv_parts[-1].startswith("message="):
        raise ValueError(f"最后一个字段必须是 message=...: {kv_parts[-1]!r}")

    # sys_name 必须是 FILE:LINE 之后、业务 kv 之前的第一段（恒存在）
    sys_part = kv_parts[0]
    prefix = SYS_NAME_KEY + "="
    if not sys_part.startswith(prefix):
        raise ValueError(
            f"FILE:LINE 之后第一个字段必须是 {prefix}...: {sys_part!r}"
        )
    sys_name = unescape(sys_part[len(prefix) :])
    if not sys_name:
        raise ValueError("sys_name 不允许为空")

    result: dict[str, Any] = {
        "level": level,
        "timestamp": parts[1],
        "file": file_part,
        "line": int(lineno_part),
        SYS_NAME_KEY: sys_name,
    }
    for part in kv_parts[1:-1]:
        key, eq, value = part.partition("=")
        if not key or not eq:
            raise ValueError(f"非法 kv 段（应为 key=value）: {part!r}")
        if key == SYS_NAME_KEY:
            raise ValueError("sys_name 为固定字段，一行日志中不允许重复出现")
        result[key] = unescape(value)
    result["message"] = unescape(kv_parts[-1][len("message=") :])
    return result


class _LineFormatterBase(logging.Formatter):
    """结构化单行格式化器的公共基类：本地时间 + 消息组装 + 有序 kv。

    汇总两种格式化器共用的逻辑：

    - ``converter = time.localtime``（显式声明使用本地时间，也是标准库默认值）；
    - :meth:`_compose_message` —— ``%`` 风格消息 + 异常栈 / stack_info 的
      追加规则（pipe 分支随后整体转义，JSON 分支交给 ``json.dumps``）；
    - :meth:`_ordered_fields` —— 从 ``record.bk_fields`` 取业务 kv 并按
      「先转 str」规则规整取值，保持调用传入顺序。
    """

    #: 显式声明使用本地时间（这也是标准库默认值）。
    converter = time.localtime  # type: ignore[assignment]

    def formatTime(  # noqa: N802 - 标准库签名
        self, record: logging.LogRecord, datefmt: "str | None" = None
    ) -> str:
        """格式化时间戳（默认秒级格式走单槽缓存，热路径免 strftime）。

        时间格式为秒级精度，同一秒内文本恒定，按 ``int(record.created)``
        做单槽缓存；非默认 ``datefmt`` 仍走标准库实现。
        """
        if datefmt != TIMESTAMP_FORMAT:
            return super().formatTime(record, datefmt)
        sec = int(record.created)
        cached = _TS_CACHE[0]
        if cached is not None and cached[0] == sec:
            return cached[1]
        text = time.strftime(TIMESTAMP_FORMAT, time.localtime(sec))
        _TS_CACHE[0] = (sec, text)
        return text

    def __init__(
        self,
        datefmt: str = TIMESTAMP_FORMAT,
        sys_name: str = DEFAULT_SYS_NAME,
    ) -> None:
        """初始化格式化器。

        Args:
            datefmt: 时间格式串，默认 ISO8601 本地时间秒级。
            sys_name: 系统标识，注入每一条日志（非空 str）。

        Raises:
            ValueError: ``sys_name`` 不是非空 ``str``。
        """
        super().__init__(datefmt=datefmt)
        self.sys_name = validate_sys_name(sys_name)

    def _compose_message(self, record: logging.LogRecord) -> str:
        """组装 message 文本（含异常栈 / stack_info，未做任何转义）。"""
        message = record.getMessage()
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        if record.stack_info:
            message = f"{message}\n{self.formatStack(record.stack_info)}"
        return message

    def _ordered_fields(self, record: logging.LogRecord) -> list[tuple[str, str]]:
        """按调用传入顺序取出业务 kv，值先转 ``str``。"""
        fields = getattr(record, FIELDS_ATTR, None) or {}
        return [(key, _to_text(value)) for key, value in fields.items()]


class PipeLogFormatter(_LineFormatterBase):
    """pipe-logfmt 格式化器。

    基于 ``logging.Formatter`` 构建，不重造轮子：时间格式化、异常栈
    格式化复用标准库实现，仅重写 :meth:`format` 负责拼装与转义。

    业务 kv 从 ``record.bk_fields``（:data:`FIELDS_ATTR`）读取——该属性由
    :class:`~lizysdk.logs.PipeLogger` 在**调用方线程**注入，因此经后台写入
    线程格式化时上下文字段也不会丢失。

    输出行结构（``sys_name`` 恒在 ``FILE:LINE`` 之后第一段，``message``
    恒为最后一段）::

        LEVEL||TIMESTAMP||FILE:LINE||sys_name=xxx||k1=v1||message=<文本>

    示例::

        fmt = PipeLogFormatter(sys_name="order-svc")
        handler.setFormatter(fmt)   # handler 收到的行即 pipe-logfmt 格式
    """

    def __init__(
        self,
        datefmt: str = TIMESTAMP_FORMAT,
        sys_name: str = DEFAULT_SYS_NAME,
    ) -> None:
        """初始化 pipe-logfmt 格式化器。

        Args:
            datefmt: 时间格式串，默认 ISO8601 本地时间秒级。
            sys_name: 系统标识，注入每一条日志（非空 str）。

        Raises:
            ValueError: ``sys_name`` 不是非空 ``str``。
        """
        super().__init__(datefmt=datefmt, sys_name=sys_name)
        # sys_name 初始化后视为只读：预计算恒定不变的 sys_name=xxx 段
        self._sys_segment = f"{SYS_NAME_KEY}={escape_value(self.sys_name)}"

    def format(self, record: logging.LogRecord) -> str:
        """把 LogRecord 拼装为一行 pipe-logfmt 文本（保证单行）。

        记录级 sys_name 覆盖（``extra={'sys_name': ...}``）优先于默认段。
        """
        override = record.__dict__.get(SYS_NAME_KEY)
        sys_segment = self._sys_segment
        if isinstance(override, str):
            override = override.strip()
            if override and len(override) <= 64 and override != self.sys_name:
                sys_segment = SYS_NAME_KEY + "=" + escape_value(override)
        parts: list[str] = [
            record.levelname,
            self.formatTime(record, self.datefmt),
            f"{_cached_basename(record.pathname)}:{record.lineno}",
            sys_segment,
        ]
        for key, value in self._ordered_fields(record):
            parts.append(f"{key}={escape_value(value)}")
        parts.append(f"message={escape_value(self._compose_message(record))}")
        return SEP.join(parts)


class JsonLogFormatter(_LineFormatterBase):
    """JSONL 格式化器：每行一个 JSON 对象。

    字段顺序：``level`` / ``timestamp`` / ``file`` / ``line`` / ``sys_name``
    固定头 + 业务 kv（按调用传入顺序）+ ``message``（恒存在）。kv 值先转
    ``str``（与 pipe 格式一致）；``json.dumps(..., ensure_ascii=False)``
    输出为单行 UTF-8 文本，中文不转义，换行/竖线由 JSON 规则编码，天然
    保持单行。

    示例::

        fmt = JsonLogFormatter(sys_name="order-svc")
        handler.setFormatter(fmt)   # handler 收到的行即 JSONL 格式
    """

    def __init__(
        self,
        datefmt: str = TIMESTAMP_FORMAT,
        sys_name: str = DEFAULT_SYS_NAME,
    ) -> None:
        """初始化 JSONL 格式化器。

        Args:
            datefmt: 时间格式串，默认 ISO8601 本地时间秒级。
            sys_name: 系统标识，注入每一条日志（非空 str）。

        Raises:
            ValueError: ``sys_name`` 不是非空 ``str``。
        """
        super().__init__(datefmt=datefmt, sys_name=sys_name)

    def _record_sys_name(self, record: logging.LogRecord) -> str:
        """记录级 sys_name 覆盖：``extra={'sys_name': 'app.blog'}`` 时优先，
        非法取值（空/超长）回退到 setup 注入的默认值。"""
        override = record.__dict__.get(SYS_NAME_KEY)
        if isinstance(override, str):
            override = override.strip()
            if override and len(override) <= 64:
                return override
        return self.sys_name

    def format(self, record: logging.LogRecord) -> str:
        """把 LogRecord 编码为一行 JSON 文本（保证单行、UTF-8、中文不转义）。"""
        payload: dict[str, Any] = {
            "level": record.levelname,
            "timestamp": self.formatTime(record, self.datefmt),
            "file": _cached_basename(record.pathname),
            "line": record.lineno,
            SYS_NAME_KEY: self._record_sys_name(record),
        }
        payload.update(self._ordered_fields(record))
        payload["message"] = self._compose_message(record)
        # 紧凑分隔符：无空格 JSONL（json.loads 无差别，行更小、序列化更快）
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
