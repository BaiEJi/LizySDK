"""lizysdk.errors 标准化错误子模块的单元测试。

覆盖：枚举完整性 / AppError 构造与消息渲染 / str 与 repr /
序列化往返 / 标准子类 / 异常链与 wrap / ensure / 边界场景 / 对象语义 /
业务错误码动态注册表 / to_dict(include_cause) cause 序列化。
"""

from __future__ import annotations

import copy
import json
import pickle
import threading
from collections.abc import Iterator
from datetime import datetime

import pytest

import lizysdk.errors as errors_module
from lizysdk.errors import (
    AppError,
    AuthError,
    ConflictError,
    ErrorCode,
    InternalError,
    NotFoundError,
    ParamError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    UpstreamTimeoutError,
    ensure,
    register_code,
    registered_codes,
    unregister_code,
    wrap,
)

# ---------------------------------------------------------------------------
# 公开 API
# ---------------------------------------------------------------------------

CONTRACT_EXPORTS = {
    "ErrorCode",
    "AppError",
    "ParamError",
    "AuthError",
    "PermissionDeniedError",
    "NotFoundError",
    "ConflictError",
    "RateLimitError",
    "InternalError",
    "ServiceUnavailableError",
    "UpstreamTimeoutError",
    "wrap",
    "ensure",
    "register_code",
    "unregister_code",
    "registered_codes",
}


def test_public_api_exports() -> None:
    """__all__ 覆盖契约要求的全部名字，且每个名字都可解析。"""
    assert CONTRACT_EXPORTS <= set(errors_module.__all__)
    for name in errors_module.__all__:
        assert hasattr(errors_module, name), f"__all__ 中的 {name} 无法解析"


# ---------------------------------------------------------------------------
# 1. 枚举完整性
# ---------------------------------------------------------------------------

EXPECTED_CODES: dict[str, tuple[str, int]] = {
    "PARAM_MISSING": ("缺少必填参数: {param}", 400),
    "PARAM_INVALID": ("参数无效: {param}", 400),
    "PARAM_TYPE_ERROR": ("参数类型错误: {param} 应为 {expected_type}", 400),
    "TOKEN_EXPIRED": ("令牌已过期", 401),
    "TOKEN_INVALID": ("令牌无效", 401),
    "CREDENTIALS_ERROR": ("用户名或密码错误", 401),
    "PERMISSION_DENIED": ("权限不足: {required_permission}", 403),
    "ACCESS_FORBIDDEN": ("禁止访问: {resource}", 403),
    "RESOURCE_NOT_FOUND": ("资源不存在: {resource}", 404),
    "RESOURCE_ALREADY_EXISTS": ("资源已存在: {resource}", 409),
    "VERSION_CONFLICT": ("版本冲突: {resource} 已被修改", 409),
    "RATE_LIMITED": ("请求过于频繁，请稍后重试", 429),
    "INTERNAL_ERROR": ("内部服务器错误", 500),
    "SERVICE_UNAVAILABLE": ("服务暂不可用", 503),
    "UPSTREAM_TIMEOUT": ("上游服务超时", 504),
}


def test_errorcode_member_set_exactly() -> None:
    """成员不多不少，且码名与枚举成员名一致、无别名。"""
    assert set(ErrorCode.__members__) == set(EXPECTED_CODES)
    assert len(ErrorCode) == len(EXPECTED_CODES)


@pytest.mark.parametrize("name, expected", sorted(EXPECTED_CODES.items()))
def test_errorcode_member_catalog(name: str, expected: tuple[str, int]) -> None:
    """每个成员的 value / template / http_status 三要素正确。"""
    template, status = expected
    member = ErrorCode[name]
    assert member.value == name
    assert member.template == template
    assert member.http_status == status


def test_errorcode_status_groups() -> None:
    """按 HTTP 状态码分组校验。"""
    by_status: dict[int, set[str]] = {}
    for member in ErrorCode:
        by_status.setdefault(member.http_status, set()).add(member.value)
    assert by_status[400] == {"PARAM_MISSING", "PARAM_INVALID", "PARAM_TYPE_ERROR"}
    assert by_status[401] == {"TOKEN_EXPIRED", "TOKEN_INVALID", "CREDENTIALS_ERROR"}
    assert by_status[403] == {"PERMISSION_DENIED", "ACCESS_FORBIDDEN"}
    assert by_status[404] == {"RESOURCE_NOT_FOUND"}
    assert by_status[409] == {"RESOURCE_ALREADY_EXISTS", "VERSION_CONFLICT"}
    assert by_status[429] == {"RATE_LIMITED"}
    assert by_status[500] == {"INTERNAL_ERROR"}
    assert by_status[503] == {"SERVICE_UNAVAILABLE"}
    assert by_status[504] == {"UPSTREAM_TIMEOUT"}


def test_errorcode_is_str_subclass() -> None:
    """ErrorCode 是 str 子类：可直接比较、转字符串、格式化。"""
    member = ErrorCode.PARAM_MISSING
    assert isinstance(member, str)
    assert member == "PARAM_MISSING"
    assert str(member) == "PARAM_MISSING"
    assert f"{member}" == "PARAM_MISSING"
    assert "%s" % member == "PARAM_MISSING"
    assert "PARAM_MISSING".__eq__(member) is True


def test_errorcode_lookup_by_value() -> None:
    """可用码名字符串按值查找成员。"""
    assert ErrorCode("RATE_LIMITED") is ErrorCode.RATE_LIMITED
    with pytest.raises(ValueError):
        ErrorCode("NO_SUCH_CODE")


def test_errorcode_json_serializable() -> None:
    """成员可被 json 直接序列化为码名字符串，pickle 往返仍是同一成员。"""
    assert json.dumps(ErrorCode.TOKEN_EXPIRED) == '"TOKEN_EXPIRED"'
    assert json.dumps({"code": ErrorCode.TOKEN_EXPIRED}) == '{"code": "TOKEN_EXPIRED"}'
    assert pickle.loads(pickle.dumps(ErrorCode.RATE_LIMITED)) is ErrorCode.RATE_LIMITED


# ---------------------------------------------------------------------------
# 2. AppError 构造
# ---------------------------------------------------------------------------


def test_apperror_defaults() -> None:
    """默认构造：INTERNAL_ERROR / 500 / 空 details / 模板消息。"""
    err = AppError()
    assert err.code == "INTERNAL_ERROR"
    assert err.http_status == 500
    assert err.message == "内部服务器错误"
    assert err.details == {}


def test_apperror_explicit_message_overrides_template() -> None:
    """显式 message 优先于模板。"""
    err = AppError(ErrorCode.PARAM_MISSING, "自定义消息", params={"param": "x"})
    assert err.message == "自定义消息"


def test_apperror_params_render_template() -> None:
    """params 渲染模板：单占位符与多占位符。"""
    err = AppError(ErrorCode.PARAM_MISSING, params={"param": "user_id"})
    assert err.message == "缺少必填参数: user_id"
    err2 = AppError(
        ErrorCode.PARAM_TYPE_ERROR,
        params={"param": "page", "expected_type": "int"},
    )
    assert err2.message == "参数类型错误: page 应为 int"


def test_apperror_params_missing_key_safe() -> None:
    """params 缺占位符对应键时安全降级：保留原占位符，不抛 KeyError。"""
    err = AppError(ErrorCode.PARAM_MISSING)  # 完全不传 params
    assert err.message == "缺少必填参数: {param}"
    err2 = AppError(ErrorCode.PARAM_MISSING, params={"别的": "键"})
    assert err2.message == "缺少必填参数: {param}"


def test_apperror_params_extra_and_non_str_values() -> None:
    """多余键被忽略；非字符串值正常 str 化。"""
    err = AppError(
        ErrorCode.PARAM_MISSING,
        params={"param": 42, "unused": "忽略我"},
    )
    assert err.message == "缺少必填参数: 42"


def test_apperror_timestamp_iso8601() -> None:
    """timestamp 非空且为带时区的 ISO8601。"""
    err = AppError()
    assert err.timestamp
    parsed = datetime.fromisoformat(err.timestamp)
    assert parsed.tzinfo is not None


def test_apperror_attributes_readonly() -> None:
    """公开属性为只读语义：不允许重绑定。"""
    err = AppError()
    for attr in ("code", "message", "http_status", "details", "timestamp"):
        with pytest.raises(AttributeError):
            setattr(err, attr, "hack")


# ---------------------------------------------------------------------------
# 3. __str__ / __repr__
# ---------------------------------------------------------------------------


def test_apperror_str_format() -> None:
    """__str__ 为 "[CODE] message"。"""
    err = AppError(ErrorCode.TOKEN_EXPIRED)
    assert str(err) == "[TOKEN_EXPIRED] 令牌已过期"


def test_apperror_repr_readable() -> None:
    """repr 可读：含类名、code、message、http_status。"""
    err = NotFoundError(message="订单不存在")
    r = repr(err)
    assert "NotFoundError" in r
    assert "RESOURCE_NOT_FOUND" in r
    assert "订单不存在" in r
    assert "404" in r


# ---------------------------------------------------------------------------
# 4. to_dict / from_dict 往返
# ---------------------------------------------------------------------------


def test_to_dict_fields() -> None:
    """to_dict 字段齐全且取值正确。"""
    err = NotFoundError(params={"resource": "order-1"}, details={"trace_id": "t-1"})
    d = err.to_dict()
    assert set(d) == {"type", "code", "message", "http_status", "details", "timestamp"}
    assert d["type"] == "NotFoundError"
    assert d["code"] == "RESOURCE_NOT_FOUND"
    assert d["message"] == "资源不存在: order-1"
    assert d["http_status"] == 404
    assert d["details"] == {"trace_id": "t-1"}
    assert d["timestamp"] == err.timestamp


def test_to_dict_json_roundtrip() -> None:
    """to_dict 可直接 json.dumps 且 loads 后内容一致。"""
    err = ConflictError(params={"resource": "用户名"}, details={"nested": {"a": [1, 2]}})
    raw = json.dumps(err.to_dict())
    loaded = json.loads(raw)
    assert loaded == err.to_dict()


def test_from_dict_roundtrip_subclass() -> None:
    """子类实例 to_dict -> from_dict 完整还原（含具体子类与 timestamp）。"""
    err = NotFoundError(params={"resource": "order-1"}, details={"trace_id": "t-1"})
    rebuilt = AppError.from_dict(err.to_dict())
    assert type(rebuilt) is NotFoundError
    assert rebuilt.code == err.code
    assert rebuilt.message == err.message
    assert rebuilt.http_status == err.http_status
    assert rebuilt.details == err.details
    assert rebuilt.timestamp == err.timestamp


def test_from_dict_roundtrip_base() -> None:
    """基类实例同样可往返。"""
    err = AppError("CUSTOM_CODE", "自定义", details={"k": "v"})
    rebuilt = AppError.from_dict(err.to_dict())
    assert type(rebuilt) is AppError
    assert rebuilt.code == "CUSTOM_CODE"
    assert rebuilt.message == "自定义"
    assert rebuilt.http_status == err.http_status
    assert rebuilt.details == {"k": "v"}
    assert rebuilt.timestamp == err.timestamp


def test_from_dict_unknown_type_falls_back() -> None:
    """未知 type 落回 AppError 本体，其余字段尽力还原。"""
    d = NotFoundError(params={"resource": "x"}).to_dict()
    d["type"] = "NoSuchError"
    rebuilt = AppError.from_dict(d)
    assert type(rebuilt) is AppError
    assert rebuilt.code == "RESOURCE_NOT_FOUND"
    assert rebuilt.http_status == 404


def test_from_dict_missing_type_falls_back() -> None:
    """type 缺失同样落回 AppError 本体。"""
    d = RateLimitError().to_dict()
    del d["type"]
    rebuilt = AppError.from_dict(d)
    assert type(rebuilt) is AppError
    assert rebuilt.code == "RATE_LIMITED"


def test_to_dict_details_is_copy() -> None:
    """to_dict 返回的 details 是拷贝，改它不影响实例。"""
    err = AppError(details={"k": {"inner": 1}})
    d = err.to_dict()
    d["details"]["k"]["inner"] = 999
    d["details"]["new"] = "x"
    assert err.details == {"k": {"inner": 1}}


# ---------------------------------------------------------------------------
# 5. 标准子类
# ---------------------------------------------------------------------------

SUBCLASS_DEFAULTS = [
    (ParamError, "PARAM_INVALID", 400),
    (AuthError, "CREDENTIALS_ERROR", 401),
    (PermissionDeniedError, "PERMISSION_DENIED", 403),
    (NotFoundError, "RESOURCE_NOT_FOUND", 404),
    (ConflictError, "RESOURCE_ALREADY_EXISTS", 409),
    (RateLimitError, "RATE_LIMITED", 429),
    (InternalError, "INTERNAL_ERROR", 500),
    (ServiceUnavailableError, "SERVICE_UNAVAILABLE", 503),
    (UpstreamTimeoutError, "UPSTREAM_TIMEOUT", 504),
]


@pytest.mark.parametrize("cls, code, status", SUBCLASS_DEFAULTS)
def test_subclass_defaults(cls: type, code: str, status: int) -> None:
    """每个子类默认 code/http_status 正确，且空构造 details 为空。"""
    err = cls()
    assert err.code == code
    assert err.http_status == status
    assert err.details == {}
    assert isinstance(err, AppError)


@pytest.mark.parametrize("cls, code, status", SUBCLASS_DEFAULTS)
def test_subclass_caught_as_app_error(cls: type, code: str, status: int) -> None:
    """子类异常可被 except AppError 捕获，且 str 保持规范格式。"""
    with pytest.raises(AppError) as excinfo:
        raise cls(message="出错了")
    err = excinfo.value
    assert type(err) is cls
    assert str(err) == f"[{code}] 出错了"


def test_subclass_message_details_override() -> None:
    """子类可覆盖 message 与 details，http_status 仍随默认 code。"""
    err = NotFoundError(message="找不到该订单", details={"order_id": "o-1"})
    assert err.message == "找不到该订单"
    assert err.details == {"order_id": "o-1"}
    assert err.http_status == 404
    assert err.code == "RESOURCE_NOT_FOUND"


def test_subclass_template_render() -> None:
    """子类不传 message 时走默认 code 的模板渲染。"""
    err = NotFoundError(params={"resource": "订单"})
    assert err.message == "资源不存在: 订单"


def test_subclass_explicit_code() -> None:
    """子类显式传 code 可覆盖默认码。"""
    err = ParamError(ErrorCode.PARAM_MISSING, params={"param": "page"})
    assert err.code == "PARAM_MISSING"
    assert err.http_status == 400
    assert err.message == "缺少必填参数: page"


# ---------------------------------------------------------------------------
# 6. 异常链与 wrap
# ---------------------------------------------------------------------------


def test_raise_from_chain() -> None:
    """raise ... from ... 时 __cause__ 保留原异常。"""
    with pytest.raises(NotFoundError) as excinfo:
        try:
            raise ValueError("boom")
        except ValueError as inner:
            raise NotFoundError(message="找不到订单", details={"order_id": "o-1"}) from inner
    err = excinfo.value
    assert isinstance(err.__cause__, ValueError)
    assert str(err.__cause__) == "boom"


def test_wrap_basic() -> None:
    """wrap 包装任意异常：code/details/cause 正确，details 含 original。"""
    orig = KeyError("missing-key")
    wrapped = wrap(orig)
    assert isinstance(wrapped, AppError)
    assert wrapped.code == "INTERNAL_ERROR"
    assert wrapped.http_status == 500
    assert wrapped.__cause__ is orig
    assert wrapped.details["original"] == "'missing-key'"
    assert wrapped.details["original_type"] == "KeyError"


def test_wrap_custom_code_message_details() -> None:
    """wrap 可指定 code / message / details。"""
    orig = TimeoutError("upstream slow")
    wrapped = wrap(
        orig,
        "上游超时了",
        code=ErrorCode.UPSTREAM_TIMEOUT,
        details={"trace_id": "t-9"},
    )
    assert wrapped.code == "UPSTREAM_TIMEOUT"
    assert wrapped.http_status == 504
    assert wrapped.message == "上游超时了"
    assert wrapped.details["trace_id"] == "t-9"
    assert wrapped.details["original"] == "upstream slow"
    assert wrapped.__cause__ is orig


def test_wrap_preserves_user_original_key() -> None:
    """调用方 details 已提供 original 时不被覆盖。"""
    orig = RuntimeError("x")
    wrapped = wrap(orig, details={"original": "保留我"})
    assert wrapped.details["original"] == "保留我"


def test_wrap_bare_string_code() -> None:
    """wrap 的 code 支持裸字符串。"""
    wrapped = wrap(ValueError("v"), code="MY_CODE")
    assert wrapped.code == "MY_CODE"
    assert wrapped.http_status == 500


def test_wrap_raised_from_within() -> None:
    """wrap 的结果可直接 raise，cause 链在 except 中可读。"""
    orig = ValueError("原始错误")
    with pytest.raises(AppError) as excinfo:
        raise wrap(orig, code=ErrorCode.SERVICE_UNAVAILABLE)
    err = excinfo.value
    assert err.code == "SERVICE_UNAVAILABLE"
    assert err.__cause__ is orig
    assert err.details["original"] == "原始错误"


# ---------------------------------------------------------------------------
# 7. ensure
# ---------------------------------------------------------------------------


def test_ensure_true_no_raise() -> None:
    """条件为真时三种形态均不抛。"""
    assert ensure(True, ErrorCode.PARAM_MISSING, param="x") is None
    assert ensure(True, NotFoundError(message="不会抛")) is None
    assert ensure(True, "ANY_CODE") is None


def test_ensure_false_with_code_and_params() -> None:
    """传码：kwargs 作为 params 渲染模板。"""
    with pytest.raises(AppError) as excinfo:
        ensure(False, ErrorCode.PARAM_MISSING, param="user_id")
    err = excinfo.value
    assert err.code == "PARAM_MISSING"
    assert err.http_status == 400
    assert err.message == "缺少必填参数: user_id"


def test_ensure_false_with_bare_string() -> None:
    """传裸字符串码。"""
    with pytest.raises(AppError) as excinfo:
        ensure(False, "WEIRD_CODE", foo="bar")
    assert excinfo.value.code == "WEIRD_CODE"


def test_ensure_false_instance_identity() -> None:
    """传实例：抛出的就是该实例本身。"""
    inst = RateLimitError(message="慢一点")
    with pytest.raises(RateLimitError) as excinfo:
        ensure(False, inst)
    assert excinfo.value is inst


def test_ensure_false_subclass_instance() -> None:
    """传子类实例：可被 AppError 与具体子类捕获。"""
    with pytest.raises(PermissionDeniedError) as excinfo:
        ensure(False, PermissionDeniedError(message="需要 admin"))
    err = excinfo.value
    assert err.code == "PERMISSION_DENIED"
    assert err.http_status == 403
    with pytest.raises(AppError):
        ensure(False, PermissionDeniedError(message="需要 admin"))


def test_ensure_false_instance_merges_kwargs_into_details() -> None:
    """传实例且带 kwargs：合并进 details 重建，不改动原实例。"""
    inst = NotFoundError(details={"order": "o-1"})
    with pytest.raises(NotFoundError) as excinfo:
        ensure(False, inst, trace_id="t-9")
    err = excinfo.value
    assert err.details == {"order": "o-1", "trace_id": "t-9"}
    assert inst.details == {"order": "o-1"}  # 原实例未被污染


def test_ensure_false_code_with_details_kwarg() -> None:
    """传码时 kwargs 中名为 details 的 dict 作为附加上下文。"""
    with pytest.raises(AppError) as excinfo:
        ensure(False, ErrorCode.RESOURCE_NOT_FOUND, resource="文档", details={"trace_id": "t-1"})
    err = excinfo.value
    assert err.message == "资源不存在: 文档"
    assert err.details == {"trace_id": "t-1"}


# ---------------------------------------------------------------------------
# 8. 边界
# ---------------------------------------------------------------------------


def test_unicode_message_chinese_emoji() -> None:
    """中文与 emoji 消息在 str/repr/to_dict/json 往返中保持一致。"""
    msg = "支付失败 🎉 请稍后重试"
    err = InternalError(msg)
    assert err.message == msg
    assert msg in str(err)
    assert msg in repr(err)
    loaded = json.loads(json.dumps(err.to_dict()))
    assert loaded["message"] == msg


def test_long_message() -> None:
    """超长消息完整保留。"""
    msg = "错" * 20_000
    err = AppError("LONG_CODE", msg)
    assert len(err.message) == 20_000
    assert err.message == msg


def test_nested_details_json_safe() -> None:
    """details 支持嵌套 dict/list，且 to_dict 可 json 往返。"""
    details = {
        "list": [1, "a", {"k": "v"}],
        "dict": {"nested": {"deep": [True, None]}},
        "int": 3,
    }
    err = AppError(ErrorCode.INTERNAL_ERROR, details=details)
    assert err.details == details
    loaded = json.loads(json.dumps(err.to_dict()))
    assert loaded["details"] == details


def test_details_none_defaults_to_empty_dict() -> None:
    """details=None 默认空 dict。"""
    assert AppError(details=None).details == {}


def test_bare_string_code_known_and_unknown() -> None:
    """code 传裸字符串：命中已知码沿用模板与状态码，未知码回退。"""
    known = AppError("RATE_LIMITED")
    assert known.code == "RATE_LIMITED"
    assert known.http_status == 429
    assert known.message == "请求过于频繁，请稍后重试"

    unknown = AppError("TOTALLY_UNKNOWN", "兜底消息")
    assert unknown.code == "TOTALLY_UNKNOWN"
    assert unknown.message == "兜底消息"
    assert unknown.http_status == 500

    unknown_no_msg = AppError("TOTALLY_UNKNOWN")  # 无模板也无消息时以码名兜底
    assert unknown_no_msg.message == "TOTALLY_UNKNOWN"


def test_pickle_roundtrip_full_fidelity() -> None:
    """pickle 往返保留类型与全部字段。"""
    err = NotFoundError(message="找不到", details={"a": [1, 2], "b": {"c": 3}})
    rebuilt = pickle.loads(pickle.dumps(err))
    assert type(rebuilt) is NotFoundError
    assert rebuilt.code == err.code
    assert rebuilt.message == err.message
    assert rebuilt.http_status == err.http_status
    assert rebuilt.details == err.details
    assert rebuilt.timestamp == err.timestamp


def test_deepcopy_roundtrip() -> None:
    """copy.deepcopy 正常工作且字段一致。"""
    err = RateLimitError(details={"k": [1, 2]})
    cloned = copy.deepcopy(err)
    assert cloned is not err
    assert cloned.code == err.code
    assert cloned.message == err.message
    assert cloned.details == err.details
    assert cloned.timestamp == err.timestamp


# ---------------------------------------------------------------------------
# 9. 对象语义
# ---------------------------------------------------------------------------


def test_same_code_not_same_identity() -> None:
    """code 相同不代表同一对象身份（未重载 __eq__，按身份比较）。"""
    a = AppError(ErrorCode.INTERNAL_ERROR, "x")
    b = AppError(ErrorCode.INTERNAL_ERROR, "x")
    assert a.code == b.code
    assert a is not b
    assert a != b


def test_details_copied_on_init() -> None:
    """构造时拷贝 details：修改外部 dict 不影响实例。"""
    outer = {"k": "v"}
    err = AppError(details=outer)
    outer["k2"] = "v2"
    assert err.details == {"k": "v"}


# ---------------------------------------------------------------------------
# 10. 业务错误码动态注册表
# ---------------------------------------------------------------------------


@pytest.fixture()
def clean_registry() -> Iterator[None]:
    """每个用例结束后自动注销其新增的错误码，避免全局注册表互相污染。"""
    before = set(registered_codes())
    yield
    for code in set(registered_codes()) - before:
        unregister_code(code)


def test_register_code_returns_code_and_snapshot(clean_registry: None) -> None:
    """注册返回码名本身；registered_codes 快照含模板与状态码。"""
    assert register_code("BIZ_ORDER_REJECTED", "订单被拒绝: {order_id}", 422) == "BIZ_ORDER_REJECTED"
    assert registered_codes()["BIZ_ORDER_REJECTED"] == {
        "template": "订单被拒绝: {order_id}",
        "http_status": 422,
    }


def test_register_code_default_status_400(clean_registry: None) -> None:
    """http_status 缺省为 400。"""
    register_code("BIZ_DEFAULT_STATUS", "默认状态")
    assert registered_codes()["BIZ_DEFAULT_STATUS"]["http_status"] == 400


def test_register_code_length_boundaries(clean_registry: None) -> None:
    """码名长度边界：2 字符与 64 字符均合法。"""
    assert register_code("AB", "两字符", 400) == "AB"
    longest = "A" + "B" * 63
    assert register_code(longest, "六十四字符", 400) == longest
    assert set(registered_codes()) == {"AB", longest}


@pytest.mark.parametrize(
    "bad_code",
    [
        "",
        "A",                # 过短（首字母外至少还要 1 个字符）
        "A" * 65,           # 过长
        "lower_case",       # 小写
        "1START_WITH_DIGIT",
        "_START_UNDERSCORE",
        "HAS-DASH",
        "HAS SPACE",
        "HAS.CHAR",
        "MixedCase",
        "ÄBC",              # 非 ASCII
        123,                # 非 str
        None,
        4.2,
        ("X",),
        ["X"],
        {"X"},
    ],
)
def test_register_code_invalid_code(clean_registry: None, bad_code: object) -> None:
    """非法码名：类型 / 格式 / 长度不合规一律 ValueError。"""
    with pytest.raises(ValueError, match="错误码非法"):
        register_code(bad_code, "模板")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "bad_template",
    ["", "   ", None, 123, [], {}, ("t",)],
)
def test_register_code_invalid_template(clean_registry: None, bad_template: object) -> None:
    """非法模板：非字符串 / 空串 / 纯空白一律 ValueError。"""
    with pytest.raises(ValueError, match="消息模板非法"):
        register_code("BIZ_BAD_TEMPLATE", bad_template)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "bad_status",
    [99, 600, 0, -1, 1000, "400", 400.0, True, False, None],
)
def test_register_code_invalid_status(clean_registry: None, bad_status: object) -> None:
    """非法 HTTP 状态码：越界 / 非 int / bool 一律 ValueError。"""
    with pytest.raises(ValueError, match="HTTP 状态码非法"):
        register_code("BIZ_BAD_STATUS", "模板", bad_status)  # type: ignore[arg-type]


def test_register_code_status_boundaries(clean_registry: None) -> None:
    """状态码边界 100 与 599 合法。"""
    register_code("BIZ_STATUS_100", "模板", 100)
    register_code("BIZ_STATUS_599", "模板", 599)
    assert registered_codes()["BIZ_STATUS_100"]["http_status"] == 100
    assert registered_codes()["BIZ_STATUS_599"]["http_status"] == 599


def test_register_code_duplicate_raises(clean_registry: None) -> None:
    """重复注册同名业务码：默认 ValueError。"""
    register_code("BIZ_DUP", "第一版", 400)
    with pytest.raises(ValueError, match="已注册"):
        register_code("BIZ_DUP", "第二版", 418)


def test_register_code_duplicate_overwrite(clean_registry: None) -> None:
    """重复注册同名业务码：overwrite=True 覆盖生效。"""
    register_code("BIZ_DUP_OVERWRITE", "第一版", 400)
    register_code("BIZ_DUP_OVERWRITE", "第二版", 418, overwrite=True)
    assert registered_codes()["BIZ_DUP_OVERWRITE"] == {
        "template": "第二版",
        "http_status": 418,
    }


def test_register_code_builtin_conflict_raises(clean_registry: None) -> None:
    """与内置 ErrorCode 成员重名：默认 ValueError。"""
    with pytest.raises(ValueError, match="内置 ErrorCode 成员同名"):
        register_code("RATE_LIMITED", "自定义限流", 418)


def test_register_code_builtin_conflict_overwrite(clean_registry: None) -> None:
    """overwrite=True 可覆盖内置成员，注册表记录可见。"""
    register_code("RATE_LIMITED", "自定义限流: {seconds}秒后重试", 418, overwrite=True)
    assert registered_codes()["RATE_LIMITED"] == {
        "template": "自定义限流: {seconds}秒后重试",
        "http_status": 418,
    }


def test_registered_codes_snapshot_is_copy(clean_registry: None) -> None:
    """registered_codes 返回拷贝快照：改快照不影响注册表。"""
    register_code("BIZ_SNAPSHOT", "原始", 400)
    snap = registered_codes()
    snap["BIZ_SNAPSHOT"]["template"] = "被篡改"
    snap["EXTRA_CODE"] = {"template": "x", "http_status": 400}
    again = registered_codes()
    assert again["BIZ_SNAPSHOT"]["template"] == "原始"
    assert "EXTRA_CODE" not in again


def test_registered_codes_excludes_builtin_members(clean_registry: None) -> None:
    """快照仅含动态注册的业务码，不含内置 ErrorCode 成员。"""
    assert "INTERNAL_ERROR" not in registered_codes()
    assert "RATE_LIMITED" not in registered_codes()
    register_code("BIZ_ONLY_ONE", "唯一", 400)
    assert set(registered_codes()) == {"BIZ_ONLY_ONE"}


def test_unregister_code_idempotent(clean_registry: None) -> None:
    """注销幂等：未注册 / 已注销的码再注销不抛错。"""
    register_code("BIZ_GONE", "稍纵即逝", 400)
    unregister_code("BIZ_GONE")
    assert "BIZ_GONE" not in registered_codes()
    unregister_code("BIZ_GONE")  # 幂等
    unregister_code("NEVER_REGISTERED")  # 从未注册过，也不抛
    unregister_code(123)  # type: ignore[arg-type]  # 非 str 静默忽略


def test_unregister_builtin_override_restores_enum(clean_registry: None) -> None:
    """注销被覆盖的内置码后，AppError 解析回落到枚举定义。"""
    register_code("RATE_LIMITED", "自定义限流消息", 418, overwrite=True)
    overridden = AppError(ErrorCode.RATE_LIMITED)
    assert overridden.http_status == 418
    unregister_code("RATE_LIMITED")
    restored = AppError(ErrorCode.RATE_LIMITED)
    assert restored.message == "请求过于频繁，请稍后重试"
    assert restored.http_status == 429


def test_unregister_business_code_falls_back_to_500(clean_registry: None) -> None:
    """注销业务码后，裸字符串码回落未知兜底（500）。"""
    register_code("BIZ_FALLBACK", "业务消息: {x}", 422)
    assert AppError("BIZ_FALLBACK").http_status == 422
    unregister_code("BIZ_FALLBACK")
    err = AppError("BIZ_FALLBACK", "显式消息")
    assert err.http_status == 500
    assert err.message == "显式消息"
    assert AppError("BIZ_FALLBACK").message == "BIZ_FALLBACK"  # 无模板无消息时以码名兜底


def test_concurrent_registration_all_visible(clean_registry: None) -> None:
    """并发注册：8 线程各注册不同码，全部可见且总数正确。"""
    codes = [f"BIZ_THREAD_{i:02d}" for i in range(8)]
    failures: list[BaseException] = []

    def worker(code: str) -> None:
        try:
            register_code(code, f"并发注册的 {code}", 400 + int(code[-2:]))
        except BaseException as exc:  # noqa: BLE001 - 记录后统一断言
            failures.append(exc)

    threads = [threading.Thread(target=worker, args=(c,)) for c in codes]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert failures == []
    snap = registered_codes()
    assert set(snap) == set(codes)
    assert len(snap) == 8
    for c in codes:
        assert snap[c] == {"template": f"并发注册的 {c}", "http_status": 400 + int(c[-2:])}


# ---------------------------------------------------------------------------
# 11. AppError 解析接入注册表
# ---------------------------------------------------------------------------


def test_apperror_bare_string_uses_registry(clean_registry: None) -> None:
    """裸字符串码命中注册表：模板渲染、http_status 生效。"""
    register_code("BIZ_ORDER_REJECTED", "订单被拒绝: {order_id}", 422)
    err = AppError("BIZ_ORDER_REJECTED", params={"order_id": "o-9"})
    assert err.code == "BIZ_ORDER_REJECTED"
    assert err.message == "订单被拒绝: o-9"
    assert err.http_status == 422


def test_apperror_registry_priority_over_enum_bare_string(clean_registry: None) -> None:
    """裸字符串码：注册表优先于枚举（覆盖内置码后以注册表为准）。"""
    register_code("PARAM_MISSING", "自定义缺失: {param}", 422, overwrite=True)
    err = AppError("PARAM_MISSING", params={"param": "name"})
    assert err.message == "自定义缺失: name"
    assert err.http_status == 422


def test_apperror_registry_priority_over_enum_member(clean_registry: None) -> None:
    """ErrorCode 成员被 overwrite 后：构造 AppError 同样以注册表为准。"""
    register_code("RESOURCE_NOT_FOUND", "自定义 404: {resource}", 418, overwrite=True)
    err = AppError(ErrorCode.RESOURCE_NOT_FOUND, params={"resource": "文档"})
    assert err.message == "自定义 404: 文档"
    assert err.http_status == 418


def test_apperror_registry_priority_over_enum_member_subclass(clean_registry: None) -> None:
    """标准子类显式传被覆盖的枚举成员：同样吃注册表覆盖。"""
    register_code("TOKEN_EXPIRED", "自定义过期提示", 419, overwrite=True)
    err = AuthError(ErrorCode.TOKEN_EXPIRED)
    assert err.message == "自定义过期提示"
    assert err.http_status == 419


def test_apperror_registry_miss_still_uses_enum(clean_registry: None) -> None:
    """注册表未命中时：裸串已知码仍走枚举模板与状态（既有行为不回归）。"""
    register_code("BIZ_PRESENT", "在场的业务码", 400)  # 证明注册表在工作
    known = AppError("RATE_LIMITED")
    assert known.message == "请求过于频繁，请稍后重试"
    assert known.http_status == 429
    unknown = AppError("STILL_TOTALLY_UNKNOWN")
    assert unknown.http_status == 500


def test_apperror_registry_message_override_still_wins(clean_registry: None) -> None:
    """注册表码上的显式 message 依旧优先于模板渲染。"""
    register_code("BIZ_MSG_OVERRIDE", "模板: {x}", 422)
    err = AppError("BIZ_MSG_OVERRIDE", "显式消息", params={"x": "1"})
    assert err.message == "显式消息"
    assert err.http_status == 422


def test_wrap_and_registry_code(clean_registry: None) -> None:
    """wrap 的 code 参数同样可传注册表中的业务码。"""
    register_code("BIZ_UPSTREAM_BUSY", "上游繁忙，请稍后再试", 503)
    wrapped = wrap(ValueError("v"), code="BIZ_UPSTREAM_BUSY")
    assert wrapped.code == "BIZ_UPSTREAM_BUSY"
    assert wrapped.message == "上游繁忙，请稍后再试"
    assert wrapped.http_status == 503


def test_from_dict_with_registered_code(clean_registry: None) -> None:
    """from_dict 往返时注册表码的模板 / 状态同样参与解析。"""
    register_code("BIZ_ROUNDTRIP", "往返: {n}", 421)
    d = AppError("BIZ_ROUNDTRIP", params={"n": 1}).to_dict()
    rebuilt = AppError.from_dict(d)
    assert rebuilt.code == "BIZ_ROUNDTRIP"
    assert rebuilt.http_status == 421


# ---------------------------------------------------------------------------
# 12. to_dict(include_cause) cause 序列化
# ---------------------------------------------------------------------------


def test_to_dict_default_has_no_cause_key() -> None:
    """默认 to_dict：不含 cause 键（与历史行为完全一致）。"""
    err = wrap(ValueError("boom"), message="外层")
    d = err.to_dict()
    assert set(d) == {"type", "code", "message", "http_status", "details", "timestamp"}


def test_to_dict_include_cause_none_when_no_cause() -> None:
    """include_cause=True 且无 cause：键存在、值为 None（行为固化）。"""
    d = AppError("SOME_CODE", "无因果").to_dict(include_cause=True)
    assert d["cause"] is None


def test_to_dict_include_cause_content_wrap() -> None:
    """include_cause=True：type / message / traceback 三键齐全且内容正确。"""
    err: AppError | None = None
    try:
        raise ValueError("boom-lizysdk-987654321")  # traceback 中应含此源码行
    except ValueError as inner:
        err = wrap(inner, message="外层错误")
    assert err is not None

    d = err.to_dict(include_cause=True)
    cause = d["cause"]
    assert isinstance(cause, dict)
    assert set(cause) == {"type", "message", "traceback"}
    assert cause["type"] == "ValueError"
    assert cause["message"] == "boom-lizysdk-987654321"
    tb = cause["traceback"]
    assert isinstance(tb, str)
    assert 'raise ValueError("boom-lizysdk-987654321")' in tb  # 含源码行
    assert "ValueError: boom-lizysdk-987654321" in tb  # 含异常终止行
    assert "Traceback (most recent call last)" in tb


def test_to_dict_include_cause_raise_from() -> None:
    """raise ... from ... 建立的 cause 同样可序列化。"""
    with pytest.raises(NotFoundError) as excinfo:
        try:
            raise KeyError("missing-key-42")
        except KeyError as inner:
            raise NotFoundError(message="找不到订单") from inner
    d = excinfo.value.to_dict(include_cause=True)
    assert d["cause"]["type"] == "KeyError"
    assert d["cause"]["message"] == "'missing-key-42'"
    assert "KeyError: 'missing-key-42'" in d["cause"]["traceback"]


def test_to_dict_include_cause_without_traceback() -> None:
    """cause 从未被 raise（无 __traceback__）：仍可序列化，traceback 为字符串。"""
    err = AppError("CODE_X", "消息")
    err.__cause__ = ValueError("从未抛出")
    d = err.to_dict(include_cause=True)
    assert d["cause"]["type"] == "ValueError"
    assert d["cause"]["message"] == "从未抛出"
    assert isinstance(d["cause"]["traceback"], str)


def test_to_dict_include_cause_json_serializable() -> None:
    """include_cause=True 的结果可整体 json.dumps / loads 往返。"""
    err = wrap(RuntimeError("json-roundtrip"), message="外层")
    d = err.to_dict(include_cause=True)
    loaded = json.loads(json.dumps(d))
    assert loaded["cause"] == d["cause"]


def test_from_dict_ignores_cause_key() -> None:
    """from_dict 遇到 cause 键必须忽略不炸，且不还原异常链。"""
    err = wrap(ValueError("会被忽略的 cause"), message="外层消息", details={"k": "v"})
    d = err.to_dict(include_cause=True)
    assert "cause" in d

    rebuilt = AppError.from_dict(d)
    assert type(rebuilt) is AppError
    assert rebuilt.code == err.code
    assert rebuilt.message == err.message
    assert rebuilt.http_status == err.http_status
    assert rebuilt.details == {"k": "v", "original": "会被忽略的 cause", "original_type": "ValueError"}
    assert rebuilt.__cause__ is None  # cause 不还原
    assert "cause" not in rebuilt.to_dict()  # 默认输出也不含 cause


def test_to_dict_include_cause_other_keys_unchanged() -> None:
    """include_cause=True 时原有六键的取值与默认输出完全一致。"""
    err = wrap(ValueError("inner"), message="外层", details={"trace_id": "t-1"})
    plain = err.to_dict()
    with_cause = err.to_dict(include_cause=True)
    assert {k: with_cause[k] for k in plain} == plain
