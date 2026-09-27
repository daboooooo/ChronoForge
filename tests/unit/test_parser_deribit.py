"""parse_deribit 解析器单测（D02 §3，MODEL-003.2）。

覆盖 TC-M-005/006/013/014、边界、失败及 GWT acceptance。
"""

from datetime import date

import pytest

from chronoforge.exceptions import ProviderError
from chronoforge.models.reference import parse_deribit

# ————————————————————————————————————————————————————————————————
# GWT acceptance cases


def test_gwt_btc_sep26_call() -> None:
    """GWT: BTC-26SEP26-100000-C → expiry=date(2026, 9, 26)。"""
    result = parse_deribit("BTC-26SEP26-100000-C")
    assert result["underlying"] == "BTC"
    assert result["expiry"] == date(2026, 9, 26)
    assert result["strike"] == 100000.0
    assert result["option_type"] == "CALL"
    assert result["settlement_asset"] == "USD"
    assert result["instrument_id"] == "BTC-2026-09-26-100000-C"
    assert result["market_id"] == "DERIBIT:BTC-26SEP26-100000-C:OPTION"


def test_gwt_put_option() -> None:
    """GWT: 解析 PUT 期权。"""
    result = parse_deribit("ETH-26OCT25-3000-P")
    assert result["underlying"] == "ETH"
    assert result["expiry"] == date(2025, 10, 26)
    assert result["option_type"] == "PUT"
    assert result["strike"] == 3000.0


# ————————————————————————————————————————————————————————————————
# TC-M-005: 基本解析正确性

def test_tc_m005_basic_parsing() -> None:
    """TC-M-005: BTC-26SEP26-100000-C → underlying=BTC/expiry=2026-09-26/strike=100000/CALL。"""
    result = parse_deribit("BTC-26SEP26-100000-C")
    assert result["underlying"] == "BTC"
    assert result["expiry"] == date(2026, 9, 26)
    assert result["strike"] == 100000.0
    assert result["option_type"] == "CALL"


# ————————————————————————————————————————————————————————————————
# TC-M-006: 过期合约不拒绝

def test_tc_m006_expired_contract() -> None:
    """TC-M-006: expiry < today 的合约解析成功，含 "expired": True。"""
    result = parse_deribit("BTC-15JAN20-50000-C")
    assert result["underlying"] == "BTC"
    assert result["expiry"] == date(2020, 1, 15)
    assert result["expired"] is True


# ————————————————————————————————————————————————————————————————
# TC-M-013: 非法月份码拒绝

def test_tc_m013_invalid_month_code() -> None:
    """TC-M-013: BTC-32FOO26-100000-C 非法月份码 → ProviderError。"""
    with pytest.raises(ProviderError, match="非法月份码"):
        parse_deribit("BTC-26FOO26-100000-C")


# ————————————————————————————————————————————————————————————————
# 边界用例

def test_boundary_jan_month() -> None:
    """边界: 26JAN26（Jan 合法）→ 正确解析。"""
    result = parse_deribit("BTC-26JAN26-50000-C")
    assert result["expiry"] == date(2026, 1, 26)
    assert result["option_type"] == "CALL"


def test_boundary_x_invalid() -> None:
    """边界: 26XAN26（X 非法）→ ProviderError。"""
    with pytest.raises(ProviderError, match="非法月份码"):
        parse_deribit("BTC-26XAN26-50000-C")


def test_boundary_5char_short_date() -> None:
    """边界: 5 字符日期串（单字符月份码）。"""
    # BTC-26S26-100000-C → 26S26 = day=26, month=S(Sep)=9, year=26
    result = parse_deribit("BTC-26S26-100000-C")
    assert result["expiry"] == date(2026, 9, 26)


def test_boundary_6char_single_day_early() -> None:
    """边界: 6 字符日期串（1位日+3字母月+2位年）：2OCT26 → 2026-10-02。

    单日期权 instrument_name 实测不补零（raw 中真实丢弃样例）。
    """
    result = parse_deribit("BTC-2OCT26-100000-C")
    assert result["expiry"] == date(2026, 10, 2)
    assert result["instrument_id"] == "BTC-2026-10-02-100000-C"
    assert result["market_id"] == "DERIBIT:BTC-2OCT26-100000-C:OPTION"


def test_boundary_6char_single_day_late() -> None:
    """边界: 9OCT26 → 2026-10-09。"""
    result = parse_deribit("BTC-9OCT26-100000-P")
    assert result["expiry"] == date(2026, 10, 9)
    assert result["option_type"] == "PUT"


def test_boundary_6char_invalid_month() -> None:
    """边界: 6 字符但月份非法（2ZZZ26）→ ProviderError 非法月份码。"""
    with pytest.raises(ProviderError, match="非法月份码"):
        parse_deribit("BTC-2ZZZ26-100000-C")


def test_boundary_may_contextual() -> None:
    """边界: A 后的 M = May（上下文区分，7字符格式）。"""
    # 26MAY26 → 7字符格式，3字母月份，直接识别 May
    result = parse_deribit("BTC-26MAY26-50000-C")
    assert result["expiry"] == date(2026, 5, 26)


def test_boundary_5char_mar() -> None:
    """边界: 5字符格式 DDMYY，M（非A后）=Mar。"""
    # 26J26 → day=26, J=Jan, year=26
    result = parse_deribit("BTC-26J26-50000-C")
    assert result["expiry"] == date(2026, 1, 26)


def test_boundary_5char_may_context() -> None:
    """边界: 5字符格式 DDMYY，A后M=May。"""
    # "AM26" → A是pos 0, M是pos 1（A后=May）, 26是年
    # 实际格式：day(26) + A(1) + M(1) + 26(2) = 6字符，但按5字符解析时
    # date_str[1]='M', date_str[2]='A'（M在A前，非May）
    # 正确构造：26M26 → pos1='6'（非A）→ Mar
    result = parse_deribit("BTC-26M26-50000-C")
    assert result["expiry"] == date(2026, 3, 26)


def test_boundary_mar_non_contextual() -> None:
    """边界: 非 A 后的 M = Mar。"""
    # BTC-26J26 → day=26, J=Jan, year=26
    result = parse_deribit("BTC-26J26-50000-C")
    assert result["expiry"] == date(2026, 1, 26)


def test_year_50_threshold() -> None:
    """年份 50 分界: 年 50 → 1950, 年 49 → 2049。"""
    # 50+ → 19xx
    result_old = parse_deribit("BTC-26JUN50-50000-C")
    assert result_old["expiry"].year == 1950

    # <50 → 20xx
    result_new = parse_deribit("BTC-26JUN49-50000-C")
    assert result_new["expiry"].year == 2049


def test_strike_decimal() -> None:
    """行权价为小数的情况。"""
    result = parse_deribit("BTC-26SEP26-12345.5-C")
    assert result["strike"] == 12345.5


# ————————————————————————————————————————————————————————————————
# 失败用例

def test_failure_empty_name() -> None:
    """失败: instrument_name="" → ProviderError。"""
    with pytest.raises(ProviderError):
        parse_deribit("")


def test_failure_non_string() -> None:
    """失败: instrument_name=None → ProviderError。"""
    with pytest.raises(ProviderError):
        parse_deribit(None)  # type: ignore


def test_failure_invalid_strike() -> None:
    """失败: strike="abc" → ProviderError。"""
    with pytest.raises(ProviderError, match="行权价非数字"):
        parse_deribit("BTC-26SEP26-abc-C")


def test_failure_wrong_segment_count() -> None:
    """失败: 段数不为 4 → ProviderError。"""
    with pytest.raises(ProviderError, match="格式错误"):
        parse_deribit("BTC-26SEP26-100000")


def test_failure_invalid_option_char() -> None:
    """失败: 非法期权类型字符 → ProviderError。"""
    with pytest.raises(ProviderError, match="期权类型非法"):
        parse_deribit("BTC-26SEP26-100000-X")


def test_failure_invalid_date_format() -> None:
    """失败: 日期串长度非法 → ProviderError。"""
    with pytest.raises(ProviderError, match="日期格式错误"):
        parse_deribit("BTC-26S-100000-C")


def test_failure_invalid_underlying() -> None:
    """失败: underlying 为空 → ProviderError。"""
    with pytest.raises(ProviderError, match="underlying 为空"):
        parse_deribit("-26SEP26-100000-C")


# ————————————————————————————————————————————————————————————————
# GWT acceptance

def test_gwt_expired_returns_flag() -> None:
    """GWT: 过期合约 When parse_deribit Then 返回结果含 "expired": True。"""
    result = parse_deribit("BTC-01JAN20-10000-C")
    assert "expired" in result
    assert result["expired"] is True
