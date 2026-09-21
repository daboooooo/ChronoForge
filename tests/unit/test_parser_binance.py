"""parse_binance 解析器单测（D02 §3 表，MODEL-003.1）。

覆盖 TC-M-004/011/012、边界、失败、property 及 GWT acceptance。
"""

import pytest

from chronoforge.exceptions import ProviderError
from chronoforge.models.reference import EntityResolver, parse_binance

# ————————————————————————————————————————————————————————————————
# TC-M-004 / TC-M-011 / TC-M-012：GWT acceptance cases

def test_tc_m004_btcusdt_spot() -> None:
    """TC-M-004: BTCUSDT SPOT → ("BTC", "BTC-SPOT", "BINANCE:BTCUSDT:SPOT")。"""
    entity_id, instrument_id, market_id = parse_binance("BTCUSDT", "SPOT")
    assert entity_id == "BTC"
    assert instrument_id == "BTC-SPOT"
    assert market_id == "BINANCE:BTCUSDT:SPOT"


def test_tc_m011_ethusdt_usdt_fut() -> None:
    """TC-M-011: ETHUSDT USDT-FUT → ("ETH", "ETH-PERP", "BINANCE:ETHUSDT:USDT-FUT")。"""
    entity_id, instrument_id, market_id = parse_binance("ETHUSDT", "USDT-FUT")
    assert entity_id == "ETH"
    assert instrument_id == "ETH-PERP"
    assert market_id == "BINANCE:ETHUSDT:USDT-FUT"


def test_tc_m012_bnbusdt_spot() -> None:
    """TC-M-012: BNBUSDT SPOT → ("BNB", "BNB-SPOT", "BINANCE:BNBUSDT:SPOT")。"""
    entity_id, instrument_id, market_id = parse_binance("BNBUSDT", "SPOT")
    assert entity_id == "BNB"
    assert instrument_id == "BNB-SPOT"
    assert market_id == "BINANCE:BNBUSDT:SPOT"


# ————————————————————————————————————————————————————————————————
# 边界用例

def test_boundary_2letter_entity_ausdt() -> None:
    """边界: symbol="AUSDT"（entity="A"，长度1<2→拒绝）。"""
    with pytest.raises(ProviderError):
        parse_binance("AUSDT", "SPOT")


def test_boundary_usdt_rejected() -> None:
    """边界: symbol="USDT"（无entity）→ ProviderError。"""
    try:
        parse_binance("USDT", "SPOT")
    except ProviderError as exc:
        assert "无法解析 Binance symbol: USDT" in str(exc)
    else:
        pytest.fail("parse_binance(\"USDT\", \"SPOT\") 应抛 ProviderError")


def test_boundary_hyphenated_symbol() -> None:
    """边界: hyphenated symbol "BTC-USDT" → 正确解析。"""
    entity_id, instrument_id, market_id = parse_binance("BTC-USDT", "SPOT")
    assert entity_id == "BTC"
    assert instrument_id == "BTC-SPOT"
    assert market_id == "BINANCE:BTC-USDT:SPOT"


def test_boundary_coin_fut() -> None:
    """边界: COIN-FUT → PERP instrument_type。"""
    entity_id, instrument_id, market_id = parse_binance("BTCUSDT", "COIN-FUT")
    assert entity_id == "BTC"
    assert instrument_id == "BTC-PERP"
    assert market_id == "BINANCE:BTCUSDT:COIN-FUT"


def test_boundary_short_symbol_btc() -> None:
    """边界: symbol="BTC"（< 4字符）→ ProviderError。"""
    try:
        parse_binance("BTC", "SPOT")
    except ProviderError as exc:
        assert "无法解析 Binance symbol: BTC" in str(exc)
    else:
        pytest.fail("parse_binance(\"BTC\", \"SPOT\") 应抛 ProviderError")


# ————————————————————————————————————————————————————————————————
# 失败用例

def test_failure_empty_symbol() -> None:
    """失败: symbol="" → ProviderError。"""
    try:
        parse_binance("", "SPOT")
    except ProviderError as exc:
        assert "无法解析 Binance symbol: " in str(exc)
    else:
        pytest.fail("parse_binance(\"\", \"SPOT\") 应抛 ProviderError")


def test_failure_invalid_symbol() -> None:
    """失败: symbol="INVALID"（无识别后缀）→ ProviderError。"""
    try:
        parse_binance("INVALID", "SPOT")
    except ProviderError as exc:
        assert "无法解析 Binance symbol: INVALID" in str(exc)
    else:
        pytest.fail('parse_binance("INVALID", "SPOT") 应抛 ProviderError')


def test_failure_invalid_market_type() -> None:
    """失败: market_type="FUTURES"（未识别）→ ProviderError。"""
    try:
        parse_binance("BTCUSDT", "FUTURES")
    except ProviderError as exc:
        assert "无法解析 Binance symbol: BTCUSDT" in str(exc)
    else:
        pytest.fail('parse_binance("BTCUSDT", "FUTURES") 应抛 ProviderError')


# ————————————————————————————————————————————————————————————————
# EntityResolver 集成

def test_entity_resolver_resolve() -> None:
    """EntityResolver.resolve 结果与 parse_binance 一致。"""
    resolver = EntityResolver()
    source = "binance_spot"
    symbol = "ETHUSDT"
    market_type = "USDT-FUT"

    result = resolver.resolve(source, symbol, market_type)
    expected = parse_binance(symbol, market_type)
    assert result == expected


# ————————————————————————————————————————————————————————————————
# Property: parse_binance → EntityResolver.resolve 结果一致

try:
    from hypothesis import given
    from hypothesis import strategies as st
except ImportError:
    hypothesis_available = False
else:
    hypothesis_available = True

if hypothesis_available:

    @given(
        symbol=st.text(min_size=4, max_size=30, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ-"),
        market_type=st.sampled_from(["SPOT", "USDT-FUT", "COIN-FUT"]),
    )
    def test_property_resolver_consistency(symbol: str, market_type: str) -> None:
        """Property: EntityResolver.resolve 与 parse_binance 结果一致。"""
        resolver = EntityResolver()
        try:
            expected = parse_binance(symbol, market_type)
        except ProviderError:
            # parse_binance 失败时，resolve 也应失败（同抛 ProviderError）
            with pytest.raises(ProviderError):
                resolver.resolve("test", symbol, market_type)
            return

        result = resolver.resolve("test", symbol, market_type)
        assert result == expected
