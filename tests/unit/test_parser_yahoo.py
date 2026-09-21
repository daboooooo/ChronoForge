"""parse_yahoo 解析器单测（MODEL-003.2）。

覆盖 TC-M-014、边界、失败用例。
"""

import pytest

from chronoforge.exceptions import ProviderError
from chronoforge.models.reference import parse_yahoo

# ————————————————————————————————————————————————————————————————
# TC-M-014: 基本解析

def test_tc_m014_basic() -> None:
    """TC-M-014: AAPL → ("AAPL", "AAPL-SPOT", "YAHOO:AAPL:SPOT")。"""
    result = parse_yahoo("AAPL")
    assert result == ("AAPL", "AAPL-SPOT", "YAHOO:AAPL:SPOT")


# ————————————————————————————————————————————————————————————————
# 边界用例

def test_boundary_mixed_case() -> None:
    """边界: 混合大小写 "aapl" → 正确转大写。"""
    result = parse_yahoo("aapl")
    assert result == ("AAPL", "AAPL-SPOT", "YAHOO:AAPL:SPOT")


def test_boundary_with_spaces() -> None:
    """边界: 带空格的 ticker " AAPL " → 正确去空格。"""
    result = parse_yahoo(" AAPL ")
    assert result == ("AAPL", "AAPL-SPOT", "YAHOO:AAPL:SPOT")


def test_boundary_multiple_stocks() -> None:
    """边界: 多个常见 ticker。"""
    for symbol in ["MSFT", "SPY", "GOOGL", "AMZN"]:
        entity_id, instrument_id, market_id = parse_yahoo(symbol)
        assert entity_id == symbol.upper()
        assert instrument_id == f"{symbol.upper()}-SPOT"
        assert market_id == f"YAHOO:{symbol.upper()}:SPOT"


# ————————————————————————————————————————————————————————————————
# 失败用例

def test_failure_empty() -> None:
    """失败: symbol="" → ProviderError。"""
    with pytest.raises(ProviderError):
        parse_yahoo("")


def test_failure_whitespace_only() -> None:
    """失败: symbol="   " → ProviderError。"""
    with pytest.raises(ProviderError):
        parse_yahoo("   ")


def test_failure_none() -> None:
    """失败: symbol=None → ProviderError。"""
    with pytest.raises(ProviderError):
        parse_yahoo(None)  # type: ignore
