"""InstrumentResolver 单测（MODEL-003.2）。

覆盖 resolve 分发、resolve_deribit、resolve_yahoo、resolve_binance 及 property。
"""

import pytest

from chronoforge.exceptions import ProviderError
from chronoforge.models.reference import InstrumentResolver, parse_deribit, parse_yahoo


@pytest.fixture
def resolver() -> InstrumentResolver:
    return InstrumentResolver()


# ————————————————————————————————————————————————————————————————
# resolve 分发

def test_resolve_deribit_source(resolver: InstrumentResolver) -> None:
    """property: resolve(source="deribit") 正确分发到 resolve_deribit。"""
    result = resolver.resolve("deribit", "BTC-26SEP26-100000-C")
    expected = parse_deribit("BTC-26SEP26-100000-C")
    assert result == expected


def test_resolve_yahoo_source(resolver: InstrumentResolver) -> None:
    """property: resolve(source="yahoo") 正确分发到 resolve_yahoo。"""
    result = resolver.resolve("yahoo", "AAPL")
    expected = parse_yahoo("AAPL")
    assert result == expected


def test_resolve_binance_source(resolver: InstrumentResolver) -> None:
    """property: resolve(source="binance") 正确分发到 parse_binance。"""
    from chronoforge.models.reference import parse_binance

    result = resolver.resolve("binance", "BTCUSDT", "SPOT")
    expected = parse_binance("BTCUSDT", "SPOT")
    assert result == {
        "entity_id": expected[0],
        "instrument_id": expected[1],
        "market_id": expected[2],
    }


def test_resolve_unknown_source(resolver: InstrumentResolver) -> None:
    """property: 未知 source → ProviderError。"""
    with pytest.raises(ProviderError, match="未知数据源"):
        resolver.resolve("unknown", "anything")


def test_resolve_binance_without_market_type(resolver: InstrumentResolver) -> None:
    """property: binance source 无 market_type → ProviderError。"""
    with pytest.raises(ProviderError, match="需要提供 market_type"):
        resolver.resolve("binance", "BTCUSDT")


# ————————————————————————————————————————————————————————————————
# resolve_deribit

def test_resolver_resolve_deribit(resolver: InstrumentResolver) -> None:
    """resolve_deribit 结果与 parse_deribit 一致。"""
    result = resolver.resolve_deribit("BTC-26SEP26-100000-C")
    expected = parse_deribit("BTC-26SEP26-100000-C")
    assert result == expected


# ————————————————————————————————————————————————————————————————
# resolve_yahoo

def test_resolver_resolve_yahoo(resolver: InstrumentResolver) -> None:
    """resolve_yahoo 结果与 parse_yahoo 一致。"""
    result = resolver.resolve_yahoo("AAPL")
    expected = parse_yahoo("AAPL")
    assert result == expected


# ————————————————————————————————————————————————————————————————
# Property: resolve → 底层解析函数结果一致

try:
    from hypothesis import given
    from hypothesis import strategies as st
except ImportError:
    hypothesis_available = False
else:
    hypothesis_available = True

if hypothesis_available:

    @given(
        instrument=st.text(
            min_size=10, max_size=30,
            alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ-0123456789CP",
        ),
    )
    def test_property_resolve_deribit_consistency(instrument: str) -> None:
        """Property: resolver.resolve_deribit 与 parse_deribit 一致。"""
        resolver = InstrumentResolver()
        try:
            expected = parse_deribit(instrument)
        except ProviderError:
            with pytest.raises(ProviderError):
                resolver.resolve_deribit(instrument)
            return

        result = resolver.resolve_deribit(instrument)
        assert result == expected

    @given(
        ticker=st.text(min_size=1, max_size=10, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
    )
    def test_property_resolve_yahoo_consistency(ticker: str) -> None:
        """Property: resolver.resolve_yahoo 与 parse_yahoo 一致。"""
        resolver = InstrumentResolver()
        expected = parse_yahoo(ticker)
        result = resolver.resolve_yahoo(ticker)
        assert result == expected
