"""时区一致性回归：同一逻辑在 TZ=UTC 与 TZ=Asia/Shanghai 下断言相同结果。

审计 H7：naive UTC datetime 上的 ``.timestamp()`` 会被解释为主机本地时区，
在 UTC+8 主机上整体偏移 -8h（窗口右界提前、未收盘误判、checkpoint 回退）。

覆盖：
1. 窗口下界（startTime）
2. 窗口上界截断（endTime 与翻页停止）
3. 未收盘判定（closeTime > now）
4. checkpoint 游标（_cursor_to_ms / checkpoint_from）
"""

from __future__ import annotations

import calendar
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from chronoforge.connectors.base import FetchRequest, RawBatch
from chronoforge.connectors.binance_futures import (
    BinanceFuturesConnector,
    _cursor_to_ms,
)
from chronoforge.connectors.binance_spot import BinanceSpotConnector
from chronoforge.connectors.ccxt_bridge import CcxtBridgeConnector
from chronoforge.connectors.deribit import DeribitConnector
from chronoforge.connectors.yahoo import YahooConnector
from chronoforge.models.market import OHLCV

# 2024-01-01T00:00:00Z
_EPOCH_2024_MS = 1_704_067_200_000
_EPOCH_2024_S = 1_704_067_200
# 2024-01-01T01:00:00Z
_END_MS = 1_704_070_800_000


@contextmanager
def _force_tz(tz: str) -> Iterator[None]:
    """临时切换进程时区（Unix tzset），退出时恢复。"""
    old = os.environ.get("TZ")
    os.environ["TZ"] = tz
    time.tzset()
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = old
        time.tzset()


@pytest.fixture(params=["UTC", "Asia/Shanghai"])
def tz(request: pytest.FixtureRequest) -> Iterator[str]:
    """同一用例分别在 UTC 与 Asia/Shanghai 下运行。"""
    with _force_tz(request.param):
        yield request.param


def _resp(data: Any) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = data
    resp.headers = {}
    resp.url = "https://api.binance.com/api/v3/klines"
    return resp


def _client(responses: list[MagicMock]) -> MagicMock:
    client = MagicMock()
    queue = list(responses)

    def _side_effect(*args: Any, **kwargs: Any) -> MagicMock:
        if queue:
            return queue.pop(0)
        return _resp([])

    client.get.side_effect = _side_effect
    return client


def _spot(client: MagicMock) -> BinanceSpotConnector:
    with patch.object(BinanceSpotConnector, "__init__", lambda self, s: None):
        conn = BinanceSpotConnector.__new__(BinanceSpotConnector)
    conn._client = client
    conn._rate_limiter = MagicMock()
    return conn


def _futures(client: MagicMock) -> BinanceFuturesConnector:
    with patch.object(BinanceFuturesConnector, "__init__", lambda self, s: None):
        conn = BinanceFuturesConnector.__new__(BinanceFuturesConnector)
    conn._client = client
    conn._rate_limiter = MagicMock()
    return conn


def _record(event_time: datetime, source: str) -> OHLCV:
    return OHLCV(
        schema_version="1.0",
        source=source,
        source_id="1",
        source_timestamp=event_time,
        ingest_timestamp=event_time,
        raw_record_id=f"{source}:test",
        market_id="BINANCE:BTCUSDT",
        event_time=event_time,
        interval="1m",
        open=1.0,
        close=1.0,
        high=1.0,
        low=1.0,
        volume=1.0,
    )


# ── 1. 窗口下界 ────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("factory", "endpoint_params"),
    [
        (_spot, {"symbol": "BTCUSDT", "interval": "1m"}),
        (_futures, {"symbol": "BTCUSDT", "interval": "1m"}),
    ],
)
def test_kline_window_start_is_utc(
    tz: str, factory: Any, endpoint_params: dict[str, str]
) -> None:
    """窗口下界：startTime 必须是 UTC epoch - 一根 K 线回退。"""
    client = _client([_resp([])])
    conn = factory(client)
    req = FetchRequest(
        dataset_id="test",
        params=endpoint_params,
        start=datetime(2024, 1, 1, 0, 0, 0),
        end=datetime(2024, 1, 1, 1, 0, 0),
        cursor=None,
    )
    list(conn.fetch(req))
    params = client.get.call_args_list[0][1]["params"]
    assert params["startTime"] == _EPOCH_2024_MS - 60_000
    assert params["endTime"] == _END_MS


# ── 2. 窗口上界截断 ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("factory", "endpoint_params"),
    [
        (_spot, {"symbol": "BTCUSDT", "interval": "1m"}),
        (_futures, {"symbol": "BTCUSDT", "interval": "1m"}),
    ],
)
def test_kline_pagination_stops_only_at_utc_end(
    tz: str, factory: Any, endpoint_params: dict[str, str]
) -> None:
    """末根 closeTime = end - 1min（仍早于真实上界）时不得提前停止。"""
    # openTime, open, high, low, close, volume, closeTime
    page1 = [[_END_MS - 120_000, "1", "1", "1", "1", "1", _END_MS - 60_000]]
    client = _client([_resp(page1), _resp([])])
    conn = factory(client)
    req = FetchRequest(
        dataset_id="test",
        params=endpoint_params,
        start=datetime(2024, 1, 1, 0, 0, 0),
        end=datetime(2024, 1, 1, 1, 0, 0),
        cursor=None,
    )
    list(conn.fetch(req))
    assert len(client.get.call_args_list) == 2


# ── 3. 未收盘判定 ──────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("factory", "endpoint"),
    [
        (_spot, "/api/v3/klines"),
        (_futures, "/fapi/v1/klines"),
    ],
)
def test_closed_kline_within_utc_tail_is_kept(
    tz: str, factory: Any, endpoint: str
) -> None:
    """1 小时前已收盘的 K 线不得被"未收盘"过滤误删。"""
    now_ms = int(datetime.now(UTC).timestamp() * 1000)
    close_ms = now_ms - 3_600_000
    open_ms = close_ms - 60_000
    payload = [[open_ms, "1", "1", "1", "1", "1", close_ms]]
    conn = factory(MagicMock())
    records = conn.normalize(
        RawBatch(
            endpoint=endpoint,
            payload=payload,
            raw_meta={"interval": "1m", "symbol": "BTCUSDT"},
        )
    )
    assert len(records) == 1


# ── 4. checkpoint 游标 ─────────────────────────────────────────────────


def test_funding_iso_cursor_is_utc(tz: str) -> None:
    """naive ISO cursor 必须按 UTC 解释。"""
    assert _cursor_to_ms("2024-01-01T00:00:00") == _EPOCH_2024_MS


@pytest.mark.parametrize(
    ("factory", "expected"),
    [
        (_spot, str(_EPOCH_2024_MS)),
        (_futures, str(_EPOCH_2024_MS)),
    ],
)
def test_binance_checkpoint_from_record_is_utc(
    tz: str, factory: Any, expected: str
) -> None:
    conn = factory(MagicMock())
    record = _record(datetime(2024, 1, 1, 0, 0, 0), "binance")
    assert conn.checkpoint_from([record]) == expected


def test_deribit_checkpoint_from_record_is_utc(tz: str) -> None:
    conn = DeribitConnector.__new__(DeribitConnector)
    record = _record(datetime(2024, 1, 1, 0, 0, 0), "deribit")
    assert conn.checkpoint_from([record]) == str(_EPOCH_2024_MS)


def test_yahoo_checkpoint_from_record_is_utc(tz: str) -> None:
    conn = YahooConnector.__new__(YahooConnector)
    record = _record(datetime(2024, 1, 1, 0, 0, 0), "yahoo")
    assert conn.checkpoint_from([record]) == str(_EPOCH_2024_S)


def test_ccxt_checkpoint_from_record_is_utc(tz: str) -> None:
    conn = CcxtBridgeConnector.__new__(CcxtBridgeConnector)
    record = _record(datetime(2024, 1, 1, 0, 0, 0), "ccxt")
    assert conn.checkpoint_from([record]) == str(_EPOCH_2024_MS)


# 纯 UTC 换算自检：不依赖当前进程时区
def test_calendar_timegm_reference() -> None:
    assert calendar.timegm(datetime(2024, 1, 1).timetuple()) == _EPOCH_2024_S
