"""DATA-SOURCE coinmarketcap 集成测试 — CoinMarketCapConnector。

覆盖：
- 响应解析：{"status":{"error_code":...},"data":{"quotes":[{timestamp, btcDominance,
  quote:[{name:"USD", totalMarketCap}]}]}}
- fetch：timeStart/timeEnd 双端必传、interval=1d、起点回退 1 天、窗口客户端
  过滤、5 年分片 + 片间重叠去重、窗口早于 2013-01-01 不发请求
- 错误映射：429 + Retry-After → RateLimitError；401/403 → AuthError；
  400 → ProviderError；5xx 非 JSON → ProviderError；TransportError；
  200 非 JSON → SchemaError；status.error_code != 0 → ProviderError；
  data.quotes 非 list → SchemaError
- normalize：NUMBER 逐字段断言（observation=D T00:00、
  release=revision=(D+1) T00:00、升序、字符串 value 强转、单位映射）、
  当天盘中滚动行跳过、坏行 skip
- checkpoint_from / capabilities / validate
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from chronoforge.connectors.base import (
    CapabilityMatrix,
    FetchRequest,
    RawBatch,
)
from chronoforge.connectors.coinmarketcap import CoinMarketCapConnector
from chronoforge.connectors.errors import (
    AuthError,
    ProviderError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.models.enums import CanonicalType
from chronoforge.models.macro import NUMBER

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "coinmarketcap"

_ENDPOINT = "/data-api/v3/global-metrics/quotes/historical"


def _load_fixture(name: str) -> Any:
    with open(_FIXTURES / name, encoding="utf-8") as f:
        return json.load(f)


def _mock_response(
    status_code: int = 200,
    json_data: Any = None,
    headers: dict[str, str] | None = None,
) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.headers = headers or {}
    if json_data is None:
        resp.json.side_effect = ValueError("no json")
    else:
        resp.json.return_value = json_data
    return resp


def _create_mock_connector(
    resp: MagicMock,
) -> tuple[CoinMarketCapConnector, MagicMock]:
    mock_client = MagicMock()
    mock_client.get.return_value = resp

    conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
    conn._client = mock_client
    conn._rate_limiter = MagicMock()
    conn._http_timeout_s = 30.0
    return conn, mock_client


def _request(
    start: datetime | None = datetime(2026, 9, 20),
    end: datetime | None = datetime(2026, 9, 23),
    params: dict[str, Any] | None = None,
) -> FetchRequest:
    return FetchRequest(
        dataset_id="coinmarketcap_btc_dominance",
        params=params or {"metric": "btc_dominance"},
        start=start,
        end=end,
        cursor=None,
    )


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    def test_capabilities(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        caps = conn.capabilities()
        assert isinstance(caps, CapabilityMatrix)
        assert caps.canonical_types == frozenset({CanonicalType.NUMBER})
        assert caps.intervals == frozenset()
        assert caps.supports_revision is False
        assert caps.supports_websocket is False
        assert caps.max_history_days is None


# ── fetch ──────────────────────────────────────────────────────────────


class TestFetch:
    def test_request_params_and_window_filter(self) -> None:
        """timeStart/timeEnd 双端必传；起点回退 1 天；窗口外行被过滤。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, _load_fixture("btc_dominance.json"))
        )

        batches = list(conn.fetch(_request()))

        assert len(batches) == 1
        call = mock_client.get.call_args
        assert call.args[0] == _ENDPOINT
        params = call.kwargs["params"]
        assert params["timeStart"] == "2026-09-19"  # 起点回退 1 天
        assert params["timeEnd"] == "2026-09-23"
        assert params["interval"] == "1d"

        # 窗口 09-20..09-23 + 回退 → 保留 09-20..09-23
        stamps = [r["timestamp"] for r in batches[0].payload["data"]["quotes"]]
        assert stamps == [
            "2026-09-20T00:00:00.000Z",
            "2026-09-21T00:00:00.000Z",
            "2026-09-22T00:00:00.000Z",
            "2026-09-23T00:00:00.000Z",
        ]
        meta = batches[0].raw_meta
        assert meta["series_id"] == "btc_dominance"  # 默认同 metric 名
        assert meta["metric"] == "btc_dominance"
        assert meta["window_start"] == "2026-09-19"
        assert meta["window_end"] == "2026-09-23"
        assert batches[0].endpoint == _ENDPOINT

    def test_explicit_series_id(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, _load_fixture("btc_dominance.json")))
        batches = list(
            conn.fetch(
                _request(
                    params={
                        "metric": "btc_dominance",
                        "series_id": "global_btc_market_dominance",
                    }
                )
            )
        )
        assert batches[0].raw_meta["series_id"] == "global_btc_market_dominance"

    def test_unsupported_metric_rejected(self) -> None:
        conn, mock_client = _create_mock_connector(
            _mock_response(200, _load_fixture("btc_dominance.json"))
        )
        with pytest.raises(ValueError, match="unsupported metric"):
            list(conn.fetch(_request(params={"metric": "nope"})))
        mock_client.get.assert_not_called()

    def test_window_before_oldest_yields_nothing(self) -> None:
        """窗口整体早于 2013-01-01 → 不发请求不产出。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, _load_fixture("btc_dominance.json"))
        )
        batches = list(
            conn.fetch(_request(start=datetime(2012, 1, 1), end=datetime(2012, 6, 1)))
        )
        assert batches == []
        mock_client.get.assert_not_called()

    def test_long_window_chunked_and_deduped(self) -> None:
        """超 5 年窗口按片拉取，片间重叠 1 天 → 按 timestamp 去重不重复。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, _load_fixture("btc_dominance.json"))
        )

        batches = list(
            conn.fetch(_request(start=datetime(2013, 1, 1), end=datetime(2026, 10, 20)))
        )

        # 2013-01-01 起每片 1827 天 → 3 片
        assert mock_client.get.call_count == 3
        stamps = [r["timestamp"] for r in batches[0].payload["data"]["quotes"]]
        assert len(stamps) == len(set(stamps)), "重叠片不得产生重复行"
        # 输出按 timestamp 升序
        assert stamps == sorted(stamps)


# ── 错误映射 ───────────────────────────────────────────────────────────


class TestErrorMapping:
    def test_http_429_retry_after_header(self) -> None:
        conn, _ = _create_mock_connector(
            _mock_response(429, {"status": {"error_code": "429"}}, headers={"retry-after": "7"})
        )
        with pytest.raises(RateLimitError) as exc_info:
            conn._get_json(_ENDPOINT, {"timeStart": "2026-09-20"})
        assert exc_info.value.retry_after == 7
        conn._rate_limiter.on_rate_limited.assert_called_once_with(7)

    def test_http_429_without_header_defaults_60(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(429, {"status": {"error_code": "429"}}))
        with pytest.raises(RateLimitError) as exc_info:
            conn._get_json(_ENDPOINT, {"timeStart": "2026-09-20"})
        assert exc_info.value.retry_after == 60

    def test_http_401_auth_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(401, None))
        with pytest.raises(AuthError):
            conn._get_json(_ENDPOINT, {"timeStart": "2026-09-20"})

    def test_http_403_auth_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(403, None))
        with pytest.raises(AuthError):
            conn._get_json(_ENDPOINT, {"timeStart": "2026-09-20"})

    def test_http_400_provider_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(400, {"status": {"error_code": "400"}}))
        with pytest.raises(ProviderError, match="400"):
            conn._get_json(_ENDPOINT, {"timeStart": "2026-09-20"})

    def test_http_500_non_json_provider_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(500, None))
        with pytest.raises(ProviderError, match="500"):
            conn._get_json(_ENDPOINT, {"timeStart": "2026-09-20"})

    def test_transport_error(self) -> None:
        conn, mock_client = _create_mock_connector(_mock_response(200, {}))
        mock_client.get.side_effect = httpx.ConnectError("boom")
        with pytest.raises(TransportError):
            conn._get_json(_ENDPOINT, {"timeStart": "2026-09-20"})

    def test_invalid_json_2xx_schema_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, None))
        with pytest.raises(SchemaError):
            conn._get_json(_ENDPOINT, {"timeStart": "2026-09-20"})

    def test_status_envelope_error_code_provider_error(self) -> None:
        """200 + status.error_code != 0（业务错误信封）→ ProviderError。"""
        conn, _ = _create_mock_connector(
            _mock_response(
                200,
                {
                    "status": {
                        "error_code": "500",
                        "error_message": "Too many items requested. Limit is 2200",
                    }
                },
            )
        )
        with pytest.raises(ProviderError, match="Too many items requested"):
            conn._get_json(_ENDPOINT, {"timeStart": "2013-01-01"})

    def test_quotes_not_list_schema_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, {"data": {"quotes": "oops"}}))
        with pytest.raises(SchemaError, match="quotes is not a list"):
            list(conn.fetch(_request()))


# ── normalize ──────────────────────────────────────────────────────────


class TestNormalize:
    def test_fields_and_t_plus_1(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        raw = RawBatch(
            endpoint=_ENDPOINT,
            payload=_load_fixture("btc_dominance.json"),
            raw_meta={"series_id": "global_btc_market_dominance", "metric": "btc_dominance"},
        )
        records = conn.normalize(raw)

        assert len(records) == 4
        dates = [r.observation_time for r in records]
        assert dates == sorted(dates), "输出按 observation 升序"

        rec = records[-1]  # 2026-09-23
        assert isinstance(rec, NUMBER)
        assert rec.source == "coinmarketcap"
        assert rec.source_id == "global_btc_market_dominance"
        assert rec.observation_time == datetime(2026, 9, 23)
        assert rec.release_time == datetime(2026, 9, 24)
        assert rec.revision_time == rec.release_time
        assert rec.value == 58.9048
        assert rec.units == "Percent"
        assert rec.seasonal_adjustment == "NOT_SEASONALLY_ADJUSTED"
        assert rec.vintage_date == ""
        assert rec.quality_status.value == "VALID"
        assert rec.raw_record_id == "coinmarketcap:global_btc_market_dominance:2026-09-23"
        assert rec.source_timestamp == datetime(2026, 9, 23)

    def test_units_by_metric(self) -> None:
        """市值类指标取 quote[] 中的 USD 报价，单位 USD。"""
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("btc_dominance.json"),
                raw_meta={"series_id": "global_total_market_cap", "metric": "total_market_cap"},
            )
        )
        assert len(records) == 4
        assert all(r.units == "USD" for r in records)
        assert records[-1].value == 2891044125461.98

    def test_eth_dominance_metric(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("btc_dominance.json"),
                raw_meta={"series_id": "global_eth_market_dominance", "metric": "eth_dominance"},
            )
        )
        assert len(records) == 4
        assert all(r.units == "Percent" for r in records)
        assert records[-1].value == 11.3488

    def test_today_row_skipped(self) -> None:
        """当天行仍在盘中滚动 → 跳过（次日经 fetch 回退重采）。"""
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        today = datetime.now(UTC).replace(tzinfo=None).date()
        payload = {
            "data": {
                "quotes": [
                    {
                        "timestamp": f"{today.isoformat()}T00:00:00.000Z",
                        "btcDominance": 60.1,
                    }
                ]
            }
        }
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=payload,
                raw_meta={"series_id": "btc_dominance", "metric": "btc_dominance"},
            )
        )
        assert records == []

    def test_edge_rows_skipped(self) -> None:
        """缺值 / 越界 / 0 占位 / 坏时间戳 → 跳过；字符串数值强转。"""
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("btc_dominance_edge.json"),
                raw_meta={"series_id": "btc_dominance", "metric": "btc_dominance"},
            )
        )
        by_date = {r.observation_time.date(): r for r in records}
        assert by_date[datetime(2026, 9, 20).date()].value == 58.9142
        assert by_date[datetime(2026, 9, 23).date()].value == 58.9048  # 字符串强转
        assert by_date[datetime(2026, 9, 24).date()].value == 58.1
        assert datetime(2026, 9, 21).date() not in by_date  # 缺值
        assert datetime(2026, 9, 22).date() not in by_date  # 越界
        assert datetime(2026, 9, 25).date() not in by_date  # 0 占位（源端缺失）

    def test_empty_data(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        assert (
            conn.normalize(
                RawBatch(
                    endpoint=_ENDPOINT,
                    payload=_load_fixture("empty.json"),
                    raw_meta={"series_id": "btc_dominance", "metric": "btc_dominance"},
                )
            )
            == []
        )

    def test_non_dict_payload(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        assert (
            conn.normalize(RawBatch(endpoint=_ENDPOINT, payload=["unexpected"], raw_meta={})) == []
        )

    def test_unknown_endpoint_raises(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        with pytest.raises(ValueError, match="unknown endpoint"):
            conn.normalize(RawBatch(endpoint="/other", payload={}, raw_meta={}))


# ── validate / checkpoint ─────────────────────────────────────────────


class TestValidateAndCheckpoint:
    def test_validate_clean(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("btc_dominance.json"),
                raw_meta={"series_id": "btc_dominance", "metric": "btc_dominance"},
            )
        )
        report = conn.validate(records)
        assert report.findings == []

    def test_checkpoint_raw(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        assert (
            conn.checkpoint_from(
                RawBatch(
                    endpoint=_ENDPOINT,
                    payload=_load_fixture("btc_dominance.json"),
                    raw_meta={},
                )
            )
            == "2026-09-23"
        )

    def test_checkpoint_records(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("btc_dominance.json"),
                raw_meta={"series_id": "btc_dominance", "metric": "btc_dominance"},
            )
        )
        assert conn.checkpoint_from(records) == "2026-09-23"

    def test_checkpoint_empty(self) -> None:
        conn = CoinMarketCapConnector.__new__(CoinMarketCapConnector)
        assert (
            conn.checkpoint_from(
                RawBatch(
                    endpoint=_ENDPOINT,
                    payload=_load_fixture("empty.json"),
                    raw_meta={},
                )
            )
            is None
        )
