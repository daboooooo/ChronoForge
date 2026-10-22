"""DATA-SOURCE coinmetrics 集成测试 — CoinMetricsConnector。

覆盖：
- 响应解析：{"data":[{"asset","time","<Metric>":"<值字符串>"}],"next_page_token"}
- fetch：start_time+end_time 双端必传、起点回退 1 天、窗口客户端过滤、
  分页按 next_page_token 向前回溯、窗口早于 2009-01-01 不发请求
- 错误映射：429 + Retry-After → RateLimitError；401/403 → AuthError；
  400（指标不受支持）→ ProviderError；5xx 非 JSON → ProviderError；
  TransportError；200 非 JSON → SchemaError；data 非 list → SchemaError；
  body.error → ProviderError
- normalize：NUMBER 逐字段断言（observation=D T00:00、
  release=revision=(D+1) T00:00、升序、字符串 value 强转、单位映射）、
  当天未冻结行跳过、坏行 skip
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
from chronoforge.connectors.coinmetrics import CoinMetricsConnector
from chronoforge.connectors.errors import (
    AuthError,
    ProviderError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.models.enums import CanonicalType
from chronoforge.models.macro import NUMBER

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "coinmetrics"

_ENDPOINT = "/v4/timeseries/asset-metrics"


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
) -> tuple[CoinMetricsConnector, MagicMock]:
    mock_client = MagicMock()
    mock_client.get.return_value = resp

    conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
    conn._client = mock_client
    conn._rate_limiter = MagicMock()
    conn._api_key = ""
    conn._http_timeout_s = 30.0
    return conn, mock_client


def _request(
    start: datetime | None = datetime(2026, 9, 20),
    end: datetime | None = datetime(2026, 9, 23),
    params: dict[str, Any] | None = None,
) -> FetchRequest:
    return FetchRequest(
        dataset_id="coinmetrics_usdt_supply",
        params=params or {"asset": "usdt", "metric": "SplyCur"},
        start=start,
        end=end,
        cursor=None,
    )


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    def test_capabilities(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
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
        """start_time/end_time 双端必传；起点回退 1 天；窗口外行被过滤。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, _load_fixture("usdt_supply.json"))
        )

        batches = list(conn.fetch(_request()))

        assert len(batches) == 1
        call = mock_client.get.call_args
        assert call.args[0] == _ENDPOINT
        params = call.kwargs["params"]
        assert params["assets"] == "usdt"
        assert params["metrics"] == "SplyCur"
        assert params["frequency"] == "1d"
        assert params["start_time"] == "2026-09-19"  # 起点回退 1 天
        assert params["end_time"] == "2026-09-23"
        assert params["page_size"] == 10000

        # 窗口 09-20..09-23 + 回退 → 保留 09-20..09-23
        times = [r["time"] for r in batches[0].payload["data"]]
        assert times == [
            "2026-09-20T00:00:00.000000000Z",
            "2026-09-21T00:00:00.000000000Z",
            "2026-09-22T00:00:00.000000000Z",
            "2026-09-23T00:00:00.000000000Z",
        ]
        meta = batches[0].raw_meta
        assert meta["series_id"] == "usdt_SplyCur"  # 默认派生
        assert meta["asset"] == "usdt"
        assert meta["metric"] == "SplyCur"
        assert meta["window_start"] == "2026-09-19"
        assert meta["window_end"] == "2026-09-23"
        assert batches[0].endpoint == _ENDPOINT

    def test_explicit_series_id(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, _load_fixture("usdt_supply.json")))
        batches = list(
            conn.fetch(
                _request(
                    params={
                        "asset": "usdt",
                        "metric": "SplyCur",
                        "series_id": "usdt_supply",
                    }
                )
            )
        )
        assert batches[0].raw_meta["series_id"] == "usdt_supply"

    def test_window_before_oldest_yields_nothing(self) -> None:
        """窗口整体早于 2009-01-01 → 不发请求不产出。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, _load_fixture("usdt_supply.json"))
        )
        batches = list(
            conn.fetch(_request(start=datetime(2008, 1, 1), end=datetime(2008, 6, 1)))
        )
        assert batches == []
        mock_client.get.assert_not_called()

    def test_missing_asset_or_metric_rejected(self) -> None:
        conn, mock_client = _create_mock_connector(
            _mock_response(200, _load_fixture("usdt_supply.json"))
        )
        with pytest.raises(ValueError, match="must contain"):
            list(conn.fetch(_request(params={"metric": "SplyCur"})))
        with pytest.raises(ValueError, match="must contain"):
            list(conn.fetch(_request(params={"asset": "usdt"})))
        mock_client.get.assert_not_called()

    def test_pagination_walks_backwards(self) -> None:
        """首页为窗口尾页，next_page_token 指向更早页 → 逐页回溯取全窗口。"""
        conn, mock_client = _create_mock_connector(
            _mock_response(200, _load_fixture("page_tail.json"))
        )
        head = _mock_response(200, _load_fixture("page_head.json"))
        mock_client.get.side_effect = [mock_client.get.return_value, head]

        batches = list(
            conn.fetch(
                _request(
                    start=datetime(2026, 9, 20),
                    end=datetime(2026, 9, 23),
                    params={"asset": "btc", "metric": "AdrBalCnt"},
                )
            )
        )

        assert mock_client.get.call_count == 2
        # 第二页带 token
        second_params = mock_client.get.call_args_list[1].kwargs["params"]
        assert second_params["next_page_token"] == "0.MjAyNi0wOS0yMlQwMDowMDowMFo"

        times = sorted(r["time"] for r in batches[0].payload["data"])
        assert times == [
            "2026-09-20T00:00:00.000000000Z",
            "2026-09-21T00:00:00.000000000Z",
            "2026-09-22T00:00:00.000000000Z",
            "2026-09-23T00:00:00.000000000Z",
        ]


# ── 错误映射 ───────────────────────────────────────────────────────────


class TestErrorMapping:
    def test_http_429_retry_after_header(self) -> None:
        conn, _ = _create_mock_connector(
            _mock_response(429, {"error": "rate limited"}, headers={"retry-after": "7"})
        )
        with pytest.raises(RateLimitError) as exc_info:
            conn._get_json(_ENDPOINT, {"assets": "usdt"})
        assert exc_info.value.retry_after == 7
        conn._rate_limiter.on_rate_limited.assert_called_once_with(7)

    def test_http_429_without_header_defaults_60(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(429, {"error": "rate limited"}))
        with pytest.raises(RateLimitError) as exc_info:
            conn._get_json(_ENDPOINT, {"assets": "usdt"})
        assert exc_info.value.retry_after == 60

    def test_http_401_auth_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(401, None))
        with pytest.raises(AuthError):
            conn._get_json(_ENDPOINT, {"assets": "usdt"})

    def test_http_403_auth_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(403, None))
        with pytest.raises(AuthError):
            conn._get_json(_ENDPOINT, {"assets": "usdt"})

    def test_http_400_unsupported_metric_provider_error(self) -> None:
        conn, _ = _create_mock_connector(
            _mock_response(400, {"error": "Value 'AdrBalNtv10' is not supported."})
        )
        with pytest.raises(ProviderError, match="400"):
            conn._get_json(_ENDPOINT, {"assets": "btc", "metrics": "AdrBalNtv10"})

    def test_http_500_non_json_provider_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(500, None))
        with pytest.raises(ProviderError, match="500"):
            conn._get_json(_ENDPOINT, {"assets": "usdt"})

    def test_transport_error(self) -> None:
        conn, mock_client = _create_mock_connector(_mock_response(200, {}))
        mock_client.get.side_effect = httpx.ConnectError("boom")
        with pytest.raises(TransportError):
            conn._get_json(_ENDPOINT, {"assets": "usdt"})

    def test_invalid_json_2xx_schema_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, None))
        with pytest.raises(SchemaError):
            conn._get_json(_ENDPOINT, {"assets": "usdt"})

    def test_body_error_provider_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, {"error": "bad request"}))
        with pytest.raises(ProviderError, match="bad request"):
            conn._get_json(_ENDPOINT, {"assets": "usdt"})

    def test_data_not_list_schema_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, {"data": {"unexpected": True}}))
        with pytest.raises(SchemaError, match="data is not a list"):
            list(conn.fetch(_request()))


# ── normalize ──────────────────────────────────────────────────────────


class TestNormalize:
    def test_fields_and_t_plus_1(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        raw = RawBatch(
            endpoint=_ENDPOINT,
            payload=_load_fixture("usdt_supply.json"),
            raw_meta={"series_id": "usdt_supply", "metric": "SplyCur"},
        )
        records = conn.normalize(raw)

        assert len(records) == 4
        dates = [r.observation_time for r in records]
        assert dates == sorted(dates), "输出按 observation 升序"

        rec = records[-1]  # 2026-09-23
        assert isinstance(rec, NUMBER)
        assert rec.source == "coinmetrics"
        assert rec.source_id == "usdt_supply"
        assert rec.observation_time == datetime(2026, 9, 23)
        assert rec.release_time == datetime(2026, 9, 24)
        assert rec.revision_time == rec.release_time
        assert rec.value == 188936405720.293841  # 源为字符串
        assert rec.units == "Tokens"
        assert rec.seasonal_adjustment == "NOT_SEASONALLY_ADJUSTED"
        assert rec.vintage_date == ""
        assert rec.quality_status.value == "VALID"
        assert rec.raw_record_id == "coinmetrics:usdt_supply:2026-09-23"
        assert rec.source_timestamp == datetime(2026, 9, 23)

    def test_units_by_metric(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("btc_addresses.json"),
                raw_meta={"series_id": "btc_addresses_with_balance", "metric": "AdrBalCnt"},
            )
        )
        assert len(records) == 3
        assert all(r.units == "Addresses" for r in records)
        assert records[0].value == 57123456.0

    def test_today_row_skipped(self) -> None:
        """当天行尚未冻结 → 跳过（次日经 fetch 回退重采）。"""
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        today = datetime.now(UTC).replace(tzinfo=None).date()
        payload = {
            "data": [
                {
                    "asset": "usdt",
                    "time": f"{today.isoformat()}T00:00:00.000000000Z",
                    "SplyCur": "123",
                }
            ]
        }
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=payload,
                raw_meta={"series_id": "usdt_supply", "metric": "SplyCur"},
            )
        )
        assert records == []

    def test_edge_rows_skipped(self) -> None:
        """缺值 / 空串 / 非数值 / 坏时间戳 → 跳过；同日期保留首条。"""
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("usdt_supply_edge.json"),
                raw_meta={"series_id": "usdt_supply", "metric": "SplyCur"},
            )
        )
        assert len(records) == 3
        by_date = {r.observation_time.date(): r for r in records}
        assert by_date[datetime(2026, 9, 20).date()].value == 187939371501.594936
        assert by_date[datetime(2026, 9, 24).date()].value == 188936405720.293841
        assert by_date[datetime(2026, 9, 25).date()].value == 999.0
        assert datetime(2026, 9, 21).date() not in by_date
        assert datetime(2026, 9, 22).date() not in by_date
        assert datetime(2026, 9, 23).date() not in by_date

    def test_empty_data(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        assert (
            conn.normalize(
                RawBatch(
                    endpoint=_ENDPOINT,
                    payload=_load_fixture("empty.json"),
                    raw_meta={"series_id": "usdt_supply", "metric": "SplyCur"},
                )
            )
            == []
        )

    def test_non_dict_payload(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        assert (
            conn.normalize(RawBatch(endpoint=_ENDPOINT, payload=["unexpected"], raw_meta={})) == []
        )

    def test_unknown_endpoint_raises(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        with pytest.raises(ValueError, match="unknown endpoint"):
            conn.normalize(RawBatch(endpoint="/other", payload={}, raw_meta={}))


# ── validate / checkpoint ─────────────────────────────────────────────


class TestValidateAndCheckpoint:
    def test_validate_clean(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("usdt_supply.json"),
                raw_meta={"series_id": "usdt_supply", "metric": "SplyCur"},
            )
        )
        report = conn.validate(records)
        assert report.findings == []

    def test_checkpoint_raw(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        assert (
            conn.checkpoint_from(
                RawBatch(
                    endpoint=_ENDPOINT,
                    payload=_load_fixture("usdt_supply.json"),
                    raw_meta={},
                )
            )
            == "2026-09-23"
        )

    def test_checkpoint_records(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
        records = conn.normalize(
            RawBatch(
                endpoint=_ENDPOINT,
                payload=_load_fixture("usdt_supply.json"),
                raw_meta={"series_id": "usdt_supply", "metric": "SplyCur"},
            )
        )
        assert conn.checkpoint_from(records) == "2026-09-23"

    def test_checkpoint_empty(self) -> None:
        conn = CoinMetricsConnector.__new__(CoinMetricsConnector)
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
