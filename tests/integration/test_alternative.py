"""DATA-SOURCE alternative 集成测试 — AlternativeConnector。

覆盖：
- 响应解析：{"name","data":[{value(字符串),timestamp(Unix秒)}],"metadata"}
- fetch：limit=0 全历史单请求、窗口客户端过滤（含起点回退 1 天）、
  窗口早于 2018-02-01 不发请求
- 错误映射：429 + Retry-After → RateLimitError（含 on_rate_limited）；
  403 → ProviderError；5xx 非 JSON → ProviderError；TransportError；
  200 非 JSON → SchemaError；metadata.error → ProviderError；
  200 data 非 list → SchemaError
- normalize：NUMBER 逐字段断言（observation=D T00:00、
  release=revision=(D+1) T00:00、升序、字符串 value 强转）、
  当天未冻结行（time_until_update）跳过、坏行 skip
- checkpoint_from / capabilities / validate
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from chronoforge.connectors.alternative import AlternativeConnector
from chronoforge.connectors.base import (
    CapabilityMatrix,
    FetchRequest,
    RawBatch,
)
from chronoforge.connectors.errors import (
    ProviderError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.models.enums import CanonicalType
from chronoforge.models.macro import NUMBER

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "alternative"


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
) -> tuple[AlternativeConnector, MagicMock]:
    mock_client = MagicMock()
    mock_client.get.return_value = resp

    conn = AlternativeConnector.__new__(AlternativeConnector)
    conn._client = mock_client
    conn._rate_limiter = MagicMock()
    conn._http_timeout_s = 30.0
    return conn, mock_client


def _request(
    start: datetime | None = datetime(2026, 9, 22),
    end: datetime | None = datetime(2026, 9, 23),
    params: dict[str, Any] | None = None,
) -> FetchRequest:
    return FetchRequest(
        dataset_id="alternative_fng_btc",
        params=params or {"metric": "fear_greed_index", "symbol": "BTC"},
        start=start,
        end=end,
        cursor=None,
    )


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    def test_capabilities(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        caps = conn.capabilities()
        assert isinstance(caps, CapabilityMatrix)
        assert caps.canonical_types == frozenset({CanonicalType.NUMBER})
        assert caps.intervals == frozenset()
        assert caps.supports_revision is False
        assert caps.supports_websocket is False
        assert caps.max_history_days is None


# ── fetch ──────────────────────────────────────────────────────────────


class TestFetch:
    def test_full_history_request_and_window_filter(self) -> None:
        """limit=0 单请求全历史；起点回退 1 天，窗口外行被过滤。"""
        fixture = _load_fixture("fng.json")
        conn, mock_client = _create_mock_connector(_mock_response(200, fixture))

        batches = list(conn.fetch(_request()))

        assert len(batches) == 1
        call = mock_client.get.call_args
        assert call.args[0] == "/fng/"
        assert call.kwargs["params"] == {"limit": 0, "format": "json"}

        # 窗口 09-22..09-23 + 回退 → 保留 09-21/22/23；
        # 09-20 过早、09-24（当天未冻结）过晚均被剔除
        timestamps = {int(r["timestamp"]) for r in batches[0].payload["data"]}
        assert timestamps == {
            1789948800,  # 2026-09-21
            1790035200,  # 2026-09-22
            1790121600,  # 2026-09-23
        }
        meta = batches[0].raw_meta
        assert meta["series_id"] == "fng_btc"
        assert meta["metric"] == "fear_greed_index"
        assert meta["window_start"] == "2026-09-21"
        assert meta["window_end"] == "2026-09-23"
        assert batches[0].endpoint == "/fng/"

    def test_start_none_keeps_full_history(self) -> None:
        """start 缺省 → 下界 2018-02-01，全历史行保留。"""
        fixture = _load_fixture("fng.json")
        conn, _ = _create_mock_connector(_mock_response(200, fixture))

        batches = list(conn.fetch(_request(start=None, end=datetime(2026, 9, 24))))

        # 09-24 当天未冻结行在 fetch 层仍保留（normalize 层才跳过）
        assert len(batches[0].payload["data"]) == 5

    def test_window_before_oldest_yields_nothing(self) -> None:
        """窗口整体早于 2018-02-01 → 不发请求不产出。"""
        conn, mock_client = _create_mock_connector(_mock_response(200, _load_fixture("fng.json")))
        batches = list(
            conn.fetch(
                _request(
                    start=datetime(2017, 1, 1),
                    end=datetime(2017, 6, 1),
                )
            )
        )
        assert batches == []
        mock_client.get.assert_not_called()

    def test_wrong_params_rejected(self) -> None:
        conn, mock_client = _create_mock_connector(_mock_response(200, _load_fixture("fng.json")))
        with pytest.raises(ValueError, match="only metric"):
            list(conn.fetch(_request(params={"metric": "other", "symbol": "BTC"})))
        mock_client.get.assert_not_called()


# ── 错误映射 ───────────────────────────────────────────────────────────


class TestErrorMapping:
    def test_http_429_retry_after_header(self) -> None:
        conn, _ = _create_mock_connector(
            _mock_response(
                429,
                {"error": "rate limited"},
                headers={"retry-after": "7"},
            )
        )
        with pytest.raises(RateLimitError) as exc_info:
            conn._get_json("/fng/", {"limit": 0})
        assert exc_info.value.retry_after == 7
        conn._rate_limiter.on_rate_limited.assert_called_once_with(7)

    def test_http_429_without_header_defaults_60(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(429, {"error": "rate limited"}))
        with pytest.raises(RateLimitError) as exc_info:
            conn._get_json("/fng/", {"limit": 0})
        assert exc_info.value.retry_after == 60

    def test_http_403_provider_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(403, None))
        with pytest.raises(ProviderError, match="403"):
            conn._get_json("/fng/", {"limit": 0})

    def test_http_500_non_json_provider_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(500, None))
        with pytest.raises(ProviderError, match="500"):
            conn._get_json("/fng/", {"limit": 0})

    def test_transport_error(self) -> None:
        conn, mock_client = _create_mock_connector(_mock_response(200, {}))
        mock_client.get.side_effect = httpx.ConnectError("boom")
        with pytest.raises(TransportError):
            conn._get_json("/fng/", {"limit": 0})

    def test_invalid_json_2xx_schema_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, None))
        with pytest.raises(SchemaError):
            conn._get_json("/fng/", {"limit": 0})

    def test_metadata_error_provider_error(self) -> None:
        conn, _ = _create_mock_connector(
            _mock_response(200, {"data": [], "metadata": {"error": "Rate limit exceeded"}})
        )
        with pytest.raises(ProviderError, match="Rate limit"):
            conn._get_json("/fng/", {"limit": 0})

    def test_data_not_list_schema_error(self) -> None:
        conn, _ = _create_mock_connector(_mock_response(200, {"data": {"unexpected": True}}))
        with pytest.raises(SchemaError, match="data is not a list"):
            list(conn.fetch(_request()))


# ── normalize ──────────────────────────────────────────────────────────


class TestNormalize:
    def test_fields_and_t_plus_1(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        raw = RawBatch(
            endpoint="/fng/",
            payload=_load_fixture("fng.json"),
            raw_meta={"series_id": "fng_btc", "metric": "fear_greed_index"},
        )
        records = conn.normalize(raw)

        # 09-24 带 time_until_update（未冻结）→ 跳过；余 4 行
        assert len(records) == 4
        dates = [r.observation_time for r in records]
        assert dates == sorted(dates), "输出按 observation 升序"

        rec = records[-1]  # 2026-09-23
        assert isinstance(rec, NUMBER)
        assert rec.source == "alternative"
        assert rec.source_id == "fng_btc"
        assert rec.observation_time == datetime(2026, 9, 23)
        assert rec.release_time == datetime(2026, 9, 24)
        assert rec.revision_time == rec.release_time
        assert rec.value == 55.0  # 源为字符串 "55"
        assert rec.units == "INDEX_POINTS"
        assert rec.seasonal_adjustment == "NOT_SEASONALLY_ADJUSTED"
        assert rec.vintage_date == ""
        assert rec.quality_status.value == "VALID"
        assert rec.raw_record_id == "alternative:fng_btc:2026-09-23"
        assert rec.source_timestamp == datetime(2026, 9, 23)

    def test_unfrozen_live_row_skipped(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        records = conn.normalize(
            RawBatch(
                endpoint="/fng/",
                payload=_load_fixture("fng.json"),
                raw_meta={"series_id": "fng_btc"},
            )
        )
        dates = {r.observation_time.date() for r in records}
        assert datetime(2026, 9, 24).date() not in dates

    def test_edge_rows_skipped(self) -> None:
        """非 dict / 非数值 / 越界 / 坏时间戳 / 空对象 / 未冻结 → 跳过。"""
        conn = AlternativeConnector.__new__(AlternativeConnector)
        records = conn.normalize(
            RawBatch(
                endpoint="/fng/",
                payload=_load_fixture("fng_edge.json"),
                raw_meta={"series_id": "fng_btc"},
            )
        )
        # 仅 09-22（数值 40）与 09-23（字符串 "50"）冻结合法
        assert len(records) == 2
        by_date = {r.observation_time.date(): r for r in records}
        assert datetime(2026, 9, 22).date() in by_date
        assert by_date[datetime(2026, 9, 23).date()].value == 50.0

    def test_empty_data(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        assert (
            conn.normalize(
                RawBatch(
                    endpoint="/fng/",
                    payload=_load_fixture("fng_empty.json"),
                    raw_meta={"series_id": "fng_btc"},
                )
            )
            == []
        )

    def test_non_dict_payload(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        assert conn.normalize(RawBatch(endpoint="/fng/", payload=["unexpected"], raw_meta={})) == []

    def test_unknown_endpoint_raises(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        with pytest.raises(ValueError, match="unknown endpoint"):
            conn.normalize(RawBatch(endpoint="/other", payload={}, raw_meta={}))


# ── validate / checkpoint ─────────────────────────────────────────────


class TestValidateAndCheckpoint:
    def test_validate_clean(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        records = conn.normalize(
            RawBatch(
                endpoint="/fng/",
                payload=_load_fixture("fng.json"),
                raw_meta={"series_id": "fng_btc"},
            )
        )
        report = conn.validate(records)
        assert report.findings == []

    def test_checkpoint_raw_includes_live_row(self) -> None:
        """raw max 含当天未冻结行（09-24），次日回退重采冻结值。"""
        conn = AlternativeConnector.__new__(AlternativeConnector)
        assert (
            conn.checkpoint_from(
                RawBatch(
                    endpoint="/fng/",
                    payload=_load_fixture("fng.json"),
                    raw_meta={},
                )
            )
            == "2026-09-24"
        )

    def test_checkpoint_records(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        records = conn.normalize(
            RawBatch(
                endpoint="/fng/",
                payload=_load_fixture("fng.json"),
                raw_meta={"series_id": "fng_btc"},
            )
        )
        assert conn.checkpoint_from(records) == "2026-09-23"

    def test_checkpoint_empty(self) -> None:
        conn = AlternativeConnector.__new__(AlternativeConnector)
        assert (
            conn.checkpoint_from(
                RawBatch(
                    endpoint="/fng/",
                    payload=_load_fixture("fng_empty.json"),
                    raw_meta={},
                )
            )
            is None
        )
