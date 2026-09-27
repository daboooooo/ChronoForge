"""DATA-SOURCE-006 集成测试 — FREDConnector。

覆盖：
- TC-C-006：fixture→canonical 逐字段比对（NUMBER）
- TC-M-007：双 vintage 并存（同 observation 两个 vintage → 两条记录共存）
- release 无日历 → 次日 + 附注
- T23:59:59Z 保守规则：release_time 正确
- 边界：value="." (missing) → None 合法
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
from chronoforge.connectors.errors import (
    ProviderError,
    RateLimitError,
)
from chronoforge.connectors.fred import (
    FREDConnector,
    _FREDSettings,
)
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.macro import NUMBER

# Fixture paths
_FIXTURES = Path(__file__).parent.parent / "fixtures" / "fred" / "observations"
_RELEASE_FIXTURES = (
    Path(__file__).parent.parent / "fixtures" / "fred" / "release_dates"
)


# ── Helpers ────────────────────────────────────────────────────────────


def _load_fixture(name: str) -> Any:
    """Load a JSON fixture file."""
    path = _FIXTURES / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_release_fixture(name: str) -> Any:
    """Load a release_dates fixture file."""
    path = _RELEASE_FIXTURES / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _mock_response(
    status_code: int = 200,
    json_data: Any = None,
    headers: dict | None = None,
    url: str = "https://api.stlouisfed.org/fred/series/observations",
) -> MagicMock:
    """Create a mock httpx Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = headers or {}
    resp.url = url
    resp.text = json.dumps(json_data) if json_data else ""
    return resp


def _mock_response_from_dict(
    json_data: Any,
    status_code: int = 200,
    url: str = "https://api.stlouisfed.org/fred/series/observations",
) -> MagicMock:
    """Wrap a dict as a mock httpx Response."""
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.headers = {}
    resp.url = url
    resp.text = json.dumps(json_data) if json_data else ""
    return resp


def _mock_client(
    resp: MagicMock | list[MagicMock] | dict[str, Any],
) -> MagicMock:
    """Create a mock httpx Client that returns the given response(s)."""
    client = MagicMock()
    if isinstance(resp, list):
        queue = list(resp)

        def _side_effect(*args: Any, **kwargs: Any) -> MagicMock:
            if not queue:
                empty = MagicMock(spec=httpx.Response)
                empty.status_code = 200
                empty.json.return_value = {"observations": []}
                empty.headers = {}
                empty.url = ""
                return empty
            return queue.pop(0)

        client.get.side_effect = _side_effect
    elif isinstance(resp, dict):
        # Wrap dict payload as mock Response
        client.get.return_value = _mock_response_from_dict(resp)
    else:
        client.get.return_value = resp
    return client


def _create_mock_connector(
    resp: MagicMock | list[MagicMock] | dict[str, Any],
) -> tuple[FREDConnector, MagicMock]:
    """Create a mock connector instance for fetch/normalize tests."""
    mock_client = _mock_client(resp)
    mock_rate_limiter = MagicMock()

    conn = FREDConnector.__new__(FREDConnector)
    conn._client = mock_client
    conn._rate_limiter = mock_rate_limiter
    conn._api_key = "test_key"

    return conn, mock_client


# ── capabilities ───────────────────────────────────────────────────────


class TestCapabilities:
    """FREDConnector.capabilities() 测试（D04 §4.6）。"""

    def test_capabilities_returns_correct_matrix(self) -> None:
        """Given valid connector When capabilities() Then NUMBER/FLOW/MACRO_EVENT."""
        conn = FREDConnector(_FREDSettings(api_key="test_key"))
        cap = conn.capabilities()

        assert isinstance(cap, CapabilityMatrix)
        assert cap.canonical_types == frozenset({
            CanonicalType.NUMBER,
            CanonicalType.FLOW,
            CanonicalType.MACRO_EVENT,
        })
        assert cap.supports_revision is True
        assert cap.supports_websocket is False
        assert cap.max_history_days is None
        assert len(cap.intervals) == 0  # FRED 无 interval
        conn.close()


# ── TC-C-006: fixture→canonical 逐字段比对 ────────────────────────────


class TestNormalizeHappy:
    """fixture→canonical 逐字段比对（TC-C-006）。"""

    def test_normalize_happy_path(self) -> None:
        """Given observations response When normalize Then NUMBER 逐字段正确。"""
        obs_data = _load_fixture("happy.json")

        conn, _ = _create_mock_connector(obs_data)
        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "UNRATE"},
        )
        records = conn.normalize(raw)

        # 3 条记录
        assert len(records) == 3
        assert all(isinstance(r, NUMBER) for r in records)

        # 逐条校验
        for i, record in enumerate(records):
            # source 和 provenance
            assert record.source == "fred"
            assert record.source_id == "UNRATE"
            assert record.quality_status == QualityStatus.VALID
            assert record.quality_reason is None

            # 时间字段
            obs_date = obs_data["observations"][i]
            expected_obs = datetime(
                year=int(obs_date["date"][:4]),
                month=int(obs_date["date"][5:7]),
                day=int(obs_date["date"][8:10]),
                hour=0, minute=0, second=0,
            )
            expected_release = datetime(
                year=int(obs_date["realtime_start"][:4]),
                month=int(obs_date["realtime_start"][5:7]),
                day=int(obs_date["realtime_start"][8:10]),
                hour=23, minute=59, second=59,
            )

            assert record.observation_time == expected_obs
            assert record.release_time == expected_release
            assert record.revision_time == expected_release
            assert record.vintage_date == obs_date["realtime_start"]

            # value
            expected_value = float(obs_data["observations"][i]["value"])
            assert record.value == expected_value

            conn.close()

    def test_normalize_provenance_fields(self) -> None:
        """normalize output contains all provenance fields."""
        obs_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "UNRATE"},
        )
        records = conn.normalize(raw)

        record = records[0]
        assert record.schema_version == "1.0"
        assert record.source == "fred"
        assert record.source_id == "UNRATE"
        assert record.source_timestamp is not None
        assert record.ingest_timestamp is not None
        assert record.raw_record_id.startswith("fred:UNRATE:")
        conn.close()


class TestSeriesIdFallback:
    """回归：真实 FRED observations 响应不回显 series_id → 从 raw_meta 回退。

    旧代码 payload.get("series_id", "") → source_id="" → SchemaError
    （NK 首列 source_id 为空）。
    """

    def test_series_id_from_raw_meta_when_payload_missing(self) -> None:
        """payload 无 series_id 时用 raw_meta。"""
        obs_data = _load_fixture("happy.json")
        obs_data.pop("series_id", None)  # 模拟真实 API：不回显 series_id

        conn, _ = _create_mock_connector(obs_data)
        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "DGS10"},
        )
        records = conn.normalize(raw)

        assert len(records) > 0
        assert all(r.source_id == "DGS10" for r in records)
        assert all(r.source_id != "" for r in records)
        conn.close()

    def test_payload_series_id_takes_precedence(self) -> None:
        """payload 显式提供 series_id 时优先。"""
        obs_data = _load_fixture("happy.json")
        obs_data["series_id"] = "EXPLICIT"

        conn, _ = _create_mock_connector(obs_data)
        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "DGS10"},
        )
        records = conn.normalize(raw)

        assert len(records) > 0
        assert all(r.source_id == "EXPLICIT" for r in records)
        conn.close()


# ── 快照折叠：单批次同日期多 vintage 仅保留最新（revision_supported=0）──


class TestSnapshotVintageCollapse:
    """快照语义：全 vintage 窗口响应内同 observation 多行 → 折叠当前值。"""

    @staticmethod
    def _normalize(payload: dict[str, Any], series: str = "CPIAUCSL") -> list:
        conn, _ = _create_mock_connector(payload)
        raw = RawBatch(
            endpoint="/series/observations",
            payload=payload,
            raw_meta={"series_id": series},
        )
        records = conn.normalize(raw)
        conn.close()
        return records

    def test_collapses_same_date_to_latest_vintage(self) -> None:
        """同一 observation 两个 vintage → 1 条记录，取最新 vintage 的值。"""
        payload = {
            "series_id": "CPIAUCSL",
            "units": "LIN1",
            "seasonal_adjustment": "SA",
            "observations": [
                {"date": "2022-01-01", "value": "184.5", "realtime_start": "2022-02-10"},
                {"date": "2022-01-01", "value": "184.2", "realtime_start": "2022-06-10"},
                {"date": "2022-02-01", "value": "187.0", "realtime_start": "2022-03-11"},
            ],
        }
        records = self._normalize(payload)

        # 两个不同 observation 日期 → 恰 2 条（折叠掉 1 条旧 vintage）
        assert len(records) == 2
        jan, feb = records
        assert jan.observation_time == datetime(2022, 1, 1)
        assert jan.value == 184.2  # 最新 vintage（2022-06-10）的当前值
        assert jan.vintage_date == "2022-06-10"
        assert jan.revision_time == datetime(2022, 6, 10, 23, 59, 59)
        assert jan.release_time == jan.revision_time
        # 输出按 observation 日期升序
        assert feb.observation_time == datetime(2022, 2, 1)
        assert feb.value == 187.0

    def test_collapse_input_order_independent(self) -> None:
        """vintage 乱序到达时折叠结果一致（按 realtime_start 而非行序）。"""
        payload = {
            "series_id": "CPIAUCSL",
            "observations": [
                {"date": "2022-01-01", "value": "184.2", "realtime_start": "2022-06-10"},
                {"date": "2022-01-01", "value": "184.5", "realtime_start": "2022-02-10"},
            ],
        }
        records = self._normalize(payload)
        assert len(records) == 1
        assert records[0].value == 184.2
        assert records[0].vintage_date == "2022-06-10"

    def test_empty_vintage_loses_to_dated_vintage(self) -> None:
        """后行 realtime_start 为空不覆盖已有真实 vintage。"""
        payload = {
            "series_id": "GDP",
            "observations": [
                {"date": "2023-01-01", "value": "21.5", "realtime_start": "2023-02-03"},
                {"date": "2023-01-01", "value": "21.6", "realtime_start": ""},
            ],
        }
        records = self._normalize(payload, series="GDP")
        assert len(records) == 1
        assert records[0].value == 21.5
        assert records[0].vintage_date == "2023-02-03"

    def test_natural_key_stable_across_reruns(self) -> None:
        """同一全 vintage 响应两次 normalize → 自然键完全一致（重跑即 upsert）。"""
        payload = {
            "series_id": "CPIAUCSL",
            "observations": [
                {"date": "2022-01-01", "value": "184.2", "realtime_start": "2022-06-10"},
                {"date": "2022-02-01", "value": "186.8", "realtime_start": "2022-07-12"},
            ],
        }
        first = self._normalize(payload)
        second = self._normalize(payload)
        assert [r.natural_key() for r in first] == [r.natural_key() for r in second]
        # revision_time 锚定真实 vintage，与采集日无关（不再漂移到当天）
        assert first[0].natural_key()[2] == datetime(2022, 6, 10, 23, 59, 59)

    def test_vintage_before_observation_clamped(self) -> None:
        """IORB 式 ALFRED 预置：vintage 早于观测日 → 时间钳制为观测时点。"""
        payload = {
            "series_id": "IORB",
            "observations": [
                {"date": "2021-07-29", "value": "0.10", "realtime_start": "2021-07-28"},
            ],
        }
        records = self._normalize(payload, series="IORB")
        assert len(records) == 1
        r = records[0]
        assert r.observation_time == datetime(2021, 7, 29)
        # release/revision 不早于观测时点
        assert r.release_time == datetime(2021, 7, 29)
        assert r.revision_time == datetime(2021, 7, 29)
        # 真实 vintage 日保留在 vintage_date
        assert r.vintage_date == "2021-07-28"


# ── TC-M-007: 双 vintage 并存 ────────────────────────────────────────


class TestDoubleVintage:
    """双 vintage 并存测试（TC-M-007）。"""

    def test_two_vintages_coexist(self) -> None:
        """Given two vintage fetches When normalize Then 同 observation 两条记录共存。"""
        vintage1 = _load_fixture("vintage1.json")
        vintage2 = _load_fixture("vintage2.json")

        conn, _ = _create_mock_connector(vintage1)
        raw1 = RawBatch(
            endpoint="/series/observations",
            payload=vintage1,
            raw_meta={"series_id": "CPIAUCSL"},
        )
        records1 = conn.normalize(raw1)

        # 第一条 vintage：3 条记录
        assert len(records1) == 3

        # 第二条 vintage：3 条记录
        conn2, _ = _create_mock_connector(vintage2)
        raw2 = RawBatch(
            endpoint="/series/observations",
            payload=vintage2,
            raw_meta={"series_id": "CPIAUCSL"},
        )
        records2 = conn2.normalize(raw2)
        assert len(records2) == 3

        # 同 observation_date 应该有不同的 revision_time
        # 验证：第一条记录来自 vintage1
        v1_record = records1[0]
        assert v1_record.observation_time == datetime(2022, 1, 1)
        assert v1_record.revision_time == datetime(
            2022, 2, 10, 23, 59, 59
        )
        assert v1_record.value == 184.5  # vintage1 value

        # 第二条记录来自 vintage2
        v2_record = records2[0]
        assert v2_record.observation_time == datetime(2022, 1, 1)
        assert v2_record.revision_time == datetime(
            2022, 6, 10, 23, 59, 59
        )
        assert v2_record.value == 184.2  # vintage2 value

        # natural_key 不同（revision_time 不同）→ 两条独立记录，不互相覆盖
        assert v1_record.natural_key() != v2_record.natural_key()

        conn.close()
        conn2.close()

    def test_two_vintages_different_values(self) -> None:
        """同 observation 但不同 revision_time → 两条独立记录。"""
        vintage1 = _load_fixture("vintage1.json")
        vintage2 = _load_fixture("vintage2.json")

        conn, _ = _create_mock_connector(vintage1)
        raw1 = RawBatch(
            endpoint="/series/observations",
            payload=vintage1,
            raw_meta={"series_id": "CPIAUCSL"},
        )
        records1 = conn.normalize(raw1)

        conn2, _ = _create_mock_connector(vintage2)
        raw2 = RawBatch(
            endpoint="/series/observations",
            payload=vintage2,
            raw_meta={"series_id": "CPIAUCSL"},
        )
        records2 = conn2.normalize(raw2)

        # 第一条记录的 value 应该不同（修订）
        # vintage1 第一条: value=184.5, revision_time=2022-02-10T23:59:59Z
        assert records1[0].value == 184.5
        assert records1[0].revision_time == datetime(
            2022, 2, 10, 23, 59, 59
        )

        # vintage2 第一条: value=184.2, revision_time=2022-06-10T23:59:59Z
        assert records2[0].value == 184.2
        assert records2[0].revision_time == datetime(
            2022, 6, 10, 23, 59, 59
        )

        # natural_key 不同（revision_time 不同）→ 两条独立记录
        assert records1[0].natural_key() != records2[0].natural_key()

        conn.close()
        conn2.close()


# ── T23:59:59Z 保守规则 ─────────────────────────────────────────────


class TestReleaseTimeConservative:
    """release_time T23:59:59Z 保守规则。"""

    def test_release_time_ends_at_23_59_59(self) -> None:
        """Given observations When normalize Then release_time 为 T23:59:59Z。"""
        obs_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "UNRATE"},
        )
        records = conn.normalize(raw)

        for record in records:
            assert record.release_time.hour == 23
            assert record.release_time.minute == 59
            assert record.release_time.second == 59

        conn.close()

    def test_observation_time_starts_at_00_00_00(self) -> None:
        """Given observations When normalize Then observation_time 为 T00:00:00Z。"""
        obs_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "UNRATE"},
        )
        records = conn.normalize(raw)

        for record in records:
            assert record.observation_time.hour == 0
            assert record.observation_time.minute == 0
            assert record.observation_time.second == 0

        conn.close()


# ── release 无日历 → 次日 + 附注 ─────────────────────────────────────


class TestMissingReleaseDate:
    """release 无日历 → observation_day + 1d。"""

    def test_missing_realtime_date_uses_next_day(self) -> None:
        """Given no realtime_date When normalize Then release_time=observation_day+1d。"""
        obs_data = _load_fixture("edge.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "GDP"},
        )
        records = conn.normalize(raw)

        # 第三条记录：realtime_date="" → release_time = observation + 1 day
        third_record = records[2]
        assert third_record.observation_time == datetime(2023, 3, 1)
        assert third_record.release_time == datetime(2023, 3, 2)
        assert third_record.revision_time == datetime(2023, 3, 2)

        conn.close()

    def test_empty_realtime_date_uses_next_day(self) -> None:
        """Given empty realtime_date When normalize Then release_time=observation_day+1d。"""
        obs_data = _load_fixture("edge.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "GDP"},
        )
        records = conn.normalize(raw)

        # 第三条记录：realtime_date=""（空字符串）
        third_record = records[2]
        assert third_record.vintage_date == ""

        conn.close()


# ── 边界：value="." (missing) → None ────────────────────────────────


class TestMissingValue:
    """value="." → None 边界测试。"""

    def test_missing_value_becomes_none(self) -> None:
        """Given value="." When normalize Then value=None。"""
        obs_data = _load_fixture("edge.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "GDP"},
        )
        records = conn.normalize(raw)

        # 第一条和第三条记录的 value="." 或 realtime_date=""
        assert records[0].value is None  # value="."
        assert records[2].value is None  # value="."

        # 其他记录有正常值
        assert records[1].value == 21.5
        assert records[3].value == 22.1

        conn.close()

    def test_missing_value_has_valid_natural_key(self) -> None:
        """Missing value 记录仍具有合法 natural_key。"""
        obs_data = _load_fixture("edge.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "GDP"},
        )
        records = conn.normalize(raw)

        missing = records[0]
        nk = missing.natural_key()
        assert len(nk) == 3
        assert nk[0] == "GDP"
        assert nk[1] == datetime(2023, 1, 1, 0, 0, 0)
        assert nk[2] == datetime(2023, 2, 3, 23, 59, 59)

        conn.close()


# ── 空结果 ──────────────────────────────────────────────────────────


class TestEmptyResult:
    """空结果测试。"""

    def test_empty_observations_returns_zero_records(self) -> None:
        """Given empty observations When normalize Then 0 行。"""
        obs_data = _load_fixture("empty.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "NONEXISTENT"},
        )
        records = conn.normalize(raw)

        assert len(records) == 0
        conn.close()

    def test_missing_observations_key_returns_zero(self) -> None:
        """Payload missing 'observations' key → 0 行。"""
        conn, _ = _create_mock_connector({"series_id": "TEST"})

        raw = RawBatch(
            endpoint="/series/observations",
            payload={"series_id": "TEST"},
            raw_meta={"series_id": "TEST"},
        )
        records = conn.normalize(raw)

        assert len(records) == 0
        conn.close()


# ── 错误路径 ────────────────────────────────────────────────────────


class TestErrorPaths:
    """错误路径测试。"""

    def test_429_raises_rate_limit_error(self) -> None:
        """Given 429 response When fetch Then RateLimitError。"""
        resp_429 = _mock_response(
            status_code=429,
            json_data={"error_message": "Rate limit exceeded"},
        )
        conn, _ = _create_mock_connector(resp_429)

        request = FetchRequest(
            dataset_id="fred:UNRATE",
            params={"series_id": "UNRATE"},
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 1, 2, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        with pytest.raises(RateLimitError):
            list(conn.fetch(request))

        conn.close()

    def test_404_raises_provider_error(self) -> None:
        """Given 404 response When fetch Then ProviderError。"""
        resp_404 = _mock_response(
            status_code=404,
            json_data={"error_message": "Series not found"},
        )
        conn, _ = _create_mock_connector(resp_404)

        request = FetchRequest(
            dataset_id="fred:NONEXISTENT",
            params={"series_id": "NONEXISTENT"},
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 1, 2, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        with pytest.raises(ProviderError):
            list(conn.fetch(request))

        conn.close()

    def test_500_raises_transport_error(self) -> None:
        """Given 500 response When fetch Then TransportError。"""
        resp_500 = _mock_response(
            status_code=500,
            json_data={"error_message": "Internal server error"},
        )
        conn, _ = _create_mock_connector(resp_500)

        request = FetchRequest(
            dataset_id="fred:UNRATE",
            params={"series_id": "UNRATE"},
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 1, 2, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        with pytest.raises(ProviderError):
            list(conn.fetch(request))

        conn.close()

    def test_missing_series_id_raises_value_error(self) -> None:
        """No series_id in params When fetch Then ValueError。"""
        obs_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(obs_data)

        request = FetchRequest(
            dataset_id="fred:test",
            params={},  # 无 series_id
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 1, 2, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        with pytest.raises(ValueError, match="series_id is required"):
            list(conn.fetch(request))

        conn.close()

    def test_unknown_endpoint_raises_value_error(self) -> None:
        """Unknown endpoint When normalize Then ValueError。"""
        conn, _ = _create_mock_connector({})

        raw = RawBatch(
            endpoint="/unknown/endpoint",
            payload={"data": "test"},
            raw_meta={"series_id": "TEST"},
        )

        with pytest.raises(ValueError, match="unknown endpoint"):
            conn.normalize(raw)

        conn.close()


# ── checkpoint ──────────────────────────────────────────────────────


class TestCheckpoint:
    """checkpoint_from 测试。"""

    def test_checkpoint_from_raw_batch(self) -> None:
        """Given RawBatch When checkpoint_from Then last observation date。"""
        obs_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "UNRATE"},
        )

        cursor = conn.checkpoint_from(raw)
        assert cursor == "2023-03-01"  # 最后一条观测日期
        conn.close()

    def test_checkpoint_from_records(self) -> None:
        """Given NUMBER records When checkpoint_from Then last observation date。"""
        obs_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "UNRATE"},
        )
        records = conn.normalize(raw)

        cursor = conn.checkpoint_from(records)
        assert cursor == "2023-03-01"
        conn.close()

    def test_checkpoint_empty_returns_none(self) -> None:
        """Given empty data When checkpoint_from Then None。"""
        obs_data = _load_fixture("empty.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "NONEXISTENT"},
        )
        cursor = conn.checkpoint_from(raw)
        assert cursor is None
        conn.close()

    def test_checkpoint_empty_list_returns_none(self) -> None:
        """Given empty list When checkpoint_from Then None。"""
        conn, _ = _create_mock_connector({})

        cursor = conn.checkpoint_from([])
        assert cursor is None
        conn.close()


# ── validate ────────────────────────────────────────────────────────


class TestValidate:
    """validate 测试（D04 §1，委托 quality.rules）。"""

    def test_validate_valid_records(self) -> None:
        """Given valid NUMBER When validate Then no findings。"""
        obs_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "UNRATE"},
        )
        records = conn.normalize(raw)
        report = conn.validate(records)

        assert len(report.findings) == 0
        conn.close()

    def test_validate_invalid_release_time(self) -> None:
        """release_time < observation_time → finding。"""
        conn, _ = _create_mock_connector({})
        # Use model_construct to bypass pydantic validators
        number = NUMBER.model_construct(
            schema_version="1.0",
            source="fred",
            source_id="TEST",
            source_timestamp=datetime(2023, 6, 1),
            ingest_timestamp=datetime(2023, 6, 2),
            raw_record_id="fred:TEST:2023-06-01",
            quality_status="VALID",
            quality_reason=None,
            observation_time=datetime(2023, 6, 1, 0, 0, 0),
            release_time=datetime(2023, 5, 1, 23, 59, 59),  # < observation
            revision_time=datetime(2023, 6, 1, 23, 59, 59),
            value=1.0,
            units="LIN1",
            seasonal_adjustment="SA",
            vintage_date="2023-06-01",
        )

        report = conn.validate([number])
        assert len(report.findings) == 1
        assert report.findings[0].rule_id == "Q-TS-001"
        assert report.findings[0].severity == "ERROR"
        conn.close()

    def test_validate_invalid_revision_time(self) -> None:
        """revision_time < observation_time → finding。"""
        conn, _ = _create_mock_connector({})
        # Use model_construct to bypass pydantic validators
        number = NUMBER.model_construct(
            schema_version="1.0",
            source="fred",
            source_id="TEST",
            source_timestamp=datetime(2023, 6, 1),
            ingest_timestamp=datetime(2023, 6, 2),
            raw_record_id="fred:TEST:2023-06-01",
            quality_status="VALID",
            quality_reason=None,
            observation_time=datetime(2023, 6, 1, 0, 0, 0),
            release_time=datetime(2023, 6, 1, 23, 59, 59),
            revision_time=datetime(2023, 5, 1, 23, 59, 59),  # < observation
            value=1.0,
            units="LIN1",
            seasonal_adjustment="SA",
            vintage_date="2023-06-01",
        )

        report = conn.validate([number])
        assert len(report.findings) == 1
        assert report.findings[0].rule_id == "Q-TS-002"
        assert report.findings[0].severity == "ERROR"
        conn.close()


# ── fetch ──────────────────────────────────────────────────────────


class TestFetch:
    """fetch 测试。"""

    def test_fetch_with_params(self) -> None:
        """Given valid request When fetch Then yields RawBatch。"""
        obs_data = _load_fixture("happy.json")
        conn, _ = _create_mock_connector(obs_data)

        request = FetchRequest(
            dataset_id="fred:UNRATE",
            params={"series_id": "UNRATE"},
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 3, 31, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        batches = list(conn.fetch(request))
        assert len(batches) == 1
        assert batches[0].endpoint == "/series/observations"
        assert batches[0].payload == obs_data
        conn.close()

    def test_fetch_with_observation_window(self) -> None:
        """fetch 应正确传递 observation_start/end。"""
        obs_data = _load_fixture("happy.json")
        mock_client = MagicMock()
        mock_client.get.return_value = _mock_response_from_dict(obs_data)

        conn, _ = _create_mock_connector(obs_data)
        conn._client = mock_client

        request = FetchRequest(
            dataset_id="fred:UNRATE",
            params={"series_id": "UNRATE"},
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 3, 31, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )

        list(conn.fetch(request))

        # 验证调用参数
        call_args = mock_client.get.call_args
        params = call_args.kwargs.get("params", call_args[1].get("params", {}))
        assert params["observation_start"] == "2023-01-01"
        assert params["observation_end"] == "2023-03-31"
        conn.close()

    def test_fetch_sets_realtime_floor(self) -> None:
        """fetch 固定传 realtime_start=1776-07-04 以取回真实 vintage。"""
        from chronoforge.connectors.fred import _REALTIME_START_FLOOR

        obs_data = _load_fixture("happy.json")
        mock_client = MagicMock()
        mock_client.get.return_value = _mock_response_from_dict(obs_data)

        conn, _ = _create_mock_connector(obs_data)
        conn._client = mock_client

        request = FetchRequest(
            dataset_id="fred:UNRATE",
            params={"series_id": "UNRATE"},
            start=datetime(2023, 1, 1, tzinfo=UTC).replace(tzinfo=None),
            end=datetime(2023, 3, 31, tzinfo=UTC).replace(tzinfo=None),
            cursor=None,
        )
        list(conn.fetch(request))

        call_args = mock_client.get.call_args
        params = call_args.kwargs.get("params", call_args[1].get("params", {}))
        assert params["realtime_start"] == _REALTIME_START_FLOOR
        conn.close()

    def test_fetch_paginates_realtime_windows(self) -> None:
        """vintage 日期 >2000 → 切互不相交 realtime 窗口，合并后折叠为当前值。"""
        from datetime import timedelta

        from chronoforge.connectors.fred import (
            _OBS_VINTAGE_WINDOW,
            _VINTAGEDATES_PAGE_SIZE,
        )

        base = datetime(2021, 1, 1)
        vdates = [
            (base + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(_OBS_VINTAGE_WINDOW + 1)
        ]
        window0_end = vdates[_OBS_VINTAGE_WINDOW - 1]
        boundary = vdates[_OBS_VINTAGE_WINDOW]

        def _dispatcher(endpoint: str, params: dict[str, Any] | None = None):
            params = params or {}
            if endpoint == "/series/vintagedates":
                offset = int(params.get("offset", 0))
                page = vdates[offset:offset + _VINTAGEDATES_PAGE_SIZE]
                return _mock_response_from_dict(
                    {"count": len(vdates), "vintage_dates": page}
                )
            # observations：两个窗口各回同一 observation 的不同 vintage 行
            if params.get("realtime_end") == window0_end:
                rows = [
                    {"date": "2021-01-01", "value": "1.0", "realtime_start": vdates[0]},
                    {"date": "2021-01-01", "value": "1.1", "realtime_start": vdates[1]},
                ]
            else:
                assert params.get("realtime_start") == boundary
                assert "realtime_end" not in params
                rows = [
                    {"date": "2021-01-01", "value": "1.2", "realtime_start": boundary},
                    {"date": "2021-02-01", "value": "2.0", "realtime_start": boundary},
                ]
            return _mock_response_from_dict({"observations": rows})

        mock_client = MagicMock()
        mock_client.get.side_effect = _dispatcher
        conn, _ = _create_mock_connector({})
        conn._client = mock_client

        request = FetchRequest(
            dataset_id="fred:DGS10",
            params={"series_id": "DGS10"},
            start=datetime(2021, 1, 1),
            end=datetime(2021, 3, 1),
            cursor=None,
        )
        batches = list(conn.fetch(request))
        assert len(batches) == 1
        raw = batches[0]
        assert raw.raw_meta["realtime_windows"] == 2

        records = conn.normalize(raw)
        assert len(records) == 2  # 两个 observation 日期
        assert records[0].observation_time == datetime(2021, 1, 1)
        assert records[0].value == 1.2  # 末窗口内最新 vintage
        assert records[0].vintage_date == boundary
        conn.close()

    def test_fetch_dedupes_overlapping_window_rows(self) -> None:
        """窗口边界返回的重复 (date,realtime_start) 行合并时只保留一份。"""
        from datetime import timedelta

        from chronoforge.connectors.fred import _OBS_VINTAGE_WINDOW

        base = datetime(2021, 1, 1)
        vdates = [
            (base + timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(_OBS_VINTAGE_WINDOW + 1)
        ]

        def _dispatcher(endpoint: str, params: dict[str, Any] | None = None):
            params = params or {}
            if endpoint == "/series/vintagedates":
                return _mock_response_from_dict(
                    {"count": len(vdates), "vintage_dates": vdates[:1000]}
                    if params.get("offset", 0) == 0
                    else {"count": len(vdates), "vintage_dates": vdates[1000:]}
                )
            # 同一行在两个窗口边界都出现
            return _mock_response_from_dict(
                {"observations": [
                    {"date": "2021-01-01", "value": "1.0", "realtime_start": vdates[0]},
                ]}
            )

        mock_client = MagicMock()
        mock_client.get.side_effect = _dispatcher
        conn, _ = _create_mock_connector({})
        conn._client = mock_client

        raw = list(
            conn.fetch(
                FetchRequest(
                    dataset_id="fred:DGS10",
                    params={"series_id": "DGS10"},
                    start=datetime(2021, 1, 1),
                    end=datetime(2021, 3, 1),
                    cursor=None,
                )
            )
        )[0]
        assert len(raw.payload["observations"]) == 1
        conn.close()


# ── GWT: Given 同 observation 修订 Then (nk,revision_time) 追加不覆盖 ─


class TestGWT:
    """Given-When-Then 验收测试。"""

    def test_gwt_revision_appends_not_overwrites(self) -> None:
        """Given 同 observation 修订 When normalize Then (nk,revision_time) 追加不覆盖。"""
        vintage1 = _load_fixture("vintage1.json")
        vintage2 = _load_fixture("vintage2.json")

        conn1, _ = _create_mock_connector(vintage1)
        raw1 = RawBatch(
            endpoint="/series/observations",
            payload=vintage1,
            raw_meta={"series_id": "CPIAUCSL"},
        )
        records1 = conn1.normalize(raw1)

        conn2, _ = _create_mock_connector(vintage2)
        raw2 = RawBatch(
            endpoint="/series/observations",
            payload=vintage2,
            raw_meta={"series_id": "CPIAUCSL"},
        )
        records2 = conn2.normalize(raw2)

        # 两条 vintage 都有 3 条记录
        assert len(records1) == 3
        assert len(records2) == 3

        # 同 observation_date 的记录有不同 revision_time → 独立存储
        v1_nk = records1[0].natural_key()
        v2_nk = records2[0].natural_key()

        # 不同 revision_time → natural_key 不同 → 不会覆盖
        assert v1_nk[2] != v2_nk[2]

        conn1.close()
        conn2.close()

    def test_gwt_missing_release_uses_next_day(self) -> None:
        """Given release 无日历 When normalize Then release_time=observation_day+1d。"""
        obs_data = _load_fixture("edge.json")
        conn, _ = _create_mock_connector(obs_data)

        raw = RawBatch(
            endpoint="/series/observations",
            payload=obs_data,
            raw_meta={"series_id": "GDP"},
        )
        records = conn.normalize(raw)

        # 第三条记录：realtime_date="" → release_time = observation + 1 day
        missing_record = records[2]
        expected_release = datetime(2023, 3, 2)
        assert missing_record.release_time == expected_release

        conn.close()
