"""NY Fed Markets API connector（SRF 用量 + SOMA 持仓，DATA-SOURCE nyfed）。

数据源：Federal Reserve Bank of New York Markets Data API（公开免 key）

数据集通过 `params.endpoint` 选择端点：
- `rp_results`（默认）：`GET /api/rp/results/search.json?startDate=...&endDate=...&operationTypes=Repo`
- `soma_summary`：`GET /api/soma/summary.json`（SOMA 按券种周频持仓，
  asOfDate 为周三、周四发布；实测 2003-07-09 → 2026-11-04 共 1218 行，
  单位为 USD 原值）

SRF 端点返回每日回购操作结果（SRF，Standing Repo Facility）。实测：
  - 2021-07 起（SRF 于 2021-07-29 启用）每个营业日 1–3 笔操作；
  - 字段 `totalAmtAccepted`（美元）、`operationDate`、`term`、`details`；
  - 2025-12 起 `operationMethod` 由 Multiple Price 改记为 Full Allotment，
    不影响 totalAmtAccepted 口径；
  - 全历史单请求可返回（实测 2021-07 → 2026-11 共 1702 笔）。

本连接器只取 SRF 口径：term == "Overnight" 的回购操作（SRF 仅隔夜），
按 operationDate 汇总 `totalAmtAccepted`，得到每日 SRF 用量（USD）。

排除 Small Value Exercise (SVE)：NY Fed 会在同一端点下发布小额测试操作，
其 `note` 字段含 "Small Value Exercise"（实测变体包括
"Small Value Exercise (SVE)"、"early settling overnight repo small value
exercise (SVE)"、带 `<p>` 包裹等），term 也可能是 Overnight 或 Term。
SVE 不是真实 SRF 用量，必须按 note 排除；`operationId` 尾号不具辨识度
（实测尾号 1/2/4/25/26/28/29/99 均有 SVE）。

时间语义：
- observation_time = operationDate 00:00（操作日）
- release_time = revision_time = 操作日 22:00 UTC（操作 13:30 ET 执行、当晚公布；
  固定值而非墙钟，保证 full-window 重拉时同一观测只产生一个 revision）
- 早于该发布时刻抓取时，源可能返回尚未执行操作的行（totalAmtAccepted=0）。
  这类行按 `not_yet_published` 跳过，不写 canonical；否则占位 0 会在 22:00
  被当成已发布值，污染 PIT（H-N2）。
- value 为源币种 USD 原值；单位标注 USD

continuity_model: TRADING_CALENDAR（仅营业日有操作结果）。
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import structlog

from chronoforge.config.settings import Settings
from chronoforge.connectors.base import (
    CapabilityMatrix,
    DataConnector,
    FetchRequest,
    HealthStatus,
    InstrumentRef,
    RawBatch,
)
from chronoforge.connectors.errors import ProviderError, RateLimitError, SchemaError, TransportError
from chronoforge.connectors.normalize_stats import note_skip
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.macro import NUMBER
from chronoforge.models.quality import QualityFinding, QualityReport

_DEFAULT_RATE = 60.0  # requests per minute（公开端点，无官方限额）
_DEFAULT_BURST = 10

_BASE_URL = "https://markets.newyorkfed.org"
_ENDPOINT = "/api/rp/results/search.json"
_SOMA_ENDPOINT = "/api/soma/summary.json"

_OLDEST_DATE = date(2021, 7, 29)  # SRF 首次操作日（FRED SRFTSYD 起点一致）
_SOMA_OLDEST_DATE = date(2003, 7, 9)  # SOMA summary 实测最早快照
_SERIES_ID = "srf_usage"
_UNITS = "USD"
# SRF 结果公布时刻：操作日 22:00 UTC（13:30 ET 执行、当晚公布）
_SRF_RELEASE_HOUR_UTC = 22
_SEASONAL_ADJUSTMENT = "NOT_SEASONALLY_ADJUSTED"
# 真实响应中 SVE 测试操作的 note 标记（不区分大小写子串匹配）
_SVE_MARKER = "small value exercise"


def _utc_now() -> datetime:
    """当前 UTC（naive）；独立成函数便于测试冻结时间。"""
    return datetime.now(UTC).replace(tzinfo=None)


def _srf_release_time(obs_date: date) -> datetime:
    """SRF 操作日的发布时刻（固定值，保证重拉幂等）。"""
    return datetime.combine(obs_date, datetime.min.time()).replace(
        hour=_SRF_RELEASE_HOUR_UTC
    )


# SOMA summary JSON 字段 → canonical entity（9 个分项，USD 原值）
_SOMA_FIELDS: dict[str, str] = {
    "soma_total": "total",
    "soma_bills": "bills",
    "soma_notesbonds": "notesbonds",
    "soma_frn": "frn",
    "soma_tips": "tips",
    "soma_tips_inflation": "tipsInflationCompensation",
    "soma_mbs": "mbs",
    "soma_cmbs": "cmbs",
    "soma_agencies": "agencies",
}

logger = structlog.get_logger()


def _classify_srf_row(row: object) -> tuple[date, float] | str:
    """SRF 行分类：产出 (观测日, 接受金额) 或跳过原因。

    跳过原因与 `normalize_stats.LEGIT_SKIP_REASONS` 对应：
    - `sve_test_operation` / `non_overnight_operation` 合法（源侧正常行为）
    - 其余（`malformed_row` / `bad_operation_date` / `missing_amount` /
      `non_numeric_amount` / `negative_amount`）不合法，计入行级丢弃率。
    """
    if not isinstance(row, dict):
        return "malformed_row"
    note = row.get("note")
    if isinstance(note, str) and _SVE_MARKER in note.lower():
        return "sve_test_operation"
    # SRF 仅隔夜；Term Repo 不属 SRF 口径
    if str(row.get("term", "")).strip().lower() != "overnight":
        return "non_overnight_operation"
    try:
        obs_date = date.fromisoformat(str(row.get("operationDate")))
    except (ValueError, TypeError):
        return "bad_operation_date"
    raw_amount = row.get("totalAmtAccepted")
    if raw_amount is None:
        return "missing_amount"
    try:
        amount = float(raw_amount)
    except (ValueError, TypeError):
        return "non_numeric_amount"
    if amount < 0:
        return "negative_amount"
    return obs_date, amount


def _classify_soma_row(row: object) -> tuple[date | None, dict[str, float], dict[str, int]]:
    """SOMA 行分类：返回 (asOfDate, 分项值, 跳过原因计数)。

    9 个分项逐个解析：缺失记 `source_missing_value`（合法），非数值记
    `non_numeric_value`（不合法）。整行无法解析时 asOfDate 为 None。
    """
    if not isinstance(row, dict):
        return None, {}, {"malformed_row": 1}
    try:
        as_of = date.fromisoformat(str(row.get("asOfDate")))
    except (ValueError, TypeError):
        return None, {}, {"bad_as_of_date": 1}
    values: dict[str, float] = {}
    reasons: dict[str, int] = {}
    for entity, field in _SOMA_FIELDS.items():
        raw_value = row.get(field)
        if raw_value is None:
            reason = "source_missing_value"
            reasons[reason] = reasons.get(reason, 0) + 1
            continue
        try:
            values[entity] = float(raw_value)
        except (ValueError, TypeError):
            reason = "non_numeric_value"
            reasons[reason] = reasons.get(reason, 0) + 1
    return as_of, values, reasons


class NYFedConnector(DataConnector):
    """NY Fed 回购操作结果 → 每日 SRF 用量（NUMBER）。"""

    source_id = "nyfed"

    def __init__(self, settings: Settings | None = None) -> None:
        if settings is None:
            settings = Settings.load()
        self._http_timeout_s = getattr(settings, "http_timeout_s", 30.0)

        self._client = httpx.Client(
            base_url=_BASE_URL,
            timeout=self._http_timeout_s,
            headers={"User-Agent": "ChronoForge/nyfed"},
            # 本机系统代理对该主机的 TLS 握手会失败（实测），curl 直连正常；
            # 公开端点无需代理，显式禁用环境代理。
            trust_env=False,
        )
        self._rate_limiter = RateLimiter(rate=_DEFAULT_RATE / 60.0, burst=_DEFAULT_BURST)

    # ------------------------------------------------------------------
    # 能力与探活
    # ------------------------------------------------------------------

    def capabilities(self) -> CapabilityMatrix:
        """NUMBER；SRF 日频（2021-07 起）/ SOMA 周频（2003-07 起）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.NUMBER}),
            intervals=frozenset(),
            supports_revision=False,
            supports_websocket=False,
            max_history_days=None,
        )

    def health(self) -> HealthStatus:
        """探活：拉取最近一个月的回购结果。"""
        import time

        start = time.monotonic()
        try:
            today = datetime.now(UTC).date()
            self._get_json(
                {
                    "startDate": (today - timedelta(days=30)).isoformat(),
                    "endDate": today.isoformat(),
                    "operationTypes": "Repo",
                }
            )
            return HealthStatus(ok=True, latency_ms=int((time.monotonic() - start) * 1000))
        except Exception as exc:
            return HealthStatus(
                ok=False, latency_ms=int((time.monotonic() - start) * 1000), detail=str(exc)
            )

    def discover(self) -> list[InstrumentRef]:
        """静态注册（同 fred/alternative），discover 不实现。"""
        return []

    # ------------------------------------------------------------------
    # fetch
    # ------------------------------------------------------------------

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """按窗口拉取数据（`params.endpoint` 选择 SRF 回购结果 / SOMA 持仓）。

        Args:
            request: start/end 为观测日期窗口；两个端点均单请求覆盖全窗口。
        """
        endpoint = str(request.params.get("endpoint", "rp_results"))
        if endpoint == "soma_summary":
            yield self._fetch_soma(request)
            return

        today = datetime.now(UTC).date()
        end_date = request.end.date() if request.end else today
        start_date = request.start.date() if request.start else _OLDEST_DATE
        if end_date < _OLDEST_DATE:
            return

        lo_date = max(start_date, _OLDEST_DATE)
        body = self._get_json(
            _ENDPOINT,
            {
                "startDate": lo_date.isoformat(),
                "endDate": end_date.isoformat(),
                "operationTypes": "Repo",
            },
        )
        rows = body.get("repo", {}).get("operations")
        if not isinstance(rows, list):
            raise SchemaError(
                "nyfed: unexpected response shape (repo.operations is not a list)",
                context={"source": "nyfed", "endpoint": _ENDPOINT},
            )

        yield RawBatch(
            endpoint=_ENDPOINT,
            payload=body,
            raw_meta={
                "http_status": 200,
                "series_id": _SERIES_ID,
                "window_start": lo_date.isoformat(),
                "window_end": end_date.isoformat(),
            },
        )

    def _fetch_soma(self, request: FetchRequest) -> RawBatch:
        """拉取 SOMA summary，并在 fetch 层裁剪到请求窗口（API 无日期参数）。"""
        today = datetime.now(UTC).date()
        end_date = request.end.date() if request.end else today
        start_date = request.start.date() if request.start else _SOMA_OLDEST_DATE
        lo_date = max(start_date, _SOMA_OLDEST_DATE)

        body = self._get_json(_SOMA_ENDPOINT, {})
        rows = body.get("soma", {}).get("summary")
        if not isinstance(rows, list):
            raise SchemaError(
                "nyfed: unexpected response shape (soma.summary is not a list)",
                context={"source": "nyfed", "endpoint": _SOMA_ENDPOINT},
            )

        kept: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                as_of = date.fromisoformat(str(row.get("asOfDate")))
            except (ValueError, TypeError):
                continue
            if lo_date <= as_of <= end_date:
                kept.append(row)

        return RawBatch(
            endpoint=_SOMA_ENDPOINT,
            payload={"soma": {"summary": kept}},
            raw_meta={
                "http_status": 200,
                "series_id": "soma_holdings",
                "window_start": lo_date.isoformat(),
                "window_end": end_date.isoformat(),
            },
        )

    def _get_json(self, endpoint: str, params: dict[str, Any]) -> dict[str, Any]:
        """单次 GET：限流、错误映射、返回 JSON dict。"""
        self._rate_limiter.acquire(1)
        try:
            response = self._client.get(endpoint, params=params)
        except httpx.TransportError as e:
            raise TransportError(
                f"nyfed: request failed on {endpoint}: {e}",
                context={"source": "nyfed", "endpoint": endpoint},
            ) from e

        status_code = response.status_code
        try:
            body = response.json()
        except ValueError as e:
            raise ProviderError(
                f"nyfed: HTTP {status_code} on {endpoint} (non-JSON body)",
                context={"source": "nyfed", "endpoint": endpoint, "status": status_code},
            ) from e

        if status_code == 429:
            self._rate_limiter.on_rate_limited(60)
            raise RateLimitError(
                f"nyfed: rate limited on {endpoint}",
                context={"source": "nyfed", "endpoint": endpoint, "status": status_code},
                retry_after=60,
            )
        if status_code >= 400:
            raise ProviderError(
                f"nyfed: HTTP {status_code} on {endpoint}",
                context={"source": "nyfed", "endpoint": endpoint, "status": status_code},
            )
        if not isinstance(body, dict):
            raise SchemaError(
                f"nyfed: unexpected response shape on {endpoint}",
                context={"source": "nyfed", "endpoint": endpoint, "status": status_code},
            )
        return body

    # ------------------------------------------------------------------
    # normalize / validate
    # ------------------------------------------------------------------

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """按端点分派：SRF 日汇总 / SOMA 周频 9 分项。"""
        if raw.endpoint == _SOMA_ENDPOINT:
            return self._normalize_soma(raw)
        if raw.endpoint != _ENDPOINT:
            raise ValueError(f"nyfed: unknown endpoint: {raw.endpoint}")

        payload = raw.payload
        rows = payload.get("repo", {}).get("operations") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return []

        now = _utc_now()
        by_date: dict[date, float] = {}
        skipped: dict[str, int] = {}

        for row in rows:
            outcome = _classify_srf_row(row)
            if isinstance(outcome, str):
                skipped[outcome] = skipped.get(outcome, 0) + 1
                note_skip(outcome)
                continue
            obs_date, amount = outcome
            # 公布时刻之前源会返回"操作尚未执行"的行（totalAmtAccepted=0）。
            # 不写占位 0：否则该行会在 22:00 被当成已发布值进入 PIT。
            if _srf_release_time(obs_date) > now:
                skipped["not_yet_published"] = skipped.get("not_yet_published", 0) + 1
                note_skip("not_yet_published")
                continue
            by_date[obs_date] = by_date.get(obs_date, 0.0) + amount

        records: list[BaseRecord] = []
        for obs_date in sorted(by_date):
            observation_time = datetime.combine(obs_date, datetime.min.time())
            release_time = _srf_release_time(obs_date)
            records.append(
                NUMBER(
                    schema_version="1.0",
                    source="nyfed",
                    source_id=_SERIES_ID,
                    source_timestamp=observation_time,
                    ingest_timestamp=now,
                    raw_record_id=f"nyfed:{_SERIES_ID}:{obs_date.isoformat()}",
                    quality_status=QualityStatus.VALID,
                    quality_reason=None,
                    observation_time=observation_time,
                    release_time=release_time,
                    revision_time=release_time,
                    value=round(by_date[obs_date], 2),
                    units=_UNITS,
                    seasonal_adjustment=_SEASONAL_ADJUSTMENT,
                    vintage_date="",
                )
            )

        if skipped:
            logger.warning(
                "connector.normalize_summary",
                source="nyfed",
                kept=len(records),
                skipped=sum(skipped.values()),
                reasons=skipped,
            )

        return records

    def _normalize_soma(self, raw: RawBatch) -> list[BaseRecord]:
        """SOMA summary → 9 个分项（USD 原值，周三快照）。

        observation_time = asOfDate（周三）00:00；
        release_time = 次日（周四）23:59:59（保守，覆盖 NY Fed 周四下午发布）。
        """
        payload = raw.payload
        rows = payload.get("soma", {}).get("summary") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return []

        now = datetime.now(UTC).replace(tzinfo=None)
        records: list[BaseRecord] = []
        skipped: dict[str, int] = {}

        for row in rows:
            as_of, values, reasons = _classify_soma_row(row)
            for reason, count in reasons.items():
                skipped[reason] = skipped.get(reason, 0) + count
                note_skip(reason, count)
            if as_of is None:
                continue
            observation_time = datetime.combine(as_of, datetime.min.time())
            release_time = observation_time + timedelta(days=1, hours=23, minutes=59, seconds=59)

            for entity, value in values.items():
                records.append(
                    NUMBER(
                        schema_version="1.0",
                        source="nyfed",
                        source_id=entity,
                        source_timestamp=observation_time,
                        ingest_timestamp=now,
                        raw_record_id=f"nyfed:{entity}:{as_of.isoformat()}",
                        quality_status=QualityStatus.VALID,
                        quality_reason=None,
                        observation_time=observation_time,
                        release_time=release_time,
                        revision_time=release_time,
                        value=round(value, 2),
                        units=_UNITS,
                        seasonal_adjustment=_SEASONAL_ADJUSTMENT,
                        vintage_date="",
                    )
                )

        if skipped:
            logger.warning(
                "connector.normalize_summary",
                source="nyfed",
                endpoint=_SOMA_ENDPOINT,
                kept=len(records),
                skipped=sum(skipped.values()),
                reasons=skipped,
            )

        return records

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        """验证 NUMBER 记录（时间序与金额非负）。"""
        findings: list[QualityFinding] = []
        for record in records:
            if not isinstance(record, NUMBER):
                continue
            key = str(record.natural_key())
            if record.release_time < record.observation_time:
                findings.append(
                    QualityFinding(
                        record_key=key,
                        rule_id="Q-TS-001",
                        severity="ERROR",
                        detail="release_time < observation_time",
                    )
                )
            if record.revision_time < record.observation_time:
                findings.append(
                    QualityFinding(
                        record_key=key,
                        rule_id="Q-TS-002",
                        severity="ERROR",
                        detail="revision_time < observation_time",
                    )
                )
            if record.value is not None and record.value < 0:
                findings.append(
                    QualityFinding(
                        record_key=key,
                        rule_id="Q-VAL-001",
                        severity="ERROR",
                        detail=f"value must be non-negative (got {record.value})",
                    )
                )
        return QualityReport(findings=findings)

    def row_units(self, raw: RawBatch) -> int | None:
        """行单位：SRF ``repo.operations`` / SOMA ``soma.summary`` 数组长度。"""
        payload = raw.payload
        if not isinstance(payload, dict):
            raise SchemaError(
                "nyfed: payload is not a dict",
                context={"source": "nyfed", "endpoint": raw.endpoint},
            )
        if raw.endpoint == _SOMA_ENDPOINT:
            soma = payload.get("soma")
            rows = soma.get("summary") if isinstance(soma, dict) else None
            if not isinstance(rows, list):
                raise SchemaError(
                    "nyfed: soma.summary is not a list",
                    context={"source": "nyfed", "endpoint": raw.endpoint},
                )
            return len(rows)
        repo = payload.get("repo")
        rows = repo.get("operations") if isinstance(repo, dict) else None
        if not isinstance(rows, list):
            raise SchemaError(
                "nyfed: repo.operations is not a list",
                context={"source": "nyfed", "endpoint": raw.endpoint},
            )
        return len(rows)

    def checkpoint_from(self, raw: RawBatch | list[BaseRecord]) -> str | None:
        """checkpoint cursor：与实际产出对齐的最新观测日期（YYYY-MM-DD）。

        - SRF：只取能产出记录的行（排除 SVE / Term / 畸形行），纯 SVE 日 → None
        - SOMA：只取至少有一个可解析分项的行，全缺 → None
        - records：取 observation_time 最大日期
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload if isinstance(raw.payload, dict) else {}
            if raw.endpoint == _SOMA_ENDPOINT:
                rows = payload.get("soma", {}).get("summary")
                if not isinstance(rows, list):
                    return None
                soma_dates: list[date] = []
                for row in rows:
                    as_of, values, _reasons = _classify_soma_row(row)
                    if as_of is not None and values:
                        soma_dates.append(as_of)
                return max(soma_dates).isoformat() if soma_dates else None
            rows = payload.get("repo", {}).get("operations")
            if not isinstance(rows, list):
                return None
            now = _utc_now()
            srf_dates: list[date] = []
            for row in rows:
                outcome = _classify_srf_row(row)
                if not isinstance(outcome, str) and _srf_release_time(outcome[0]) <= now:
                    srf_dates.append(outcome[0])
            return max(srf_dates).isoformat() if srf_dates else None
        if isinstance(raw, list):
            obs = [r.observation_time for r in raw if isinstance(r, NUMBER)]
            return max(obs).strftime("%Y-%m-%d") if obs else None
        return None

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass
