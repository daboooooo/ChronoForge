"""treasury_refunding connector（Phase 4，依据实现方案 §3.5）。

抓取财政部 Quarterly Refunding「Most Recent Documents」页中的
"Treasury Announces Marketable Borrowing Estimates" HTML 新闻稿，
正则解析出当前季与下季的：
- privately-held net marketable borrowing estimate（净融资估计）
- end-of-quarter cash balance assumption（现金余额假设）

输出 TREASURY_ANNOUNCEMENT（document_type=BORROWING_ESTIMATE），
publication_time/revision_time = 新闻稿发布日；同季多次公告按
revision_time 版本追加（自然键含 revision_time，不覆盖历史）。

P0 限制（如实记录，方案 §3.5/§14.2）：
- 历史新闻稿归档页为 JS 渲染，无法可靠枚举历史 PIT 版本；
  connector 每次抓取最新新闻稿，季度增量自然累积版本供未来回测；
  Phase 5 回测窗口内 refunding 输入可能缺失，按"数据不足"如实标注。
- 正文解析失败 → SchemaError 熔断；不猜测、不用 LLM 补数（Q-TGA-010）。
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Literal

import httpx

from chronoforge.config.settings import Settings
from chronoforge.connectors.base import (
    CapabilityMatrix,
    FetchRequest,
    HealthStatus,
    RawBatch,
)
from chronoforge.connectors.errors import (
    ChronoForgeError,
    RateLimitError,
    SchemaError,
    TransportError,
    map_http_status,
)
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.quality import QualityReport
from chronoforge.models.treasury import TREASURY_ANNOUNCEMENT

_BASE_URL = "https://home.treasury.gov"
_DOCS_PAGE = (
    "/policy-issues/financing-the-government/quarterly-refunding/"
    "most-recent-quarterly-refunding-documents"
)
_DEFAULT_RATE = 0.1  # req/s（HTML 页面，温和限流）
_DEFAULT_BURST = 3


def _retry_after_seconds(response: httpx.Response) -> int:
    """解析 Retry-After（仅支持秒数；缺省/非法值回退 60s）。"""
    raw = response.headers.get("retry-after")
    if raw is None:
        return 60
    try:
        return max(0, int(raw))
    except ValueError:
        return 60


_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}

# "During the October–December 2026 quarter, Treasury expects to borrow
#  $618 billion in privately-held net marketable debt, assuming an
#  end-of-December cash balance of $850 billion."
_FORECAST_RE = re.compile(
    r"During the (?P<start_month>[A-Za-z]+)[–-](?P<end_month>[A-Za-z]+)\s+"
    r"(?P<year>20\d\d)\s+quarter, Treasury expects to borrow "
    r"\$(?P<borrowing>[\d,.]+)\s+(?P<borrowing_unit>billion|trillion) in "
    r"privately-held net marketable debt, assuming an end-of-"
    r"(?P<cash_month>[A-Za-z]+)\s+cash balance of "
    r"\$(?P<cash>[\d,.]+)\s+(?P<cash_unit>billion|trillion)\.",
    re.I,
)
# 新闻稿标题行 + 发布日期："Treasury Announces Marketable Borrowing
# Estimates November 2, 2026"
_PUB_DATE_RE = re.compile(
    r"Treasury Announces Marketable Borrowing Estimates\s+"
    r"(?P<month>[A-Za-z]+)\s+(?P<day>\d{1,2}),\s+(?P<year>20\d\d)",
    re.I,
)
# 去重锚：新闻稿 URL（如 /news/press-releases/sb0675）。
# most-recent 文档页的锚文本是 "Financing Estimates: 2026 - 4th Quarter"；
# 新闻稿标题（"Treasury Announces Marketable Borrowing Estimates"）在正文页内。
_RELEASE_LINK_RE = re.compile(
    r"<a[^>]+href=\"?(/news/press-releases/[a-z]{2}\d{3,5})\"?[^>]*>"
    r"[^<]*Financing\s+Estimates[^<]*",
    re.I,
)


def _to_millions(value: str, unit: str) -> float:
    amount = float(value.replace(",", ""))
    scale = 1_000_000.0 if unit.lower() == "trillion" else 1_000.0
    return amount * scale


def _month_span(m1: str, m2: str, year: int) -> tuple[datetime, datetime]:
    """月份区间 → (季初 00:00Z, 季末 23:59:59Z)。"""
    start = datetime(year, _MONTHS[m1.lower()], 1)
    end_month = _MONTHS[m2.lower()]
    if end_month == 12:
        end = datetime(year, 12, 31, 23, 59, 59)
    else:
        end = datetime(year, end_month + 1, 1) - timedelta(seconds=1)
    return start, end


class TreasuryRefundingConnector:
    """Quarterly Refunding 文档 connector（HTML 新闻稿解析）。"""

    source_id = "treasury_refunding"

    def __init__(self, settings: Settings | None = None) -> None:
        if settings is None:
            settings = Settings.load()
        self._http_timeout_s = settings.http_timeout_s
        self._client = httpx.Client(
            base_url=_BASE_URL,
            timeout=self._http_timeout_s,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (ChronoForge/treasury_refunding)"},
        )
        self._rate_limiter = RateLimiter(rate=_DEFAULT_RATE, burst=_DEFAULT_BURST)

    # ------------------------------------------------------------------
    # protocol
    # ------------------------------------------------------------------

    def capabilities(self) -> CapabilityMatrix:
        return CapabilityMatrix(
            canonical_types=frozenset({CanonicalType.TREASURY_ANNOUNCEMENT}),
            intervals=frozenset(),
            supports_revision=True,
            supports_websocket=False,
            max_history_days=None,
        )

    def health(self) -> HealthStatus:
        import time

        start = time.monotonic()
        try:
            resp = self._client.get(_DOCS_PAGE)
            if resp.status_code != 200:
                raise SchemaError(
                    f"treasury_refunding: docs page status {resp.status_code}"
                )
            elapsed = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed)
        except Exception as exc:  # noqa: BLE001
            elapsed = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=False, latency_ms=elapsed, detail=str(exc))

    def discover(self) -> list[dict[str, str]]:
        return []

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """抓取 most-recent 文档页与最新借款估计新闻稿（单批次全量）。"""
        self._rate_limiter.acquire()
        try:
            index_resp = self._client.get(_DOCS_PAGE)
        except httpx.TransportError as exc:
            raise TransportError(
                f"treasury_refunding: docs page transport failed: {exc}",
                context={"source": "treasury_refunding", "path": _DOCS_PAGE},
            ) from exc
        if index_resp.status_code != 200:
            raise self._http_error("docs page", _DOCS_PAGE, index_resp)
        index_html = index_resp.text
        link_match = _RELEASE_LINK_RE.search(index_html)
        if not link_match:
            raise SchemaError(
                "treasury_refunding: 未在 most-recent 文档页找到 "
                "Marketable Borrowing Estimates 新闻稿链接（页面结构漂移？）"
            )
        release_path = link_match.group(1)
        self._rate_limiter.acquire()
        try:
            release_resp = self._client.get(release_path)
        except httpx.TransportError as exc:
            raise TransportError(
                f"treasury_refunding: release page transport failed: {exc}",
                context={"source": "treasury_refunding", "path": release_path},
            ) from exc
        if release_resp.status_code != 200:
            raise self._http_error("release page", release_path, release_resp)
        yield RawBatch(
            endpoint=release_path,
            payload={
                "index_url": f"{_BASE_URL}{_DOCS_PAGE}",
                "release_url": f"{_BASE_URL}{release_path}",
                "release_html": release_resp.text,
            },
            raw_meta={
                "http_status": release_resp.status_code,
                "release_path": release_path,
            },
        )

    def _http_error(
        self, label: str, path: str, response: httpx.Response
    ) -> ChronoForgeError:
        """非 200 → D04 §3 错误类；429 同时进入全局冷却（M8/M4）。"""
        status = response.status_code
        context = {"source": "treasury_refunding", "path": path, "status": status}
        if status == 429:
            retry_after = _retry_after_seconds(response)
            self._rate_limiter.on_rate_limited(retry_after)
            return RateLimitError(
                f"treasury_refunding: {label} status 429",
                context=context,
                retry_after=retry_after,
            )
        exc_cls = map_http_status(status)
        return exc_cls(
            f"treasury_refunding: {label} status {status}", context=context
        )

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        payload = raw.payload if isinstance(raw.payload, dict) else {}
        html = str(payload.get("release_html") or "")
        fetched_at = raw.raw_meta.get("fetched_at")
        if not isinstance(fetched_at, datetime):
            fetched_at = datetime.now(UTC).replace(tzinfo=None)

        pub = _PUB_DATE_RE.search(_strip_tags(html))
        if not pub:
            raise SchemaError(
                "treasury_refunding: 新闻稿缺少标题/发布日期（结构漂移？）"
            )
        pub_date = datetime(
            int(pub["year"]), _MONTHS[pub["month"].lower()], int(pub["day"])
        )
        release_time, release_source = _release_time(pub_date, fetched_at)

        records: list[BaseRecord] = []
        for m in _FORECAST_RE.finditer(_strip_tags(html)):
            period_start, period_end = _month_span(
                m["start_month"], m["end_month"], int(m["year"])
            )
            records.append(TREASURY_ANNOUNCEMENT(
                schema_version="1.0",
                source="treasury_refunding",
                source_id="quarterly_refunding",
                source_timestamp=period_start,
                ingest_timestamp=fetched_at,
                raw_record_id=f"treasury_refunding:borrowing_estimate:{pub_date:%Y%m%d}:{period_start:%Y%m%d}",
                quality_status=QualityStatus.VALID,
                quality_reason=None,
                release_time=release_time,
                release_time_source=release_source,
                publication_time=pub_date,
                document_type="BORROWING_ESTIMATE",
                forecast_period_start=period_start.date(),
                forecast_period_end=period_end.date(),
                security_type=None,
                maturity_bucket=None,
                borrowing_estimate_usd_millions=_to_millions(
                    m["borrowing"], m["borrowing_unit"]
                ),
                cash_balance_assumption_usd_millions=_to_millions(
                    m["cash"], m["cash_unit"]
                ),
                financing_requirement_usd_millions=None,
                source_url=str(payload.get("release_url") or ""),
                extraction_method="HTML_TEXT",
                extraction_confidence=0.9,
                revision_time=pub_date,
            ))
        if not records:
            raise SchemaError(
                "treasury_refunding: 新闻稿未解析出任何 borrowing estimate "
                "（正则未命中，结构漂移？Q-TGA-010 告警）"
            )
        return records

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        from chronoforge.models.quality import QualityFinding

        findings: list[QualityFinding] = []
        for record in records:
            if not isinstance(record, TREASURY_ANNOUNCEMENT):
                continue
            key = str(record.natural_key())
            if (
                record.borrowing_estimate_usd_millions is None
                or record.cash_balance_assumption_usd_millions is None
            ):
                findings.append(QualityFinding(
                    record_key=key, rule_id="Q-TGA-010", severity="WARNING",
                    detail="borrowing estimate 或 cash balance 缺失",
                ))
        return QualityReport(findings=findings)

    def row_units(self, raw: RawBatch) -> int | None:
        """单文档源（季度再融资公告）：无行级粒度，返回 None。

        normalize 从整份 HTML 解析公告记录；无法产生记录时抛异常（不静默），
        因此不需要行级丢弃门禁。
        """
        del raw
        return None

    def checkpoint_from(self, raw: RawBatch | list[BaseRecord]) -> str | None:
        """checkpoint = 新闻稿发布日（季度事件语义）。"""
        if isinstance(raw, RawBatch):
            fetched = raw.raw_meta.get("fetched_at")
            return fetched.isoformat() if fetched is not None else None
        if raw:
            pub = max(r.publication_time for r in raw)  # type: ignore[attr-defined]
            return str(pub.isoformat())
        return None

    def close(self) -> None:
        """关闭 HTTP 客户端（与其余连接器一致，供 backfill finally 释放资源）。"""
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass


def _strip_tags(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", text)


def _release_time(
    pub_date: datetime, fetched_at: datetime
) -> tuple[datetime, Literal["OBSERVED", "ESTIMATED"]]:
    """发布时刻：新闻稿日 21:00 UTC 估算（ESTIMATED，方案 §5.7）。"""
    estimated = pub_date.replace(hour=21, minute=0)
    if estimated > fetched_at:
        # 未来发布时刻（不应发生）→ 用抓取时刻
        return fetched_at, "OBSERVED"
    return estimated, "ESTIMATED"
