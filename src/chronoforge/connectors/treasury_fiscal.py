"""U.S. Treasury Fiscal Data connector（TGA 子系统 Phase 1）。

覆盖：
- DTS Table I  operating_cash_balance
- DTS Table II deposits_withdrawals_operating_cash
- DTS Table IIIA public_debt_transactions
- DTS Table IIIB adjustment_public_debt_transactions_cash_basis
- DTS Table IV inter_agency_tax_transfers
- DTS Table V  income_tax_refunds_issued
- Treasury auctions: auctions_query / upcoming_auctions

设计要点（依据 docs/strategy/TGA前瞻模型-实现方案.md）：
- 分页 `page[size]` + `page[number]`，逐页到 `meta.total-pages`；
- 原始响应原样进 RawBatch，不落盘、不写库（DataConnector 协议）；
- 金额保留 original_value/original_unit，normalize 统一为 USD millions；
- DTS 金额本身为 USD millions；拍卖金额为 USD，需 /1e6；
- Table I 只取 TGA 行（不含 Federal Reserve Account / Tax and Loan Note Accounts）；
  实测 TGA Closing Balance 行余额落在 `open_today_bal`，`close_today_bal` 为 null；
- Table II 只取 account_type=TGA；Table IV/V 标 internal/duplicate，不计入净现金流；
- Table IIIA/IIIB 的 table_nbr 实测有 "III-A"/"IIIA"、"III-B"/"IIIB" 变体，统一归一；
- Table V 的 table_nbr 早期为 "VI"，统一按端点归为 "V"；
- 拍卖结果字段在结果发布前可能为 null；`data_status` 区分 ANNOUNCED/RESULT；
- release_time 优先使用 raw.raw_meta.fetched_at（近期数据），历史回填用估算发布日，
  并标记 ESTIMATED，供 PIT 回测过滤。
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Literal

import httpx
import structlog

from chronoforge.config.settings import Settings
from chronoforge.config.update_schedule import publish_time_utc
from chronoforge.connectors.base import (
    CapabilityMatrix,
    DataConnector,
    FetchRequest,
    HealthStatus,
    InstrumentRef,
    RawBatch,
)
from chronoforge.connectors.errors import (
    AuthError,
    ProviderError,
    RateLimitError,
    SchemaError,
    TransportError,
)
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.enums import CanonicalType
from chronoforge.models.quality import QualityFinding, QualityReport
from chronoforge.models.treasury import (
    TREASURY_ACCOUNT_BALANCE,
    TREASURY_AUCTION,
    TREASURY_CASH_FLOW,
    TREASURY_DEBT_TRANSACTION,
)

logger = structlog.get_logger()

_BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
_ENDPOINTS = {
    "operating_cash_balance": "/v1/accounting/dts/operating_cash_balance",
    "deposits_withdrawals": "/v1/accounting/dts/deposits_withdrawals_operating_cash",
    "public_debt_transactions": "/v1/accounting/dts/public_debt_transactions",
    "public_debt_cash_basis": (
        "/v1/accounting/dts/adjustment_public_debt_transactions_cash_basis"
    ),
    "inter_agency_tax_transfers": "/v1/accounting/dts/inter_agency_tax_transfers",
    "income_tax_refunds": "/v1/accounting/dts/income_tax_refunds_issued",
    "auctions_query": "/v1/accounting/od/auctions_query",
    "upcoming_auctions": "/v1/accounting/od/upcoming_auctions",
}

_TABLE_CANONICAL_TYPE: dict[str, str] = {
    "operating_cash_balance": "TREASURY_ACCOUNT_BALANCE",
    "deposits_withdrawals": "TREASURY_CASH_FLOW",
    "public_debt_transactions": "TREASURY_DEBT_TRANSACTION",
    "public_debt_cash_basis": "TREASURY_DEBT_TRANSACTION",
    "inter_agency_tax_transfers": "TREASURY_CASH_FLOW",
    "income_tax_refunds": "TREASURY_CASH_FLOW",
    "auctions_query": "TREASURY_AUCTION",
    "upcoming_auctions": "TREASURY_AUCTION",
}

_PAGE_SIZE = 10_000
_MAX_PAGES = 2_000
_DTS_TABLES = frozenset({
    "operating_cash_balance",
    "deposits_withdrawals",
    "public_debt_transactions",
    "public_debt_cash_basis",
    "inter_agency_tax_transfers",
    "income_tax_refunds",
})

# 字段漂移熔断：每张表的最小必需字段集合（来自 Phase 0 实测；只列 normalizer
# 必需字段，可选结果字段不在此列，避免把尚未公布的拍卖结果误判为漂移）。
_REQUIRED_FIELDS: dict[str, frozenset[str]] = {
    "operating_cash_balance": frozenset({
        "record_date", "account_type", "open_today_bal", "close_today_bal",
        "src_line_nbr",
    }),
    "deposits_withdrawals": frozenset({
        "record_date", "account_type", "transaction_type", "transaction_catg",
        "transaction_today_amt", "src_line_nbr",
    }),
    "public_debt_transactions": frozenset({
        "record_date", "transaction_type", "security_market", "security_type",
        "transaction_today_amt", "src_line_nbr",
    }),
    "public_debt_cash_basis": frozenset({
        "record_date", "transaction_type", "adj_type", "adj_today_amt",
        "src_line_nbr",
    }),
    "inter_agency_tax_transfers": frozenset({
        "record_date", "classification", "today_amt", "src_line_nbr",
    }),
    "income_tax_refunds": frozenset({
        "record_date", "tax_refund_type", "tax_refund_today_amt", "src_line_nbr",
    }),
    "auctions_query": frozenset({
        "security_type", "security_term", "announcemt_date", "auction_date",
        "issue_date", "offering_amt",
    }),
    "upcoming_auctions": frozenset({
        "security_type", "security_term", "announcemt_date", "auction_date",
        "issue_date", "offering_amt",
    }),
}

# Table I 的 2022-04-18 之后四行结构：账户行名 → balance_type。
_TGA_FOUR_ROW_BALANCE: dict[
    str, Literal["OPENING", "CLOSING", "TOTAL_DEPOSITS", "TOTAL_WITHDRAWALS"]
] = {
    "Treasury General Account (TGA) Opening Balance": "OPENING",
    "Treasury General Account (TGA) Closing Balance": "CLOSING",
    "Total TGA Deposits (Table II)": "TOTAL_DEPOSITS",
    "Total TGA Withdrawals (Table II) (-)": "TOTAL_WITHDRAWALS",
}

# Table I 单行结构（≤2022-04-15）：一行同时携带 open_today_bal/close_today_bal。
_TGA_SINGLE_ROW_ACCOUNTS = frozenset({
    "Treasury General Account (TGA)",
    # 2021-09-30 及以前，TGA 在 DTS 中沿用的历史行名。
    "Federal Reserve Account",
})

_DEFAULT_RATE = 60.0 / 60.0
_DEFAULT_BURST = 10


def _is_tga_account(account_type: str) -> bool:
    """是否为 TGA 账户行（含 2021-10 前的 Federal Reserve Account 历史名称）。"""
    normalized = account_type.strip().lower()
    return "tga" in normalized or normalized == "federal reserve account"


def _text(value: Any) -> str | None:
    """API 的 'null' 字符串 → None；其余转 str。"""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "null":
        return None
    return text


def _float(value: Any) -> float | None:
    text = _text(value)
    if text is None:
        return None
    try:
        number = float(text.replace(",", ""))
    except (ValueError, TypeError):
        return None
    return number if math.isfinite(number) else None


def _date(value: Any) -> date | None:
    text = _text(value)
    if text is None:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _obs_time(record_date: date) -> datetime:
    return datetime.combine(record_date, datetime.min.time())


def _next_business_day(value: date) -> date:
    nxt = value + timedelta(days=1)
    while nxt.weekday() >= 5:  # 周六/周日
        nxt += timedelta(days=1)
    return nxt


def _release_time(
    record_date: date,
    fetched_at: datetime | None,
    *,
    prefer_observed: bool = True,
    publish_hour: int = 0,
    publish_minute: int = 0,
) -> tuple[datetime, str]:
    """返回 (release_time, release_time_source)。

    DTS 官方通常次日发布；历史回填没有真实发布时间，统一用
    record_date 的下一个联邦工作日 + 配置发布时刻，标 ESTIMATED。
    近期抓取且 fetched_at 已晚于估算发布时刻时，用 fetched_at 标 OBSERVED。
    """
    estimated = datetime.combine(
        _next_business_day(record_date), time(publish_hour, publish_minute)
    )
    if prefer_observed and fetched_at is not None:
        fetched_date = fetched_at.date()
        if fetched_date > record_date and abs((fetched_date - record_date).days) <= 7:
            if fetched_at > estimated:
                return fetched_at, "OBSERVED"
    return estimated, "ESTIMATED"


def _normalize_table_code(value: str | None, *, default: str) -> str:
    text = (value or "").upper().replace("-", "").replace(" ", "")
    if text in {"IIIA", "IIIB"}:
        return text
    return default


def _classify_cash_flow(raw_category: str, direction: str) -> dict[str, Any]:
    """DTS Table II 原始分类 → 归一分类。

    Phase 1 内置确定性映射；Phase 2 由 config/tga/taxonomy.yaml 覆盖并版本化。
    """
    cat = raw_category.lower()
    if "inter-agency" in cat or "inter agency" in cat:
        return {
            "category": "INTERNAL_TRANSFER", "flow_type": "INTERNAL",
            "direction": "INTERNAL", "include_in_net": False,
            "is_internal": True, "confidence": 0.9,
        }
    if "refund" in cat:
        return {
            "category": "TAX_REFUND", "flow_type": "REFUND",
            "direction": direction, "include_in_net": True,
            "is_internal": False, "confidence": 0.9,
        }
    if any(k in cat for k in ("tax", "withheld", "customs", "excise", "futa", "fica")):
        return {
            "category": "TAX", "flow_type": "TAX",
            "direction": direction, "include_in_net": True,
            "is_internal": False, "confidence": 0.9,
        }
    if any(k in cat for k in ("public debt", "issue", "redemption", "borrow")):
        flow = "DEBT_ISSUANCE" if direction == "INFLOW" else "DEBT_REDEMPTION"
        return {
            "category": flow, "flow_type": flow,
            "direction": direction, "include_in_net": True,
            "is_internal": False, "confidence": 0.85,
        }
    if "interest" in cat:
        return {
            "category": "INTEREST", "flow_type": "INTEREST",
            "direction": direction, "include_in_net": True,
            "is_internal": False, "confidence": 0.9,
        }
    if any(k in cat for k in ("social security", "medicare", "medicaid", "veterans")):
        return {
            "category": "BENEFIT", "flow_type": "BENEFIT",
            "direction": direction, "include_in_net": True,
            "is_internal": False, "confidence": 0.85,
        }
    return {
        "category": "OTHER", "flow_type": "OPERATING",
        "direction": direction, "include_in_net": True,
        "is_internal": False, "confidence": 0.5,
    }


class TreasuryFiscalConnector(DataConnector):
    """Fiscal Data API connector。"""

    source_id = "treasury_fiscal"

    def __init__(self, settings: Settings | None = None) -> None:
        if settings is None:
            settings = Settings.load()
        self._http_timeout_s = settings.http_timeout_s
        self._client = httpx.Client(
            base_url=_BASE_URL,
            timeout=self._http_timeout_s,
            headers={"User-Agent": "ChronoForge/treasury_fiscal"},
        )
        self._rate_limiter = RateLimiter(rate=_DEFAULT_RATE, burst=_DEFAULT_BURST)

    # ------------------------------------------------------------------
    # protocol
    # ------------------------------------------------------------------

    def capabilities(self) -> CapabilityMatrix:
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.TREASURY_ACCOUNT_BALANCE,
                CanonicalType.TREASURY_CASH_FLOW,
                CanonicalType.TREASURY_DEBT_TRANSACTION,
                CanonicalType.TREASURY_AUCTION,
            }),
            intervals=frozenset(),
            supports_revision=True,
            supports_websocket=False,
            max_history_days=None,
        )

    def health(self) -> HealthStatus:
        import time
        start = time.monotonic()
        try:
            body = self._get_page(
                _ENDPOINTS["operating_cash_balance"],
                table="operating_cash_balance",
                start_date=None,
                end_date=None,
                sort="-record_date",
                size=1,
                page=1,
            )
            if not isinstance(body.get("data"), list):
                raise SchemaError("treasury_fiscal: health response missing data")
            elapsed = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=True, latency_ms=elapsed)
        except Exception as exc:  # noqa: BLE001
            elapsed = int((time.monotonic() - start) * 1000)
            return HealthStatus(ok=False, latency_ms=elapsed, detail=str(exc))

    def discover(self) -> list[InstrumentRef]:
        return []

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        table = str(request.params.get("table", "")).strip()
        if table not in _ENDPOINTS:
            raise ValueError(
                f"treasury_fiscal: unknown table {table!r}; "
                f"expected one of {sorted(_ENDPOINTS)}"
            )
        endpoint = _ENDPOINTS[table]
        start_date = request.start.date() if request.start else None
        end_date = request.end.date() if request.end else None
        sort = "record_date"

        page = 1
        while page <= _MAX_PAGES:
            body = self._get_page(
                endpoint,
                table=table,
                start_date=start_date,
                end_date=end_date,
                sort=sort,
                size=_PAGE_SIZE,
                page=page,
            )
            meta = body.get("meta")
            meta_dict: dict[str, Any] = meta if isinstance(meta, dict) else {}
            total_pages = meta_dict.get("total-pages")
            yield RawBatch(
                endpoint=endpoint,
                payload=body,
                raw_meta={
                    "http_status": 200,
                    "table": table,
                    "page": page,
                    "total_pages": total_pages,
                    "window_start": start_date.isoformat() if start_date else None,
                    "window_end": end_date.isoformat() if end_date else None,
                },
            )
            if not isinstance(total_pages, int) or page >= total_pages:
                break
            page += 1

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        table = str(raw.raw_meta.get("table", "")).strip()
        payload = raw.payload if isinstance(raw.payload, dict) else {}
        raw_rows = payload.get("data")
        rows: list[Any] = raw_rows if isinstance(raw_rows, list) else []
        fetched_at = raw.raw_meta.get("fetched_at")
        if not isinstance(fetched_at, datetime):
            fetched_at = None

        if table == "operating_cash_balance":
            return self._normalize_balance(rows, fetched_at)
        if table == "deposits_withdrawals":
            return self._normalize_cash_flow(rows, fetched_at, source_table="II")
        if table in ("public_debt_transactions", "public_debt_cash_basis"):
            return self._normalize_debt(rows, fetched_at, table)
        if table == "inter_agency_tax_transfers":
            return self._normalize_cash_flow(rows, fetched_at, source_table="IV")
        if table == "income_tax_refunds":
            return self._normalize_cash_flow(rows, fetched_at, source_table="V")
        if table in ("auctions_query", "upcoming_auctions"):
            return self._normalize_auction(rows, fetched_at, table)
        raise ValueError(f"treasury_fiscal: unknown table {table!r}")

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        findings: list[QualityFinding] = []
        for record in records:
            key = str(record.natural_key())  # type: ignore[attr-defined]
            if isinstance(record, TREASURY_AUCTION):
                if record.maturity_date and record.maturity_date <= record.issue_date:
                    findings.append(QualityFinding(
                        record_key=key, rule_id="Q-TGA-008", severity="ERROR",
                        detail="maturity_date <= issue_date",
                    ))
                if record.issue_date < record.announcement_date:
                    findings.append(QualityFinding(
                        record_key=key, rule_id="Q-TGA-008", severity="ERROR",
                        detail="issue_date < announcement_date",
                    ))
                if record.data_status == "RESULT" and record.total_accepted_usd_millions is None:
                    findings.append(QualityFinding(
                        record_key=key, rule_id="Q-TGA-009", severity="WARNING",
                        detail="RESULT status but total_accepted is null",
                    ))
                elif (
                    record.total_accepted_usd_millions is None
                    and record.issue_date < date.today()
                ):
                    findings.append(QualityFinding(
                        record_key=key, rule_id="Q-TGA-009", severity="WARNING",
                        detail="auction result missing after issue_date",
                    ))
        return QualityReport(findings=findings)

    def checkpoint_from(self, raw: RawBatch | list[BaseRecord]) -> str | None:
        if isinstance(raw, RawBatch):
            payload = raw.payload if isinstance(raw.payload, dict) else {}
            raw_rows = payload.get("data")
            rows: list[Any] = raw_rows if isinstance(raw_rows, list) else []
            dates = [
                d for row in rows
                if isinstance(row, dict) and (d := _date(row.get("record_date"))) is not None
            ]
            if dates:
                return max(dates).isoformat()
        elif isinstance(raw, list):
            obs = [
                r.observation_time for r in raw
                if isinstance(r, (TREASURY_ACCOUNT_BALANCE, TREASURY_CASH_FLOW,
                                  TREASURY_DEBT_TRANSACTION))
            ]
            if obs:
                return max(obs).strftime("%Y-%m-%d")
            auctions = [r.auction_date for r in raw if isinstance(r, TREASURY_AUCTION)]
            if auctions:
                return max(auctions).isoformat()
        return None

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    def _get_page(
        self,
        endpoint: str,
        *,
        table: str,
        start_date: date | None,
        end_date: date | None,
        sort: str,
        size: int,
        page: int,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "page[size]": size,
            "page[number]": page,
            "sort": sort,
            "format": "json",
        }
        filters: list[str] = []
        if start_date is not None:
            filters.append(f"record_date:gte:{start_date.isoformat()}")
        if end_date is not None:
            filters.append(f"record_date:lte:{end_date.isoformat()}")
        if filters:
            params["filter"] = ",".join(filters)

        self._rate_limiter.acquire(1)
        try:
            response = self._client.get(endpoint, params=params)
        except httpx.TransportError as exc:
            raise TransportError(
                f"treasury_fiscal: request failed on {endpoint}: {exc}",
                context={"source": "treasury_fiscal", "endpoint": endpoint},
            ) from exc

        status = response.status_code
        if status == 429:
            retry_after = int(float(response.headers.get("retry-after", "60")))
            self._rate_limiter.on_rate_limited(retry_after)
            raise RateLimitError(
                f"treasury_fiscal: rate limited on {endpoint}",
                context={"source": "treasury_fiscal", "endpoint": endpoint, "status": status},
                retry_after=retry_after,
            )
        if status in (401, 403):
            raise AuthError(
                f"treasury_fiscal: access denied on {endpoint}",
                context={"source": "treasury_fiscal", "endpoint": endpoint, "status": status},
            )
        if status >= 400:
            raise ProviderError(
                f"treasury_fiscal: HTTP {status} on {endpoint}",
                context={"source": "treasury_fiscal", "endpoint": endpoint, "status": status},
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise SchemaError(
                f"treasury_fiscal: invalid JSON on {endpoint}",
                context={"source": "treasury_fiscal", "endpoint": endpoint, "status": status},
            ) from exc
        if not isinstance(body, dict):
            raise SchemaError(
                f"treasury_fiscal: unexpected response shape on {endpoint}",
                context={"source": "treasury_fiscal", "endpoint": endpoint},
            )
        if not isinstance(body.get("data"), list):
            raise SchemaError(
                f"treasury_fiscal: response missing data list on {endpoint}",
                context={"source": "treasury_fiscal", "endpoint": endpoint},
            )
        rows = body["data"]
        required = _REQUIRED_FIELDS.get(table)
        if required and rows and isinstance(rows[0], dict):
            missing = sorted(required - set(rows[0]))
            if missing:
                raise SchemaError(
                    f"treasury_fiscal: schema drift on {table}, missing fields {missing}",
                    context={
                        "source": "treasury_fiscal",
                        "endpoint": endpoint,
                        "table": table,
                        "missing_fields": missing,
                    },
                )
        return body

    # ------------------------------------------------------------------
    # normalizers
    # ------------------------------------------------------------------

    def _base_fields(
        self,
        *,
        source_table: str,
        record_date: date,
        src_line: int,
        fetched_at: datetime | None,
    ) -> dict[str, Any]:
        publish = publish_time_utc("treasury_fiscal")
        release_time, release_source = _release_time(
            record_date,
            fetched_at,
            publish_hour=publish.hour,
            publish_minute=publish.minute,
        )
        return {
            "schema_version": "1.0",
            "source": "treasury_fiscal",
            "source_id": "TGA",
            "source_timestamp": _obs_time(record_date),
            "ingest_timestamp": datetime.now(UTC).replace(tzinfo=None),
            "raw_record_id": (
                f"treasury_fiscal:{source_table}:{record_date.isoformat()}:{src_line}"
            ),
            "quality_status": "VALID",
            "quality_reason": None,
            "observation_time": _obs_time(record_date),
            "release_time": release_time,
            "release_time_source": release_source,
        }

    def _normalize_balance(
        self, rows: list[Any], fetched_at: datetime | None
    ) -> list[BaseRecord]:
        records: list[BaseRecord] = []
        skipped = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            record_date = _date(row.get("record_date"))
            account_type = _text(row.get("account_type"))
            if record_date is None or account_type is None:
                skipped += 1
                continue
            if not _is_tga_account(account_type):
                skipped += 1
                continue
            src_line = int(_float(row.get("src_line_nbr")) or 0)
            base = self._base_fields(
                source_table="I", record_date=record_date,
                src_line=src_line, fetched_at=fetched_at,
            )

            # 2022-04-18+：四行结构，Closing/Total 行金额落在 open_today_bal。
            balance_type = _TGA_FOUR_ROW_BALANCE.get(account_type)
            if balance_type is not None:
                amount = _float(row.get("close_today_bal"))
                if amount is None:
                    amount = _float(row.get("open_today_bal"))
                if amount is None:
                    skipped += 1
                    continue
                to_emit = [(balance_type, amount)]
            elif account_type.strip() in _TGA_SINGLE_ROW_ACCOUNTS:
                # ≤2022-04-15：单行结构，同一行同时携带 opening/closing。
                opening = _float(row.get("open_today_bal"))
                closing = _float(row.get("close_today_bal"))
                if opening is None and closing is None:
                    skipped += 1
                    continue
                to_emit = []
                if opening is not None:
                    to_emit.append(("OPENING", opening))
                if closing is not None:
                    to_emit.append(("CLOSING", closing))
            else:
                skipped += 1
                continue

            for emitted_type, emitted_amount in to_emit:
                records.append(TREASURY_ACCOUNT_BALANCE(
                    **base,
                    account_type_raw=account_type,
                    balance_type=emitted_type,
                    amount_usd_millions=emitted_amount,
                    original_value=emitted_amount,
                    original_unit="USD_MILLIONS",
                    is_tga=True,
                    source_table="I",
                    src_line_nbr=src_line,
                ))
        if skipped:
            logger.warning("treasury_fiscal.normalize_skipped", table="balance", skipped=skipped)
        return records

    def _normalize_cash_flow(
        self, rows: list[Any], fetched_at: datetime | None, *, source_table: str
    ) -> list[BaseRecord]:
        records: list[BaseRecord] = []
        skipped = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            record_date = _date(row.get("record_date"))
            if record_date is None:
                skipped += 1
                continue
            account_type = _text(row.get("account_type")) or (
                "Treasury General Account (TGA)" if source_table in ("IV", "V") else None
            )
            if account_type is None:
                skipped += 1
                continue
            if source_table == "II" and not _is_tga_account(account_type):
                skipped += 1
                continue
            src_line = int(_float(row.get("src_line_nbr")) or 0)
            if source_table == "II":
                raw_category = _text(row.get("transaction_catg")) or "UNKNOWN"
                raw_desc = _text(row.get("transaction_catg_desc"))
                transaction_type = _text(row.get("transaction_type")) or "Unknown"
                amount = _float(row.get("transaction_today_amt"))
                if amount is None:
                    skipped += 1
                    continue
                direction = "INFLOW" if transaction_type == "Deposits" else (
                    "OUTFLOW" if transaction_type == "Withdrawals" else "UNKNOWN"
                )
                is_duplicate = False
            elif source_table == "IV":
                raw_category = _text(row.get("classification")) or "UNKNOWN"
                raw_desc = None
                transaction_type = "Internal"
                amount = _float(row.get("today_amt"))
                direction = "INTERNAL"
                is_duplicate = True
            else:  # V
                raw_category = _text(row.get("tax_refund_type")) or "UNKNOWN"
                raw_desc = _text(row.get("tax_refund_type_desc"))
                transaction_type = "Withdrawals"
                amount = _float(row.get("tax_refund_today_amt"))
                direction = "OUTFLOW"
                is_duplicate = True
            if amount is None:
                skipped += 1
                continue

            classification: dict[str, Any]
            if source_table == "IV":
                classification = {
                    "category": "INTERNAL_TRANSFER", "flow_type": "INTERNAL",
                    "direction": "INTERNAL", "include_in_net": False,
                    "is_internal": True, "confidence": 0.9,
                }
            elif source_table == "V":
                classification = {
                    "category": "TAX_REFUND", "flow_type": "REFUND",
                    "direction": "OUTFLOW", "include_in_net": False,
                    "is_internal": False, "confidence": 0.9,
                }
            else:
                classification = _classify_cash_flow(raw_category, direction)

            base = self._base_fields(
                source_table=source_table, record_date=record_date,
                src_line=src_line, fetched_at=fetched_at,
            )
            records.append(TREASURY_CASH_FLOW(
                **base,
                account_type_raw=account_type,
                transaction_type_raw=transaction_type,
                source_table=source_table,
                raw_category=raw_category,
                raw_category_desc=raw_desc,
                category=classification["category"],
                subcategory=None,
                flow_type=classification["flow_type"],
                direction=classification["direction"],
                amount_usd_millions=amount,
                original_value=amount,
                original_unit="USD_MILLIONS",
                is_internal_transfer=classification["is_internal"],
                is_duplicate_candidate=is_duplicate,
                is_subtotal=False,
                include_in_net=classification["include_in_net"],
                classification_confidence=classification["confidence"],
                taxonomy_version="builtin-v1",
                src_line_nbr=src_line,
            ))
        if skipped:
            logger.warning(
                "treasury_fiscal.normalize_skipped",
                table=f"cash_flow_{source_table}", skipped=skipped,
            )
        return records

    def _normalize_debt(
        self, rows: list[Any], fetched_at: datetime | None, table: str
    ) -> list[BaseRecord]:
        records: list[BaseRecord] = []
        skipped = 0
        source_table: Literal["IIIA", "IIIB"] = (
            "IIIA" if table == "public_debt_transactions" else "IIIB"
        )
        for row in rows:
            if not isinstance(row, dict):
                continue
            record_date = _date(row.get("record_date"))
            if record_date is None:
                skipped += 1
                continue
            table_code = _normalize_table_code(
                _text(row.get("table_nbr")), default=source_table
            )
            transaction_type = _text(row.get("transaction_type")) or "Unknown"
            if "issue" in transaction_type.lower():
                transaction_type = "Issues"
            elif "redemption" in transaction_type.lower():
                transaction_type = "Redemptions"
            security_market = _text(row.get("security_market"))
            security_type = _text(row.get("security_type"))
            security_desc = _text(row.get("security_type_desc"))
            adjustment_type = _text(row.get("adj_type"))
            amount = _float(row.get("transaction_today_amt")) if source_table == "IIIA" else (
                _float(row.get("adj_today_amt"))
            )
            if amount is None:
                skipped += 1
                continue
            src_line = int(_float(row.get("src_line_nbr")) or 0)
            # GAS / FFB 属政府内部债务；Savings Securities 是非市场化但真实现金，
            # 只通过 is_marketable=False 标记，不视为内部转移。
            is_internal = (security_type or "") in {
                "Government Account Series", "Federal Financing Bank",
            }
            base = self._base_fields(
                source_table=source_table, record_date=record_date,
                src_line=src_line, fetched_at=fetched_at,
            )
            records.append(TREASURY_DEBT_TRANSACTION(
                **base,
                source_table=source_table,
                transaction_type_raw=transaction_type,
                security_market=security_market,
                security_type=security_type,
                security_type_desc=security_desc,
                adjustment_type=adjustment_type,
                amount_basis="FACE" if source_table == "IIIA" else "CASH_ADJUSTMENT",
                amount_usd_millions=amount,
                original_value=amount,
                original_unit="USD_MILLIONS",
                is_marketable=security_market == "Marketable",
                is_internal=is_internal,
                src_line_nbr=src_line,
            ))
            if table_code != source_table:
                # 早期 table_nbr 变体（III-A/III-B）已归一；记录一次便于审计。
                logger.info(
                    "treasury_fiscal.table_code_variant",
                    raw=_text(row.get("table_nbr")),
                    normalized=source_table,
                )
        if skipped:
            logger.warning(
                "treasury_fiscal.normalize_skipped",
                table=f"debt_{source_table}", skipped=skipped,
            )
        return records

    def _normalize_auction(
        self, rows: list[Any], fetched_at: datetime | None, table: str
    ) -> list[BaseRecord]:
        records: list[BaseRecord] = []
        skipped = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            cusip = _text(row.get("cusip"))
            security_type = _text(row.get("security_type")) or "Unknown"
            security_term = _text(row.get("security_term")) or "Unknown"
            auction_date = _date(row.get("auction_date"))
            issue_date = _date(row.get("issue_date"))
            announcement_date = _date(row.get("announcemt_date"))
            maturity_date = _date(row.get("maturity_date"))
            if auction_date is None or issue_date is None or announcement_date is None:
                skipped += 1
                continue
            offering = _float(row.get("offering_amt"))
            accepted = _float(row.get("total_accepted"))
            tendered = _float(row.get("total_tendered"))
            current_outstanding = _float(row.get("currently_outstanding"))
            # 拍卖金额原始单位 USD → USD millions
            offering_m = offering / 1e6 if offering is not None else None
            accepted_m = accepted / 1e6 if accepted is not None else None
            tendered_m = tendered / 1e6 if tendered is not None else None
            outstanding_m = (
                current_outstanding / 1e6 if current_outstanding is not None else None
            )
            data_status: Literal["ANNOUNCED", "RESULT"] = (
                "ANNOUNCED" if table == "upcoming_auctions" else (
                    "RESULT" if accepted_m is not None else "ANNOUNCED"
                )
            )
            auction_key = (
                f"{cusip}:{auction_date.isoformat()}:{data_status}"
                if cusip is not None
                else f"{security_type}:{security_term}:{auction_date.isoformat()}:{data_status}"
            )
            release_date = (
                announcement_date if data_status == "ANNOUNCED" else auction_date
            )
            release_time, release_source = _release_time(
                release_date, fetched_at, prefer_observed=True
            )
            base: dict[str, Any] = {
                "schema_version": "1.0",
                "source": "treasury_fiscal",
                # entity 分区口径固定为 US_TREASURY（方案 §4.3 注册声明）；
                # CUSIP 已单独存储在 cusip/auction_key 字段，不作为分区实体
                "source_id": "US_TREASURY",
                "source_timestamp": _obs_time(release_date),
                "ingest_timestamp": datetime.now(UTC).replace(tzinfo=None),
                "raw_record_id": f"treasury_fiscal:{table}:{auction_key}",
                "quality_status": "VALID",
                "quality_reason": None,
                "release_time": release_time,
                "release_time_source": release_source,
            }
            records.append(TREASURY_AUCTION(
                **base,
                auction_key=auction_key,
                cusip=cusip,
                security_type=security_type,
                security_term=security_term,
                announcement_date=announcement_date,
                auction_date=auction_date,
                issue_date=issue_date,
                maturity_date=maturity_date,
                offering_amount_usd_millions=offering_m,
                total_accepted_usd_millions=accepted_m,
                total_tendered_usd_millions=tendered_m,
                interest_rate=_float(row.get("int_rate")),
                high_yield=_float(row.get("high_yield")),
                price_per_100=_float(row.get("price_per100")),
                interest_payment_frequency=_text(row.get("int_payment_frequency")),
                is_reopening=(_text(row.get("reopening")) or "").lower() == "yes",
                is_cmb=(_text(row.get("cash_management_bill_cmb")) or "").lower() == "yes",
                # 实测 auctions_query 的 TIPS/FRN 行 security_type 仍是 "Note"，
                # 需以 inflation_index_security / floating_rate 字段判定；
                # upcoming_auctions 则用 "TIPS Note" 等类型名。
                is_tips=(
                    "tips" in security_type.lower()
                    or (_text(row.get("inflation_index_security")) or "").lower() == "yes"
                ),
                is_frn=(
                    "frn" in security_type.lower()
                    or "floating" in security_type.lower()
                    or (_text(row.get("floating_rate")) or "").lower() == "yes"
                ),
                currently_outstanding_usd_millions=outstanding_m,
                data_status=data_status,
            ))
        if skipped:
            logger.warning(
                "treasury_fiscal.normalize_skipped", table=table, skipped=skipped,
            )
        return records

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001
            pass
