"""SEC EDGAR 连接器（D04 §4.7，DATA-SOURCE-007）。

通过 SEC EDGAR HTTP API 获取：
- submissions/CIK{cik}.json — FILING 索引
- companyfacts/CIK{cik}.json — FUNDAMENTAL（GAAP/IFRS 财务数据）

时间语义（审计 F-09）：
- acceptanceDateTime 带 EDT/EST 后缀 → 先转 UTC 再去 tzinfo
- filingDate 日期型 → T23:59:59Z（保守，防 look-ahead）
- continuity_model: EVENT_BASED

"""

from __future__ import annotations

import math
import re
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import structlog

from chronoforge.connectors.base import (
    CapabilityMatrix,
    DataConnector,
    FetchRequest,
    HealthStatus,
    InstrumentRef,
    RawBatch,
)
from chronoforge.connectors.errors import (
    ConfigError,
    ProviderError,
    RateLimitError,
    TransportError,
)
from chronoforge.connectors.ratelimit import RateLimiter
from chronoforge.models.base import BaseRecord
from chronoforge.models.enums import CanonicalType, QualityStatus
from chronoforge.models.fundamental import FILING, FUNDAMENTAL
from chronoforge.models.quality import QualityFinding, QualityReport

# SEC EDGAR 限流：≤10 req/s（D04 §4.7）
_DEFAULT_RATE = 10.0
_DEFAULT_BURST = 10

logger = structlog.get_logger()


@dataclass(frozen=True)
class _SECSettings:
    """SEC EDGAR 连接器最小配置。"""

    sec_contact_email: str
    http_timeout_s: float = 30.0


class SECEdgarConnector(DataConnector):
    """SEC EDGAR 数据连接器（D04 §4.7）。

    Args:
        settings: 配置对象，必须包含 sec_contact_email。

    Raises:
        ConfigError: sec_contact_email 未配置。
    """

    source_id = "sec_edgar"

    def __init__(self, settings: _SECSettings) -> None:
        contact_email = settings.sec_contact_email
        if not contact_email:
            raise ConfigError(
                "CHRONOFORGE_SEC_CONTACT not configured",
                context={"source": "sec"},
            )
        self.contact_email = contact_email
        version = "0.1.0"
        self.user_agent = f"ChronoForge/{version} research ({contact_email})"
        self._settings = settings
        self._client = self._make_client()
        self._rate_limiter = RateLimiter(
            rate=_DEFAULT_RATE, burst=_DEFAULT_BURST
        )

    def _make_client(self) -> httpx.Client:
        """构造 SEC client（D04 §4.7 User-Agent 必填）。"""
        return httpx.Client(
            base_url="https://data.sec.gov/submissions",
            headers={"User-Agent": self.user_agent},
            timeout=self._settings.http_timeout_s,
        )

    def capabilities(self) -> CapabilityMatrix:
        """能力矩阵（D04 §4.7）。"""
        return CapabilityMatrix(
            canonical_types=frozenset({
                CanonicalType.FILING,
                CanonicalType.FUNDAMENTAL,
                CanonicalType.DOCUMENT,
            }),
            intervals=frozenset(),  # SEC 无 interval 概念
            supports_revision=False,
            supports_websocket=False,
            max_history_days=None,  # 全量可用
        )

    def health(self) -> HealthStatus:
        """健康检查（D04 §1）。"""
        import time

        start = time.monotonic()
        try:
            self._rate_limiter.acquire(1)
            # 使用 Alphabet/Google CIK0001652044（实测稳定返回 200）；
            # Apple CIK0000325023 在部分网络环境下返回 404，不适合做探活
            resp = self._client.get("/CIK0001652044.json")
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(
                ok=(resp.status_code == 200),
                latency_ms=elapsed_ms,
            )
        except Exception as exc:
            elapsed_ms = int((time.monotonic() - start) * 1000)
            return HealthStatus(
                ok=False, latency_ms=elapsed_ms, detail=str(exc)
            )

    def discover(self) -> list[InstrumentRef]:
        """SEC discover 可选（P0 不实现）。"""
        return []

    def fetch(self, request: FetchRequest) -> Iterator[RawBatch]:
        """获取 SEC EDGAR 数据（D04 §4.7）。

        Args:
            request: 包含 cik 参数，可选 data_type 指定拉取 submissions 或 companyfacts。

        Yields:
            RawBatch 迭代器。
        """
        cik_raw = str(request.params.get("cik", "")).strip()
        if not cik_raw:
            raise ValueError("sec_edgar: cik is required in request.params")
        # SEC API 要求 10 位零填充 CIK（如 1652044 → CIK0001652044）
        cik = re.sub(r"\D", "", cik_raw).zfill(10)

        data_type = request.params.get("data_type", "all")

        if data_type in ("", "all", "filing"):
            yield self._fetch_submissions(cik)
        elif data_type == "fundamental":
            yield self._fetch_companyfacts(cik)
        else:
            raise ProviderError(
                f"sec_edgar: unknown data_type: {data_type}",
                context={"source": "sec_edgar", "data_type": data_type},
            )

    def _fetch_submissions(self, cik: str) -> RawBatch:
        """获取 FILING 索引（D04 §4.7）。

        Args:
            cik: CIK 号码。

        Returns:
            RawBatch 原始响应。
        """
        self._rate_limiter.acquire(1)

        try:
            response = self._client.get(f"/CIK{cik}.json")
        except httpx.TransportError as e:
            raise TransportError(
                f"sec_edgar: submissions request failed: {e}",
                context={
                    "source": "sec_edgar",
                    "endpoint": f"/CIK{cik}.json",
                    "cik": cik,
                },
            ) from e

        self._handle_response(response, cik, "/CIK{cik}.json")
        data = response.json()
        return RawBatch(
            endpoint=f"/CIK{cik}.json",
            payload=data,
            raw_meta={"http_status": response.status_code, "cik": cik},
        )

    def _fetch_companyfacts(self, cik: str) -> RawBatch:
        """获取 FUNDAMENTAL 数据（D04 §4.7）。

        Args:
            cik: CIK 号码。

        Returns:
            RawBatch 原始响应。
        """
        self._rate_limiter.acquire(1)

        # 真实 companyfacts endpoint 为 /api/xbrl/companyfacts/CIK##########.json；
        # /submissions/CIK{cik}/companyfacts.json 实测 404（2026-11-05）。
        # base_url 指向 /submissions，必须用绝对 URL 覆盖，否则会被拼接成
        # /submissions/api/xbrl/...（实测 404）。
        companyfacts_path = f"/api/xbrl/companyfacts/CIK{cik}.json"
        companyfacts_url = f"https://data.sec.gov{companyfacts_path}"
        try:
            response = self._client.get(companyfacts_url)
        except httpx.TransportError as e:
            raise TransportError(
                f"sec_edgar: companyfacts request failed: {e}",
                context={
                    "source": "sec_edgar",
                    "endpoint": companyfacts_path,
                    "cik": cik,
                },
            ) from e

        self._handle_response(response, cik, companyfacts_path)
        data = response.json()
        return RawBatch(
            endpoint=companyfacts_path,
            payload=data,
            raw_meta={"http_status": response.status_code, "cik": cik},
        )

    def _handle_response(
        self,
        response: httpx.Response,
        cik: str,
        endpoint_template: str,
    ) -> None:
        """处理 HTTP 响应，映射错误（D04 §3）。"""
        status_code = response.status_code

        if status_code == 429:
            raise RateLimitError(
                f"sec_edgar: rate limited on {endpoint_template.format(cik=cik)}",
                context={
                    "source": "sec_edgar",
                    "endpoint": endpoint_template.format(cik=cik),
                    "cik": cik,
                },
                retry_after=1,
            )
        if status_code in (401, 403):
            raise ProviderError(
                f"sec_edgar: auth failed for CIK {cik}",
                context={
                    "source": "sec_edgar",
                    "endpoint": endpoint_template.format(cik=cik),
                    "status": status_code,
                    "cik": cik,
                },
            )
        if status_code >= 400:
            raise ProviderError(
                f"sec_edgar: HTTP {status_code} on {endpoint_template.format(cik=cik)}",
                context={
                    "source": "sec_edgar",
                    "endpoint": endpoint_template.format(cik=cik),
                    "status": status_code,
                    "cik": cik,
                },
            )

    def _parse_acceptance_datetime(self, accepted_dt: str | None) -> datetime | None:
        """解析 acceptanceDateTime，处理 EDT/EST 后缀（审计 F-09）。

        规则：带 EDT/EST 后缀 → 先转 UTC 再去 tzinfo。

        Args:
            accepted_dt: 源侧时间字符串。

        Returns:
            UTC naive datetime 或 None。
        """
        if not accepted_dt:
            return None

        # 现代 data.sec.gov 格式：ISO 8601（如 "2026-09-22T20:12:40.000Z"）
        try:
            dt = datetime.fromisoformat(
                accepted_dt.strip().replace("Z", "+00:00")
            )
            if dt.tzinfo is not None:
                dt = dt.astimezone(UTC).replace(tzinfo=None)
            return dt
        except ValueError:
            pass

        # SEC 格式示例："2023-01-15 16:30:00 EDT"
        # 尝试带时区后缀的格式
        tz_pattern = re.compile(
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*(EDT|EST|CST|MST|PDT|PST|UTC|GMT)"
        )
        match = tz_pattern.match(accepted_dt.strip())
        if match:
            dt_str, tz_str = match.groups()
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
            # 转换为 UTC offset
            tz_offsets = {
                "UTC": 0, "GMT": 0,
                "EST": -5, "EDT": -4,
                "CST": -6, "CDT": -5,
                "MST": -7, "MDT": -6,
                "PST": -8, "PDT": -7,
            }
            offset_hours = tz_offsets.get(tz_str, 0)
            # 转为 UTC naive datetime
            dt = dt - timedelta(hours=offset_hours)
            return dt

        # 纯日期格式 "2023-01-15"
        if len(accepted_dt.strip()) == 10:
            try:
                return datetime.strptime(accepted_dt.strip(), "%Y-%m-%d")
            except ValueError:
                return None

        # 尝试直接解析 ISO 格式
        try:
            dt = datetime.strptime(accepted_dt.strip(), "%Y-%m-%d %H:%M:%S")
            return dt
        except ValueError:
            return None

    def _normalize_filings(self, payload: dict[str, Any], cik: str) -> list[BaseRecord]:
        """将 submissions 响应转为 FILING 记录（D04 §4.7）。

        SEC EDGAR submissions API 返回并行数组格式：
        filings.recent = { accessionNumber: [...], filingDate: [...], ... }

        Args:
            payload: submissions API 响应。
            cik: CIK 号码。

        Returns:
            FILING 记录列表。
        """
        recent = payload.get("filings", {}).get("recent", {})
        if not isinstance(recent, dict):
            return []

        accession_numbers = recent.get("accessionNumber", [])
        if not isinstance(accession_numbers, list):
            return []

        filing_dates = recent.get("filingDate", [])
        report_dates = recent.get("reportDate", [])
        # SEC submissions 的实际字段名是 acceptanceDateTime（非 acceptanceDate）；
        # 读错字段 → accepted_datetime=None → source_timestamp 空 → Q-PROV-001 阻断
        acceptance_dates = (
            recent.get("acceptanceDateTime") or recent.get("acceptanceDate") or []
        )
        forms = recent.get("form", [])

        records: list[FILING] = []

        for i, accession_number in enumerate(accession_numbers):
            if not accession_number:
                continue

            form_type = forms[i] if i < len(forms) else ""
            filing_date_str = filing_dates[i] if i < len(filing_dates) else ""
            report_period = report_dates[i] if i < len(report_dates) else None
            accepted_dt_str = acceptance_dates[i] if i < len(acceptance_dates) else None

            # 解析 filing_date → date
            filing_date = self._parse_date(filing_date_str)
            if filing_date is None:
                continue

            # 解析 report_period_end
            report_period_date = (
                self._parse_date(report_period) if report_period else None
            )

            # 解析 acceptance_datetime（审计 F-09：EDT/EST → UTC）
            accepted_datetime = self._parse_acceptance_datetime(accepted_dt_str)

            # entity_id = CIK 标准化（补零到 10 位）
            entity_id = f"CIK{cik.zfill(10)}"

            now = datetime.now(UTC).replace(tzinfo=None)

            filing_rec = FILING(
                schema_version="1.0",
                source="sec_edgar",
                source_id=cik,
                source_timestamp=accepted_datetime,
                ingest_timestamp=now,
                raw_record_id=f"sec_edgar:{cik}:{accession_number}",
                quality_status=QualityStatus.VALID,
                quality_reason=None,
                entity_id=entity_id,
                cik=cik,
                accession_number=accession_number,
                form_type=form_type,
                filing_date=filing_date,
                accepted_datetime=accepted_datetime or now,
                report_period_end=report_period_date or filing_date,
                primary_doc_url=self._extract_doc_url(accession_number),
            )
            records.append(filing_rec)

        return list[BaseRecord](records)

    def _normalize_fundamentals(
        self, payload: dict[str, Any], cik: str
    ) -> list[BaseRecord]:
        """将 companyfacts 响应转为 FUNDAMENTAL 记录（D04 §4.7）。

        真实结构（2026-11-05 依据 SEC data.sec.gov companyfacts API 实测）：

            facts[taxonomy][concept]["units"][unit] = [
                {"end": "2023-09-30", "val": 352755000000,
                 "accn": "0000320193-23-000106", "fy": 2023, "fp": "FY",
                 "form": "10-K", "filed": "2023-11-03",
                 "frame": "CY2023Q3I"}  # frame 可选
            ]

        units 是以单位（USD/shares）为键的 object，不是 list；观测日与数值
        分别来自 end/val；i/f/$ 与 taxonomy.entry.url 均不存在。
        """
        facts = payload.get("facts")
        if facts is None:
            raise ProviderError(
                "sec_edgar: companyfacts payload missing facts object",
                context={"source": "sec_edgar", "cik": cik},
            )
        if not isinstance(facts, dict):
            raise ProviderError(
                "sec_edgar: companyfacts facts is not an object",
                context={"source": "sec_edgar", "cik": cik},
            )
        if not facts:
            return []

        records: list[FUNDAMENTAL] = []
        skipped: dict[str, int] = defaultdict(int)
        entity_id = f"CIK{cik.zfill(10)}"
        now = datetime.now(UTC).replace(tzinfo=None)

        for taxon, concepts in facts.items():
            if not isinstance(concepts, dict):
                skipped["taxonomy_not_object"] += 1
                continue

            for concept, concept_data in concepts.items():
                if not isinstance(concept_data, dict):
                    skipped["concept_not_object"] += 1
                    continue

                units = concept_data.get("units")
                if not isinstance(units, dict):
                    skipped["units_not_object"] += 1
                    continue

                for unit, observations in units.items():
                    if not isinstance(observations, list):
                        skipped["observations_not_list"] += 1
                        continue

                    for obs in observations:
                        if not isinstance(obs, dict):
                            skipped["observation_not_object"] += 1
                            continue

                        end_str = obs.get("end")
                        value_raw = obs.get("val")
                        if not end_str or value_raw is None:
                            skipped["missing_end_or_val"] += 1
                            continue

                        try:
                            observation_time = datetime.fromisoformat(
                                str(end_str)
                            ).replace(hour=23, minute=59, second=59)
                        except (ValueError, TypeError):
                            skipped["invalid_end"] += 1
                            continue

                        try:
                            value = float(value_raw)
                        except (ValueError, TypeError):
                            skipped["invalid_val"] += 1
                            continue

                        if not math.isfinite(value):
                            skipped["non_finite_val"] += 1
                            continue

                        fiscal_year_raw = obs.get("fy")
                        try:
                            fiscal_year = int(str(fiscal_year_raw))
                        except (ValueError, TypeError):
                            fiscal_year = observation_time.year
                        if not 2000 < fiscal_year < 2100:
                            skipped["invalid_fiscal_year"] += 1
                            continue

                        accn = str(obs.get("accn") or "")
                        frame = str(obs.get("frame") or "")
                        fiscal_period = str(obs.get("fp") or "")

                        fundamental = FUNDAMENTAL(
                            schema_version="1.0",
                            source="sec_edgar",
                            source_id=cik,
                            source_timestamp=observation_time,
                            ingest_timestamp=now,
                            raw_record_id=(
                                f"sec_edgar:{cik}:{taxon}:{concept}:"
                                f"{unit}:{end_str}:{accn}"
                            ),
                            quality_status=QualityStatus.VALID,
                            quality_reason=None,
                            entity_id=entity_id,
                            cik=cik,
                            concept=concept,
                            taxon=taxon,
                            unit=str(unit),
                            observation_time=observation_time,
                            value=value,
                            fiscal_year=fiscal_year,
                            fiscal_period=fiscal_period,
                            frame=frame,
                        )
                        records.append(fundamental)

        for reason, count in skipped.items():
            logger.warning(
                "connector.normalize_skip",
                source="sec_edgar",
                cik=cik,
                reason=reason,
                count=count,
            )
        if skipped:
            logger.warning(
                "connector.normalize_summary",
                source="sec_edgar",
                cik=cik,
                kept=len(records),
                skipped=sum(skipped.values()),
                reasons=dict(skipped),
            )

        if not records:
            # 非空 facts 但解析 0 行：不允许静默空产出；抛可诊断错误，
            # 交由 pipeline 的 normalize 失败率门禁（C1）处置。
            raise ProviderError(
                "sec_edgar: companyfacts parsed 0 rows from non-empty facts",
                context={"source": "sec_edgar", "cik": cik},
            )

        return list[BaseRecord](records)

    @staticmethod
    def _parse_date(date_str: str | None) -> date | None:
        """解析日期字符串为 date 对象。

        Args:
            date_str: YYYY-MM-DD 格式。

        Returns:
            date 或 None。
        """
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _extract_doc_url(accession_number: str) -> str:
        """从 accession number 提取主文档 URL。

        SEC EDGAR 文档 URL 格式：
        https://www.sec.gov/Archives/edgar/data/{CIK padded}/{file number}/{accession number}

        简化版：https://www.sec.gov/Archives/edgar/data/{CIK padded}/{accession_number}

        Args:
            accession_number: 接入号。

        Returns:
            文档 URL。
        """
        # 去除连字符用于路径构造
        acc_no_dash = accession_number.replace("-", "")
        return f"https://www.sec.gov/Archives/edgar/data/{acc_no_dash}"

    def normalize(self, raw: RawBatch) -> list[BaseRecord]:
        """将 SEC EDGAR 原始响应转为 canonical 记录（D04 §4.7）。

        Args:
            raw: RawBatch from fetch().

        Returns:
            FILING 或 FUNDAMENTAL 记录列表。
        """
        cik = raw.raw_meta.get("cik", "")
        if not cik:
            return []

        match raw.endpoint:
            case endpoint if "companyfacts" in endpoint:
                return self._normalize_fundamentals(raw.payload, cik)
            case _:
                return self._normalize_filings(raw.payload, cik)

    def validate(self, records: list[BaseRecord]) -> QualityReport:
        """验证 FILING/FUNDAMENTAL 记录（D04 §1，委托 quality.rules）。

        P0：基础校验（filing_date <= accepted_datetime）。
        """
        findings: list[QualityFinding] = []

        for record in records:
            if not isinstance(record, (FILING, FUNDAMENTAL)):
                continue

            key = str(record.natural_key())

            if isinstance(record, FILING):
                # filing_date <= accepted_datetime
                if record.filing_date > date.today():
                    findings.append(QualityFinding(
                        record_key=key,
                        rule_id="Q-TS-001",
                        severity="WARNING",
                        detail=(
                            f"filing_date {record.filing_date} "
                            f"> today (future filing)"
                        ),
                    ))

            elif isinstance(record, FUNDAMENTAL):
                # value must be finite
                if not math.isfinite(record.value):
                    findings.append(QualityFinding(
                        record_key=key,
                        rule_id="Q-SCHEMA-001",
                        severity="ERROR",
                        detail="value is not finite (NaN/Inf)",
                    ))

        return QualityReport(findings=findings)

    def checkpoint_from(
        self, raw: RawBatch | list[BaseRecord]
    ) -> str | None:
        """从 raw batch 或 records 中提取 checkpoint cursor（D04 §1）。

        返回最后一条记录的 accession_number 或 observation date。
        """
        if isinstance(raw, RawBatch):
            payload = raw.payload
            if isinstance(payload, dict):
                # submissions 格式（并行数组）
                recent = payload.get("filings", {}).get("recent", {})
                if isinstance(recent, dict):
                    acc_numbers = recent.get("accessionNumber", [])
                    if acc_numbers and len(acc_numbers) > 0:
                        return str(acc_numbers[-1])

                # companyfacts 格式：facts[taxon][concept]["units"][unit] = [...]
                facts = payload.get("facts", {})
                if isinstance(facts, dict) and facts:
                    last_end: str | None = None
                    for concepts in facts.values():
                        if not isinstance(concepts, dict):
                            continue
                        for concept_data in concepts.values():
                            if not isinstance(concept_data, dict):
                                continue
                            units = concept_data.get("units")
                            if not isinstance(units, dict):
                                continue
                            for observations in units.values():
                                if not isinstance(observations, list):
                                    continue
                                for obs in observations:
                                    if isinstance(obs, dict) and obs.get("end"):
                                        last_end = str(obs["end"])
                    if last_end:
                        try:
                            return datetime.fromisoformat(
                                last_end
                            ).strftime("%Y-%m")
                        except ValueError:
                            return None

        elif isinstance(raw, list) and raw and len(raw) > 0:
            last_record = raw[-1]
            if isinstance(last_record, FILING):
                return last_record.accession_number
            elif isinstance(last_record, FUNDAMENTAL):
                return last_record.observation_time.strftime("%Y-%m")

        return None

    def close(self) -> None:
        """关闭 HTTP 客户端。"""
        try:
            self._client.close()
        except Exception:
            pass

    def __del__(self) -> None:
        """确保资源清理。"""
        try:
            self.close()
        except Exception:
            pass
