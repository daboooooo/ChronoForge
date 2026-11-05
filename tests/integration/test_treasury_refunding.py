"""TreasuryRefundingConnector 契约测试（真实新闻稿 HTML）。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from chronoforge.connectors.base import FetchRequest, RawBatch
from chronoforge.connectors.errors import SchemaError
from chronoforge.connectors.treasury_refunding import (
    _DOCS_PAGE,
    TreasuryRefundingConnector,
)
from chronoforge.models.treasury import TREASURY_ANNOUNCEMENT

_FIXTURES = Path(__file__).parent.parent / "fixtures" / "treasury_refunding"
_RELEASE_PATH = "/news/press-releases/sb0675"


def _connector() -> TreasuryRefundingConnector:
    return TreasuryRefundingConnector(
        settings=SimpleNamespace(http_timeout_s=30.0)  # type: ignore[arg-type]
    )


def _client(index_html: str, release_html: str) -> tuple[
    TreasuryRefundingConnector, httpx.Client
]:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == _DOCS_PAGE:
            return httpx.Response(200, text=index_html)
        if request.url.path == _RELEASE_PATH:
            return httpx.Response(200, text=release_html)
        return httpx.Response(404)

    client = httpx.Client(
        base_url="https://home.treasury.gov",
        transport=httpx.MockTransport(handler),
    )
    connector = _connector()
    connector._client = client
    return connector, client


def _fetch_request() -> FetchRequest:
    return FetchRequest(
        dataset_id="treasury_refunding",
        params={},
        start=None,
        end=None,
        cursor=None,
    )


def test_fetch_to_normalize_e2e() -> None:
    """真实索引页 + 新闻稿 → fetch → normalize：2 条季度估计且字段正确。"""
    index_html = (_FIXTURES / "most_recent_documents.html").read_text(
        encoding="utf-8"
    )
    release_html = (_FIXTURES / "press_release_sb0675.html").read_text(
        encoding="utf-8"
    )
    connector, client = _client(index_html, release_html)

    batches = list(connector.fetch(_fetch_request()))
    assert len(batches) == 1
    assert batches[0].endpoint == _RELEASE_PATH
    assert batches[0].raw_meta["release_path"] == _RELEASE_PATH

    records = connector.normalize(batches[0])
    assert len(records) == 2
    assert all(isinstance(r, TREASURY_ANNOUNCEMENT) for r in records)

    first = records[0]
    assert first.document_type == "BORROWING_ESTIMATE"
    assert first.publication_time == datetime(2026, 11, 2)
    assert first.forecast_period_start == date(2026, 10, 1)
    assert first.forecast_period_end == date(2026, 12, 31)
    assert first.borrowing_estimate_usd_millions == pytest.approx(618_000.0)
    assert first.cash_balance_assumption_usd_millions == pytest.approx(850_000.0)
    assert first.source_url.endswith(_RELEASE_PATH)

    second = records[1]
    assert second.forecast_period_start == date(2027, 1, 1)
    assert second.forecast_period_end == date(2027, 3, 31)
    assert second.borrowing_estimate_usd_millions == pytest.approx(800_000.0)
    assert second.cash_balance_assumption_usd_millions == pytest.approx(
        1_000_000.0
    )
    client.close()


def test_fetch_missing_release_link_raises_schema_error() -> None:
    """索引页结构漂移（无 Financing Estimates 链接）→ SchemaError。"""
    connector, client = _client("<html>no link here</html>", "<html></html>")
    with pytest.raises(SchemaError, match="未在 most-recent 文档页找到"):
        list(connector.fetch(_fetch_request()))
    client.close()


def test_fetch_release_http_error_raises_schema_error() -> None:
    """新闻稿页非 200 → SchemaError。"""
    index_html = (_FIXTURES / "most_recent_documents.html").read_text(
        encoding="utf-8"
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == _DOCS_PAGE:
            return httpx.Response(200, text=index_html)
        return httpx.Response(503, text="unavailable")

    client = httpx.Client(
        base_url="https://home.treasury.gov",
        transport=httpx.MockTransport(handler),
    )
    connector = _connector()
    connector._client = client
    with pytest.raises(SchemaError, match="release page status 503"):
        list(connector.fetch(_fetch_request()))
    client.close()


def test_normalize_missing_title_date_raises_schema_error() -> None:
    """新闻稿缺标题/发布日期 → SchemaError。"""
    connector = _connector()
    with pytest.raises(SchemaError, match="缺少标题/发布日期"):
        connector.normalize(RawBatch(
            endpoint=_RELEASE_PATH,
            payload={"release_html": "<html><body>plain</body></html>"},
            raw_meta={},
        ))


def test_normalize_missing_forecast_sentence_raises_schema_error() -> None:
    """有标题/日期但借款估计句缺失（字段缺失）→ SchemaError。"""
    release_html = (_FIXTURES / "press_release_sb0675.html").read_text(
        encoding="utf-8"
    )
    title = "Treasury Announces Marketable Borrowing Estimates November 2, 2026"
    broken = (
        f"<html><body><h2>{title}</h2>"
        "<p>During the October–December 2026 quarter, Treasury expects to "
        "borrow in privately-held net marketable debt.</p></body></html>"
    )
    connector = _connector()
    with pytest.raises(SchemaError, match="未解析出任何 borrowing estimate"):
        connector.normalize(RawBatch(
            endpoint=_RELEASE_PATH,
            payload={"release_html": broken},
            raw_meta={},
        ))
    # 真实新闻稿正常解析（对照，确保上面的失败来自缺失句而非模板错误）
    assert len(connector.normalize(RawBatch(
        endpoint=_RELEASE_PATH,
        payload={"release_html": release_html},
        raw_meta={},
    ))) == 2
