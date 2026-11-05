#!/usr/bin/env python3
"""TGA Phase 0 数据可行性探测（只读，不写生产数据目录）。

用途：
- 核对 Fiscal Data API 各表的字段、单位、覆盖范围、枚举值；
- 生成 docs/tga/DATA_DICTIONARY.md 与 docs/tga/data_coverage.json；
- 保存脱敏/截断 fixture 到 tests/fixtures/treasury_fiscal/，供连接器契约测试。

设计约束：
- 只读官方 API；不写 data/raw、data/canonical、meta。
- 字段名以 API 返回的 meta.labels/dataTypes 为准；脚本只做记录，不做猜测。
- 任何字段缺失/更名写入报告，由 Phase 0 人工评审。

用法：
    uv run python scripts/tga_probe.py
    uv run python scripts/tga_probe.py --page-size 5 --timeout 30
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

BASE_URL = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service"
USER_AGENT = "ChronoForge/tga-probe (research; contact via repo owner)"

# (table_key, endpoint, 关键字段候选)
ENDPOINTS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "dts_operating_cash_balance",
        "/v1/accounting/dts/operating_cash_balance",
        (
            "record_date", "account_type", "open_today_bal", "close_today_bal",
            "open_month_bal", "open_fiscal_year_bal", "src_line_nbr", "table_nbr",
        ),
    ),
    (
        "dts_deposits_withdrawals",
        "/v1/accounting/dts/deposits_withdrawals_operating_cash",
        (
            "record_date", "account_type", "transaction_type", "transaction_catg",
            "transaction_catg_desc", "transaction_today_amt", "transaction_mtd_amt",
            "transaction_fytd_amt", "src_line_nbr", "table_nbr",
        ),
    ),
    (
        "dts_public_debt_transactions",
        "/v1/accounting/dts/public_debt_transactions",
        (
            "record_date", "transaction_type", "security_market", "security_type",
            "security_type_desc", "transaction_today_amt", "src_line_nbr", "table_nbr",
        ),
    ),
    (
        "dts_public_debt_cash_basis",
        "/v1/accounting/dts/adjustment_public_debt_transactions_cash_basis",
        (
            "record_date", "transaction_type", "adj_type", "adj_type_desc",
            "adj_today_amt", "adj_mtd_amt", "adj_fytd_amt", "src_line_nbr", "table_nbr",
        ),
    ),
    (
        "dts_inter_agency_tax_transfers",
        "/v1/accounting/dts/inter_agency_tax_transfers",
        (
            "record_date", "classification", "today_amt", "mtd_amt", "fytd_amt",
            "src_line_nbr", "table_nbr",
        ),
    ),
    (
        "dts_income_tax_refunds",
        "/v1/accounting/dts/income_tax_refunds_issued",
        (
            "record_date", "tax_refund_type", "tax_refund_type_desc",
            "tax_refund_today_amt", "tax_refund_mtd_amt", "tax_refund_fytd_amt",
            "src_line_nbr", "table_nbr",
        ),
    ),
    (
        "auction_results",
        "/v1/accounting/od/auctions_query",
        (
            "record_date", "cusip", "security_type", "security_term",
            "announcemt_date", "auction_date", "issue_date", "maturity_date",
            "offering_amt", "total_accepted", "total_tendered", "int_rate",
            "high_yield", "int_payment_frequency", "reopening",
            "cash_management_bill_cmb", "price_per100", "currently_outstanding",
        ),
    ),
    (
        "auction_upcoming",
        "/v1/accounting/od/upcoming_auctions",
        (
            "record_date", "security_type", "security_term", "reopening", "cusip",
            "offering_amt", "announcemt_date", "auction_date", "issue_date",
        ),
    ),
)

# 需要列出枚举值的字段（用于 account_type / 分类映射）
ENUM_FIELDS = (
    "account_type", "transaction_type", "transaction_catg", "security_market",
    "security_type", "security_type_desc", "adj_type", "classification",
    "tax_refund_type",
)


# Table I/II 账户命名在 2021-10 / 2022-04 发生切换；首尾页采样会漏掉中间 regime。
ERA_SAMPLE_DATES: dict[str, tuple[str, ...]] = {
    "dts_operating_cash_balance": ("2015-01-02", "2021-12-31", "2022-04-18"),
    "dts_deposits_withdrawals": ("2015-01-02", "2021-12-31", "2022-04-18"),
}


def _client(timeout: float) -> httpx.Client:
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    return httpx.Client(
        base_url=BASE_URL,
        timeout=timeout,
        headers={"User-Agent": USER_AGENT},
        proxy=proxy or None,
    )


def _get_page(
    client: httpx.Client,
    endpoint: str,
    *,
    sort: str,
    size: int,
    page: int = 1,
    filter_expr: str | None = None,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            params: dict[str, Any] = {
                "page[size]": size,
                "page[number]": page,
                "sort": sort,
                "format": "json",
            }
            if filter_expr:
                params["filter"] = filter_expr
            response = client.get(endpoint, params=params)
            if response.status_code == 429:
                retry_after = int(float(response.headers.get("retry-after", "30")))
                time.sleep(max(retry_after, 5))
                continue
            if response.status_code >= 500:
                time.sleep(2 ** attempt)
                continue
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict):
                raise ValueError(f"unexpected response type: {type(body)}")
            return body
        except Exception as exc:  # noqa: BLE001 - probe 只做诊断
            last_error = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"probe failed for {endpoint}: {last_error}")


def _rows(body: dict[str, Any]) -> list[dict[str, Any]]:
    data = body.get("data")
    return data if isinstance(data, list) else []


def _meta(body: dict[str, Any]) -> dict[str, Any]:
    meta = body.get("meta")
    return meta if isinstance(meta, dict) else {}


def _field_table(meta: dict[str, Any], key_fields: tuple[str, ...]) -> list[dict[str, str]]:
    labels = meta.get("labels") if isinstance(meta.get("labels"), dict) else {}
    types = meta.get("dataTypes") if isinstance(meta.get("dataTypes"), dict) else {}
    formats = meta.get("dataFormats") if isinstance(meta.get("dataFormats"), dict) else {}
    names = list(labels.keys()) or list(types.keys())
    rows: list[dict[str, str]] = []
    for name in names:
        rows.append({
            "field": name,
            "label": str(labels.get(name, "")),
            "type": str(types.get(name, "")),
            "format": str(formats.get(name, "")),
            "key": "yes" if name in key_fields else "no",
        })
    return rows


def probe(
    client: httpx.Client, key: str, endpoint: str, key_fields: tuple[str, ...], page_size: int
) -> dict[str, Any]:
    first = _get_page(client, endpoint, sort="record_date", size=page_size)
    last = _get_page(client, endpoint, sort="-record_date", size=page_size)
    first_rows = _rows(first)
    last_rows = _rows(last)
    era_rows: list[dict[str, Any]] = []
    for sample_date in ERA_SAMPLE_DATES.get(key, ()):
        era_rows.extend(_rows(_get_page(
            client,
            endpoint,
            sort="record_date",
            size=page_size,
            filter_expr=f"record_date:eq:{sample_date}",
        )))
    meta = _meta(first)
    fields = _field_table(meta, key_fields)

    enum_values: dict[str, list[str]] = {}
    for field in ENUM_FIELDS:
        values = {
            str(row.get(field))
            for row in first_rows + last_rows + era_rows
            if row.get(field) not in (None, "null", "")
        }
        if values:
            enum_values[field] = sorted(values)

    dates = [
        str(row.get("record_date"))
        for row in first_rows + last_rows + era_rows
        if row.get("record_date")
    ]
    return {
        "key": key,
        "endpoint": endpoint,
        "total_count": meta.get("total-count"),
        "total_pages": meta.get("total-pages"),
        "available_from": min(dates) if dates else None,
        "available_to": max(dates) if dates else None,
        "fields": fields,
        "key_fields": list(key_fields),
        "enum_values": enum_values,
        "first_rows": first_rows[:3],
        "last_rows": last_rows[:3],
        "raw_meta": {
            "labels": meta.get("labels"),
            "dataTypes": meta.get("dataTypes"),
            "dataFormats": meta.get("dataFormats"),
        },
    }


def write_fixtures(fixtures_dir: Path, results: list[dict[str, Any]]) -> None:
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    for result in results:
        table_dir = fixtures_dir / result["key"]
        table_dir.mkdir(parents=True, exist_ok=True)
        (table_dir / "first_page.json").write_text(
            json.dumps(
                {"data": result["first_rows"], "meta": result["raw_meta"]},
                ensure_ascii=False, indent=2,
            ),
            encoding="utf-8",
        )
        (table_dir / "last_page.json").write_text(
            json.dumps(
                {"data": result["last_rows"], "meta": result["raw_meta"]},
                ensure_ascii=False, indent=2,
            ),
            encoding="utf-8",
        )


def write_markdown(path: Path, results: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = [
        "# TGA 数据字典（Phase 0 探测）",
        "",
        f"> 探测时间：{datetime.now(UTC).replace(tzinfo=None).isoformat()}Z",
        "> 数据来源：美国财政部 Fiscal Data API",
        "> 说明：字段名/类型来自 API `meta`；任何字段缺失或更名必须在 Phase 0 评审中确认。",
        "",
        "## 1. 覆盖范围总表",
        "",
        "| 数据集 | 端点 | total-count | total-pages | 最早 record_date | 最新 record_date |",
        "|---|---|---:|---:|---|---|",
    ]
    for result in results:
        lines.append(
            f"| `{result['key']}` | `{result['endpoint']}` | {result['total_count']} | "
            f"{result['total_pages']} | {result['available_from']} | {result['available_to']} |"
        )
    lines.append("")

    for idx, result in enumerate(results, start=2):
        lines.extend([
            f"## {idx}. {result['key']}",
            "",
            f"- 端点：`{result['endpoint']}`",
            f"- total-count：{result['total_count']}",
            f"- total-pages：{result['total_pages']}",
            f"- 最早/最新 record_date：{result['available_from']} / {result['available_to']}",
            "",
            "### 字段",
            "",
            "| 字段 | 标签 | 类型 | 格式 | 关键字段 |",
            "|---|---|---|---|---|",
        ])
        for field in result["fields"]:
            lines.append(
                f"| `{field['field']}` | {field['label']} | {field['type']} | "
                f"{field['format']} | {field['key']} |"
            )
        lines.append("")
        if result["enum_values"]:
            lines.extend(["### 枚举值（首尾页样本）", ""])
            for field, values in result["enum_values"].items():
                lines.append(f"- `{field}`：{', '.join(f'`{v}`' for v in values[:30])}")
            lines.append("")
        lines.extend(["### 首行样例（截断）", "", "```json"])
        lines.append(json.dumps(result["first_rows"][:1], ensure_ascii=False, indent=2))
        lines.extend(["```", ""])

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="TGA Phase 0 Fiscal Data API 探测")
    parser.add_argument("--page-size", type=int, default=5, help="探测页大小（默认 5）")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--output", type=Path, default=Path("docs/tga/DATA_DICTIONARY.md"))
    parser.add_argument("--json", type=Path, default=Path("docs/tga/data_coverage.json"))
    parser.add_argument("--fixtures-dir", type=Path, default=Path("tests/fixtures/treasury_fiscal"))
    args = parser.parse_args()

    results: list[dict[str, Any]] = []
    with _client(args.timeout) as client:
        for key, endpoint, key_fields in ENDPOINTS:
            print(f"probe {key} ...", file=sys.stderr)
            results.append(probe(client, key, endpoint, key_fields, args.page_size))

    write_fixtures(args.fixtures_dir, results)
    write_markdown(args.output, results)
    args.json.parent.mkdir(parents=True, exist_ok=True)
    args.json.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"written: {args.output}, {args.json}, {args.fixtures_dir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
