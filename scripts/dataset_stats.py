#!/usr/bin/env python3
"""查看 canonical 层数据集整体信息（只读统计，不修改任何数据）。

扫描 {data_dir}/canonical/{type}/entity=*/ 下的 parquet 分区文件，按
canonical 类型分组，统计每个实体（symbol）的数据量与时间范围（起始/
结束时间），并关联注册表输出对应 dataset_id。OHLCV 等含 interval 列
的类型会按周期细分统计。

dataset 关联策略（两级，取先命中者）：
1. 注册表 entity_id 与分区实体精确匹配（FRED/SoSoValue 等）；
2. 回退按 params 标识符（symbol/instrument_name/series_id/cik 等）与
   分区实体分段匹配，并按 CCXT- 前缀与 spot/futures 关键词消歧
   （binance/ccxt 等 market_id 分区）。

用法:
    # 全部类型
    python scripts/dataset_stats.py

    # 只看指定类型 / 数据源
    python scripts/dataset_stats.py --types OHLCV
    python scripts/dataset_stats.py --types FLOW NUMBER --sources sosovalue

输出:
    按 canonical 类型分节，每节先给汇总行（实体数 / dataset 数 / 总行数 /
    全局时间范围），再逐行列出每个实体（含 interval 细分）的数据量与
    起止时间。末尾列出已注册但未匹配到任何 canonical 数据的 dataset。

依赖:
    只读本地 parquet 与 meta 注册表，无需网络与 API key。
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings
from chronoforge.logging import setup_logging
from chronoforge.models.enums import CanonicalType
from chronoforge.models.identity import partition_time_field
from chronoforge.registry.service import list_datasets

GroupKey = tuple[str, str, str]  # (canonical_type, entity_id, interval)

FREQ_EVERY = 200
_IDENT_KEYS = ("symbol", "instrument_name", "series_id", "cik", "ticker", "currency")


# ── 扫描统计 ──────────────────────────────────────────────────────────


def _min_max(column: Any) -> tuple[Any, Any]:
    """列的 (min, max)，全 null 时返回 (None, None)。"""
    result = pc.min_max(column).as_py()
    return result.get("min"), result.get("max")


def _acc(
    groups: dict[GroupKey, dict[str, Any]],
    ctype: str,
    entity: str,
    interval: str,
    rows: int,
    tmin: Any,
    tmax: Any,
) -> None:
    g = groups.setdefault((ctype, entity, interval), {"rows": 0, "min": None, "max": None})
    g["rows"] += rows
    if tmin is not None and (g["min"] is None or tmin < g["min"]):
        g["min"] = tmin
    if tmax is not None and (g["max"] is None or tmax > g["max"]):
        g["max"] = tmax


def scan_file(
    groups: dict[GroupKey, dict[str, Any]],
    f: Path,
    ctype: str,
    entity: str,
    time_field: str | None,
) -> None:
    """统计单个 parquet 文件（只读时间列与 interval 列）。"""
    schema = pq.read_schema(f)
    cols: list[str] = []
    if time_field and time_field in schema.names:
        cols.append(time_field)
    has_interval = "interval" in schema.names
    if has_interval:
        cols.append("interval")

    rows = pq.read_metadata(f).num_rows
    table = pq.read_table(f, columns=cols) if cols else None
    if table is None:
        _acc(groups, ctype, entity, "", rows, None, None)
        return

    tmin = tmax = None
    if time_field and time_field in schema.names:
        tmin, tmax = _min_max(table.column(time_field))

    if not has_interval:
        _acc(groups, ctype, entity, "", rows, tmin, tmax)
        return

    iv_col = table.column("interval")
    for iv in pc.unique(iv_col).to_pylist():
        sub = table.filter(
            pc.is_null(iv_col) if iv is None else pc.equal(iv_col, iv)
        )
        smin, smax = (
            _min_max(sub.column(time_field))
            if time_field and time_field in schema.names
            else (None, None)
        )
        _acc(groups, ctype, entity, str(iv or ""), sub.num_rows, smin, smax)


def scan_canonical(
    canonical_root: Path,
) -> tuple[dict[GroupKey, dict[str, Any]], int, int]:
    """扫描 canonical 目录，返回 (按 (type, entity, interval) 聚合的统计, 文件数, 跳过数)。"""
    files = sorted(canonical_root.rglob("part-*.parquet"))
    groups: dict[GroupKey, dict[str, Any]] = {}
    skipped = 0
    for idx, f in enumerate(files, 1):
        if idx % FREQ_EVERY == 0 or idx == len(files):
            print(f"\r扫描 parquet 文件 {idx}/{len(files)}", end="", flush=True)
        rel = f.relative_to(canonical_root).parts
        if len(rel) < 2 or not rel[1].startswith("entity="):
            skipped += 1
            continue
        ctype, entity = rel[0], rel[1].removeprefix("entity=")
        try:
            ct = CanonicalType(ctype)
        except ValueError:
            print(f"\n警告: 未知 canonical 类型目录 {ctype}，跳过", file=sys.stderr)
            skipped += 1
            continue
        time_field = partition_time_field(ct)
        try:
            scan_file(groups, f, ctype, entity, time_field)
        except Exception as e:  # noqa: BLE001 — 统计脚本不因单个坏文件中断
            print(f"\n警告: 读取失败 {f}: {e}", file=sys.stderr)
            skipped += 1
    if files:
        print()
    return groups, len(files), skipped


# ── 注册表关联 ────────────────────────────────────────────────────────


def build_registry_index(meta: Any) -> dict[str, list[dict[str, Any]]]:
    """按 canonical_type 索引注册表 dataset（保留 entity_id 与 params）。"""
    idx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in list_datasets(meta):
        try:
            params = json.loads(row.get("params_json") or "{}")
        except json.JSONDecodeError:
            params = {}
        params = params if isinstance(params, dict) else {}
        idx[str(row["canonical_type"])].append(
            {
                "dataset_id": str(row["dataset_id"]),
                "source_id": str(row["source_id"]),
                "entity_id": str(row.get("entity_id") or ""),
                "interval": params.get("interval"),
                "params": params,
            }
        )
    return idx


def _ident_variants(params: dict[str, Any]) -> set[str]:
    """从 dataset params 提取标识符匹配变体（symbol 去斜杠、cik 零填充等）。"""
    variants: set[str] = set()
    for key in _IDENT_KEYS:
        v = params.get(key)
        if v is None:
            continue
        v = str(v)
        variants.add(v)
        variants.add(v.replace("/", ""))  # ccxt 原生符号 BTC/USDT → BTCUSDT
        variants.add(v.split(":")[0].replace("/", ""))  # 1000PEPE/USDT:USDT → 1000PEPEUSDT
        if key == "cik":
            variants.add(v.zfill(10))  # sec_edgar 分区实体为 10 位零填充 CIK
    return {v for v in variants if v}


def _interval_ok(ds: dict[str, Any], interval: str) -> bool:
    """dataset 与统计组的 interval 兼容（dataset 无 interval 参数视为通配）。"""
    return ds["interval"] is None or ds["interval"] == interval


def match_datasets(
    idx: dict[str, list[dict[str, Any]]],
    ctype: str,
    entity: str,
    interval: str,
) -> list[dict[str, Any]]:
    """两级关联：entity_id 精确匹配 → params 标识符分段匹配（含消歧）。"""
    cands = idx.get(ctype, [])
    # 1) 注册表 entity_id 与分区实体精确一致
    exact = [d for d in cands if d["entity_id"] == entity and _interval_ok(d, interval)]
    if exact:
        return exact
    # 2) 回退：标识符分段匹配；CCXT- 前缀分区仅匹配带 exchange 参数且一致
    #    的 dataset，非 CCXT 分区排除带 exchange 参数的 dataset
    is_ccxt = entity.startswith("CCXT-")
    ccxt_exchange = entity.split(":")[0][len("CCXT-"):].lower() if is_ccxt else None
    segments = set(entity.split(":")) | {entity}
    pool: list[tuple[int, dict[str, Any]]] = []  # (特异度, dataset)
    for d in cands:
        if not _interval_ok(d, interval):
            continue
        exchange = str(d["params"].get("exchange") or "").lower()
        if is_ccxt:
            if exchange != ccxt_exchange:
                continue
        elif exchange:
            continue
        variants = _ident_variants(d["params"])
        # 多段符号（如 DOGE/USDT:USDT）：完整去斜杠串必须被实体包含，
        # 否则该 dataset 对应的是另一个分区（spot vs swap）
        symbol = str(d["params"].get("symbol") or "")
        if ":" in symbol and symbol.replace("/", "") not in entity:
            continue
        score = max(
            (len(v) for v in variants if v in segments or v in entity),
            default=0,
        )
        if score:
            pool.append((score, d))
    if not pool:
        return []
    # 只保留特异度最高（最长标识符命中）的候选
    best = max(s for s, _ in pool)
    pool = [d for s, d in pool if s == best]
    # spot / futures 细分消歧（如 binance_spot vs binance_futures 同符号）
    if len(pool) > 1:
        low = entity.lower()
        hint = (
            "futures" if "-fut" in low
            else "spot" if "spot" in low
            else None
        )
        if hint:
            narrowed = [d for d in pool if hint in d["source_id"]]
            if narrowed:
                pool = narrowed
    return pool


# ── 输出 ──────────────────────────────────────────────────────────────


def _fmt_ts(v: Any) -> str:
    if v is None:
        return "-"
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    return str(v)


def _fmt_rows(n: int) -> str:
    return f"{n:,}"


def print_report(
    groups: dict[GroupKey, dict[str, Any]],
    idx: dict[str, list[dict[str, Any]]],
    args: argparse.Namespace,
    n_files: int,
    skipped: int,
    elapsed: float,
) -> None:
    """按 canonical 类型分节输出统计报告。"""
    by_type: dict[str, list[GroupKey]] = defaultdict(list)
    for key in groups:
        by_type[key[0]].append(key)

    print(f"{'=' * 100}")
    print(
        f"Canonical 数据集统计  数据目录: {args.data_dir}"
        f" | 文件 {n_files} 个{' | 跳过 %d 个' % skipped if skipped else ''}"
        f" | 耗时 {elapsed:.1f}s"
    )
    print(f"{'=' * 100}")

    matched_ds: set[str] = set()
    total_rows = 0

    # 全量预匹配：missing 判定需覆盖 --sources 过滤后未显示的组
    all_matches = {k: match_datasets(idx, *k) for k in groups}
    matched_ds |= {d["dataset_id"] for ms in all_matches.values() for d in ms}

    shown_keys: set[GroupKey] = set()
    for ctype in sorted(by_type):
        keys = sorted(by_type[ctype], key=lambda k: (k[1], k[2]))
        if args.sources:
            keys = [
                k for k in keys
                if any(d["source_id"] in args.sources for d in all_matches[k])
            ]
            if not keys:
                continue
        shown_keys.update(keys)
        matches = {k: all_matches[k] for k in keys}
        matched_ds |= {d["dataset_id"] for ms in matches.values() for d in ms}
        type_rows = sum(groups[k]["rows"] for k in keys)
        total_rows += type_rows
        entities = {k[1] for k in keys}
        gmin = min((groups[k]["min"] for k in keys if groups[k]["min"] is not None), default=None)
        gmax = max((groups[k]["max"] for k in keys if groups[k]["max"] is not None), default=None)
        ds_count = len({d["dataset_id"] for ms in matches.values() for d in ms})
        show_interval = any(k[2] for k in keys)

        print(
            f"\n[{ctype}] 实体 {len(entities)} | dataset {ds_count}"
            f" | 共 {_fmt_rows(type_rows)} 行"
            f" | {_fmt_ts(gmin)} ~ {_fmt_ts(gmax)}"
        )
        w_entity = max(max(len(k[1]) for k in keys), 6)
        ds_names = {k: ", ".join(d["dataset_id"] for d in matches[k]) for k in keys}
        w_ds = max(max((len(s) for s in ds_names.values()), default=10), 10)
        if show_interval:
            print(
                f"  {'entity':<{w_entity}}  {'interval':<9}"
                f"  {'dataset_id':<{w_ds}}  {'行数':>10}  起始 ~ 结束"
            )
        else:
            print(f"  {'entity':<{w_entity}}  {'dataset_id':<{w_ds}}  {'行数':>10}  起始 ~ 结束")
        for k in keys:
            g = groups[k]
            tail = f"{_fmt_rows(g['rows']):>10}  {_fmt_ts(g['min'])} ~ {_fmt_ts(g['max'])}"
            if show_interval:
                print(
                    f"  {k[1]:<{w_entity}}  {k[2] or '-':<9}"
                    f"  {ds_names[k] or '-':<{w_ds}}  {tail}"
                )
            else:
                print(f"  {k[1]:<{w_entity}}  {ds_names[k] or '-':<{w_ds}}  {tail}")

    # 已注册但未匹配到任何 canonical 数据的 dataset（--types 时限定类型）
    type_filter = set(args.types) if args.types else None
    missing = sorted(
        (
            d for ctype, datasets in idx.items()
            if not type_filter or ctype in type_filter
            for d in datasets
            if d["dataset_id"] not in matched_ds
            and (not args.sources or d["source_id"] in args.sources)
        ),
        key=lambda d: d["dataset_id"],
    )
    print(f"\n{'-' * 100}")
    n_entities = len({k[1] for k in shown_keys})
    print(f"汇总: 类型 {len(by_type)} | 实体 {n_entities} | 总行数 {_fmt_rows(total_rows)}")
    if missing:
        print(f"\n已注册但无 canonical 数据 ({len(missing)}):")
        for d in missing:
            print(f"  {d['dataset_id']} (source={d['source_id']})")


# ── 主流程 ────────────────────────────────────────────────────────────


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="查看 canonical 层数据集整体信息（按类型分组，每个实体的数据量与时间范围）"
    )
    parser.add_argument(
        "--types", type=str, nargs="+", default=None,
        help="只统计指定 canonical 类型（如 OHLCV FLOW NUMBER；默认全部）",
    )
    parser.add_argument(
        "--sources", type=str, nargs="+", default=None,
        help="只统计指定数据源的数据（按数据内 source_id 过滤）",
    )
    parser.add_argument(
        "--data-dir", type=str, default=None,
        help="数据目录（默认取 Settings.data_dir）",
    )
    return parser.parse_args()


def run(args: argparse.Namespace) -> int:
    setup_logging("WARNING")
    t0 = time.perf_counter()

    settings = Settings.load()
    data_dir = Path(args.data_dir or settings.data_dir)
    args.data_dir = str(data_dir)
    canonical_root = data_dir / "canonical"
    if not canonical_root.is_dir():
        print(f"错误: canonical 目录不存在: {canonical_root}", file=sys.stderr)
        return 1
    if args.types:
        args.types = [t.upper() for t in args.types]

    groups, n_files, skipped = scan_canonical(canonical_root)
    if args.types:
        groups = {k: v for k, v in groups.items() if k[0] in args.types}

    meta = open_meta(settings)
    try:
        idx = build_registry_index(meta)
    finally:
        meta.close()

    print_report(groups, idx, args, n_files, skipped, time.perf_counter() - t0)
    return 0


def main() -> None:
    raise SystemExit(run(parse_args()))


if __name__ == "__main__":
    main()
