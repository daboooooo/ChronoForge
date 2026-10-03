#!/usr/bin/env python3
"""从注册表中注销 symbol，并清理本地数据。

用法:
    # 从 z_manual_unregister.json.json 读取 ticker 列表注销
    echo '["LILY"]' > scripts/symbols/z_manual_unregister.json.json
    python scripts/unregister_symbols.py

    # 交互式扫描：列出所有无 canonical 数据的已注册 symbol，逐个确认注销
    python scripts/unregister_symbols.py --scan

    # 结合两种模式：先注销 json 中的，再扫描清理
    python scripts/unregister_symbols.py --scan --unregister.json-json scripts/symbols/z_manual_unregister.json.json

行为:
    - 读取 scripts/symbols/z_manual_unregister.json.json 中的 ticker 列表
    - 搜索 dataset_registry，匹配 dataset_id 中含 ticker 的记录
    - 删除注册表行及关联的 checkpoints/run_log/quality_flags
    - 检查并提示删除本地原始数据（raw/）和 canonical 数据
    - --scan 模式下，遍历所有已注册 symbol，发现无 canonical 数据的提示注销
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

from chronoforge.cli._wiring import open_meta
from chronoforge.config.settings import Settings

SYMBOLS_DIR = Path(__file__).resolve().parent / "symbols"


def load_unregister_list(path: Path) -> list[str]:
    """读取 z_manual_unregister.json.json，返回 ticker 列表。"""
    if not path.exists():
        print(f"错误: {path} 不存在", file=sys.stderr)
        sys.exit(1)

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        print(f"错误: {path} 应为 JSON 数组", file=sys.stderr)
        sys.exit(1)

    tickers = [str(t).strip().upper() for t in data if str(t).strip()]
    if not tickers:
        print(f"警告: {path} 中没有有效的 ticker", file=sys.stderr)
    return tickers


def has_canonical_data_for_dataset(
    conn: Any,
    data_dir: Path,
    dataset_id: str,
    source_id: str,
    params: dict[str, Any] | None = None,
) -> tuple[bool, list[Path], str | None]:
    """检查 dataset 是否有 canonical 数据。

    Returns:
        (有数据, 数据文件列表, entity_id)
    """
    canonical_root = data_dir / "canonical"
    if not isinstance(canonical_root, Path):
        canonical_root = Path(canonical_root)
    if not canonical_root.is_dir():
        return False, [], None

    # 从注册表获取 entity_id 和 params
    entity_id = None
    db_params: dict[str, Any] = {}
    if params is None:
        if hasattr(conn, "execute"):
            row = conn.execute(
                "SELECT entity_id, params_json FROM dataset_registry WHERE dataset_id = ?",
                (dataset_id,),
            ).fetchone()
            if row:
                entity_id = row[0]
                try:
                    import json as _json
                    db_params = _json.loads(row[1]) if row[1] else {}
                except Exception:
                    db_params = {}
            # 确保游标释放（fetchone 后 commit 可释放 shared lock）
            conn.commit()

    if params is None:
        params = db_params

    # 策略：先尝试 entity_id 精确匹配
    entity_dirs: list[tuple[str, Path]] = []  # (ct_name, entity_path)

    if entity_id:
        for ct_dir in canonical_root.iterdir():
            if not ct_dir.is_dir() or ct_dir.name.startswith("."):
                continue
            entity_path = ct_dir / f"entity={entity_id}"
            if entity_path.is_dir():
                entity_dirs.append((ct_dir.name, entity_path))

    # 如果 entity_id 未命中，尝试用 params 中的标识符匹配
    # 例如 symbol=AAPL → 尝试匹配 entity=YAHOO:AAPL:SPOT 中包含 AAPL 的目录
    if not entity_dirs and params:
        ident = params.get("symbol") or params.get("instrument_name") or ""
        if ident:
            ident_clean = ident.replace("/", "")
            for ct_dir in canonical_root.iterdir():
                if not ct_dir.is_dir() or ct_dir.name.startswith("."):
                    continue
                # 查找 entity={xxx} 目录，其中 xxx 包含 ident_clean
                for ep in ct_dir.iterdir():
                    if not ep.is_dir() or not ep.name.startswith("entity="):
                        continue
                    dir_entity = ep.name[len("entity="):]
                    if ident_clean in dir_entity or dir_entity.replace("/", "") in ident_clean:
                        entity_dirs.append((ct_dir.name, ep))
                        break

    # 搜索匹配的实体目录中的 parquet 文件
    found_files: list[Path] = []
    for _ct_name, entity_path in entity_dirs:
        found_files.extend(entity_path.rglob("part-*.parquet"))

    return len(found_files) > 0, sorted(found_files), entity_id


def has_raw_data(data_dir: Path, source_id: str, dataset_id: str) -> tuple[bool, list[Path]]:
    """检查 dataset 是否有原始数据。

    Returns:
        (有数据, 数据文件列表)
    """
    if not isinstance(data_dir, Path):
        data_dir = Path(data_dir)
    raw_dir = data_dir / "raw" / source_id / dataset_id
    if not raw_dir.is_dir():
        return False, []

    files = sorted(raw_dir.rglob("*.jsonl"))
    return len(files) > 0, files


def get_local_data_size(
    conn: Any,
    data_dir: Path,
    source_id: str,
    dataset_id: str,
) -> tuple[int, int, str | None]:
    """统计 local 数据量（raw 行数 + canonical 行数）。

    Returns:
        (raw_files, canonical_files, entity_id)
    """
    _, raw_files = has_raw_data(data_dir, source_id, dataset_id)
    _, canonical_files, entity_id = has_canonical_data_for_dataset(
        conn, data_dir, dataset_id, source_id
    )
    return len(raw_files), len(canonical_files), entity_id


def delete_local_data(
    data_dir: Path, source_id: str, dataset_id: str, entity_id: str | None = None,
) -> bool:
    """删除 dataset 的本地数据（raw + canonical）。

    Args:
        entity_id: canonical 分区目录中的 entity_id（如果未知会自动查询）

    Returns:
        是否删除了数据
    """
    if not isinstance(data_dir, Path):
        data_dir = Path(data_dir)
    deleted = False

    # 删除 raw 数据
    raw_dir = data_dir / "raw" / source_id / dataset_id
    if raw_dir.is_dir():
        file_count = sum(1 for _ in raw_dir.rglob("*.jsonl"))
        import shutil
        shutil.rmtree(raw_dir, ignore_errors=True)
        if file_count > 0:
            print(f"    已删除 {file_count} 个 raw 文件")
            deleted = True

    # 删除 canonical 数据
    canonical_root = data_dir / "canonical"
    if canonical_root.is_dir():
        if entity_id is None:
            # 尝试从 entity_id 推断
            entity_id = dataset_id.split(":")[0] if ":" in dataset_id else dataset_id
        for ct_dir in canonical_root.iterdir():
            if not ct_dir.is_dir() or ct_dir.name.startswith("."):
                continue
            entity_dir = ct_dir / f"entity={entity_id}"
            if entity_dir.is_dir():
                file_count = sum(1 for _ in entity_dir.rglob("part-*.parquet"))
                import shutil
                shutil.rmtree(entity_dir, ignore_errors=True)
                # 清理空的分区目录
                year_dirs = list(entity_dir.parent.glob("year=*"))
                for yd in year_dirs:
                    if not any(yd.iterdir()):
                        yd.rmdir()
                if file_count > 0:
                    print(f"    已删除 {file_count} 个 canonical 文件 ({ct_dir.name})")
                    deleted = True

    return deleted


def prompt_delete(
    conn: Any,
    data_dir: Path,
    source_id: str,
    dataset_id: str,
    entity_id: str | None = None,
) -> bool:
    """交互询问是否删除本地数据。"""
    if not isinstance(data_dir, Path):
        data_dir = Path(data_dir)

    # entity_id 可能已从注册表删除（通过 delete_local_data 传入），
    # 直接用它检查 canonical 目录；无 entity_id 时回退注册表查询
    canonical_root = data_dir / "canonical"
    if not isinstance(canonical_root, Path):
        canonical_root = Path(canonical_root)

    canonical_files = 0
    if canonical_root.is_dir():
        if entity_id:
            entity_path = canonical_root / f"entity={entity_id}"
            if entity_path.is_dir():
                canonical_files += sum(1 for _ in entity_path.rglob("part-*.parquet"))
        # 回退：遍历所有 canonical type 查找 entity 包含 symbol 的目录
        for ct_dir in canonical_root.iterdir():
            if not ct_dir.is_dir() or ct_dir.name.startswith("."):
                continue
            for ep in ct_dir.iterdir():
                if not ep.is_dir() or not ep.name.startswith("entity="):
                    continue
                dir_entity = ep.name[len("entity="):]
                dir_entity_clean = dir_entity.replace("/", "")
                # 匹配：entity_id 或 dataset_id symbol 部分是 dir_entity 的子串
                if entity_id and entity_id in dir_entity_clean:
                    canonical_files += sum(1 for _ in ep.rglob("part-*.parquet"))
                    break
                elif dir_entity_clean in dataset_id or dataset_id.replace("/", "") in dir_entity_clean:
                    canonical_files += sum(1 for _ in ep.rglob("part-*.parquet"))
                    break

    raw_dir = data_dir / "raw" / source_id / dataset_id
    raw_files = sum(1 for _ in raw_dir.rglob("*.jsonl")) if raw_dir.is_dir() else 0

    if raw_files == 0 and canonical_files == 0:
        print("    本地无数据")
        return False

    parts = []
    if raw_files > 0:
        parts.append("%d 个 raw 文件" % raw_files)
    if canonical_files > 0:
        parts.append("%d 个 canonical 文件" % canonical_files)

    desc = "、".join(parts)
    print("    存在本地数据: %s" % desc)

    while True:
        resp = input("    是否删除 (%s)? [y/N]: " % desc).strip().lower()
        if resp in ("y", "yes"):
            return True
        elif resp in ("n", "no", ""):
            return False


def unregister_symbols(
    conn: sqlite3.Connection,
    data_dir: Path,
    tickers: list[str],
) -> list[tuple[str, str, int, int, str | None]]:
    """从注册表中删除匹配 tickers 的 dataset。

    Returns:
        删除记录列表 [(dataset_id, source_id, raw_files, canonical_files, entity_id), ...]
    """
    deleted: list[tuple[str, str, int, int, str | None]] = []

    for _attempt in range(10):
        matches: list[dict] = []
        for ticker in tickers:
            cur = conn.execute(
                "SELECT * FROM dataset_registry WHERE dataset_id LIKE ?",
                (f"%{ticker}%",),
            )
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                matches.append(dict(zip(cols, row, strict=True)))

        if not matches:
            break

        # 先收集信息（SELECT），再批量删除
        to_delete: list[dict[str, Any]] = []
        for m in matches:
            dataset_id = m["dataset_id"]
            source_id = m["source_id"]
            raw_files, canonical_files, entity_id = get_local_data_size(
                conn, data_dir, source_id, dataset_id
            )
            to_delete.append({
                "dataset_id": dataset_id,
                "source_id": source_id,
                "raw_files": raw_files,
                "canonical_files": canonical_files,
                "entity_id": entity_id,
            })

        # 批量删除注册表行
        for td in to_delete:
            ds_id = td["dataset_id"]
            conn.execute(
                "DELETE FROM quality_flags WHERE dataset_id = ?", (ds_id,)
            )
            conn.execute(
                "DELETE FROM run_log WHERE dataset_id = ?", (ds_id,)
            )
            conn.execute(
                "DELETE FROM checkpoints WHERE dataset_id = ?", (ds_id,)
            )
            conn.execute(
                "DELETE FROM dataset_registry WHERE dataset_id = ?", (ds_id,)
            )
            print("  已删除注册: %s (source=%s)" % (ds_id, td["source_id"]))

        conn.commit()

        deleted.extend([
            (td["dataset_id"], td["source_id"], td["raw_files"],
             td["canonical_files"], td["entity_id"])
            for td in to_delete
        ])

    return deleted


def find_no_data_datasets(
    meta: Any,
    data_dir: Path,
    types: set[str] | None = None,
    sources: set[str] | None = None,
) -> list[dict[str, Any]]:
    """查找所有已注册但无 canonical 数据的 dataset。"""
    conn = meta.connection if hasattr(meta, "connection") else meta
    cur = conn.execute("SELECT * FROM dataset_registry")
    cols = [d[0] for d in cur.description]
    datasets = [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    results: list[dict[str, Any]] = []
    for ds in datasets:
        if types and ds["canonical_type"] not in types:
            continue
        if sources and ds["source_id"] not in sources:
            continue

        has_data, _, entity_id = has_canonical_data_for_dataset(
            conn, data_dir, ds["dataset_id"], ds["source_id"]
        )
        if not has_data:
            results.append({
                "dataset_id": ds["dataset_id"],
                "source_id": ds["source_id"],
                "canonical_type": ds["canonical_type"],
                "entity_id": ds["entity_id"],
                "frequency": ds["frequency"],
            })

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="从注册表中注销 symbol 并清理本地数据")
    parser.add_argument(
        "--unregister-json",
        type=Path,
        default=SYMBOLS_DIR / "z_manual_unregister.json.json",
        help="unregister.json 路径",
    )
    parser.add_argument(
        "--scan", action="store_true",
        help="扫描所有已注册 symbol，发现无 canonical 数据的提示注销",
    )
    parser.add_argument(
        "--scan-types", type=str, nargs="+", default=None,
        help="--scan 时只检查指定 canonical 类型",
    )
    parser.add_argument(
        "--scan-sources", type=str, nargs="+", default=None,
        help="--scan 时只检查指定数据源",
    )
    parser.add_argument(
        "--no-delete", action="store_true",
        help="删除注册表时不询问删除本地数据",
    )
    parser.add_argument(
        "--auto-delete", action="store_true",
        help="删除注册表时自动删除所有本地数据（不询问）",
    )
    args = parser.parse_args()

    settings = Settings.load()
    data_dir = Path(settings.data_dir)
    meta = open_meta(settings, repair=True)

    # 统一获取 sqlite3 连接
    conn = meta.connection if hasattr(meta, "connection") else meta

    try:
        tickers = []
        if not args.unregister_json.exists():
            print(f"未找到 {args.unregister_json}，跳过 ticker 注销")
        else:
            tickers = load_unregister_list(args.unregister_json)
            if tickers:
                print(f"扫描 {len(tickers)} 个 ticker: {tickers}")
                deleted = unregister_symbols(conn, data_dir, tickers)

                for ds_id, source_id, raw_files, canon_files, entity_id in deleted:
                    if args.no_delete:
                        msg = (
                            f"  [{ds_id}] 本地: "
                            f"{raw_files} raw + {canon_files} canonical"
                        )
                        print(msg)
                    elif raw_files > 0 or canon_files > 0:
                        if args.auto_delete:
                            delete_local_data(
                                data_dir, source_id, ds_id, entity_id
                            )
                            print(f"  [{ds_id}] 本地数据已删除")
                        else:
                            msg = (
                                f"  [{ds_id}] 本地: "
                                f"{raw_files} raw + {canon_files} canonical"
                            )
                            print(msg)
                            if prompt_delete(
                                data_dir, source_id, ds_id, entity_id
                            ):
                                delete_local_data(
                                    data_dir, source_id, ds_id, entity_id
                                )
                                print(f"  [{ds_id}] 本地数据已删除")
                            else:
                                print(f"  [{ds_id}] 本地数据保留")
                    else:
                        print(f"  [{ds_id}] 本地无数据")

        # --scan 模式：查找无 canonical 数据的已注册 dataset
        if args.scan:
            types_set = set(t.upper() for t in args.scan_types) if args.scan_types else None
            sources_set = set(args.scan_sources) if args.scan_sources else None

            no_data = find_no_data_datasets(meta, data_dir, types_set, sources_set)

            if not no_data:
                print("\n所有已注册 dataset 均有 canonical 数据")
                return 0

            print("\n发现 %d 个已注册但无 canonical 数据的 dataset:" % len(no_data))
            for ds in no_data:
                print(
                    "  %s (source=%s, type=%s)"
                    % (ds["dataset_id"], ds["source_id"], ds["canonical_type"])
                )

            while True:
                resp = input("\n是否批量注销这些 dataset? [y/N]: ").strip().lower()
                if resp in ("y", "yes"):
                    break
                elif resp in ("n", "no", ""):
                    print("跳过")
                    return 0

            for ds in no_data:
                ds_id = ds["dataset_id"]
                source_id = ds["source_id"]
                entity_id = ds.get("entity_id")

                conn.execute(
                    "DELETE FROM quality_flags WHERE dataset_id = ?", (ds_id,)
                )
                conn.execute(
                    "DELETE FROM run_log WHERE dataset_id = ?", (ds_id,)
                )
                conn.execute(
                    "DELETE FROM checkpoints WHERE dataset_id = ?", (ds_id,)
                )
                conn.execute(
                    "DELETE FROM dataset_registry WHERE dataset_id = ?", (ds_id,)
                )

                print("  已注销: %s (source=%s)" % (ds_id, source_id))

                if not args.no_delete:
                    if args.auto_delete:
                        delete_local_data(data_dir, source_id, ds_id, entity_id)
                        print("  [%s] 本地数据已删除" % ds_id)
                    else:
                        print("  [%s] 本地: 0 raw + 0 canonical" % ds_id)
                        if prompt_delete(conn, data_dir, source_id, ds_id, entity_id):
                            delete_local_data(data_dir, source_id, ds_id, entity_id)
                            print("  [%s] 本地数据已删除" % ds_id)

            conn.commit()
            print(f"\n共注销 {len(no_data)} 个 dataset")

    finally:
        meta.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
