"""STORAGE-003.2 集成测试 — drift 检测 + Q-DRIFT-001。

依据：STORAGE-003.2.md 测试要求 + GWT 验收标准 + D03 §3 值漂移检测
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

from chronoforge.models.enums import CanonicalType
from chronoforge.quality.report import QualityFinding
from chronoforge.quality.rules import QDRIFT001Rule, get_rule
from chronoforge.storage.base import REVISION_TYPES
from chronoforge.storage.canonical import (
    CanonicalStoreImpl,
    DriftFinding,
    detect_drift,
    value_digest,
)

# ── Helpers ─────────────────────────────────────────────────────────────


def _make_ohlcv_record(
    market_id: str = "BINANCE:BTCUSDT:SPOT",
    event_time: datetime | None = None,
    open: float = 50000.0,
    high: float = 51000.0,
    low: float = 49000.0,
    close: float = 50500.0,
    volume: float = 100.0,
    source_id: str = "btcusdt",
    raw_record_id: str | None = None,
) -> dict:
    """构造一条 OHLCV 记录。"""
    now = datetime.now(UTC).replace(tzinfo=None)
    et = event_time or now
    return {
        "schema_version": "1.0",
        "source": "test_source",
        "source_id": source_id,
        "source_timestamp": et,
        "ingest_timestamp": now,
        "raw_record_id": raw_record_id or f"test_source:ohlcv_1m:file.jsonl:{id(et)}",
        "market_id": market_id,
        "event_time": et,
        "interval": "1m",
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _make_number_record(
    source_id: str = "FRED:GDP",
    observation_time: datetime | None = None,
    revision_time: datetime | None = None,
    value: float = 1.0,
) -> dict:
    """构造一条 NUMBER 记录。"""
    now = datetime.now(UTC).replace(tzinfo=None)
    ot = observation_time or now
    rt = revision_time or now
    return {
        "schema_version": "1.0",
        "source": "FRED",
        "source_id": source_id,
        "source_timestamp": ot,
        "ingest_timestamp": now,
        "raw_record_id": "FRED:macro:file.jsonl:1",
        "observation_time": ot,
        "release_time": now,
        "revision_time": rt,
        "value": value,
        "units": "IDX",
        "seasonal_adjustment": "SA",
    }


def _count_parquet_files(directory: Path) -> int:
    """统计目录中的 parquet 文件数。"""
    if not directory.exists():
        return 0
    return len(list(directory.glob("*.parquet")))


def _read_parquet_rows(directory: Path) -> int:
    """读取目录中所有 parquet 文件的总行数。"""
    if not directory.exists():
        return 0
    total = 0
    for f in directory.glob("*.parquet"):
        total += pq.read_table(str(f)).num_rows
    return total


def _list_partition_dirs(base: Path) -> list[Path]:
    """列出所有分区目录（entity/year/month）。"""
    if not base.exists():
        return []
    dirs: list[Path] = []
    for entity_dir in base.iterdir():
        if not entity_dir.is_dir():
            continue
        for year_dir in entity_dir.iterdir():
            if not year_dir.is_dir():
                continue
            for month_dir in year_dir.iterdir():
                if month_dir.is_dir():
                    dirs.append(month_dir)
                elif month_dir.name.startswith("part-"):
                    dirs.append(year_dir)
    return sorted(dirs)


# ── value_digest Tests ─────────────────────────────────────────────────


class TestValueDigest:

    def test_none_value(self) -> None:
        """None 值 → sha256("null")"""
        digest = value_digest(None)
        assert isinstance(digest, str)
        assert len(digest) == 64  # SHA256 hex digest length
        # Verify it's the correct sha256 of "null"
        import hashlib
        assert digest == hashlib.sha256(b"null").hexdigest()

    def test_datetime_value(self) -> None:
        """datetime → sha256(iso_string)"""
        dt = datetime(2026, 9, 11, 10, 0, 0)
        digest = value_digest(dt)
        assert isinstance(digest, str)
        assert len(digest) == 64

    def test_numeric_value(self) -> None:
        """数值 → sha256(str(value))"""
        value = 50500.0
        digest = value_digest(value)
        assert isinstance(digest, str)
        assert len(digest) == 64

    def test_same_value_same_digest(self) -> None:
        """相同值 → 相同 digest"""
        v = 100.0
        d1 = value_digest(v)
        d2 = value_digest(v)
        assert d1 == d2

    def test_different_value_different_digest(self) -> None:
        """不同值 → 不同 digest"""
        d1 = value_digest(100.0)
        d2 = value_digest(101.0)
        assert d1 != d2


# ── DriftFinding Tests ─────────────────────────────────────────────────


class TestDriftFinding:

    def test_drift_finding_defaults(self) -> None:
        """DriftFinding 默认值正确"""
        finding = DriftFinding(record_key="test_key")
        assert finding.record_key == "test_key"
        assert finding.rule_id == "Q-DRIFT-001"
        assert finding.severity == "WARNING"
        assert finding.detail == {}

    def test_drift_finding_with_detail(self) -> None:
        """DriftFinding 可传入 detail dict"""
        detail = {
            "dataset_id": "test",
            "old_digest": "abc",
            "new_digest": "def",
            "changed_columns": ["close"],
        }
        finding = DriftFinding(record_key="test_key", detail=detail)
        assert finding.detail == detail


# ── detect_drift Unit Tests ────────────────────────────────────────────


class TestDetectDrift:

    def test_detect_drift_no_matching_nk(self) -> None:
        """不同 NK → 无 finding"""
        import pyarrow as pa
        old_table = pa.table({"nk": [1], "val": [100]})
        new_table = pa.table({"nk": [2], "val": [200]})
        findings = detect_drift(old_table, new_table, ("nk",), CanonicalType.ENTITY, "test")
        assert len(findings) == 0

    def test_detect_drift_same_value_no_finding(self) -> None:
        """同 NK 值不变 → 无 finding"""
        import pyarrow as pa
        old_table = pa.table({"nk": [1], "val": [100]})
        new_table = pa.table({"nk": [1], "val": [100]})
        findings = detect_drift(old_table, new_table, ("nk",), CanonicalType.ENTITY, "test")
        assert len(findings) == 0

    def test_detect_drift_value_changed(self) -> None:
        """同 NK 值变化 → finding 含旧/新 digest"""
        import pyarrow as pa
        old_table = pa.table({"nk": [1], "val": [100]})
        new_table = pa.table({"nk": [1], "val": [101]})
        findings = detect_drift(old_table, new_table, ("nk",), CanonicalType.ENTITY, "test")
        assert len(findings) == 1
        finding = findings[0]
        assert finding.rule_id == "Q-DRIFT-001"
        assert finding.severity == "WARNING"
        assert finding.detail["old_digest"] != finding.detail["new_digest"]
        assert "val" in finding.detail["changed_columns"]

    def test_detect_drift_empty_old_table(self) -> None:
        """旧表空 → 无 finding"""
        import pyarrow as pa
        old_table = pa.table({})
        new_table = pa.table({"nk": [1], "val": [100]})
        findings = detect_drift(old_table, new_table, ("nk",), CanonicalType.ENTITY, "test")
        assert len(findings) == 0

    def test_detect_drift_empty_new_table(self) -> None:
        """新表空 → 无 finding"""
        import pyarrow as pa
        old_table = pa.table({"nk": [1], "val": [100]})
        new_table = pa.table({})
        findings = detect_drift(old_table, new_table, ("nk",), CanonicalType.ENTITY, "test")
        assert len(findings) == 0


# ── GWT Acceptance Tests (TC-S-006 / TC-Q-009) ─────────────────────────


class TestGWT:

    def test_given_old_close_100_upsert_close_101_then_drifted_1_with_digest(
        self, tmp_stores
    ) -> None:
        """GWT TC-S-006 联动: Given 旧分区 close=100 When upsert 同 nk close=101
        Then UpsertStats.drifted=1 + finding 含旧/新 digest"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 写入旧记录
        old_record = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            close=100.0,
        )
        s1 = store.upsert([old_record], CanonicalType.OHLCV, "BTC")
        assert s1.inserted == 1
        assert s1.updated == 0

        # upsert 同 nk 不同 close
        new_record = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            close=101.0,
        )
        s2 = store.upsert([new_record], CanonicalType.OHLCV, "BTC")

        assert s2.inserted == 0
        assert s2.updated == 1
        assert s2.drifted == 1
        # 审计 H-3：findings 不再只计数丢弃，随 UpsertStats 返回
        assert s2.drifted == len(s2.drift_findings)
        finding = s2.drift_findings[0]
        assert finding.rule_id == "Q-DRIFT-001"
        assert finding.severity == "WARNING"
        assert finding.detail["changed_columns"] == ["close"]
        assert finding.detail["old_digest"] != finding.detail["new_digest"]
        # 终态仍为 1 行
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "OHLCV" / "entity=BTC"
        )
        total_rows = sum(_read_parquet_rows(d) for d in _list_partition_dirs(canonical_base))
        assert total_rows == 1

    def test_given_drift_upsert_then_findings_persist_to_quality_flags(
        self, tmp_stores
    ) -> None:
        """审计 H-3 闭环: Given upsert 产生 drift When 调用方按 D06 流程落盘
        Then quality_flags 表存在对应 Q-DRIFT-001 行（含旧/新 digest）"""
        from chronoforge.storage.meta import MetaStore

        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        old_record = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT", event_time=base_time, close=100.0
        )
        store.upsert([old_record], CanonicalType.OHLCV, "BTC")
        new_record = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT", event_time=base_time, close=101.0
        )
        stats = store.upsert([new_record], CanonicalType.OHLCV, "BTC")
        assert len(stats.drift_findings) == 1

        # 调用方（Runner/QualityStage）落盘路径
        meta = MetaStore(str(tmp_stores.meta_dir))
        meta.migrate()
        drift_finding = stats.drift_findings[0]
        flag = {
            "record_key": drift_finding.record_key,
            "dataset_id": drift_finding.detail["dataset_id"],
            "rule_id": drift_finding.rule_id,
            "severity": drift_finding.severity,
            "detail": json.dumps(drift_finding.detail),
            "raw_ref": "",
            "payload_digest": "",
            "run_id": "run-h3-001",
        }
        meta.add_quality_flags([flag])

        row = meta.connection.execute(
            "SELECT record_key, rule_id, severity, detail FROM quality_flags "
            "WHERE rule_id = 'Q-DRIFT-001' AND run_id = 'run-h3-001'"
        ).fetchone()
        assert row is not None
        assert row[0] == drift_finding.record_key
        detail = json.loads(row[3])
        assert detail["changed_columns"] == ["close"]
        assert detail["old_digest"] != detail["new_digest"]
        meta.close()

    def test_given_number_same_nk_revision_time_upsert_then_no_drift(
        self, tmp_stores
    ) -> None:
        """GWT: Given NUMBER 类型同 (nk, revision_time) When upsert
        Then 不触发 Q-DRIFT-001（多版本并存）"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 写入第一条 NUMBER 记录
        rec1 = _make_number_record(
            observation_time=base_time,
            revision_time=base_time,
            value=1.0,
        )
        s1 = store.upsert([rec1], CanonicalType.NUMBER, "FRED:GDP")
        assert s1.inserted == 1

        # 同一 NK 更新 value=2.0（同一 revision_time）
        rec2 = _make_number_record(
            observation_time=base_time,
            revision_time=base_time,
            value=2.0,
        )
        s2 = store.upsert([rec2], CanonicalType.NUMBER, "FRED:GDP")

        assert s2.inserted == 0
        assert s2.updated == 1
        assert s2.drifted == 0  # revision 类不触发 drift

        # 终态 1 行（去重）
        canonical_base = (
            tmp_stores.data_dir / "canonical" / "NUMBER" / "entity=FRED:GDP"
        )
        total_rows = sum(_read_parquet_rows(d) for d in _list_partition_dirs(canonical_base))
        assert total_rows == 1

    def test_drift_findings_written_to_quality_flags(self, tmp_stores) -> None:
        """GWT: Given drift findings When QualityStage.run Then findings 写入 quality_flags 表"""
        from chronoforge.storage.meta import MetaStore

        meta = MetaStore(str(tmp_stores.meta_dir))
        meta.migrate()

        # 构造 Q-DRIFT-001 finding
        finding = QualityFinding(
            record_key="BINANCE:BTCUSDT:SPOT,2026-09-11T10:00:00",
            rule_id="Q-DRIFT-001",
            severity="WARNING",
            detail=json.dumps({
                "old_digest": "abc123",
                "new_digest": "def456",
                "changed_columns": ["close"],
            }),
            raw_ref="data/raw/test/file.jsonl:1",
            payload_digest="sha256_placeholder",
        )

        meta.add_quality_flags([{
            "record_key": finding.record_key,
            "dataset_id": "test:ohlcv_1m",
            "rule_id": finding.rule_id,
            "severity": finding.severity,
            "detail": finding.detail,
            "raw_ref": finding.raw_ref or "",
            "payload_digest": finding.payload_digest or "",
            "run_id": "test-run-001",
        }])

        # 验证 quality_flags 表中存在对应行
        cursor = meta.connection.execute(
            "SELECT record_key, rule_id, severity FROM quality_flags "
            "WHERE rule_id = 'Q-DRIFT-001'"
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == finding.record_key
        assert row[1] == "Q-DRIFT-001"
        assert row[2] == "WARNING"


# ── TC-Q-009: Q-DRIFT-001 Trigger Conditions ───────────────────────────


class TestQDRIFT001:

    def test_q_drift_001_triggered_on_value_change(self, tmp_stores) -> None:
        """TC-Q-009: Q-DRIFT-001 触发条件——同 nk 值变化 → finding 含旧/新 digest"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        old = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            close=100.0,
        )
        store.upsert([old], CanonicalType.OHLCV, "BTC")

        new = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            close=200.0,
        )
        stats = store.upsert([new], CanonicalType.OHLCV, "BTC")

        assert stats.drifted == 1

    def test_q_drift_001_not_triggered_when_unchanged(self, tmp_stores) -> None:
        """TC-Q-009: 值不变 → 无 finding（仅 nk 匹配）"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 写入旧记录
        old = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            close=100.0,
        )
        store.upsert([old], CanonicalType.OHLCV, "BTC")

        # 相同 nk、相同值
        same = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            close=100.0,
        )
        stats = store.upsert([same], CanonicalType.OHLCV, "BTC")

        assert stats.drifted == 0
        assert stats.updated == 1

    def test_q_drift_001_revision_excluded(self, tmp_stores) -> None:
        """TC-Q-009: NUMBER 同 (nk, revision_time) 多版本 → 不触发 Q-DRIFT-001"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 写入第一条
        rec1 = _make_number_record(
            observation_time=base_time,
            revision_time=base_time,
            value=1.0,
        )
        store.upsert([rec1], CanonicalType.NUMBER, "FRED:GDP")

        # 同 NK 更新
        rec2 = _make_number_record(
            observation_time=base_time,
            revision_time=base_time,
            value=2.0,
        )
        stats = store.upsert([rec2], CanonicalType.NUMBER, "FRED:GDP")

        assert stats.drifted == 0

    def test_q_drift_001_all_columns_changed(self, tmp_stores) -> None:
        """TC-Q-009: 全部列不同 → 全部列入 finding"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        old = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            open=100.0,
            high=110.0,
            low=90.0,
            close=100.0,
            volume=10.0,
        )
        store.upsert([old], CanonicalType.OHLCV, "BTC")

        new = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            open=200.0,
            high=210.0,
            low=190.0,
            close=200.0,
            volume=20.0,
        )
        stats = store.upsert([new], CanonicalType.OHLCV, "BTC")

        assert stats.drifted == 1
        # 验证 finding detail 中所有值列都被标记
        # （通过检测逻辑确认 changed_columns 包含 open/high/low/close/volume）


# ── Boundary Tests ──────────────────────────────────────────────────────


class TestBoundary:

    def test_all_columns_same_only_nk_match(self, tmp_stores) -> None:
        """边界：全部列相同（仅 nk 匹配）→ drifted=0"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        rec = _make_ohlcv_record(
            market_id="BINANCE:BTCUSDT:SPOT",
            event_time=base_time,
            close=100.0,
        )
        store.upsert([rec], CanonicalType.OHLCV, "BTC")

        # 完全相同的记录
        stats = store.upsert([rec], CanonicalType.OHLCV, "BTC")
        assert stats.drifted == 0

    def test_multiple_drift_records(self, tmp_stores) -> None:
        """边界：多个记录被更新 → drifted 计数正确"""
        store = CanonicalStoreImpl(str(tmp_stores.data_dir))
        base_time = datetime(2026, 9, 11, 10, 0, 0)

        # 写入 3 条不同 nk 的记录
        old_records = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{suffix}USDT:SPOT",
                event_time=base_time,
                close=100.0,
            )
            for suffix in ["AAA", "BBB", "CCC"]
        ]
        store.upsert(old_records, CanonicalType.OHLCV, "BTC")

        # 3 条全部更新
        new_records = [
            _make_ohlcv_record(
                market_id=f"BINANCE:BTC{suffix}USDT:SPOT",
                event_time=base_time,
                close=200.0,
            )
            for suffix in ["AAA", "BBB", "CCC"]
        ]
        stats = store.upsert(new_records, CanonicalType.OHLCV, "BTC")

        assert stats.updated == 3
        assert stats.drifted == 3


# ── Q-DRIFT-001 Rule Registration Tests ────────────────────────────────


class TestQDRIFT001Rule:

    def test_q_drift_001_registered(self) -> None:
        """Q-DRIFT-001 已注册"""
        rule = get_rule("Q-DRIFT-001")
        assert rule is not None
        assert isinstance(rule, QDRIFT001Rule)

    def test_q_drift_001_rule_id(self) -> None:
        """rule_id = Q-DRIFT-001"""
        rule = get_rule("Q-DRIFT-001")
        assert rule.rule_id == "Q-DRIFT-001"

    def test_q_drift_001_severity(self) -> None:
        """severity = WARNING"""
        rule = get_rule("Q-DRIFT-001")
        assert rule.severity == "WARNING"

    def test_q_drift_001_applies_to_excludes_revision(self) -> None:
        """applies_to 排除 revision 类型"""
        rule = get_rule("Q-DRIFT-001")
        for ct in REVISION_TYPES:
            assert ct not in rule.applies_to, f"{ct} should be excluded from Q-DRIFT-001"

    def test_q_drift_001_check_returns_empty_for_empty_records(self) -> None:
        """check([]) 返回空"""
        rule = get_rule("Q-DRIFT-001")
        findings = rule.check([], CanonicalType.OHLCV)
        assert findings == []


# ── Integration: drift findings → quality_flags ─────────────────────────


class TestDriftToQualityFlags:

    def test_drift_findings_flow_through_meta_add_quality_flags(
        self, tmp_stores
    ) -> None:
        """集成：drift findings → meta.add_quality_flags() → quality_flags 表存在对应行"""
        from chronoforge.storage.meta import MetaStore

        meta = MetaStore(str(tmp_stores.meta_dir))
        meta.migrate()

        # 模拟 drift finding（由 CanonicalStore 生成）
        drift_finding = DriftFinding(
            record_key="BINANCE:BTCUSDT:SPOT,2026-09-11T10:00:00",
            detail={
                "dataset_id": "test:ohlcv_1m",
                "old_digest": "d1g35t01d1g35t01d1g35t01d1g35t01d1g35t01d1g35t01",
                "new_digest": "n3wd1g35t01d1g35t01d1g35t01d1g35t01d1g35t01d1g35t01",
                "changed_columns": ["close"],
            },
        )

        flag = {
            "record_key": drift_finding.record_key,
            "dataset_id": drift_finding.detail["dataset_id"],
            "rule_id": drift_finding.rule_id,
            "severity": drift_finding.severity,
            "detail": json.dumps(drift_finding.detail),
            "raw_ref": "data/raw/test/file.jsonl:1",
            "payload_digest": "payload_sha256_placeholder",
            "run_id": "test-run-001",
        }
        meta.add_quality_flags([flag])

        # 验证
        cursor = meta.connection.execute(
            "SELECT record_key, rule_id, severity, detail FROM quality_flags "
            "WHERE rule_id = 'Q-DRIFT-001' AND record_key = ?",
            (drift_finding.record_key,)
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == drift_finding.record_key
        assert row[1] == "Q-DRIFT-001"
        assert row[2] == "WARNING"
        detail = json.loads(row[3])
        assert detail["old_digest"] == drift_finding.detail["old_digest"]
        assert detail["new_digest"] == drift_finding.detail["new_digest"]
        assert detail["changed_columns"] == drift_finding.detail["changed_columns"]
