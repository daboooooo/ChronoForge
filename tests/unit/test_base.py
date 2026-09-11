"""BaseRecord 基座单测（MODEL-001，依据 D02 §1 / D09 §3 TC-M 组）。

与 D09 §3 对应的用例，其 docstring 首行为 TC-ID（TC-M-001/003/009 的模型层部分）；
无 D09 编号的补充边界用例（D10 MODEL-001 tests.boundary/failure）以描述性首行标注。
"""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from chronoforge.models.base import BaseRecord
from chronoforge.models.enums import CanonicalType, QualityStatus


class _Sample(BaseRecord):
    """最小 BaseRecord 具体子类（本文件的合法样例载体，仅测试用）。"""


def _valid_kwargs() -> dict:
    """构造一份全部必填字段齐备的合法字段集。"""
    return {
        "schema_version": "1.0",
        "source": "binance_spot",
        "source_id": "BTCUSDT",
        "source_timestamp": datetime(2026, 9, 11, 0, 0, 0),
        "ingest_timestamp": datetime(2026, 9, 11, 0, 0, 1),
        "raw_record_id": "binance_spot:klines:20260911-000000-0001.jsonl:42",
    }


def test_valid_sample_instantiates() -> None:
    """TC-M-001: 合法样例通过（BaseRecord 部分；OHLCV 断言属 MODEL-002）。"""
    rec = _Sample(**_valid_kwargs())
    assert rec.schema_version == "1.0"
    assert rec.source == "binance_spot"
    assert rec.source_id == "BTCUSDT"
    assert rec.source_timestamp == datetime(2026, 9, 11)
    assert rec.ingest_timestamp == datetime(2026, 9, 11, 0, 0, 1)
    assert rec.raw_record_id == "binance_spot:klines:20260911-000000-0001.jsonl:42"


def test_bare_timestamp_rejected() -> None:
    """TC-M-003: 裸 timestamp 拒绝（extra=forbid，构造与赋值两条路径）。"""
    # 构造路径：传入未声明字段 timestamp
    with pytest.raises(ValidationError) as excinfo:
        _Sample(**_valid_kwargs() | {"timestamp": datetime(2026, 9, 11)})
    errors = excinfo.value.errors()
    assert any(e["type"] == "extra_forbidden" and e["loc"] == ("timestamp",) for e in errors)
    # 赋值路径：创建后追加未声明属性同样被拒
    rec = _Sample(**_valid_kwargs())
    with pytest.raises(ValidationError):
        rec.timestamp = datetime(2026, 9, 11)  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    "missing_field",
    [
        "source",
        "source_id",
        "source_timestamp",
        "ingest_timestamp",
        "raw_record_id",
    ],
)
def test_provenance_missing_field_rejected(missing_field: str) -> None:
    """TC-M-009: provenance 缺字段 → ValidationError（Q-PROV-001 属 VALIDATION-001）。"""
    kwargs = _valid_kwargs()
    del kwargs[missing_field]
    with pytest.raises(ValidationError) as excinfo:
        _Sample(**kwargs)
    errors = excinfo.value.errors()
    assert any(e["type"] == "missing" and e["loc"] == (missing_field,) for e in errors)


def test_canonical_type_member_roundtrip() -> None:
    """CanonicalType 全枚举遍历：成员数 == 27，str 值与名相同且 str(value) 往返。"""
    members = list(CanonicalType)
    assert len(members) == 27
    for member in members:
        assert member.value == member.name
        # str 值往返：str(value) 能还原同一成员
        assert CanonicalType(str(member.value)) is member
    # 非法枚举值（D10 failure：非法枚举）——枚举层直接构造失败
    with pytest.raises(ValueError):
        CanonicalType("NOT_A_TYPE")


def test_invalid_quality_status_rejected() -> None:
    """非法枚举值 → ValidationError（quality_status 传入非枚举字符串）。"""
    with pytest.raises(ValidationError) as excinfo:
        _Sample(**_valid_kwargs() | {"quality_status": "NOT_A_STATUS"})
    errors = excinfo.value.errors()
    assert any(e["type"] == "enum" and e["loc"] == ("quality_status",) for e in errors)


def test_validate_assignment_rejects_invalid_enum() -> None:
    """validate_assignment：实例创建后赋非法 quality_status 当场抛 ValidationError。"""
    rec = _Sample(**_valid_kwargs())
    with pytest.raises(ValidationError) as excinfo:
        rec.quality_status = "BOGUS"
    errors = excinfo.value.errors()
    assert any(e["type"] == "enum" and e["loc"] == ("quality_status",) for e in errors)
    # 赋值失败后原值不变
    assert rec.quality_status is QualityStatus.VALID
    # 合法赋值生效
    rec.quality_status = QualityStatus.SUSPECT
    assert rec.quality_status is QualityStatus.SUSPECT


def test_quality_field_defaults() -> None:
    """quality 字段默认值：不传时 quality_status=VALID、quality_reason=None。"""
    rec = _Sample(**_valid_kwargs())
    assert rec.quality_status is QualityStatus.VALID
    assert rec.quality_reason is None


def test_utc_naive_type_contract() -> None:
    """UTC naive 约定：aware datetime（UTC）被接受且原样保留（tz 剥离属 normalize 层，D01 §4）。"""
    aware = datetime(2026, 9, 11, 8, 30, 0, tzinfo=UTC)
    rec = _Sample(**_valid_kwargs() | {"ingest_timestamp": aware, "source_timestamp": aware})
    # 本层只验证类型契约：aware 值原样保留、不做 tz 剥离（naive 与 aware 比较恒不等）
    assert rec.ingest_timestamp == aware
    assert rec.source_timestamp == aware
    assert rec.ingest_timestamp.tzinfo is UTC
    # naive datetime 同样被接受（UTC naive 为全局存储约定，转换职责不在模型层）
    rec_naive = _Sample(**_valid_kwargs())
    assert rec_naive.ingest_timestamp.tzinfo is None
