"""tga_common 单元测试（Phase 2）：taxonomy 匹配 / config_hash / 工作日。"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from tga_common import (  # noqa: E402
    config_hash,
    is_business_day,
    load_taxonomy,
    load_yaml,
)


@pytest.fixture(scope="module")
def taxonomy():
    return load_taxonomy()


def test_tga_config_has_no_dead_keys():
    """C-2：不得保留未被脚本读取的死键（改键只变 config_hash、不生效）。"""
    root = Path(__file__).resolve().parents[2]
    model = load_yaml(str(root / "config" / "tga" / "model.yaml"))
    calendar = load_yaml(str(root / "config" / "tga" / "calendar.yaml"))
    assert "monthly_median_weeks" not in model["windows"]
    assert "same_month_years" not in model["windows"]
    assert "layer2_confidence" not in model["issuance"]
    assert "layer3_confidence" not in model["issuance"]
    assert "components" not in model
    assert "refund_season_months" not in calendar
    assert "federal_pay" not in calendar["outlay_cycles"]


def test_taxonomy_version_loaded(taxonomy):
    assert taxonomy.version == "tga-taxonomy-1.0"
    assert len(taxonomy.rules) > 20


def test_subtotal_row_excluded(taxonomy):
    cls = taxonomy.classify("Sub-Total Deposits", "Deposits")
    assert cls.is_subtotal is True
    assert cls.include_in_net is False
    assert cls.category == "SUBTOTAL"


def test_source_unclassified_kept_in_net(taxonomy):
    cls = taxonomy.classify("Unclassified", "Withdrawals")
    assert cls.category == "UNCLASSIFIED"
    assert cls.include_in_net is True
    assert cls.direction == "OUTFLOW"
    assert cls.confidence <= 0.2


def test_refund_beats_generic_tax(taxonomy):
    cls = taxonomy.classify("Taxes - Individual Tax Refunds (EFT)", "Withdrawals")
    assert cls.category == "REFUND"
    assert cls.flow_type == "REFUND"


def test_debt_direction_split(taxonomy):
    inflow = taxonomy.classify("Public Debt Cash Issues (Table III-B)", "Deposits")
    outflow = taxonomy.classify("Public Debt Cash Redemp. (Table IIIB)", "Withdrawals")
    assert inflow.category == "DEBT_ISSUANCE"
    assert outflow.category == "DEBT_REDEMPTION"
    assert inflow.include_in_net and outflow.include_in_net


def test_interest_and_tax_patterns(taxonomy):
    interest = taxonomy.classify("Interest on Treasury Securities", "Withdrawals")
    assert interest.category == "INTEREST"
    withheld = taxonomy.classify("Taxes - Withheld Individual/FICA", "Deposits")
    assert withheld.subcategory == "withheld_individual"
    corporate = taxonomy.classify("Taxes - Corporate Income", "Deposits")
    assert corporate.subcategory == "corporate"
    ftd = taxonomy.classify("Cash FTD's Received (Table IV)", "Deposits")
    assert ftd.subcategory == "federal_tax_deposits"


def test_benefit_patterns(taxonomy):
    ssa = taxonomy.classify("Social Security Benefits (EFT)", "Withdrawals")
    assert ssa.subcategory == "social_security"
    medicaid = taxonomy.classify("HHS - Grants to States for Medicaid", "Withdrawals")
    assert medicaid.subcategory == "medicaid"
    assert taxonomy.classify("Medicare", "Withdrawals").subcategory == "medicare"
    assert taxonomy.classify("VA - Benefits", "Withdrawals").subcategory == "veterans"


def test_agency_misc_falls_to_other_not_unclassified(taxonomy):
    """机构杂项兜底 → OTHER（§6.7 组件），不进 UNCLASSIFIED。"""
    cls = taxonomy.classify("Education Department programs", "Withdrawals")
    assert cls.category == "OTHER"
    assert cls.include_in_net is True
    assert cls.confidence >= 0.5


def test_unmatched_only_for_no_direction(taxonomy):
    """无方向（未知 transaction_type）才落入 unmatched。"""
    cls = taxonomy.classify("Totally New Category", None)
    assert cls.category == "UNCLASSIFIED"
    assert cls.matched_rule == "UNMATCHED"
    assert cls.direction == "UNKNOWN"


def test_variable_name_categories_matched(taxonomy):
    """变长类名（Uncollected Funds）与小写混合稳定匹配。"""
    cls = taxonomy.classify(
        "Change in Balance of Uncollected Funds (closing balance $ 217)", "Deposits"
    )
    assert cls.subcategory == "uncollected_funds"


def test_config_hash_stable(taxonomy):
    h1 = config_hash(taxonomy)
    h2 = config_hash(load_taxonomy())
    assert h1 == h2
    assert len(h1) == 16


def test_federal_holidays_weekend():
    assert not is_business_day(date(2026, 7, 4))  # 周六
    assert not is_business_day(date(2026, 7, 3))  # 独立日前夕（联邦假日）
    assert is_business_day(date(2026, 7, 6))  # 周一
