"""已知事件配置（config/events/known_events.yaml）校验与读取行为测试。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from chronoforge.report import events as ev_mod
from chronoforge.report import facts as facts_mod


def _config(events: str) -> str:
    return f"""version: "1"
as_of: "2026-11-08"
sources:
  BLS_CPI:
    name: "BLS CPI 日程"
    url: "https://www.bls.gov/schedule/news_release/cpi.htm"
    accessed_at: "2026-11-08"
events:
{events}
"""


def _write(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "events.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_repo_config_is_valid_and_chronological() -> None:
    events = ev_mod.load_known_events()
    assert {e.event_type for e in events} == {"FOMC", "CPI", "NFP"}
    assert all(a.day <= b.day for a, b in zip(events, events[1:], strict=False))
    first_cpi = next(e for e in events if e.event_type == "CPI")
    assert first_cpi.day == date(2026, 11, 10)
    assert first_cpi.source_url.startswith("https://")
    assert first_cpi.accessed_at == date(2026, 11, 8)


def test_upcoming_selection_and_summary() -> None:
    events = ev_mod.load_known_events()
    summary = ev_mod.summarize_events(events, date(2026, 11, 9), horizon_days=90)
    assert summary["next_by_type"]["CPI"]["date"] == "2026-11-10"
    assert summary["next_by_type"]["NFP"]["date"] == "2026-12-04"
    assert summary["next_by_type"]["FOMC"]["date"] == "2026-12-09"
    assert summary["next_by_type"]["FOMC"]["status"] == "confirmed"
    assert summary["upcoming_count"] == len(summary["items"])
    assert summary["tentative_upcoming"] >= 1


def test_config_change_changes_facts_behavior(tmp_path: Path, monkeypatch) -> None:
    """改配置必须改变报告侧取到的事件；证明不是模板里写死。"""
    path = _write(
        tmp_path,
        _config(
            """  - id: CPI-X
    type: CPI
    date: "2026-11-10"
    time_et: "08:30"
    status: confirmed
    source: BLS_CPI
"""
        ),
    )
    monkeypatch.setattr(ev_mod, "EVENTS_PATH", path)
    first = facts_mod.load_events_block(date(2026, 11, 9))
    assert first["next_by_type"]["CPI"]["date"] == "2026-11-10"

    path.write_text(
        _config(
            """  - id: CPI-X
    type: CPI
    date: "2026-11-12"
    time_et: "08:30"
    status: confirmed
    source: BLS_CPI
"""
        ),
        encoding="utf-8",
    )
    second = facts_mod.load_events_block(date(2026, 11, 9))
    assert second["next_by_type"]["CPI"]["date"] == "2026-11-12"


def test_invalid_config_degrades_to_explicit_error(tmp_path: Path, monkeypatch) -> None:
    path = _write(tmp_path, "version: '1'\nas_of: '2026-11-08'\nevents: []\n")
    monkeypatch.setattr(ev_mod, "EVENTS_PATH", path)
    block = facts_mod.load_events_block(date(2026, 11, 9))
    assert "error" in block
    assert block["next_by_type"] == {}


@pytest.mark.parametrize(
    ("events", "match"),
    [
        (
            """  - id: A
    type: CPI
    date: "2026-11-10"
    time_et: "08:30"
    status: confirmed
    source: BLS_CPI
  - id: B
    type: CPI
    date: "2026-11-10"
    time_et: "08:30"
    status: confirmed
    source: BLS_CPI
""",
            "重复事件",
        ),
        (
            """  - id: A
    type: CPI
    date: "2026-12-10"
    time_et: "08:30"
    status: confirmed
    source: BLS_CPI
  - id: B
    type: NFP
    date: "2026-11-06"
    time_et: "08:30"
    status: confirmed
    source: BLS_CPI
""",
            "升序",
        ),
        (
            """  - id: A
    type: CPI
    date: "2026-11-10"
    time_et: "8:30"
    status: confirmed
    source: BLS_CPI
""",
            "time_et",
        ),
        (
            """  - id: A
    type: CPI
    date: "2026-11-10"
    time_et: "08:30"
    status: maybe
    source: BLS_CPI
""",
            "status",
        ),
        (
            """  - id: A
    type: CPI
    date: "2026-11-10"
    time_et: "08:30"
    status: confirmed
    source: NOPE
""",
            "未定义的 source",
        ),
    ],
)
def test_config_validation_rejects_bad_input(
    tmp_path: Path, events: str, match: str
) -> None:
    path = _write(tmp_path, _config(events))
    with pytest.raises(ev_mod.EventConfigError, match=match):
        ev_mod.load_known_events(path)
