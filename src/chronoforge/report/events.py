"""已知发布事件（FOMC/CPI/NFP）配置加载与校验。

配置只人工维护日期，不做自动抓取。本模块负责：
- 字段存在性与格式校验（日期、ET 时间、状态、来源引用）；
- 文件内时序单调与重复检测；
- 按报告日筛选未来事件，供「前瞻 ⑤」使用。

配置改动会直接改变报告内容；``load_known_events`` 不缓存，避免读到旧日程。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

PROJECT_ROOT = Path(__file__).resolve().parents[3]
EVENTS_PATH = PROJECT_ROOT / "config" / "events" / "known_events.yaml"
EVENT_TYPES = frozenset({"FOMC", "CPI", "NFP"})
STATUSES = frozenset({"confirmed", "tentative"})
_TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class EventConfigError(ValueError):
    """known_events.yaml 结构、取值或顺序校验失败。"""


@dataclass(frozen=True)
class KnownEvent:
    event_id: str
    event_type: str
    day: date
    time_et: str
    status: str
    source_id: str
    source_name: str
    source_url: str
    accessed_at: date
    as_of: date
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.event_id,
            "type": self.event_type,
            "date": self.day.isoformat(),
            "time_et": self.time_et,
            "status": self.status,
            "source": self.source_id,
            "source_name": self.source_name,
            "source_url": self.source_url,
            "accessed_at": self.accessed_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "note": self.note,
        }


def _parse_date(value: Any, where: str) -> date:
    if not isinstance(value, str) or not _DATE_RE.fullmatch(value):
        raise EventConfigError(f"{where}: 日期必须是 YYYY-MM-DD 字符串，实际 {value!r}")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise EventConfigError(f"{where}: 非法日期 {value!r}") from exc


def _require_text(raw: dict[str, Any], key: str, where: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise EventConfigError(f"{where}: 缺少非空字符串字段 {key!r}")
    return value.strip()


def _load_sources(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    sources = data.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise EventConfigError("sources: 必须是非空映射")
    out: dict[str, dict[str, Any]] = {}
    for source_id, raw in sources.items():
        where = f"sources.{source_id}"
        if not isinstance(raw, dict):
            raise EventConfigError(f"{where}: 必须是映射")
        url = _require_text(raw, "url", where)
        if not url.startswith(("https://", "http://")):
            raise EventConfigError(f"{where}: url 必须是 http(s) 地址，实际 {url!r}")
        accessed = _parse_date(raw.get("accessed_at"), f"{where}.accessed_at")
        out[str(source_id)] = {
            "name": _require_text(raw, "name", where),
            "url": url,
            "accessed_at": accessed,
        }
    return out


def load_known_events(path: str | Path | None = None) -> list[KnownEvent]:
    """读取并校验配置；返回按日期升序的事件列表。"""
    config_path = Path(path) if path else EVENTS_PATH
    try:
        with open(config_path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except OSError as exc:
        raise EventConfigError(f"无法读取事件配置 {config_path}: {exc}") from exc
    if not isinstance(data, dict):
        raise EventConfigError("配置文件根节点必须是映射")
    version = data.get("version")
    if not isinstance(version, str) or not version.strip():
        raise EventConfigError("缺少非空字符串字段 'version'")

    root_as_of = _parse_date(data.get("as_of"), "as_of")
    sources = _load_sources(data)
    raw_events = data.get("events")
    if not isinstance(raw_events, list) or not raw_events:
        raise EventConfigError("events: 必须是非空列表")

    events: list[KnownEvent] = []
    seen_ids: set[str] = set()
    seen_keys: dict[tuple[str, date], str] = {}
    previous_day: date | None = None

    for index, raw in enumerate(raw_events):
        where = f"events[{index}]"
        if not isinstance(raw, dict):
            raise EventConfigError(f"{where}: 必须是映射")
        event_id = _require_text(raw, "id", where)
        if event_id in seen_ids:
            raise EventConfigError(f"{where}: 事件 id 重复 {event_id!r}")
        seen_ids.add(event_id)

        event_type = _require_text(raw, "type", where)
        if event_type not in EVENT_TYPES:
            raise EventConfigError(
                f"{where}: type 必须是 {sorted(EVENT_TYPES)} 之一，实际 {event_type!r}"
            )
        status = _require_text(raw, "status", where)
        if status not in STATUSES:
            raise EventConfigError(
                f"{where}: status 必须是 {sorted(STATUSES)} 之一，实际 {status!r}"
            )
        day = _parse_date(raw.get("date"), f"{where}.date")
        time_et = _require_text(raw, "time_et", where)
        if not _TIME_RE.fullmatch(time_et):
            raise EventConfigError(f"{where}: time_et 必须是 24 小时制 HH:MM，实际 {time_et!r}")

        key = (event_type, day)
        if key in seen_keys:
            raise EventConfigError(
                f"{where}: 重复事件 {event_type} {day.isoformat()}（已由 {seen_keys[key]} 定义）"
            )
        seen_keys[key] = event_id

        if previous_day is not None and day < previous_day:
            raise EventConfigError(
                f"{where}: 事件必须按日期升序排列，"
                f"{day.isoformat()} 早于前一条 {previous_day.isoformat()}"
            )
        previous_day = day

        source_id = _require_text(raw, "source", where)
        source = sources.get(source_id)
        if source is None:
            raise EventConfigError(
                f"{where}: 引用了未定义的 source {source_id!r}（已定义 {sorted(sources)}）"
            )
        event_as_of = (
            _parse_date(raw["as_of"], f"{where}.as_of")
            if "as_of" in raw else root_as_of
        )
        note = raw.get("note", "")
        if not isinstance(note, str):
            raise EventConfigError(f"{where}: note 必须是字符串")

        events.append(
            KnownEvent(
                event_id=event_id,
                event_type=event_type,
                day=day,
                time_et=time_et,
                status=status,
                source_id=source_id,
                source_name=source["name"],
                source_url=source["url"],
                accessed_at=source["accessed_at"],
                as_of=event_as_of,
                note=note.strip(),
            )
        )
    return events


def select_upcoming(
    events: list[KnownEvent], report_day: date, horizon_days: int = 90
) -> list[KnownEvent]:
    """返回 [report_day, report_day + horizon_days] 内的已配置事件。"""
    end = report_day + timedelta(days=horizon_days)
    return [e for e in events if report_day <= e.day <= end]


def summarize_events(
    events: list[KnownEvent], report_day: date, horizon_days: int = 90
) -> dict[str, Any]:
    """供 facts 使用的事件摘要：每种类型最近一次 + 展望窗口计数。"""
    upcoming = select_upcoming(events, report_day, horizon_days)
    next_by_type: dict[str, dict[str, Any]] = {}
    for event in upcoming:
        if event.event_type not in next_by_type:
            next_by_type[event.event_type] = event.to_dict()
    latest_as_of = max((e.as_of for e in events), default=None)
    latest_access = max((e.accessed_at for e in events), default=None)
    coverage = max((e.day for e in events), default=None)
    try:
        display_path = str(EVENTS_PATH.relative_to(PROJECT_ROOT))
    except ValueError:
        display_path = str(EVENTS_PATH)
    return {
        "config_path": display_path,
        "as_of": latest_as_of.isoformat() if latest_as_of else None,
        "accessed_at": latest_access.isoformat() if latest_access else None,
        "coverage_through": coverage.isoformat() if coverage else None,
        "horizon_days": horizon_days,
        "upcoming_count": len(upcoming),
        "tentative_upcoming": sum(1 for e in upcoming if e.status == "tentative"),
        "next_by_type": next_by_type,
        "items": [e.to_dict() for e in upcoming],
    }
