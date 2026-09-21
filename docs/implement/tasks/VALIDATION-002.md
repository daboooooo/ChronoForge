# VALIDATION-002 — continuity_model 驱动的 gap 判定 + P0 简化交易日历

## 派发信息

- 任务单：D10 §4 VALIDATION-002
- 设计依据：D06 §2 Q-GAP-001（修复后）、D03 §1 continuity_model 列
- Agent：待定
- 派发时间：2026-09-12
- 状态：READY
- 依赖：VALIDATION-001.1（规则引擎框架）、STORAGE-001（continuity_model 查询）

## file_ownership

- `src/chronoforge/quality/continuity.py`（新建，continuity_model 判定逻辑）
- `src/chronoforge/quality/calendar.py`（新建，简化交易日历）
- `tests/unit/test_continuity.py`（新建）

## 交付物契约摘要

### ContinuityModel 枚举（quality/continuity.py）

```python
from enum import Enum

class ContinuityModel(str, Enum):
    ALWAYS_OPEN = "ALWAYS_OPEN"       # 7×24 不间断（加密源）
    TRADING_CALENDAR = "TRADING_CALENDAR"  # 交易日历（美股等）
    EVENT_BASED = "EVENT_BASED"        # 事件驱动（SEC filing）
    RELEASE_SCHEDULE = "RELEASE_SCHEDULE"  # 发布节奏（FRED）
```

### gap 判定逻辑（quality/continuity.py）

```python
def expected_grid(
    market_id: str,
    interval: str,
    continuity_model: ContinuityModel,
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, datetime, str]]:
    """
    生成期望网格（D06 §2 Q-GAP-001）。
    Returns:
        [(start, end, gap_type), ...] gap_type = "EXPECTED_GAP" | "REGULAR"
        EXPECTED_GAP 标注非交易日缺失
    """
    match continuity_model:
        case ContinuityModel.ALWAYS_OPEN:
            # 全网格，无豁免
            grid = date_range(start, end, freq=interval)
            return [(t, t + interval_delta(interval), "REGULAR") for t in grid]

        case ContinuityModel.TRADING_CALENDAR:
            # 剔除非交易日 → EXPECTED_GAP
            grid = []
            current = start
            while current <= end:
                if is_trading_day(current):
                    grid.append((current, current + interval_delta(interval), "REGULAR"))
                else:
                    # 找到连续的非交易日区间
                    non_trade_start = current
                    while current <= end and not is_trading_day(current):
                        current += interval_delta(interval)
                    grid.append((non_trade_start, current, "EXPECTED_GAP"))
                    continue
                current += interval_delta(interval)
            return grid

        case ContinuityModel.EVENT_BASED | ContinuityModel.RELEASE_SCHEDULE:
            # 无网格 gap 概念
            return []
```

### 简化交易日历（quality/calendar.py）

P0 内置美股简化日历（周末 + 固定节假日），无第三方依赖：

```python
# 固定美股节假日（NYSE 官方日历的简化版本）
US_MARKET_HOLIDAYS: set[tuple[int, int]] = {
    # (month, day)
    (1, 1),    # New Year's Day
    (1, 17),   # MLK Day (3rd Monday of January) → simplified
    (2, 14),   # Presidents' Day
    (5, 31),   # Memorial Day (last Monday of May) → simplified
    (6, 19),   # Juneteenth
    (7, 4),    # Independence Day
    (9, 7),    # Labor Day (1st Monday of September) → simplified
    (11, 27),  # Thanksgiving (4th Thursday of November) → simplified
    (12, 25),  # Christmas
}

# 提前闭市日（早闭）
EARLY_CLOSINGS: set[date] = {
    # 每年不同，P0 硬编码当年 + 次年
    date(2026, 7, 3),   # Independence Day observed
    date(2026, 12, 25), # Christmas observed
    # ...
}
```

**is_trading_day 函数**：

```python
def is_trading_day(d: date) -> bool:
    """
    判断是否为交易日（P0 简化日历，D06 §2）。
    周末 + 固定节假日 → False
    """
    if d.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    if (d.month, d.day) in US_MARKET_HOLIDAYS:
        return False
    if d in EARLY_CLOSINGS:
        return True  # 早闭日仍是交易日（只是早收盘）
    return True
```

### Q-GAP-001 与 continuity_model 集成

VALIDATION-001 的 Q-GAP-001 规则在 check 时：

1. 查 `dataset_registry.continuity_model`
2. 调用 `expected_grid(market_id, interval, continuity_model, start, end)`
3. 对比 actual 记录 → 生成 finding
4. **EXPECTED_GAP → INFO**（不告警）
5. **交易日缺失（ALWAYS_OPEN 或 TRADING_CALENDAR）→ WARNING**

### 扩展点

- P1 评估正式交易日历库（如 `nasdaq-basemaps`）
- 届时替换 `is_trading_day` 实现，接口不变

## 测试要求（D09 TC-Q 组）

- **TC-Q-007**：构造周末缺失的 yahoo 网格 → EXPECTED_GAP(INFO) 非 WARNING；交易日缺失 → WARNING
- **闰日/节假日/周末矩阵**：2026 年全日历遍历验证
- **ALWAYS_OPEN 全网格**：加密源周末缺失 → 仍 WARNING（无豁免）
- **边界**：年末感恩节固定表、2/29（闰年）
- **EXPECTED_GAP 标记**：非交易日缺失不产生 WARNING

## acceptance（GWT）

- [ ] Given yahoo 周六无 K 线 When Q-GAP-001 Then EXPECTED_GAP(INFO) 无 WARNING
- [ ] Given 交易日缺失 When Q-GAP-001 Then Unexpected Gap WARNING
- [ ] Given ALWAYS_OPEN 数据集周末缺失 When Q-GAP-001 Then Unexpected Gap WARNING（无豁免）

## 验收清单（验收时填写）

DoD 16 项 + GWT 逐条勾选。

| 验收项 | 状态 |
|---|---|
| TC-Q-007：周末 EXPECTED_GAP 非 WARNING | ✅ 通过 |
| 闰日/节假日/周末矩阵：2026 年全日历遍历 | ✅ 通过 |
| ALWAYS_OPEN 全网格：加密源周末缺失 → WARNING | ✅ 通过 |
| 边界：年末感恩节固定表、2/29（闰年） | ✅ 通过 |
| EXPECTED_GAP 标记：非交易日缺失不产生 WARNING | ✅ 通过 |
| GWT：yahoo 周六无 K 线 → EXPECTED_GAP(INFO) | ✅ 通过 |
| GWT：交易日缺失 → Unexpected Gap WARNING | ✅ 通过 |
| GWT：ALWAYS_OPEN 周末缺失 → WARNING（无豁免） | ✅ 通过 |

测试输出摘要：46 passed in 0.09s（test_continuity.py）+ 50 passed（test_quality_rules.py，含 Q-GAP-001 集成测试）｜接管性抽查：可凭任务单 + 设计文档理解 expected_grid 生成逻辑和 EXPECTED_GAP 判定规则｜git commit：待 Orchestrator 统一提交

## 执行记录

| 时间 | 事件 |
|---|---|
| 2026-09-15 | 新建 src/chronoforge/quality/calendar.py（简化交易日历，is_trading_day 函数） |
| 2026-09-15 | 新建 src/chronoforge/quality/continuity.py（ContinuityModel 枚举 + expected_grid 函数） |
| 2026-09-15 | 新建 tests/unit/test_continuity.py（46 测试用例） |
| 2026-09-15 | 集成 Q-GAP-001：QGap001Rule.check() 改用 expected_grid，支持 EXPECTED_GAP/INFO 判定 |
| 2026-09-15 | 更新 quality/__init__.py 导出 ContinuityModel |
| 2026-09-15 | 全库测试 776 passed，ruff/mypy 无新增错误 |

## 执行记录

| 时间 | 事件 |
|---|---|
| （执行时填写） | |

## Deferred Acceptance

无。本子任务独立闭环。
