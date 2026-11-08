"""数据更新时刻配置：每日单一批次。

所有数据集统一在每日固定批次刷新，不再按数据源/数据集分散发布时刻：

- 批次时刻：北京时间 15:40 = **07:40 UTC**；
- 紧随其后的 16:00（08:00 UTC）触发每日流动性报告，报告读取的是本批次
  完成后的状态，不存在"定时时刻与实际发布时刻错配"的缝隙；
- 日频及以上数据集在该批次时刻到期（见 scripts/backfill_datasets.py 的
  `_next_due_at`）；小时级/4 小时级数据集仍由 scripts/hourly_update.sh 的
  整点循环按周期网格增量更新，二者共用同一把写锁；
- 批次完成状态写入 var/run/daily_update.json，报告以该文件为前置条件，
  而不是只按日历时刻硬碰。

为什么 07:40 UTC 是上限而不是缺陷：
07:40 UTC = 美东 03:40（夏令时）/ 02:40（冬令时），当日 EFFR（约 09:00 ET）
与 SOFR（约 08:00 ET）尚未发布，因此批次能拿到的最新利率是前一工作日
（报告口径 T-2）。除非把报告改到晚间，T-2 就是 16:00 报告的理论最好值。

DTS 的 PIT 发布时间（canonical `release_time`）与抓取批次是两件事，单独保留：
官方记录日次一工作日约 16:00 ET 发布，保守取次日 21:00 UTC。批次提前抓取
不会改变该记录"何时公开可用"的语义。
"""

from __future__ import annotations

# 每日统一更新批次（UTC）：北京时间 15:40 → 07:40 UTC。
DAILY_UPDATE_TIME_UTC: tuple[int, int] = (7, 40)

# U.S. Treasury DTS 的公开可用时刻（UTC）：记录日次一工作日约 16:00 ET，
# 保守取 21:00 UTC（北京次日 05:00）。仅用于 canonical release_time（PIT），
# 不参与抓取调度。
TREASURY_DTS_RELEASE_TIME_UTC: tuple[int, int] = (21, 0)
