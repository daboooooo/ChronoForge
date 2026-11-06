# fred fixtures

- 来源类型：官方 API 文档（`https://api.stlouisfed.org/fred`）
- 实抓尝试：2026-11-05 无 `api_key` 时返回 HTTP 400
  `variable api_key is not set`，未能抓包
- 裁剪说明：真实响应过大，`observations` 每文件保留 3–4 行；
  保留真实信封 `series_id/units/seasonal_adjustment/observations`，
  每条 observation 含 `date/value/realtime_start/realtime_end`
- 2026-11-05 补齐 `realtime_end="9999-12-31"`（此前缺失），
  表示当前 vintage 的有效期末；`vintage1/vintage2` 用于修订/双 vintage 测试
- `release_dates` 同样按官方 `series/release-dates` 响应形状保留
- 2026-11-06 用本地 `FRED_API_KEY` 实抓
  `GET /series?series_id=UNRATE&file_type=json`，完整响应保存为
  `series/UNRATE.json`（`units="Percent"`、
  `seasonal_adjustment="Seasonally Adjusted"`；审计 M7 元数据来源）
- 2026-11-06 同 key 实测 `observations`：`realtime_start=今天-60` 时，
  FRED 把早于窗口起点的旧观测 `realtime_start` 钳制到窗口起点
  （UNRATE 2023-01 观测返回 `2026-09-06`）；normalize 据此对钳制行
  使用稳定回退，避免 revision_time 随采集日漂移
