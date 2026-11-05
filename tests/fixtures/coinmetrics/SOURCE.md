# coinmetrics fixtures

- 来源类型：真实抓包（社区 API）+ 历史实测形状
- 实抓（2026-11-05，HTTP 200）：
  `https://community-api.coinmetrics.io/v4/timeseries/asset-metrics?assets=btc&metrics=AdrBalCnt&frequency=1d&page_size=3`
  → `live_20261105.json`（3 行，2026-11-02..04，含 `next_page_token` /
  `next_page_url`）
- 已有 fixture：`usdt_supply.json`（4 行）、`btc_addresses.json`、
  `page_head/page_tail.json`、`usdt_supply_edge.json`、`empty.json`
- 已知边界（写入连接器约定）：
  - 社区层 20 请求/分钟限流，可选 `COINMETRICS_API_KEY` 提升额度；
  - `start_time` 与 `end_time` 必须同时传；
  - 指标为 T+1 结算：`observation_time` = 源 `time`，`release_time` = 次日。
- 核对日期：2026-11-05
- 说明：`usdt_supply` 等历史 fixture 为真实形状裁剪；字段名/类型未改动。
