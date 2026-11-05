# yahoo fixtures

- 来源类型：官方 `v8/finance/chart` 响应 schema / 客户端模型
- 实抓尝试：2026-11-05 请求 `query1.finance.yahoo.com/v8/finance/chart/AAPL`
  返回 HTTP 429（Edge: Too Many Requests），未能刷新抓包
- 真实路径：OHLCV 数组位于 `chart.result[0].indicators.quote[0]`，
  时间戳位于 `chart.result[0].timestamp`，事件位于 `chart.result[0].events`
- `happy.json`：真实 `indicators.quote[0]` 路径 + 5 个交易日
- `split.json`：按真实 v8 schema 重构（原为遗留的 `result[0].quote` 假形状，
  已改为 `indicators.quote[0]`），保留 `events.splits`
- `empty.json` / `error_no_result.json`：空结果与 `chart.error` 分支
- 数值说明：数值为示例值，非真实行情；契约测试依赖路径与字段类型。
