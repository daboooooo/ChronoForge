# coinmarketcap fixtures

- 来源类型：连接器注释 + 历史实测（未文档化网页端点，非官方 API 文档）
- 端点：`https://api.coinmarketcap.com/data-api/v3/global-metrics/quotes/historical`
  （浏览器网页端调用的内部接口，官方开发者文档未公开）
- 实抓尝试（2026-11-05）：连接超时（无响应），未能刷新抓包
- 核对日期：2026-11-05（形状与连接器 `_fetch_quotes` / `normalize`
  的字段访问一致：`data.quotes[]` 含 `timestamp`、`btcDominance` 等）
- 说明：fixture 为历史实测形状；数值为样本值，非逐条抓包。
