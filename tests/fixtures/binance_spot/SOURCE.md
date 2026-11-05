# binance_spot fixtures

- 来源类型：官方文档示例 / 客户端模型（Binance Spot API docs）
- 实抓尝试：2026-11-05 访问 `api.binance.com` 连接超时（网络不可达），未能刷新抓包
- 形状核对（2026-11-05，与官方文档一致）：
  - `klines`：12 元素数组 `[openTime, open, high, low, close, volume, closeTime,
    quoteVolume, trades, takerBuyBase, takerBuyQuote, ignore]`
  - `aggTrades`：`a/p/q/f/l/T/m/M`
  - `ticker24hr`：`symbol/lastPrice/openPrice/highPrice/lowPrice/volume/closeTime` 等
- 数值说明：fixture 中的行情数值为官方文档示例值，非生产抓包；
  契约测试只依赖字段形状与类型。
