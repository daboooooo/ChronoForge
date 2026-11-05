# binance_futures fixtures

- 来源类型：官方文档示例 / 客户端模型（Binance USDⓈ-M Futures API docs）
- 实抓尝试：2026-11-05 访问 `fapi.binance.com` 连接超时（网络不可达），未能刷新抓包
- 形状核对（2026-11-05，与官方文档一致）：
  - `fundingRate`：`symbol/fundingRate/fundingTime/markPrice/nextFundingTime`
  - `klines`：12 元素数组
  - `openInterest`：`openInterest/symbol/time`
- 数值说明：fixture 中的数值为示例值，非生产抓包；契约测试只依赖字段形状与类型。
- 清理：`fundingRate/multi_page.json` 为非法 JSON 且无测试引用，已删除（2026-11-05）。
