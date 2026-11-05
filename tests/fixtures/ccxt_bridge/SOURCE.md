# ccxt_bridge fixtures

- 来源类型：客户端模型（ccxt unified API 返回结构）
- 依据：ccxt 统一接口约定
  - `fetch_ohlcv`：每行 `[timestamp, open, high, low, close, volume]`
    （6 元素）；部分交易所兼容层额外返回第 7 元素，连接器需两种变体都接受
  - `fetch_trades`：`{id, timestamp, datetime, symbol, price, amount, side, ...}`
- 实抓说明：ccxt_bridge 不是独立数据源，而是封装 ccxt 客户端；
  fixture 用于锁定统一的输入形状，不代表某一家交易所的原始响应。
- 历史事故点：OHLCV 7 元素变体曾被误判为 6 元素，导致列位错配；
  `test_normalize_ohlcv_legacy_7_element_candle` 与 fetch 全链路用例共同锁定。
- 核对日期：2026-11-05
