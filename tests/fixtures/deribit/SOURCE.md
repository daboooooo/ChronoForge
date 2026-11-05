# Fixture 来源与形状说明

- 目录：`tests/fixtures/deribit/`
- 形状核对日期：2026-11-05
- 依据：
  - Deribit v2 JSON-RPC 公共接口（`POST /api/v2`，响应为
    `{"jsonrpc":"2.0","result":...}` envelope）。
  - `public/get_tradingview_chart_data`：result 为并行数组
    `status/ticks/open/high/low/close/volume/cost/continuation`
    （参考 docs.rs `deribit-http` 0.7 `TradingViewChartData`）。
  - `public/get_book_summary_by_currency`：result 为 BookSummary 列表
    （参考 docs.rs `deribit-http` 0.7 `BookSummary`）。
- 说明：样本为最小必要字段的真实形状裁剪，数值非逐笔抓包；
  2026-11-05 当前网络环境无法直连 Deribit 公共 API（curl 无响应），
  形状以官方 v2 接口与上述客户端模型定义为准。
- fixture 清单：
  - `get_instruments/{happy,empty}.json`：`result` 为 instrument 列表。
  - `get_tradingview_chart_data/{happy,empty}.json`：JSON-RPC envelope，
    `result` 为并行数组（此前误写为 `candles` 列表）。
  - `get_book_summary/happy.json`：JSON-RPC envelope，`result` 为
    BookSummary 列表（此前误写为按币种键控的 dict）。
