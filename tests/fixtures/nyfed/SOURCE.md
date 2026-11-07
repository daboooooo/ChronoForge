# Fixture 来源与形状说明

- 目录：`tests/fixtures/nyfed/`
- 形状核对日期：2026-11-07
- 来源：Federal Reserve Bank of New York Markets Data API（公开免 key）
  - `GET /api/rp/results/search.json?startDate=...&endDate=...&operationTypes=Repo`
  - `GET /api/soma/summary.json`
- 快照出处：本仓 `data/raw/nyfed/` 的真实抓取记录
  - `nyfed_repo_srf_usage/ingest_date=2026-11-06/142913-0001.jsonl`
  - `nyfed_soma_holdings/ingest_date=2026-11-06/152323-0001.jsonl`
  仅裁剪行数/分项，字段名与数值未改写。

## fixture 清单

- `repo_results/happy.json`：2025-11-05 的真实 3 笔 Overnight 操作
  （`RP 110525 1` 接受额 0 / `RP 110525 25` SVE 99M / `RP 110525 3` 真实 3M），
  预期 SRF 汇总 = 3,000,000。
- `repo_results/sve_only.json`：从 `happy.json` 中裁剪出的单条 SVE 行，
  用于"纯 SVE 日不产出记录"的负向用例（真实数据中不存在纯 SVE 日）。
- `repo_results/mixed_malformed.json`：1 条真实行 + 5 条合成畸形行
  （非数值金额 / 空日期 / Term 口径 / 缺金额 / 非对象），
  用于 note_skip 与丢弃门禁的负向用例；合成行以 `SYNTH-` 前缀标识。
- `soma_summary/happy.json`：2025-11-05 与 2026-11-04 两个真实快照
  （数值为字符串形态，与原响应一致），每行 9 个分项（含 total）。
