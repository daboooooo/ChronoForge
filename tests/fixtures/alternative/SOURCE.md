# alternative fixtures

- 来源类型：真实抓包（公开 API，无需 key）
- 实抓（2026-11-05，HTTP 200）：
  `https://api.alternative.me/fng/?limit=3&format=json`
  → `fng_live_20261105.json`（3 行；首行含 `time_until_update`，为未冻结行）
- 真实信封：`{name, data[], metadata{error}}`；data 行为字符串 `value` +
  `value_classification` + 秒级 `timestamp`
- 已知语义：带 `time_until_update` 的未冻结行在 normalize 中跳过；
  冻结行 `release_time = observation_time + 1 day`
- 已有 fixture：`fng.json`、`fng_edge.json`、`fng_empty.json`
- 核对日期：2026-11-05
