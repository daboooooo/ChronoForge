# tests/fixtures — 源响应 fixture 规范（INFRA-001）

依据：`docs/design/09-testing-plan.md` §5、`docs/design/04-connectors.md` §6。

fixture 是 connector 契约测试的基准：内容为**源 API 真实响应的脱敏快照**
（RawBatch.payload 保持源响应原样，D04 §1），normalize 输出须与之逐字段比对
（TC-C-001~007）；错误响应 fixture 驱动重试/错误映射断言（TC-C-008~011）。

## 1. 目录命名

```
fixtures/{source}/{endpoint}/{form}.json
```

- `{source}` ∈ D04 §4 七源：`binance_spot` / `binance_futures` / `deribit` /
  `ccxt_binance` / `yahoo` / `fred` / `sec`
- `{endpoint}`：源侧端点名，snake_case（见 §4 矩阵）
- `{form}`：形态名（§2 三件套 + §3 十形态，连字符保留）
- 目录内仅存 JSON；禁止提交 py / 二进制 / 真实全量导出

## 2. 三件套规范（每 endpoint 最低要求）

每个 endpoint **至少**三个 JSON，缺一不可：

| 文件 | 语义 |
|---|---|
| `happy.json` | 正常 200 响应；normalize 输出与手写 canonical 期望逐字段一致（契约比对基准） |
| `edge.json` | 边界形态：页界/分区界/精度界（跨月、闰日 02-29、DST 切换日、年边界 12-31→01-01、max/min page size 1000/1、午夜 00:00 归属——TC-C-016） |
| `error.json` | 错误响应：401/429（含 Retry-After）/422/5xx 及 schema 破坏类；断言错误类型与重试次数 |

## 3. 十形态覆盖清单（D09 §5，最终审计 §38）

> 映射关系：happy≈normal、edge≈boundary、error≈malformed；十形态是三件套
> 之上的扩展维度，各源 fixture 按此清单补齐，**禁止只有"漂亮正常数据"**。
> 超出三件套的形态以形态名为文件名（如 `duplicated.json`、`large-volume.json`）。

- `normal`：常规正常数据（即三件套的 happy）
- `boundary`：页界/分区界/精度界（即三件套的 edge；含源侧秒/ms → us 的时间精度边界）
- `malformed`：结构不符录制契约（即三件套 error 中的 schema 破坏类，触发 SchemaError 熔断）
- `incomplete`：响应截断/字段缺失（分页末页不满页、可选字段缺失）
- `duplicated`：重复数据（同 natural key 重复出现，驱动 dedup/幂等）
- `out-of-order`：乱序到达（event_time 非单调，驱动排序与 continuity 判定）
- `late-arriving`：修订迟达（旧 observation 的更正晚到，驱动 revision/vintage 路径）
- `corrected`：值漂移（同 natural key 字段值变化，驱动 Q-DRIFT-001）
- `large-volume`：≥10⁴ 行合成批次（contract 一致性 + performance 冒烟；**合成生成，勿提交真实全量导出**）
- `empty`：空结果（空数组/空 observations，驱动 TC-P-008 空结果=SUCCESS(0 行)）

## 4. endpoint 矩阵（D09 §5 必备清单）

| source | endpoint | 形态补充说明（D09 §5） |
|---|---|---|
| binance_spot | klines | 1 页 / 跨月边界 / 空数组 |
| binance_spot | aggtrades | 跳号样例（Q-SEQ-001 对账） |
| binance_spot | ticker24hr | — |
| binance_futures | klines | — |
| binance_futures | funding_rate | fundingTime 滚动分页（cursor 推进无重叠） |
| binance_futures | open_interest | 快照全量 upsert |
| deribit | get_instruments | discover 全量 → INSTRUMENT 记录 |
| deribit | get_book_summary | OPTION 快照 |
| deribit | tvchart_data | OHLCV/IV 历史 |
| ccxt_binance | fetch_ohlcv | 2 页（cursor 推进、无重叠无缺口——TC-C-011） |
| yahoo | chart | normal / split / dividend / 429 页 |
| fred | observations | vintage1 / vintage2 / dates（双 vintage 并存——TC-M-007） |
| sec | submissions_recent | filing-recent 增量 |
| sec | companyfacts | 含 ETag/Last-Modified 条件请求样例 |

每行仍须满足 §2 三件套最低要求；矩阵与 D09 §5 清单一一对应，新增 endpoint
须同步更新本文件与 D09 §5。

## 5. 脱敏规范（强制）

- **禁止出现任何真实 key/token/cookie/签名**：命中 D08 §4 `REDACT_KEYS`
  同表键名（`api_key` / `apikey` / `token` / `authorization` / `x-api-key` /
  `fred_api_key` / `cookie` / `set-cookie`）的值一律以 `***` 替换
- URL query 中的凭证参数（如 `apikey=...`）同样掩码
- FRED fixture 不得包含有效 key；SEC fixture 仅含公开提交数据
- 提交前自查：`grep -rniE "(api[_-]?key|token|secret|authorization|cookie)" tests/fixtures/`
  应无明文命中
- 文件为 UTF-8、可被 `json.loads` 解析；单文件 ≤1MB（large-volume 用脚本合成生成，不落盘提交）

## 6. 使用约定

- 加载方式：connector 契约测试读取 JSON 后作为源响应注入（mock transport /
  本地响应对象），**禁止测试运行期联网**（pytest -m "not smoke" 本地与 CI 结果一致）
- fixture 内容为源响应原样（不清洗、不改字段名）；期望的 canonical 输出由
  各 connector 测试自带，不放在本目录
