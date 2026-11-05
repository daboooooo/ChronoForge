# sosovalue fixtures

- 来源类型：历史实测响应形状（连接器注释记录的真实 API 形状）
- 实抓尝试（2026-11-05）：
  - `https://openapi.sosovalue.com/openapi/v1/etfs?symbol=BTC&country_code=US`
    返回 HTTP 403（Cloudflare 拦截，无 API key / 历史回看受限）
  - 已知边界：历史回看 ≥ `today-30d` 直接 403；summary 单响应 ≤20 行
    （与文档 `limit=300` 不符），连接器已按 20 行上限解析
- 核对日期：2026-11-05（形状与连接器注释/历史实测一致；未能刷新抓包）
- `etf_list.json` / `summary_history.json` / `etf_history_ibit.json`：
  保留真实信封 `{code, message, data}` 与字段名；数值为历史样本。
- 说明：未按官方文档推断字段，未新增未在连接器中出现的字段。
