# 美元流动性研究知识库

**Dollar Liquidity Research Knowledge Base**

由五份美元流动性主题 PDF 全文整理构建的离线研究知识库：适合人类递进式学习、研究者查询验证、AI Agent 检索推理。

- **来源**：`../pdf/` 下 5 份 PDF（共 49 页），全部逐页提取，无扫描页、无位图
- **规模**：121 个知识点 · 54 个概念 · 36 个变量 · 36 条关系 · 11 条公式 · 8 个案例 · 6 个宏观状态 · 8 个策略卡 · 12 条速查组合 · 10 项待核查
- **构建**：2026-10-27 ｜ 覆盖率 121/121（100%）

## 快速开始

### 人类使用（最简单）

双击打开 `index.html` 即可——**支持 file:// 直接离线运行**，无需本地服务器、无任何外部网络请求。

```bash
# macOS / Linux
open index.html          # 或直接双击
# Windows
start index.html
```

界面功能：六层递进章节（入门/进阶/研究三种阅读模式）、全站搜索、知识图谱（点击节点/连线查看详情）、15 张带完整解释的图表、案例时间线、策略卡片、数据字典、来源与覆盖矩阵。

> 若使用早期仅支持 http 的版本说明：本项目数据已内嵌为 `assets/js/data.js`，`file://` 协议可用；如需以服务方式运行：`python3 -m http.server 8000` 后访问 `http://localhost:8000`（可选，非必需）。

### AI Agent 使用

结构化数据全部位于 `data/*.json`（稳定 ID + 交叉引用），无需解析 HTML：

| 文件 | 内容 |
|---|---|
| `knowledge_points.json` | 121 条知识点登记表（原文摘录 + 规范化表述 + 来源页码 + 证据标签 + 验证状态） |
| `concepts.json` / `variables.json` | 概念字典（54）/ 变量与数据字典（36，含 FRED 序列与口径限制） |
| `relationships.json` | 36 条关系（类型、机制、条件、滞后、例外、置信度） |
| `formulas.json` | 11 条公式（含成立范围与省略项） |
| `cases.json` / `regimes.json` | 历史案例库（8）/ 宏观状态分类（6） |
| `strategies.json` / `patterns.json` | 策略卡（8）/ 模式匹配引擎设计 + 速查组合（12） |
| `evidence.json` | 证据标签定义、17 条证据记录、4 项冲突处理 |
| `open_questions.json` | 10 项待核查问题 |
| `agent_templates.json` | 4 个可复用研究任务模板（A 流动性分析 / B TGA 预测 / C 模式匹配 / D 策略评估） |
| `coverage_matrix.json` | 知识点 → 章节/锚点/状态 全量追溯矩阵 |
| `chapters.json` | 六层递进章节内容（HTML 界面的唯一内容源） |

可复用研究入口：
1. `TEMPLATE_A_LIQUIDITY_SNAPSHOT` —— 当前美元流动性分析
2. `TEMPLATE_B_TGA_RESERVE_FORECAST` —— 未来 1/2/4 周 TGA 与准备金预测
3. `TEMPLATE_C_PATTERN_MATCH` —— 历史模式匹配
4. `TEMPLATE_D_STRATEGY_EVAL` —— 交易策略评估

**权限边界**：知识库仅提供研究依据；下单、转账等高影响操作须独立授权机制。

## 目录结构

```text
GLM_5_3_flash/
├── index.html                  # 离线知识库入口（file:// 可用）
├── assets/
│   ├── css/main.css
│   └── js/
│       ├── data.js             # 由 data/*.json 自动打包（勿手工编辑）
│       ├── main.js             # 路由/渲染/搜索/阅读模式
│       ├── diagrams.js         # 15 张 SVG/HTML 图表
│       └── graph.js            # 交互式知识图谱
├── content/                    # （内容统一由 data/chapters.json 生成，此目录保留为空以匹配规范）
├── data/
│   ├── kp_{fed,liq,tga,tsy,dxy}.json   # 阶段A逐页知识点登记（按文档拆分）
│   ├── knowledge_points.json           # 合并后的主登记表（构建产物）
│   ├── concepts.json variables.json formulas.json relationships.json
│   ├── cases.json regimes.json strategies.json patterns.json
│   ├── evidence.json open_questions.json agent_templates.json
│   ├── chapters.json coverage_matrix.json sources.json
├── sources/
│   ├── source_manifest.json            # PDF 文件清单（uuid/页数/大小）
│   └── extraction_records/             # 阶段A提取记录 + 每文档逐页文本 page_NNN.txt
├── scripts/
│   ├── build_knowledge_base.py # 构建：合并知识点→覆盖矩阵→data.js→覆盖报告
│   ├── validate_json.py        # JSON 结构验证
│   ├── validate_coverage.py    # 覆盖率与引用完整性验证
│   └── run_tests.py            # 运行测试并生成 quality_report.md
├── tests/                      # 29 项单元测试（schema/引用/覆盖/链接/离线HTML）
└── reports/
    ├── coverage_report.md      # 覆盖率报告
    ├── quality_report.md       # 质量检查报告
    ├── conflicts_report.md     # 4 项文档间冲突及处理
    └── unresolved_questions.md # 10 项待核查清单
```

## 重建流程

```bash
cd GLM_5_3_flash
python3 scripts/build_knowledge_base.py   # 1. 合并知识点、生成覆盖矩阵与 data.js
python3 scripts/validate_json.py          # 2. JSON 结构验证
python3 scripts/validate_coverage.py      # 3. 覆盖率与引用验证
python3 scripts/run_tests.py              # 4. 29 项测试 + 质量报告
```

依赖：仅 Python 3 标准库。修改知识内容时**只编辑 `data/` 下 JSON**，然后重新执行上述四步——HTML 与 JSON 永不同步（单一数据源原则）。

## 核心研究规范（摘自 howto.md，本库全程执行）

1. **证据分层**：会计关系（ACCOUNTING）＞理论机制（MECHANISM）＞经验关系（EMPIRICAL）＞假设（HYPOTHESIS）＞策略（STRATEGY）。每张知识卡片带证据标签与验证状态，禁止跨层等同。
2. **典型示范**：『TGA 上升→准备金下降』是会计关系；『准备金下降→BTC 下跌』是需要检验的经验命题；『10 月下旬是 BTC 最佳买点』证据不足（DOC_TGA 的完整拆解见第三层与 L5-2）。
3. **不编造精确性**：原文未给出的时滞、概率、阈值一律标『未给出』；所有策略当前均为**尚未回测**，禁止虚构收益率/胜率/回撤。
4. **可追溯**：任一知识点 → 来源 PDF 页码 → 逐页文本文件；任一章节卡片 → 覆盖矩阵 → 原始知识点。
5. **数据时效**：回测禁止使用发布前不可得的数据（防前视偏差五元组：observation_date / release_date / retrieved_at / vintage_date / forecast_created_at）。

## 已知限制与后续扩展建议

- 原文数字（准备金 2.96/3.4/2.85 万亿、BTC 样本价格等）均为**作者口径，未经独立核验**（OPEN_QUESTION_LIQ_001）——已按规范保留原文并标注。
- 所有策略无回测结果；下一步建议按 DOC_TGA 三期方案实现数据管线（DTS/QRA/FRED API），对策略 A/C 做滚动样本外检验。
- 阈值类知识（3 万亿红线、12bp/17bp 利差、SRF 3B）为作者经验值，建议事件研究后修正。
- `content/` 目录按 howto 结构预留；实际内容以 `data/chapters.json` 为单一来源生成，避免 HTML 与 JSON 双头维护。
