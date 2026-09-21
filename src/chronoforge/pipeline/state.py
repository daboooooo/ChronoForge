"""RunStatus 运行状态机（D05 §3，PIPELINE-001）。

六个状态与 run_log.status 的 CHECK 约束一一对应，成员名与值相同。

转换矩阵（D05 §3）：

- PENDING + 阶段成功 → RUNNING；
  PENDING + Auth/Schema/Config 异常 → FAILED；
  PENDING + 用户取消 → CANCELLED。
- RUNNING + 阶段成功 → 末阶段 SUCCESS，中途保持 RUNNING；
  RUNNING + 可重试类异常耗尽 → PARTIAL_SUCCESS（保留已落盘）；
  RUNNING + Auth/Schema/Config 异常 → FAILED；
  RUNNING + 用户取消 → CANCELLED。
- SUCCESS / PARTIAL_SUCCESS / FAILED / CANCELLED 为终态；重跑 = 新 run_id。

配套不变量（D05 §3）：
- cursor 推进不变量：cursor = 最后一个已落盘且通过校验的 chunk 右边界
  （exclusive = 下窗口 start）；SUCCESS 与 PARTIAL_SUCCESS 均按此推进，
  失败 chunk 之后所有窗口不推进，恒有 checkpoint ≤ durable_valid_data_boundary。
- cursor 回退防护：新 cursor ≤ 旧 cursor → 跳过更新 + WARNING。
- 熔断：同 dataset 连续 3 次 FAILED → 后续 run 直接 CANCELLED
  （error_summary="circuit open"）。
"""

from __future__ import annotations

from enum import Enum


class RunStatus(str, Enum):  # noqa: UP042 （D05 §3 契约冻结为 str+Enum，与 market.Interval 同例）
    """run 生命周期状态（D05 §3 转换矩阵，值与成员名相同）。

    终态四值：SUCCESS / PARTIAL_SUCCESS / FAILED / CANCELLED；
    非终态两值：PENDING（try_lock_dataset 创建行时）/ RUNNING（执行中）。
    """

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


__all__ = ["RunStatus"]
