#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tickers 价差监控工具

监控 Binance Spot、OKX Spot、Binance UM Future 三个市场的 tickers 价差，
按价差大小分为10档（每档0.5%），实时展示价差分布情况。
"""

import logging
import subprocess
import sys
import time
import threading
import requests
import traceback
from typing import Dict, List, Optional
from dataclasses import dataclass
from collections import defaultdict
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich.logging import RichHandler


# 日志收集器类
class LogCollector:
    """收集日志用于在界面中显示"""

    def __init__(self, max_lines: int = 100):
        self.max_lines = max_lines
        self.logs: List[str] = []
        self._lock = threading.Lock()

    def add_log(self, message: str):
        """添加日志"""
        with self._lock:
            timestamp = time.strftime("%H:%M:%S")
            self.logs.append(f"[{timestamp}] {message}")
            # 保持最大行数
            if len(self.logs) > self.max_lines:
                self.logs = self.logs[-self.max_lines:]

    def get_logs(self, limit: int = 20) -> str:
        """获取最近的日志"""
        with self._lock:
            return "\n".join(self.logs[-limit:])

    def clear(self):
        """清空日志"""
        with self._lock:
            self.logs.clear()


# 全局日志收集器
log_collector = LogCollector()


# 自定义日志处理器
class UICollectorHandler(logging.Handler):
    """将日志发送到UI收集器"""

    def emit(self, record):
        try:
            msg = self.format(record)
            log_collector.add_log(msg)
        except Exception:
            self.handleError(record)


# 配置rich日志
console = Console()
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        RichHandler(rich_tracebacks=True, console=console),
        UICollectorHandler()
    ]
)
logger = logging.getLogger(__name__)

# API基础URL
API_BASE_URL = "http://localhost:8000/api"

# 档位配置：10档，每档0.5%
GAP_BUCKETS = [
    (0.0, 0.5, "极小区差", "green"),
    (0.5, 1.0, "小区差", "bright_green"),
    (1.0, 1.5, "中小差", "cyan"),
    (1.5, 2.0, "中等差", "bright_cyan"),
    (2.0, 2.5, "中大差", "yellow"),
    (2.5, 3.0, "较大差", "bright_yellow"),
    (3.0, 3.5, "大差", "orange3"),
    (3.5, 4.0, "很大差", "bright_red"),
    (4.0, 4.5, "极大差", "red"),
    (4.5, float('inf'), "超极差", "red3"),
]


@dataclass
class TickerData:
    """Ticker 数据结构"""
    symbol: str  # 完整交易对，如 BTC/USDT
    base: str    # 基础币种，如 BTC
    quote: str   # 计价币种，如 USDT
    price: float
    volume: float
    exchange: str
    market_type: str  # 'spot' 或 'um_future'


@dataclass
class PriceGap:
    """价差数据结构 - 以 Binance Spot 为基准"""
    base: str
    binance_spot_price: float
    okx_spot_price: float
    binance_um_price: float
    okx_spot_gap: float      # OKX Spot vs Binance Spot 价差%
    binance_um_gap: float    # Binance UM vs Binance Spot 价差%
    okx_um_gap: float        # OKX Spot vs Binance UM 价差%
    max_gap: float           # 最大价差%（用于分类）


class TickersGapMonitor:
    """Tickers 价差监控器"""

    def __init__(
        self,
        interval_seconds: int = 10,
        service_process: Optional[subprocess.Popen] = None
    ):
        self.interval = interval_seconds
        self.tickers_data: Dict[str, Dict[str, TickerData]] = defaultdict(dict)
        # 结构: {base_token: {market_key: TickerData}}
        # market_key 格式: "binance_spot", "okx_spot", "binance_um_future"

        self.price_gaps: List[PriceGap] = []
        self.gap_distribution: Dict[int, List[PriceGap]] = defaultdict(list)
        self._lock = threading.Lock()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._service_process = service_process
        self._consecutive_failures = 0

    def start(self):
        """启动监控"""
        if self._running:
            console.print("[yellow]⚠️ 监控器已在运行[/yellow]")
            return

        self._running = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            name="TickersGapMonitor",
            daemon=True
        )
        self._worker_thread.start()
        console.print(f"[green]✅ 价差监控器已启动（每{self.interval}秒刷新）[/green]")

    def stop(self):
        """停止监控"""
        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        console.print("[yellow]⚠️ 价差监控器已停止[/yellow]")

    def _worker_loop(self):
        """工作线程循环"""
        while self._running:
            try:
                # 检查服务健康状态
                if not self._check_service_health():
                    self._consecutive_failures += 1
                    logger.warning(
                        f"⚠️ 服务健康检查失败 ({self._consecutive_failures}/3)"
                    )
                    if self._consecutive_failures >= 3:
                        self._restart_service()
                        self._consecutive_failures = 0
                    time.sleep(self.interval)
                    continue

                self._consecutive_failures = 0
                logger.info("🔄 开始获取 tickers 数据...")
                self._fetch_all_tickers()
                self._calculate_gaps()
                self._classify_gaps()
                logger.info(
                    f"✅ 数据更新: {len(self.tickers_data)} tokens, "
                    f"{len(self.price_gaps)} gaps"
                )
                time.sleep(self.interval)
            except Exception as e:
                logger.error(f"❌ 工作线程出错: {e}")
                self._consecutive_failures += 1
                time.sleep(self.interval)

    def _check_service_health(self) -> bool:
        """检查服务健康状态"""
        return check_service_running(timeout=3)

    def _restart_service(self):
        """重启后端服务"""
        if self._service_process:
            self._service_process = restart_chronoforge_service(
                self._service_process
            )
        else:
            logger.warning("⚠️ 无法重启服务：没有服务进程引用")

    def _fetch_all_tickers(self):
        """获取所有市场的 tickers"""
        with self._lock:
            # 清空旧数据
            self.tickers_data.clear()

            # 1. 获取 Binance Spot tickers
            self._fetch_binance_spot()

            # 2. 获取 OKX Spot tickers
            self._fetch_okx_spot()

            # 3. 获取 Binance UM Future tickers
            self._fetch_binance_um_future()

    def _fetch_binance_spot(self):
        """获取 Binance Spot tickers"""
        try:
            delegate_call_url = f"{API_BASE_URL}/plugins/delegate-call"
            request_data = {
                "plugin_name": "CryptoSpotDataSource",
                "plugin_type": "data_source",
                "function_name": "tickers",
                "kwargs": {
                    "exchange_name": "binance",
                    "quote": "USDT"
                }
            }
            response = requests.post(delegate_call_url, json=request_data, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    tickers = result.get("result", {})
                    count = 0
                    for symbol, data in tickers.items():
                        if '/' in symbol:
                            parts = symbol.split('/')
                            if len(parts) == 2:
                                base, quote = parts
                                if quote == "USDT":
                                    price = float(data.get('last', 0))
                                    if price > 0:
                                        ticker = TickerData(
                                            symbol=symbol,
                                            base=base,
                                            quote=quote,
                                            price=price,
                                            volume=float(data.get('quoteVolume', 0)),
                                            exchange="binance",
                                            market_type="spot"
                                        )
                                        self.tickers_data[base]["binance_spot"] = ticker
                                        count += 1
                    logger.info(f"✅ Binance Spot: {count} 个交易对")
        except Exception as e:
            logger.warning(f"⚠️ Binance Spot 失败: {e}")

    def _fetch_okx_spot(self):
        """获取 OKX Spot tickers"""
        try:
            delegate_call_url = f"{API_BASE_URL}/plugins/delegate-call"
            request_data = {
                "plugin_name": "CryptoSpotDataSource",
                "plugin_type": "data_source",
                "function_name": "tickers",
                "kwargs": {
                    "exchange_name": "okx",
                    "quote": "USDT"
                }
            }
            response = requests.post(delegate_call_url, json=request_data, timeout=10)
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    tickers = result.get("result", {})
                    count = 0
                    for symbol, data in tickers.items():
                        if '/' in symbol:
                            parts = symbol.split('/')
                            if len(parts) == 2:
                                base, quote = parts
                                if quote == "USDT":
                                    price = float(data.get('last', 0))
                                    if price > 0:
                                        ticker = TickerData(
                                            symbol=symbol,
                                            base=base,
                                            quote=quote,
                                            price=price,
                                            volume=float(data.get('quoteVolume', 0)),
                                            exchange="okx",
                                            market_type="spot"
                                        )
                                        self.tickers_data[base]["okx_spot"] = ticker
                                        count += 1
                    logger.info(f"✅ OKX Spot: {count} 个交易对")
        except Exception as e:
            logger.warning(f"⚠️ OKX Spot 失败: {e}")

    def _fetch_binance_um_future(self):
        """获取 Binance UM Future tickers - 使用 API"""
        try:
            delegate_call_url = f"{API_BASE_URL}/plugins/delegate-call"
            request_data = {
                "plugin_name": "CryptoUMFutureDataSource",
                "plugin_type": "data_source",
                "function_name": "tickers",
                "kwargs": {
                    "quote": "USDT"
                }
            }
            response = requests.post(
                delegate_call_url, json=request_data, timeout=10
            )
            if response.status_code == 200:
                result = response.json()
                if result.get("success"):
                    tickers = result.get("result", {})
                    count = 0
                    for symbol, data in tickers.items():
                        # 解析交易对: BTC/USDT
                        if '/' in symbol:
                            parts = symbol.split('/')
                            if len(parts) == 2:
                                base, quote = parts
                                if quote == "USDT":
                                    price = float(data.get('last', 0))
                                    if price > 0:
                                        ticker = TickerData(
                                            symbol=symbol,
                                            base=base,
                                            quote=quote,
                                            price=price,
                                            volume=float(data.get('quoteVolume', 0)),
                                            exchange="binance",
                                            market_type="um_future"
                                        )
                                        self.tickers_data[base]["binance_um_future"] = ticker
                                        count += 1
                    logger.info(f"✅ Binance UM Future: {count} 个交易对")
                else:
                    logger.warning(
                        f"⚠️ Binance UM Future API 错误: {result.get('detail')}"
                    )
            else:
                logger.warning(
                    f"⚠️ Binance UM Future HTTP 错误: {response.status_code}"
                )
        except Exception as e:
            logger.warning(f"⚠️ Binance UM Future 失败: {e}")

    def _calculate_gaps(self):
        """计算价差 - 以 Binance Spot 为基准"""
        self.price_gaps = []

        with self._lock:
            for base, markets in self.tickers_data.items():
                # 必须以 Binance Spot 为基准
                binance_spot = markets.get("binance_spot")
                if not binance_spot or binance_spot.price <= 0:
                    continue

                okx_spot = markets.get("okx_spot")
                binance_um = markets.get("binance_um_future")

                # 至少需要一个其他市场数据
                if not okx_spot and not binance_um:
                    continue

                # 获取价格（没有数据则为0）
                okx_price = okx_spot.price if okx_spot else 0
                um_price = binance_um.price if binance_um else 0
                spot_price = binance_spot.price

                # 计算各价差
                okx_spot_gap = ((okx_price - spot_price) / spot_price * 100) if okx_price > 0 else 0
                binance_um_gap = ((um_price - spot_price) / spot_price * 100) if um_price > 0 else 0
                okx_um_gap = 0
                if okx_price > 0 and um_price > 0:
                    okx_um_gap = ((okx_price - um_price) / um_price * 100)

                # 取最大绝对价差用于分类
                gaps = [abs(g) for g in [okx_spot_gap, binance_um_gap, okx_um_gap] if g != 0]
                max_gap = max(gaps) if gaps else 0

                gap = PriceGap(
                    base=base,
                    binance_spot_price=spot_price,
                    okx_spot_price=okx_price,
                    binance_um_price=um_price,
                    okx_spot_gap=okx_spot_gap,
                    binance_um_gap=binance_um_gap,
                    okx_um_gap=okx_um_gap,
                    max_gap=max_gap
                )
                self.price_gaps.append(gap)

    def _classify_gaps(self):
        """将价差分类到10个档位"""
        self.gap_distribution.clear()

        for gap in self.price_gaps:
            # 使用最大绝对价差进行分类
            abs_gap = abs(gap.max_gap)

            for i, (min_val, max_val, _, _) in enumerate(GAP_BUCKETS):
                if min_val <= abs_gap < max_val:
                    self.gap_distribution[i].append(gap)
                    break

    def get_statistics(self) -> Dict:
        """获取统计信息"""
        with self._lock:
            total_tokens = len(self.tickers_data)
            tokens_with_all_markets = sum(
                1 for markets in self.tickers_data.values()
                if len(markets) >= 3
            )

            return {
                "total_tokens": total_tokens,
                "tokens_with_all_markets": tokens_with_all_markets,
                "total_gaps": len(self.price_gaps),
                "distribution": {
                    i: len(gaps) for i, gaps in self.gap_distribution.items()
                }
            }

    def get_gap_table(self) -> Table:
        """生成价差分布表格"""
        table = Table(title="Tickers 价差分布", show_header=True, header_style="bold magenta")
        table.add_column("档位", style="bold", justify="center")
        table.add_column("价差范围", justify="center")
        table.add_column("描述", justify="center")
        table.add_column("数量", justify="right")
        table.add_column("占比", justify="right")
        table.add_column("前10个Token", max_width=50)

        total_gaps = len(self.price_gaps)

        with self._lock:
            for i, (min_val, max_val, desc, color) in enumerate(GAP_BUCKETS):
                gaps = self.gap_distribution.get(i, [])
                count = len(gaps)
                percent = (count / total_gaps * 100) if total_gaps > 0 else 0

                # 取前10个token显示
                top_tokens = []
                for gap in gaps[:10]:
                    direction = "↑" if gap.max_gap > 0 else "↓"
                    top_tokens.append(f"{gap.base}{direction}")

                tokens_str = ", ".join(top_tokens) if top_tokens else "-"
                if len(gaps) > 10:
                    tokens_str += f" ... 等{len(gaps)-10}个"

                if max_val != float('inf'):
                    range_str = f"{min_val}% - {max_val}%"
                else:
                    range_str = f"> {min_val}%"

                table.add_row(
                    f"[bold {color}]{i+1}[/bold {color}]",
                    f"[{color}]{range_str}[/{color}]",
                    f"[{color}]{desc}[/{color}]",
                    str(count),
                    f"{percent:.1f}%",
                    tokens_str
                )

        return table

    def get_detail_table(self, bucket_index: int, limit: int = 50) -> Optional[Table]:
        """获取指定档位的详细列表 - 显示三个市场价格"""
        if bucket_index < 0 or bucket_index >= len(GAP_BUCKETS):
            return None

        min_val, max_val, desc, color = GAP_BUCKETS[bucket_index]

        with self._lock:
            gaps = self.gap_distribution.get(bucket_index, [])

        if not gaps:
            return None

        # 按最大价差绝对值排序
        gaps_sorted = sorted(gaps, key=lambda x: abs(x.max_gap), reverse=True)

        table = Table(
            title=f"档位 {bucket_index+1} 详情 - {desc} ({min_val}% - {max_val}%)",
            show_header=True,
            header_style="bold magenta"
        )
        table.add_column("排名", justify="right")
        table.add_column("Token")
        table.add_column("Binance Spot", justify="right")
        table.add_column("OKX Spot", justify="right")
        table.add_column("Binance UM", justify="right")
        table.add_column("OKX-Spot%", justify="right")
        table.add_column("UM-Spot%", justify="right")
        table.add_column("OKX-UM%", justify="right")

        for rank, gap in enumerate(gaps_sorted[:limit], 1):
            # 根据价差方向设置颜色
            okx_gap = gap.okx_spot_gap
            um_gap = gap.binance_um_gap
            okx_um_gap = gap.okx_um_gap

            okx_color = "green" if okx_gap > 0 else "red" if okx_gap < 0 else "white"
            um_color = "green" if um_gap > 0 else "red" if um_gap < 0 else "white"
            okx_um_color = "green" if okx_um_gap > 0 else "red" if okx_um_gap < 0 else "white"

            # 格式化价格和价差
            spot_price = f"{gap.binance_spot_price:.4f}" if gap.binance_spot_price > 0 else "-"
            okx_price = f"{gap.okx_spot_price:.4f}" if gap.okx_spot_price > 0 else "-"
            um_price = f"{gap.binance_um_price:.4f}" if gap.binance_um_price > 0 else "-"

            okx_gap_str = "-"
            if gap.okx_spot_price > 0:
                okx_gap_str = f"[{okx_color}]{okx_gap:+.2f}%[/{okx_color}]"

            um_gap_str = "-"
            if gap.binance_um_price > 0:
                um_gap_str = f"[{um_color}]{um_gap:+.2f}%[/{um_color}]"

            okx_um_gap_str = "-"
            if gap.okx_spot_price > 0 and gap.binance_um_price > 0:
                okx_um_gap_str = f"[{okx_um_color}]{okx_um_gap:+.2f}%[/{okx_um_color}]"

            table.add_row(
                str(rank),
                f"[bold]{gap.base}[/bold]",
                spot_price,
                okx_price,
                um_price,
                okx_gap_str,
                um_gap_str,
                okx_um_gap_str
            )

        if len(gaps) > limit:
            table.add_row(
                "...", "", "", "", "", "", "", "",
                f"[dim]共 {len(gaps)} 个，显示前 {limit} 个[/dim]"
            )

        return table


def check_service_running(timeout: int = 5) -> bool:
    """检查ChronoForge服务是否正在运行"""
    try:
        response = requests.get(f"{API_BASE_URL}/status", timeout=timeout)
        return response.status_code == 200
    except Exception:
        return False


def restart_chronoforge_service(process: Optional[subprocess.Popen]) -> Optional[subprocess.Popen]:
    """重启ChronoForge服务"""
    logger.warning("🔄 检测到服务无响应，正在重启...")

    # 先停止旧服务
    if process:
        try:
            process.terminate()
            process.wait(timeout=5)
            logger.info("✅ 旧服务已停止")
        except Exception as e:
            logger.warning(f"⚠️ 停止旧服务时出错: {e}")
            try:
                process.kill()
            except Exception:
                pass

    # 等待端口释放
    time.sleep(2)

    # 启动新服务
    return start_chronoforge_service()


def start_chronoforge_service() -> Optional[subprocess.Popen]:
    """启动ChronoForge服务"""
    if check_service_running():
        console.print("[green]✅ ChronoForge服务已经在运行[/green]")
        return None

    console.print("[bold blue]启动ChronoForge服务...[/bold blue]")
    try:
        process = subprocess.Popen(
            [sys.executable, "-m", "chronoforge.cli", "serve"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        time.sleep(3)

        if check_service_running():
            console.print("[green]✅ ChronoForge服务启动成功[/green]")
            return process
        else:
            console.print("[red]❌ 服务启动失败[/red]")
            process.terminate()
            return None
    except Exception as e:
        console.print(f"[red]❌ 启动服务时出错: {e}[/red]")
        return None


def display_auto_monitor(monitor: TickersGapMonitor):
    """自动监控模式：每30秒刷新，显示统计表和最高档位详情"""
    console.print("\n[bold cyan]=== 开始自动监控（每30秒刷新，按 Ctrl+C 停止）===[/bold cyan]\n")

    try:
        with Live(console=console, refresh_per_second=1) as live:
            while True:
                # 生成显示内容
                stats = monitor.get_statistics()

                # 创建布局
                layout = Layout()
                layout.split_column(
                    Layout(name="header", size=3),
                    Layout(name="main", ratio=3),
                    Layout(name="logs", size=12)
                )

                # 头部统计
                header_text = (
                    f"[bold]总Token数:[/bold] {stats['total_tokens']} | "
                    f"[bold]全市场覆盖:[/bold] {stats['tokens_with_all_markets']} | "
                    f"[bold]价差对数:[/bold] {stats['total_gaps']} | "
                    f"[bold]刷新时间:[/bold] {time.strftime('%H:%M:%S')}"
                )
                layout["header"].update(Panel(header_text, title="统计概览"))

                # 主区域分为统计表和档位详情
                main_layout = Layout()
                main_layout.split_row(
                    Layout(name="stats", ratio=1),
                    Layout(name="detail", ratio=2)
                )

                # 价差分布统计表
                main_layout["stats"].update(monitor.get_gap_table())

                # 找到满足条件的最大两个档位：档位>1 且 有token
                buckets_with_data = []
                for bucket_idx in range(len(GAP_BUCKETS) - 1, -1, -1):
                    # 条件1: 档位索引 > 1（即第3档及以上）
                    if bucket_idx <= 1:
                        continue
                    gaps = monitor.gap_distribution.get(bucket_idx, [])
                    # 条件2: 档位内有token（不为空）
                    if len(gaps) > 0:
                        buckets_with_data.append(bucket_idx)
                    if len(buckets_with_data) >= 2:
                        break

                # 显示找到的档位详情
                if buckets_with_data:
                    from rich.columns import Columns
                    detail_tables = []
                    for bucket_idx in buckets_with_data:
                        detail_table = monitor.get_detail_table(bucket_idx, limit=10)
                        if detail_table:
                            detail_tables.append(detail_table)
                    if detail_tables:
                        main_layout["detail"].update(Columns(detail_tables, equal=True))
                    else:
                        main_layout["detail"].update(Panel("[dim]暂无详情数据[/dim]", title="档位详情"))
                else:
                    main_layout["detail"].update(
                        Panel("[dim]暂无符合条件的档位（档位>1且非空）[/dim]", title="档位详情")
                    )

                layout["main"].update(main_layout)

                # 日志框
                log_content = log_collector.get_logs(limit=10)
                layout["logs"].update(
                    Panel(
                        log_content if log_content else "[dim]暂无日志...[/dim]",
                        title="运行日志",
                        border_style="blue"
                    )
                )

                live.update(layout)

                # 等待30秒
                time.sleep(30)

    except KeyboardInterrupt:
        console.print("\n[yellow]⚠️ 监控已停止[/yellow]")


def main():
    """主函数"""
    process = None

    try:
        # 显示页眉
        console.print("=" * 70)
        console.print("[bold cyan]Tickers 价差监控工具[/bold cyan]")
        console.print("[dim]监控 Binance Spot / OKX Spot / Binance UM Future 价差[/dim]")
        console.print("=" * 70)

        # 启动服务
        process = start_chronoforge_service()

        if not check_service_running():
            console.print("[red]❌ 服务未运行，无法继续[/red]")
            return False

        # 创建并启动监控器（30秒刷新间隔），传递服务进程用于自动重启
        monitor = TickersGapMonitor(
            interval_seconds=30,
            service_process=process
        )
        monitor.start()

        # 等待首次数据获取
        console.print("[dim]等待首次数据获取...[/dim]")
        time.sleep(3)

        # 进入自动监控模式
        display_auto_monitor(monitor)

        # 停止监控器
        monitor.stop()

        # 停止服务（如果是本脚本启动的）
        if process:
            console.print("[bold blue]停止ChronoForge服务...[/bold blue]")
            process.terminate()
            process.wait()
            console.print("[green]✅ 服务已停止[/green]")

        console.print("=" * 70)
        console.print("[bold cyan]程序执行完成[/bold cyan]")
        console.print("=" * 70)

        return True

    except Exception as e:
        console.print(f"\n[red]❌ 程序异常: {e}[/red]")
        traceback.print_exc()
        return False
    finally:
        if process:
            process.terminate()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
