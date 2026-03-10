"""
DuckDB存储示例和性能测试模块

提供完整的功能演示和性能基准测试，展示DuckDB存储系统的各项能力。
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import time
import os
from typing import Dict, List, Any

# 导入DuckDB存储模块
from chronoforge.storage import (
    FinancialDataWarehouse,
    DUCKDBStorage,
    SmartIncrementalManager,
    DataValidator,
    DataQualityMonitor
)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DuckDBStorageDemo:
    """DuckDB存储系统功能演示"""

    def __init__(self, db_path: str = "./demo_warehouse.db"):
        self.db_path = db_path
        self.warehouse = None
        self.storage = None
        self.incremental_manager = None

    async def setup(self):
        """初始化存储系统"""
        print("🚀 初始化DuckDB存储系统...")

        # 清理旧数据库
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        # 创建数据仓库
        self.warehouse = FinancialDataWarehouse(self.db_path)

        # 创建统一存储适配器
        self.storage = DUCKDBStorage({"db_path": self.db_path, "enable_cache": True})

        # 创建增量管理器
        self.incremental_manager = SmartIncrementalManager(self.warehouse)

        print("✅ DuckDB存储系统初始化完成")

    async def cleanup(self):
        """清理资源"""
        if self.warehouse:
            await self.warehouse.close()
        if self.storage:
            await self.storage.close()

        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    async def demo_basic_operations(self):
        """演示基本操作"""
        print("\n📊 演示基本操作...")

        # 1. 插入symbols数据
        symbols = [
            {
                'symbol': 'BTC/USDT',
                'exchange': 'binance',
                'market_type': 'spot',
                'base_asset': 'BTC',
                'quote_asset': 'USDT',
                'active': True
            },
            {
                'symbol': 'ETH/USDT',
                'exchange': 'binance',
                'market_type': 'spot',
                'base_asset': 'ETH',
                'quote_asset': 'USDT',
                'active': True
            }
        ]

        success = await self.warehouse.insert_symbols(symbols)
        print(f"✓ Symbols插入: {success}")

        # 2. 插入OHLCV数据
        ohlcv_data = []
        base_time = datetime.now() - timedelta(days=10)

        for i in range(10):
            current_time = base_time + timedelta(days=i)
            close_price = 40000 + i * 100 + (i % 3) * 50

            record = {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': 'BTC/USDT',
                'timeframe': '1d',
                'ts': current_time,
                'open': close_price - np.random.uniform(100, 200),
                'high': close_price + np.random.uniform(0, 300),
                'low': close_price - np.random.uniform(200, 400),
                'close': close_price,
                'volume': np.random.uniform(1000, 5000),
                'quote_volume': close_price * np.random.uniform(1000, 5000),
                'source': 'demo'
            }
            ohlcv_data.append(record)

        success = await self.warehouse.insert_ohlcv(ohlcv_data)
        print(f"✓ OHLCV数据插入: {success}")

        # 3. 查询数据
        result = await self.warehouse.get_ohlcv('BTC/USDT', '1d')
        if result is not None:
            print(f"✓ 查询到 {len(result)} 条OHLCV记录")
            print("前3条数据预览:")
            print(result[['ts', 'open', 'high', 'low', 'close', 'volume']].head(3))

        # 4. 获取统计信息
        stats = await self.warehouse.get_stats()
        print(f"✓ 数据库统计: {stats}")

    async def demo_unified_storage(self):
        """演示统一存储接口"""
        print("\n🔄 演示统一存储接口...")

        # 使用统一存储保存数据（自动识别数据类型）
        ohlcv_df = pd.DataFrame({
            'ts': [datetime.now() - timedelta(days=i) for i in range(5)],
            'open': [100.0, 101.0, 102.0, 103.0, 104.0],
            'high': [101.0, 102.0, 103.0, 104.0, 105.0],
            'low': [99.0, 100.0, 101.0, 102.0, 103.0],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5],
            'volume': [1000, 1100, 1200, 1300, 1400]
        })

        success = await self.storage.save("ETH_USDT_1d", ohlcv_df)
        print(f"✓ 统一存储保存: {success}")

        # 加载数据
        loaded_data = await self.storage.load("ETH_USDT_1d")
        if loaded_data is not None:
            print(f"✓ 统一存储加载成功，数据形状: {loaded_data.shape}")

        # 列出所有数据
        data_list = await self.storage.lists()
        print(f"✓ 找到 {len(data_list)} 个数据项")

    async def demo_incremental_updates(self):
        """演示增量更新功能"""
        print("\n⚡ 演示增量更新功能...")

        # 1. 获取数据更新建议
        suggestion = await self.incremental_manager.suggest_data_fetch_range(
            'BTC/USDT', '1d'
        )
        print(f"✓ 数据更新建议: {suggestion}")

        # 2. 插入增量数据
        latest_ts = await self.incremental_manager.get_latest_timestamp(
            'ohlcv', 'BTC/USDT', '1d'
        )

        if latest_ts:
            new_data = []
            for i in range(1, 4):  # 添加3天的新数据
                record = {
                    'exchange': 'binance',
                    'market_type': 'spot',
                    'symbol': 'BTC/USDT',
                    'timeframe': '1d',
                    'ts': latest_ts + timedelta(days=i),
                    'open': 40700 + i * 100,
                    'high': 40800 + i * 100,
                    'low': 40600 + i * 100,
                    'close': 40750 + i * 100,
                    'volume': 1700 + i * 100,
                    'quote_volume': 40700000 + i * 1000000,
                    'source': 'incremental'
                }
                new_data.append(record)

            result = await self.incremental_manager.smart_insert_ohlcv(new_data)
            print(f"✓ 增量更新结果: {result}")

        # 3. 批量更新建议
        batch_suggestions = await self.incremental_manager.get_batch_update_suggestions(
            ['BTC/USDT', 'ETH/USDT'], ['1d', '1h']
        )
        summary = batch_suggestions['summary']
        print(f"✓ 批量更新摘要: {summary}")

    async def demo_data_quality(self):
        """演示数据质量控制"""
        print("\n🔍 演示数据质量控制...")

        # 创建一些包含质量问题的测试数据
        test_data = pd.DataFrame({
            'ts': [datetime.now() - timedelta(days=i) for i in range(10)],
            'open': [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0],
            'high': [101.0, 102.0, 103.0, 99.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0],  # 第4条 high < open
            'low': [99.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0],
            'close': [100.5, 101.5, 102.5, 103.5, 104.5, 105.5, 106.5, 107.5, 108.5, 109.5],
            'volume': [1000, 1100, 0, 1300, 1400, 1500, 1600, 1700, 1800, 1900],  # 第3条交易量为0
            'exchange': ['binance'] * 10,
            'market_type': ['spot'] * 10,
            'symbol': ['BTC/USDT'] * 10,
            'timeframe': ['1d'] * 10
        })

        validator = DataValidator()
        validation_result = validator.validate_data(test_data, 'ohlcv')

        print("✓ 数据验证结果:")
        print(f"  - 有效性: {'通过' if validation_result['valid'] else '失败'}")
        print(f"  - 错误数: {len(validation_result['errors'])}")
        print(f"  - 警告数: {len(validation_result['warnings'])}")

        if validation_result['errors']:
            print(f"  - 错误详情: {validation_result['errors']}")

        # 质量监控
        monitor = DataQualityMonitor()
        quality_report = monitor.monitor_data_quality(test_data, 'ohlcv', 'demo')
        print(f"✓ 质量监控报告: 质量分数 {quality_report['overall_quality_score']}")


class PerformanceBenchmark:
    """性能基准测试"""

    def __init__(self, db_path: str = "./benchmark.db"):
        self.db_path = db_path
        self.warehouse = None

    async def setup(self):
        """初始化测试环境"""
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        self.warehouse = FinancialDataWarehouse(self.db_path)
        print("🚀 性能测试环境初始化完成")

    async def cleanup(self):
        """清理测试环境"""
        if self.warehouse:
            await self.warehouse.close()

        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def generate_test_data(self, symbol: str, timeframe: str, days: int) -> List[Dict[str, Any]]:
        """生成测试数据"""
        data = []
        base_time = datetime.now() - timedelta(days=days)

        # 根据时间框架确定数据点数量
        if timeframe == '1d':
            points_per_day = 1
        elif timeframe == '1h':
            points_per_day = 24
        elif timeframe == '5m':
            points_per_day = 288  # 12 * 24
        else:
            points_per_day = 1

        total_points = days * points_per_day

        for i in range(total_points):
            current_time = base_time + timedelta(
                seconds=i * (86400 // points_per_day)
            )

            base_price = 40000 + (i // points_per_day) * 100
            close_price = base_price + np.random.uniform(-500, 500)

            record = {
                'exchange': 'binance',
                'market_type': 'spot',
                'symbol': symbol,
                'timeframe': timeframe,
                'ts': current_time,
                'open': close_price - np.random.uniform(100, 200),
                'high': close_price + np.random.uniform(0, 300),
                'low': close_price - np.random.uniform(200, 400),
                'close': close_price,
                'volume': np.random.uniform(1000, 5000),
                'quote_volume': close_price * np.random.uniform(1000, 5000),
                'source': 'benchmark'
            }
            data.append(record)

        return data

    async def benchmark_insert_performance(self):
        """测试插入性能"""
        print("\n⚡ 测试插入性能...")

        test_cases = [
            {'symbol': 'BTC/USDT', 'timeframe': '1d', 'days': 365},    # 365条
            {'symbol': 'BTC/USDT', 'timeframe': '1h', 'days': 30},     # 720条
            {'symbol': 'BTC/USDT', 'timeframe': '5m', 'days': 7},      # 2016条
        ]

        for case in test_cases:
            print(f"\n测试用例: {case['symbol']} {case['timeframe']} {case['days']}天")

            # 生成测试数据
            data = self.generate_test_data(**case)
            print(f"数据量: {len(data)} 条记录")

            # 测试插入性能
            start_time = time.time()
            success = await self.warehouse.insert_ohlcv(data)
            insert_time = time.time() - start_time

            if success:
                speed = len(data) / insert_time
                print(f"✓ 插入耗时: {insert_time:.3f} 秒")
                print(f"✓ 插入速度: {speed:.0f} 条/秒")
            else:
                print("✗ 插入失败")

    async def benchmark_query_performance(self):
        """测试查询性能"""
        print("\n🔍 测试查询性能...")

        # 先插入测试数据
        test_data = self.generate_test_data('BTC/USDT', '1d', 1000)
        await self.warehouse.insert_ohlcv(test_data)

        # 测试不同查询场景
        test_queries = [
            {'name': '全表查询', 'params': {'symbol': 'BTC/USDT', 'timeframe': '1d'}},
            {'name': '时间范围查询', 'params': {'symbol': 'BTC/USDT', 'timeframe': '1d', 'limit': 100}},
            {'name': '最近数据查询', 'params': {'symbol': 'BTC/USDT', 'timeframe': '1d', 'limit': 10}},
        ]

        for query in test_queries:
            print(f"\n查询测试: {query['name']}")

            # 预热缓存
            await self.warehouse.get_ohlcv(**query['params'])

            # 正式测试
            start_time = time.time()
            result = await self.warehouse.get_ohlcv(**query['params'])
            query_time = time.time() - start_time

            if result is not None:
                print(f"✓ 查询耗时: {query_time:.4f} 秒")
                print(f"✓ 返回记录: {len(result)} 条")
                if len(result) > 0:
                    print(f"✓ 查询速度: {len(result)/query_time:.0f} 条/秒")

    async def benchmark_incremental_performance(self):
        """测试增量更新性能"""
        print("\n⚡ 测试增量更新性能...")

        # 先插入基础数据
        base_data = self.generate_test_data('BTC/USDT', '1d', 100)
        await self.warehouse.insert_ohlcv(base_data)

        # 测试增量插入
        incremental_data = self.generate_test_data('BTC/USDT', '1d', 10)

        incremental_manager = SmartIncrementalManager(self.warehouse)

        start_time = time.time()
        result = await incremental_manager.smart_insert_ohlcv(incremental_data)
        incremental_time = time.time() - start_time

        print(f"✓ 增量更新耗时: {incremental_time:.3f} 秒")
        print(f"✓ 处理记录: {len(incremental_data)} 条")
        print(f"✓ 增量更新速度: {len(incremental_data)/incremental_time:.0f} 条/秒")

        # 检查增量结果
        if result['status'] == 'success':
            print(f"✓ 插入记录: {result['inserted']} 条")
            print(f"✓ 跳过记录: {result['skipped']} 条")


async def main():
    """主函数 - 运行完整的演示和性能测试"""

    print("🎯 DuckDB存储系统 - 功能演示和性能测试")
    print("=" * 60)

    # 功能演示
    demo = DuckDBStorageDemo()
    await demo.setup()

    try:
        # 运行功能演示
        await demo.demo_basic_operations()
        await demo.demo_unified_storage()
        await demo.demo_incremental_updates()
        await demo.demo_data_quality()

        print("\n" + "=" * 60)
        print("🏁 功能演示完成，开始性能基准测试...")

        # 性能基准测试
        benchmark = PerformanceBenchmark()
        await benchmark.setup()

        try:
            await benchmark.benchmark_insert_performance()
            await benchmark.benchmark_query_performance()
            await benchmark.benchmark_incremental_performance()

            print("\n" + "=" * 60)
            print("🎉 所有测试完成！")

        finally:
            await benchmark.cleanup()

    finally:
        await demo.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
