#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import logging
import sys
import time
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 导入存储类
try:
    from chronoforge.storage import DUCKDBStorage
    from chronoforge.storage import MongoDBStorage
except ImportError as e:
    print(f"❌ 导入存储类失败: {e}")
    sys.exit(1)

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def generate_test_data(size: int = 10000) -> pd.DataFrame:
    """
    生成测试数据
    
    Args:
        size: 数据大小
        
    Returns:
        pd.DataFrame: 生成的测试数据
    """
    logger.info(f"生成 {size} 条测试数据...")
    
    # 生成时间序列
    end_time = datetime.now()
    start_time = end_time - timedelta(days=size)
    dates = pd.date_range(start=start_time, end=end_time, periods=size)
    
    # 生成随机数据
    np.random.seed(42)  # 固定随机种子，确保结果可复现
    data = {
        'time': dates,
        'open': np.random.normal(100, 10, size),
        'high': np.random.normal(105, 10, size),
        'low': np.random.normal(95, 10, size),
        'close': np.random.normal(100, 10, size),
        'volume': np.random.normal(10000, 1000, size).astype(int)
    }
    
    df = pd.DataFrame(data)
    return df


async def test_storage_performance(storage_name: str, storage_config: dict, test_data: pd.DataFrame, test_id: str = "test_symbol") -> dict:
    """
    测试存储性能
    
    Args:
        storage_name: 存储名称
        storage_config: 存储配置
        test_data: 测试数据
        test_id: 测试数据ID
        
    Returns:
        dict: 性能测试结果
    """
    logger.info(f"开始测试 {storage_name} 性能...")
    
    # 初始化存储
    if storage_name == "DUCKDBStorage":
        storage = DUCKDBStorage(storage_config)
    elif storage_name == "MongoDBStorage":
        storage = MongoDBStorage(storage_config)
    else:
        raise ValueError(f"不支持的存储类型: {storage_name}")
    
    results = {
        "storage_name": storage_name,
        "data_size": len(test_data)
    }
    
    try:
        # 测试保存性能
        start_time = time.time()
        save_result = await storage.save(test_id, test_data, sub="test_performance", metadata=None)
        save_time = time.time() - start_time
        results["save_time"] = save_time
        results["save_success"] = save_result
        
        logger.info(f"{storage_name} 保存 {len(test_data)} 条数据耗时: {save_time:.4f} 秒")
        
        # 测试加载性能
        start_time = time.time()
        loaded_data = await storage.load(test_id, sub="test_performance", metadata=None)
        load_time = time.time() - start_time
        results["load_time"] = load_time
        results["load_success"] = loaded_data is not None and not loaded_data.empty
        
        logger.info(f"{storage_name} 加载 {len(test_data)} 条数据耗时: {load_time:.4f} 秒")
        
        # 验证数据完整性
        if loaded_data is not None and not loaded_data.empty:
            results["data_match"] = len(loaded_data) == len(test_data)
            logger.info(f"{storage_name} 数据完整性验证: {'通过' if results['data_match'] else '失败'}")
        else:
            results["data_match"] = False
            logger.warning(f"{storage_name} 数据加载失败，无法验证完整性")
            
    finally:
        # 清理测试数据
        await storage.delete(test_id, sub="test_performance")
        # 关闭存储连接 - 处理不同存储类的关闭方法差异
        if hasattr(storage, "close"):
            await storage.close()
        elif hasattr(storage, "_close"):
            await storage._close()
        # 对于没有关闭方法的存储类，跳过
    
    return results


async def main():
    """
    主函数 - 对比DuckDB和MongoDB存储性能
    """
    logger.info("===== 存储性能对比测试 =====")
    
    # 测试配置
    test_configs = [
        # {"size": 1000, "label": "1K"},
        {"size": 10000, "label": "10K"},
        {"size": 50000, "label": "50K"},
        {"size": 100000, "label": "100K"}
    ]
    
    # 存储配置
    duckdb_config = {
        "db_path": "./tmp/performance_test.db"
    }
    
    mongodb_config = {
        "uri": "mongodb://localhost:27017",
        "db_name": "performance_test",
        "collection_prefix": "test_"
    }
    
    all_results = []
    
    for config in test_configs:
        size = config["size"]
        label = config["label"]
        
        logger.info(f"\n--- 测试数据量: {label} ({size} 条) ---")
        
        # 生成测试数据
        test_data = await generate_test_data(size)
        
        # 测试DuckDB
        duckdb_results = await test_storage_performance(
            "DUCKDBStorage", duckdb_config, test_data, f"test_{label}"
        )
        all_results.append(duckdb_results)
        
        # 测试MongoDB
        mongodb_results = await test_storage_performance(
            "MongoDBStorage", mongodb_config, test_data, f"test_{label}"
        )
        all_results.append(mongodb_results)
    
    # 输出测试结果
    logger.info("\n" + "="*60)
    logger.info("性能测试结果汇总")
    logger.info("="*60)
    
    print("\n{:<15} {:<10} {:<15} {:<15} {:<10} {:<10}".format(
        "存储类型", "数据量", "保存时间(秒)", "加载时间(秒)", "保存成功", "数据匹配"
    ))
    print("-" * 80)
    
    for result in all_results:
        print("{:<15} {:<10} {:<15.4f} {:<15.4f} {:<10} {:<10}".format(
            result["storage_name"],
            result["data_size"],
            result["save_time"],
            result["load_time"],
            "✓" if result["save_success"] else "✗",
            "✓" if result["data_match"] else "✗"
        ))
    
    # 分析结果
    logger.info("\n" + "="*60)
    logger.info("性能分析")
    logger.info("="*60)
    
    # 按数据量分组分析
    data_sizes = sorted(list(set([r["data_size"] for r in all_results])))
    for size in data_sizes:
        size_results = [r for r in all_results if r["data_size"] == size]
        duckdb_result = next(r for r in size_results if r["storage_name"] == "DUCKDBStorage")
        mongodb_result = next(r for r in size_results if r["storage_name"] == "MongoDBStorage")
        
        logger.info(f"\n数据量: {size} 条")
        
        # 保存性能对比
        save_ratio = duckdb_result["save_time"] / mongodb_result["save_time"]
        if save_ratio < 1:
            logger.info(f"保存性能: DuckDB 比 MongoDB 快 {1/save_ratio:.2f} 倍")
        else:
            logger.info(f"保存性能: MongoDB 比 DuckDB 快 {save_ratio:.2f} 倍")
        
        # 加载性能对比
        load_ratio = duckdb_result["load_time"] / mongodb_result["load_time"]
        if load_ratio < 1:
            logger.info(f"加载性能: DuckDB 比 MongoDB 快 {1/load_ratio:.2f} 倍")
        else:
            logger.info(f"加载性能: MongoDB 比 DuckDB 快 {load_ratio:.2f} 倍")
    
    logger.info("\n" + "="*60)
    logger.info("性能测试完成")
    logger.info("="*60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
        sys.exit(0)
    except Exception as e:
        logger.error(f"❌ 程序执行异常: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
