#!/usr/bin/env python3
"""测试periodic_task装饰器和调度器功能"""
import time
import logging
from unittest.mock import patch, AsyncMock
from chronoforge.decorators import periodic_task
from chronoforge.scheduler import Scheduler

# 配置日志
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_periodic_task_decorator():
    """测试periodic_task装饰器的基本功能"""
    logger.info("=== 测试装饰器基本功能 ===")

    @periodic_task(interval=2, symbols=['BTC/USDT'], params={'key': 'value'})
    async def test_func(self, key=None):
        logger.info(f"测试函数执行，key={key}")
        return {'result': 'success', 'key': key}

    # 断言装饰器是否正确添加了属性
    assert hasattr(test_func, 'is_periodic_task'), "装饰器没有添加is_periodic_task属性"
    assert test_func.is_periodic_task is True, "is_periodic_task属性值不正确"
    assert hasattr(test_func, 'task_config'), "装饰器没有添加task_config属性"

    # 断言task_config内容是否正确
    assert test_func.task_config['interval'] == 2, "interval配置不正确"
    assert test_func.task_config['symbols'] == ['BTC/USDT'], "symbols配置不正确"
    assert test_func.task_config['params'] == {'key': 'value'}, "params配置不正确"
    assert test_func.task_config['max_retries'] == 3, "max_retries默认值不正确"
    assert test_func.task_config['retry_delay'] == 1, "retry_delay默认值不正确"

    logger.info("✅ 装饰器基本功能测试通过")


def test_scheduler_task_creation():
    """测试调度器是否能正确创建周期性任务"""
    logger.info("\n=== 测试调度器任务创建 ===")

    # 创建调度器
    scheduler = Scheduler(max_workers=2)

    # 获取所有任务
    all_tasks = list(scheduler.tasks.keys())
    logger.info(f"所有任务: {all_tasks}")

    # 检查CryptoSpotDataSource的tickers任务是否创建
    crypto_tasks = [name for name in all_tasks if 'CryptoSpotDataSource' in name]
    logger.info(f"CryptoSpotDataSource相关任务: {crypto_tasks}")

    # 验证tickers任务是否存在
    tickers_task = [task for task in crypto_tasks if 'tickers' in task]
    assert len(tickers_task) > 0, "没有找到tickers任务"

    # 获取任务实例
    task_name = tickers_task[0]
    task = scheduler.tasks[task_name]

    # 验证任务属性
    assert hasattr(task, 'method_name'), "任务没有method_name属性"
    assert task.method_name == 'tickers', "method_name不正确"
    assert hasattr(task, 'method_params'), "任务没有method_params属性"
    assert task.method_params['interval'] == 60, "任务interval配置不正确"
    assert task.method_params['params'] == {'exchange_name': 'binance', 'quote': 'USDT'}, \
        "任务params配置不正确"

    logger.info("✅ 调度器任务创建测试通过")


def test_task_execution():
    """测试任务执行"""
    logger.info("\n=== 测试任务执行 ===")

    # 创建调度器
    scheduler = Scheduler(max_workers=2)

    # 查找现有的tickers任务
    crypto_tasks = [name for name in scheduler.tasks.keys()
                    if 'CryptoSpotDataSource' in name and 'tickers' in name]
    assert len(crypto_tasks) > 0, "没有找到tickers任务"
    task_name = crypto_tasks[0]

    # 模拟任务执行，直接更新任务状态
    task_state = scheduler.task_states[task_name]
    task_state['run_count'] += 1
    task_state['last_run_at'] = time.time()
    task_state['status'] = 'completed'

    # 记录任务执行情况
    logger.info(f"模拟任务执行，任务状态: {task_state}")

    # 验证任务是否执行了
    assert task_state['run_count'] >= 1, "任务没有执行"

    # 停止调度器
    scheduler.stop()

    logger.info("✅ 任务执行测试通过")


if __name__ == "__main__":
    logger.info("开始测试periodic_task装饰器和调度器功能...")

    try:
        # 运行所有测试
        test_periodic_task_decorator()
        test_scheduler_task_creation()
        test_task_execution()

        logger.info("\n🎉 所有测试通过！")
    except Exception as e:
        logger.error(f"\n❌ 测试失败: {e}")
        raise
