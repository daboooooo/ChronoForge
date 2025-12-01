#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import time
import traceback

from chronoforge.scheduler import Scheduler
from chronoforge.utils import TimeSlot

# 直接使用正确格式的symbol，包含交易所信息
crypto_symbols = ['binance:BTC/USDT', 'okx:ETH/USDT']

fred_api_key = "64a2def57e5b65c216e35e580f78f0f7"

# ST. LOUIS FRED API SERIES
FRED_RATES = {
    "Daily": {
        "IORB": {
            "name": "Interest Rate Overnight Reverse Repurchase Agreements",
            "id": "IORB",
            "comment": "银行在美联储的准备金利率，是联邦基金利率的‘天花板’，处于美联储利率区间的中间。"
        },
        "ON_RRP_Award": {
            "name": "Overnight Reverse Repurchase Agreements Award Rate",
            "id": "RRPONTSYAWARD",
            "comment": "隔夜逆回购利率，是联邦基金利率区间的‘地板’"
        },
        "EFFR": {
            "name": "Federal Funds Effective Rate",
            "id": "EFFR",
            "comment": "联邦基金有效利率，是美联储在每个工作日发布的利率，是市场中间值。ON RRP < EFFR < IORB"
        },
        "SOFR": {
            "name": "Secured Overnight Financing Rate",
            "id": "SOFR",
            "high_freq_data": {
                "URL": (
                    "https://markets.newyorkfed.org/read?"
                    "productCode=50&eventCodes=520&limit=25&"
                    "startPosition=0&sort=postDt:-1&format=xml"
                ),
                "format": "xml"
            },
            "comment": "以国债作为抵押的回购市场利率。SOFR高于EFFR 0.17%时，相当于美元流动性'心肌梗塞'"
        },
        "T-Bill_1M": {
            "name": "1-Month T-Bill Secondary Market Rate",
            "id": "DTB4WK",
            "comment": "1月国债收益率，是指在1月到期的国债的收益率"
        },
        "T-Bill_3M": {
            "name": "3-Month T-Bill Secondary Market Rate",
            "id": "DTB3",
            "comment": "3月国债收益率，是指在3月到期的国债的收益率"
        },
        "T-Bill_6M": {
            "name": "6-Month T-Bill Secondary Market Rate",
            "id": "DTB6",
            "comment": "6月国债收益率，是指在6月到期的国债的收益率"
        },
        "T-Bill_1Y": {
            "name": "1-Year T-Bill Secondary Market Rate",
            "id": "DTB1YR",
            "comment": "1年国债收益率，是指在1年到期的国债的收益率"
        },
        "T-Note_2Y": {
            "name": "2-Year T-Note Secondary Market Rate",
            "id": "DGS2",
            "comment": "2年国债收益率，是指在2年到期的国债的收益率"
        },
        "T-Note_5Y": {
            "name": "5-Year T-Note Secondary Market Rate",
            "id": "DGS5",
            "comment": "5年国债收益率，是指在5年到期的国债的收益率"
        },
        "T-Note_10Y": {
            "name": "10-Year T-Note Secondary Market Rate",
            "id": "DGS10",
            "comment": "10年国债收益率，是指在10年到期的国债的收益率"
        },
        "T-Bond_20Y": {
            "name": "20-Year T-Bond Secondary Market Rate",
            "id": "DGS20",
            "comment": "20年国债收益率，是指在20年到期的国债的收益率"
        },
        "T-Bond_30Y": {
            "name": "30-Year T-Bond Secondary Market Rate",
            "id": "DGS30",
            "comment": "30年国债收益率，是指在30年到期的国债的收益率"
        }
    },
    "Weekly": {
    }
}

fred_daily_rates = [
    "IORB",
    "RRPONTSYAWARD",
    "EFFR",
    "SOFR",
    "DTB4WK",
    "DTB3",
    "DTB6",
    "DTB1YR",
    "DGS2",
    "DGS5",
    "DGS10",
    "DGS20",
    "DGS30"
]

FRED_VOLUMES = {
    "Daily": {
        "ON_RRP": {
            "name": "Overnight Reverse Repurchase Agreements",
            "id": "RRPONTSYD",
            "comment": "隔夜逆回购成交量"
        },
        "EFFR": {
            "name": "Effective Federal Funds Volume",
            "id": "EFFRVOL",
            "comment": "联邦基金市场成交量"
        },
        "SOFR": {
            "name": "Secured Overnight Financing Rate Volume",
            "id": "SOFRVOL",
            "comment": "以国债作为抵押的回购市场交易的成交量",
        },
        "SRF_TS": {
            "name": "Standing Repo Facility - Treasury Securities Purchased in OMO",
            "id": "RPONTSYD",
            "url": "https://www.newyorkfed.org/markets/desk-operations/repo",
            "comment": "回购国债的常备回购便利成交量"
        },
        "SRF_MBS": {
            "name": "Standing Repo Facility - MBS Purchased in OMO",
            "id": "RPMBSD",
            "url": "https://www.newyorkfed.org/markets/desk-operations/repo",
            "comment": "以住宅或商业抵押贷款为基础打包证券的常备回购便利成交量"
        },
        "SRF_Agency": {
            "name": "Standing Repo Facility - Agency Securities Purchased in OMO",
            "id": "RPAGYD",
            "url": "https://www.newyorkfed.org/markets/desk-operations/repo",
            "comment": "回购机构债券的常备回购便利成交量"
        },
    },
    "Weekly": {
        "Reserve_Balances": {
            "name": "Reserve Balances with Federal Reserve Banks: Wednesday Level",
            "id": "WRBWFRBL",
            "comment": "准备金余额。接近 3,000,000 说明流动性紧张, 区域银行可能会暴雷。美联储官员认为的红线是 2,700,000, 会主动干预。",
            "pattern_1": "准备金余额连续两周低于3万亿之后，次日做空。退出条件：下跌总幅度超过5%后，遇到第一个日线收阳。止损条件：上涨超过2%后，次日平仓。",
            "pattern_2": "准备金余额连续4周下跌。"
        },
    }
}

fred_daily_volumes = [
    "RRPONTSYD",
    "EFFRVOL",
    "SOFRVOL",
    "RPONTSYD",
    "RPMBSD",
    "RPAGYD",
]

fred_weekly_volumes = [
    "WRBWFRBL",
]

um_future_symbols = [
    "BTCUSDT",
    "ETHUSDT",
    "LINKUSDT",
    "XLMUSDT",
    "XMRUSDT",
    "UNIUSDT",
    "AAVEUSDT",
    "COMPUSDT",
    "CRVUSDT"
]

global_market_symbols = [
    "^GSPC",  # S&P 500
    "^DJI",   # Dow Jones
    "^IXIC",  # NASDAQ
    "^RUT",   # Russell 2000
    "^VIX",   # 波动率指数
    "DX=F",   # 美元指数
    "GC=F",   # 黄金价格
    "SI=F",   # 白银价格
    "HG=F",   # 铜价格
    "CNY=X",     # 人民币/美元汇率
    "JPY=X"      # 日元/美元汇率
]


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def main():
    """
    主函数 - 简化版，专注于正确传递交易所名称
    """
    try:
        print("===== 加密货币数据下载测试 =====")

        # 初始化调度器
        print("初始化调度器...")
        scheduler = Scheduler()

        # 获取当前时间作为时间槽
        hourly_slot = TimeSlot(start="00:30", end="59:00")  # 全天运行, hourly
        print(f"当前时间槽: {hourly_slot}")

        # 添加任务，使用正确的symbol格式
        print("尝试添加任务...")
        try:
            scheduler.add_task(
                name="crypto_1d",
                data_source_name="CryptoSpotDataSource",
                data_source_config={},
                storage_name="DUCKDBStorage",
                storage_config={
                    'db_path': './tmp/geigei.db'
                },
                time_slot=hourly_slot,
                symbols=crypto_symbols,  # 直接使用"exchange:symbol"格式
                timeframe="1d",
                timerange_str="20240101-",
            )

            scheduler.add_task(
                name="crypto_4h",
                data_source_name="CryptoSpotDataSource",
                data_source_config={},
                storage_name="DUCKDBStorage",
                storage_config={
                    'db_path': './tmp/geigei.db'
                },
                time_slot=hourly_slot,
                symbols=crypto_symbols,  # 直接使用"exchange:symbol"格式
                timeframe="4h",
                timerange_str="20240101-",
            )

            scheduler.add_task(
                name="crypto_1h",
                data_source_name="CryptoSpotDataSource",
                data_source_config={},
                storage_name="DUCKDBStorage",
                storage_config={
                    'db_path': './tmp/geigei.db'
                },
                time_slot=hourly_slot,
                symbols=crypto_symbols,  # 直接使用"exchange:symbol"格式
                timeframe="1h",
                timerange_str="20240101-",
            )

            scheduler.add_task(
                name="fred_daily_test",
                data_source_name="FREDDataSource",
                data_source_config={
                    "api_key": fred_api_key
                },
                storage_name="DUCKDBStorage",
                storage_config={
                    'db_path': './tmp/geigei.db'
                },
                time_slot=hourly_slot,
                symbols=fred_daily_rates + fred_daily_volumes,
                timeframe="1d",
                timerange_str="20240101-",
            )

            scheduler.add_task(
                name="fred_weekly_test",
                data_source_name="FREDDataSource",
                data_source_config={
                    "api_key": fred_api_key
                },
                storage_name="DUCKDBStorage",
                storage_config={
                    'db_path': './tmp/geigei.db'
                },
                time_slot=hourly_slot,
                symbols=fred_weekly_volumes,
                timeframe="1w",
                timerange_str="20240101-",
            )

            scheduler.add_task(
                name="crypto_um_future_test",
                data_source_name="CryptoUMFutureDataSource",
                data_source_config={},
                storage_name="DUCKDBStorage",
                storage_config={
                    'db_path': './tmp/geigei.db'
                },
                time_slot=hourly_slot,
                symbols=um_future_symbols,
                timeframe="1h",
                timerange_str="20251101-",
            )

            scheduler.add_task(
                name="bitcoin_fgi",
                data_source_name="BitcoinFGIDataSource",
                data_source_config={},
                storage_name="DUCKDBStorage",
                storage_config={
                    'db_path': './tmp/geigei.db'
                },
                time_slot=hourly_slot,
                symbols=["bitcoin_fgi"],
                timeframe="1d",
                timerange_str="20240101-",
            )

            scheduler.add_task(
                name="global_market_test",
                data_source_name="GlobalMarketDataSource",
                data_source_config={},
                storage_name="DUCKDBStorage",
                storage_config={
                    'db_path': './tmp/geigei.db'
                },
                time_slot=hourly_slot,
                symbols=global_market_symbols,
                timeframe="1d",
                timerange_str="20240101-",
            )

            print("✅ 任务添加成功")
            print(f"当前任务数量: {len(scheduler.tasks)}")

        except Exception as e:
            print(f"❌ 添加任务时出错: {type(e).__name__}: {e}")
            traceback.print_exc()
            return False

        # 启动调度器
        print("启动调度器...")
        scheduler.start()
        print("Scheduler started. 运行30秒...")

        # 运行一段时间后停止
        try:
            time.sleep(60)  # 给任务执行时间
        except KeyboardInterrupt:
            print("检测到用户中断")
        finally:
            print("停止调度器...")
            scheduler.stop()
            print("Scheduler stopped")

        return True

    except Exception as e:
        print(f"❌ 主函数执行异常: {type(e).__name__}: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
