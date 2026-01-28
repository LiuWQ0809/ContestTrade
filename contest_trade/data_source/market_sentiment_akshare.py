"""
基于 akshare 的大盘情绪数据源
整合全市场涨跌家数比、北向资金流向、大盘情绪指标等
"""
import pandas as pd
import asyncio
from datetime import datetime
from data_source.data_source_base import DataSourceBase
from utils.akshare_utils import akshare_cached
from models.llm_model import GLOBAL_LLM, GLOBAL_THINKING_LLM
from loguru import logger
from utils.date_utils import get_previous_trading_date

class MarketSentimentAkshare(DataSourceBase):
    def __init__(self):
        super().__init__("market_sentiment_akshare")
        
    async def get_data(self, trigger_time: str) -> pd.DataFrame:
        try:
            df = self.get_data_cached(trigger_time)
            if df is not None:
                return df
            
            trade_date = get_previous_trading_date(trigger_time)     
            logger.info(f"获取 {trade_date} 的全市场情绪数据")

            sentiment_data = await self.get_sentiment_metrics(trade_date)
            
            prompt = f"""
当前时间: {trigger_time}
交易日期: {trade_date}

请基于以下全市场情绪指标进行深度分析：

1. 市场涨跌分布:
- 上涨家数: {sentiment_data.get('up_count', '未知')}
- 下跌家数: {sentiment_data.get('down_count', '未知')}
- 平盘家数: {sentiment_data.get('flat_count', '未知')}
- 赚钱效应（上涨占比）: {sentiment_data.get('profit_effect', '未知')}%

2. 北向资金 (陆股通):
- 当日净流入: {sentiment_data.get('north_money_net', '未知')} 亿元

3. 市场活跃度:
- 今日炸板率: {sentiment_data.get('broken_limit_rate', '未知')}%

请以资深策略分析师的角度，总结当前大盘的情绪周期（如：恐慌衰竭期、震荡修复期、过热回落期、主升浪等），并给出对后续操作的“情绪面倾向”建议。
"""
            
            messages = [
                {"role": "system", "content": "你是一位专注于 A 股市场情绪周期研究的策略专家。请基于数据给出客观、深刻的市场情绪画像。"},
                {"role": "user", "content": prompt}
            ]
            
            # 使用 Thinking LLM 进行深度分析
            response = await GLOBAL_THINKING_LLM.a_run(messages=messages)
            llm_summary = response.content if response else "分析失败"

            data = [{
                "title": f"{trade_date}:全市场情绪深度分析",
                "content": llm_summary,
                "pub_time": trigger_time,
                "url": None
            }]
            df = pd.DataFrame(data)
            self.save_data_cached(trigger_time, df)
            return df
                
        except Exception as e:
            logger.error(f"获取全市场情绪数据失败: {e}")
            return pd.DataFrame()

    async def get_sentiment_metrics(self, trade_date: str) -> dict:
        metrics = {}
        try:
            # 1. 市场涨跌分布 - 通过实时行情统计
            df_spot = akshare_cached.run(func_name="stock_zh_a_spot_em", func_kwargs={})
            if not df_spot.empty:
                up_count = len(df_spot[df_spot['涨跌幅'] > 0])
                down_count = len(df_spot[df_spot['涨跌幅'] < 0])
                flat_count = len(df_spot[df_spot['涨跌幅'] == 0])
                metrics['up_count'] = up_count
                metrics['down_count'] = down_count
                metrics['flat_count'] = flat_count
                metrics['profit_effect'] = round(up_count / len(df_spot) * 100, 2) if len(df_spot) > 0 else 0

            # 2. 北向资金
            try:
                df_hsgt = akshare_cached.run(func_name="stock_hsgt_north_net_flow_in_em", func_kwargs={"symbol": "北上"})
                if not df_hsgt.empty:
                    # 寻找对应日期
                    df_hsgt['date'] = pd.to_datetime(df_hsgt['date']).dt.strftime('%Y%m%d')
                    row = df_hsgt[df_hsgt['date'] <= trade_date].iloc[0]
                    metrics['north_money_net'] = round(row['当日净流入'] / 100, 2) # 假设单位是百万，转为亿元
            except:
                pass

            # 3. 炸板率等指标 (从热钱数据源借鉴)
            try:
                df_zt = akshare_cached.run(func_name="stock_zt_pool_em", func_kwargs={"date": trade_date})
                df_broken = akshare_cached.run(func_name="stock_zt_pool_zbgc_em", func_kwargs={"date": trade_date})
                if not df_zt.empty and not df_broken.empty:
                    zt_count = len(df_zt)
                    broken_count = len(df_broken)
                    metrics['broken_limit_rate'] = round(broken_count / (zt_count + broken_count) * 100, 2)
            except:
                pass

            return metrics
        except Exception as e:
            logger.warning(f"获取情绪指标部分失败: {e}")
            return metrics
