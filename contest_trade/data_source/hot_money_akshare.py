"""
基于 akshare 的热钱市场数据源
整合涨跌停、龙虎榜、游资等热钱市场数据

主要功能：
1. 获取涨停跌停股票数据（当日）
2. 获取龙虎榜数据和机构明细（近10天）
3. 获取概念板块资金流向（实时）
4. 获取游资营业部资金数据（实时）
5. 使用LLM分析并生成热钱市场分析报告
"""
import pandas as pd
import asyncio
import traceback
from datetime import datetime, timedelta
from data_source.data_source_base import DataSourceBase
from utils.akshare_utils import akshare_cached
from models.llm_model import GLOBAL_LLM
from loguru import logger
from utils.date_utils import get_previous_trading_date

class HotMoneyAkshare(DataSourceBase):
    def __init__(self):
        super().__init__("hot_money_akshare")

    async def get_data(self, trigger_time: str) -> pd.DataFrame:
        try:
            df = self.get_data_cached(trigger_time)
            if df is not None:
                return df
            
            trade_date = get_previous_trading_date(trigger_time)     
            logger.info(f"获取 {trade_date} 的热钱市场数据")

            llm_summary_dict = await self.get_llm_summary(trade_date)
            data = [{
                "title": f"{trade_date}:热钱市场数据汇总",
                "content": llm_summary_dict["llm_summary"],
                "pub_time": trigger_time,
                "url": None
            }]
            df = pd.DataFrame(data)
            self.save_data_cached(trigger_time, df)
            return df
                
        except Exception as e:
            logger.error(f"获取热钱市场数据失败: {e}")
            return pd.DataFrame()

    def get_zt_data(self, trade_date: str) -> pd.DataFrame:
        """获取涨停股票数据"""
        try:
            df = akshare_cached.run(
                func_name="stock_zt_pool_em",
                func_kwargs={"date": trade_date},
                verbose=False
            )
            
            if df.empty:
                logger.warning(f"{trade_date} 无涨停数据")
                return pd.DataFrame()
                
            # --- 过滤不支持的板块 (创业板300/科创板688) ---
            if '代码' in df.columns:
                original_len = len(df)
                # 只保留非 300 和 688 开头的股票
                df = df[~df['代码'].astype(str).str.startswith(('300', '688'))].copy()
                filtered_len = len(df)
                if original_len != filtered_len:
                    logger.info(f"已过滤 {original_len - filtered_len} 只创业板/科创板股票 (涨停池)")
            
            logger.info(f"获取 {trade_date} 涨停数据成功，{len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取涨停数据失败: {e}")
            return pd.DataFrame()

    def get_dt_data(self, trade_date: str) -> pd.DataFrame:
        """获取跌停股票数据"""
        try:
            df = akshare_cached.run(
                func_name="stock_zt_pool_dtgc_em",
                func_kwargs={"date": trade_date},
                verbose=False
            )
            
            if df.empty:
                logger.warning(f"{trade_date} 无跌停数据")
                return pd.DataFrame()
            
            # --- 过滤不支持的板块 (创业板300/科创板688) ---
            if '代码' in df.columns:
                original_len = len(df)
                df = df[~df['代码'].astype(str).str.startswith(('300', '688'))].copy()
                filtered_len = len(df)
                if original_len != filtered_len:
                    logger.info(f"已过滤 {original_len - filtered_len} 只创业板/科创板股票 (跌停池)")
            
            logger.info(f"获取 {trade_date} 跌停数据成功，{len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取跌停数据失败: {e}")
            return pd.DataFrame()

    def get_lhb_data(self, trade_date: str) -> pd.DataFrame:
        """获取龙虎榜数据（近10天）"""
        try:
            # 计算10天前的日期
            end_date = trade_date
            start_date_obj = datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=10)
            start_date = start_date_obj.strftime('%Y%m%d')
            
            df = akshare_cached.run(
                func_name="stock_lhb_detail_em",
                func_kwargs={"start_date": start_date, "end_date": end_date},
                verbose=False
            )
            
            if df.empty:
                logger.warning(f"{start_date}到{end_date} 无龙虎榜数据")
                return pd.DataFrame()
            
            # --- 过滤不支持的板块 (创业板300/科创板688) ---
            if '代码' in df.columns:
                original_len = len(df)
                df = df[~df['代码'].astype(str).str.startswith(('300', '688'))].copy()
                filtered_len = len(df)
                if original_len != filtered_len:
                    logger.info(f"已过滤 {original_len - filtered_len} 只创业板/科创板股票 (龙虎榜)")

            logger.info(f"获取 {start_date}到{end_date} 龙虎榜数据成功，{len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取龙虎榜数据失败: {e}")
            return pd.DataFrame()

    def get_lhb_jg_data(self, trade_date: str) -> pd.DataFrame:
        """获取龙虎榜机构明细数据（近10天）"""
        try:
            # 计算10天前的日期
            end_date = trade_date
            start_date_obj = datetime.strptime(trade_date, '%Y%m%d') - timedelta(days=10)
            start_date = start_date_obj.strftime('%Y%m%d')
            
            df = akshare_cached.run(
                func_name="stock_lhb_jgmmtj_em",
                func_kwargs={"start_date": start_date, "end_date": end_date},
                verbose=False
            )
            
            if df.empty:
                logger.warning(f"{start_date}到{end_date} 无龙虎榜机构数据")
                return pd.DataFrame()
            
            # --- 过滤不支持的板块 (创业板300/科创板688) ---
            if '代码' in df.columns:
                original_len = len(df)
                df = df[~df['代码'].astype(str).str.startswith(('300', '688'))].copy()
                filtered_len = len(df)
                if original_len != filtered_len:
                    logger.info(f"已过滤 {original_len - filtered_len} 只创业板/科创板股票 (机构明细)")

            logger.info(f"获取 {start_date}到{end_date} 龙虎榜机构数据成功，{len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取龙虎榜机构数据失败: {e}")
            return pd.DataFrame()

    def get_concept_data(self) -> pd.DataFrame:
        """获取概念板块资金流数据"""
        try:
            df = akshare_cached.run(
                func_name="stock_board_concept_name_em",
                func_kwargs={},
                verbose=False
            )
            
            if df.empty:
                logger.warning("无概念板块数据")
                return pd.DataFrame()
            
            logger.info(f"获取概念板块数据成功，{len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取概念板块数据失败: {e}")
            return pd.DataFrame()

    def get_yyb_data(self) -> pd.DataFrame:
        """获取游资营业部资金数据"""
        try:
            df = akshare_cached.run(
                func_name="stock_lh_yyb_capital",
                func_kwargs={},
                verbose=False
            )
            
            if df.empty:
                logger.warning("无游资营业部数据")
                return pd.DataFrame()
            
            logger.info(f"获取游资营业部数据成功，{len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"获取游资营业部数据失败: {e}")
            return pd.DataFrame()

    async def get_llm_summary(self, trade_date: str) -> dict:
        """获取LLM分析总结"""
        try:
            logger.info(f"获取 {trade_date} 的热钱市场LLM分析总结")
            
            # 获取各类数据
            zt_data = self.get_zt_data(trade_date)
            dt_data = self.get_dt_data(trade_date)
            lhb_data = self.get_lhb_data(trade_date)
            lhb_jg_data = self.get_lhb_jg_data(trade_date)
            concept_data = self.get_concept_data()
            yyb_data = self.get_yyb_data()
            
            # 构建分析文本
            analysis_text = self._construct_analysis_text(
                trade_date, zt_data, dt_data, lhb_data, lhb_jg_data, concept_data, yyb_data
            )
            
            available_sources = sum([
                1 if not zt_data.empty else 0,
                1 if not dt_data.empty else 0,
                1 if not lhb_data.empty else 0,
                1 if not lhb_jg_data.empty else 0,
                1 if not concept_data.empty else 0,
                1 if not yyb_data.empty else 0
            ])
            
            if available_sources == 0:
                return {
                    'trade_date': trade_date,
                    'raw_data': "无数据",
                    'llm_summary': "当日无热钱市场数据",
                    'data_count': 0
                }
            
            prompt = f"""
请分析以下{trade_date}的A股热钱市场数据，并给出专业的热钱活跃度分析报告（2000字符以内）：

{analysis_text}

## 分析要求
请综合以上信息，客观描述热钱市场活跃度：

## 输出要求
- 总结当日涨停跌停情况、龙虎榜活跃度、概念板块热度和游资参与情况
- 分析热钱流向和市场情绪
- 避免主观判断、情绪化描述和未来预测
- 重点突出热钱市场的客观基本面事实描述
- **请把输出的分析严格控制在2000字符以内，不要超过2000字符**

请基于事实数据生成客观的热钱市场分析报告：
"""
            
            user_message = {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt}
                ]
            }
            
            messages = [
                {"role": "system", "content": "你是一位资深的量化投资分析师，专长于热钱流向、游资行为和市场情绪分析。请基于多维度数据生成专业的热钱市场分析报告。"},
                user_message
            ]
            
            if GLOBAL_LLM:
                response = await GLOBAL_LLM.a_run(
                    messages=messages,
                    thinking=False,
                    temperature=0.3,
                    max_tokens=2000
                )
                
                if response and response.content:
                    llm_summary = response.content
                else:
                    logger.error(f"LLM分析未返回内容")
                    llm_summary = "LLM分析失败"
            else:
                llm_summary = "LLM未配置"
            
            return {
                'trade_date': trade_date,
                'raw_data': analysis_text,
                'llm_summary': llm_summary,
                'data_count': available_sources,
                'data_sources': {
                    'zt_data': not zt_data.empty,
                    'dt_data': not dt_data.empty,
                    'lhb_data': not lhb_data.empty,
                    'lhb_jg_data': not lhb_jg_data.empty,
                    'concept_data': not concept_data.empty,
                    'yyb_data': not yyb_data.empty
                }
            }
                
        except Exception as e:
            traceback.print_exc()
            logger.error(f"获取热钱市场LLM总结失败: {e}")
            return {
                'trade_date': trade_date,
                'raw_data': "数据获取失败",
                'llm_summary': f"分析失败: {str(e)}",
                'data_count': 0
            }

    def _construct_analysis_text(self, trade_date: str, zt_data: pd.DataFrame, dt_data: pd.DataFrame,
                                lhb_data: pd.DataFrame, lhb_jg_data: pd.DataFrame, 
                                concept_data: pd.DataFrame, yyb_data: pd.DataFrame) -> str:
        """构建分析文本"""
        
        sections = [f"## {trade_date} 热钱市场数据分析\n"]
        
        # 涨停跌停分析
        if not zt_data.empty or not dt_data.empty:
            sections.append("### 一、涨跌停情况")
            
            if not zt_data.empty:
                zt_count = len(zt_data)
                # 连板股统计
                lianbao_stats = zt_data['连板数'].value_counts().sort_index(ascending=False)
                top_zt = zt_data.head(5)
                
                sections.append(f"**涨停股票**: 共{zt_count}只")
                sections.append(f"**连板分布**: {dict(lianbao_stats)}")
                sections.append("**主要涨停股票**:")
                for _, row in top_zt.iterrows():
                    sections.append(f"- {row['名称']}({row['代码']}): {row['涨跌幅']:.2f}%, 连板{row['连板数']}天, 炸板{row['炸板次数']}次")
            
            if not dt_data.empty:
                dt_count = len(dt_data)
                sections.append(f"**跌停股票**: 共{dt_count}只")
                if dt_count <= 5:
                    for _, row in dt_data.iterrows():
                        sections.append(f"- {row['名称']}({row['代码']}): {row['涨跌幅']:.2f}%, 连续跌停{row['连续跌停']}天")
        
        # 龙虎榜分析
        if not lhb_data.empty:
            sections.append("\n### 二、龙虎榜活跃度")
            
            lhb_count = len(lhb_data)
            # 按上榜日期统计最近几天的数据
            recent_lhb = lhb_data[lhb_data['上榜日'] == trade_date] if '上榜日' in lhb_data.columns else lhb_data
            recent_count = len(recent_lhb)
            
            sections.append(f"**龙虎榜上榜股票**: 近10天共{lhb_count}只，{trade_date}当日{recent_count}只")
            
            if not recent_lhb.empty and recent_count <= 10:
                sections.append("**当日主要龙虎榜股票**:")
                for _, row in recent_lhb.head(5).iterrows():
                    net_buy = row.get('龙虎榜净买额', 0)
                    net_buy_str = f"{net_buy/10000:.0f}万" if abs(net_buy) < 100000000 else f"{net_buy/100000000:.2f}亿"
                    sections.append(f"- {row['名称']}({row['代码']}): {row['涨跌幅']:.2f}%, 净买额{net_buy_str}")
        
        # 机构数据分析
        if not lhb_jg_data.empty:
            sections.append("\n### 三、机构参与情况")
            
            jg_count = len(lhb_jg_data)
            total_net_buy = lhb_jg_data['机构买入净额'].sum() if '机构买入净额' in lhb_jg_data.columns else 0
            
            sections.append(f"**机构参与股票**: {jg_count}只")
            sections.append(f"**机构净买入**: {total_net_buy/100000000:.2f}亿元")
            
            top_jg = lhb_jg_data.head(3)
            if not top_jg.empty:
                sections.append("**主要机构参与股票**:")
                for _, row in top_jg.iterrows():
                    net_buy = row.get('机构买入净额', 0)
                    sections.append(f"- {row['名称']}: 机构净买额{net_buy/10000:.0f}万元")
        
        # 概念板块热度
        if not concept_data.empty:
            sections.append("\n### 四、概念板块热度")
            
            top_concepts = concept_data.head(5)
            sections.append("**热门概念板块**:")
            for _, row in top_concepts.iterrows():
                up_count = row.get('上涨家数', 0)
                down_count = row.get('下跌家数', 0)
                total_count = up_count + down_count
                up_ratio = (up_count / total_count * 100) if total_count > 0 else 0
                sections.append(f"- {row['板块名称']}: {row['涨跌幅']:.2f}%, 上涨率{up_ratio:.0f}%({up_count}/{total_count})")
        
        # 游资营业部活跃度
        if not yyb_data.empty:
            sections.append("\n### 五、游资营业部活跃度")
            
            yyb_count = len(yyb_data)
            sections.append(f"**活跃游资营业部**: {yyb_count}家")
            
            top_yyb = yyb_data.head(3)
            sections.append("**主要活跃营业部**:")
            for _, row in top_yyb.iterrows():
                yyb_name = row['营业部名称']
                sections.append(f"- {yyb_name}: 今日操作{row['今日最高操作']}次, 最高金额{row['今日最高金额']}")
        
        return "\n".join(sections)

if __name__ == "__main__":
    hot_money = HotMoneyAkshare()
    df = asyncio.run(hot_money.get_data("2025-08-19 09:00:00"))
    print(df.head())
    if len(df) > 0:
        print("热钱市场分析内容:")
        print(df.content.values[0])
