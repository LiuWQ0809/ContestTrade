"""
Data Summary Based On Alpha Vantage for US Market
"""
import asyncio
import hashlib
from pathlib import Path
from pydantic import BaseModel, Field
from utils.alpha_vantage_utils import alpha_vantage_cached
from models.llm_model import GLOBAL_VISION_LLM, GLOBAL_LLM
from tools.tool_utils import smart_tool
from tools.search_web import search_web
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tools.corp_info_us import company_earnings

TOOL_HOME = Path(__file__).parent.resolve()
TOOL_CACHE = TOOL_HOME / "stock_summary_us_cache"


def _fmt_value(value, fmt: str = ".2f") -> str:
    try:
        if value is None:
            return "NA"
        if isinstance(value, float) and np.isnan(value):
            return "NA"
        return format(value, fmt)
    except Exception:
        return "NA"


def get_stock_name_by_code(symbol, market):
    """Gets the US stock name from its symbol using Alpha Vantage."""
    try:
        if market == "US-Stock":
            params = {'function': 'OVERVIEW', 'symbol': symbol}
            result = alpha_vantage_cached.run(params, verbose=False)
            return result.get('Name', symbol) if result else symbol
        else:
            return symbol
    except Exception:
        return symbol


def _fetch_us_kline(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch US stock daily data using Alpha Vantage."""
    try:
        params = {
            'function': 'TIME_SERIES_DAILY',
            'symbol': symbol,
            'outputsize': 'full'
        }
        result = alpha_vantage_cached.run(params, verbose=False)
        
        if 'Time Series (Daily)' not in result:
            return pd.DataFrame()
        
        time_series = result['Time Series (Daily)']
        df = pd.DataFrame.from_dict(time_series, orient='index')
        
        if df.empty:
            return pd.DataFrame()
        
        # Rename columns and convert types
        df.columns = ['open', 'high', 'low', 'close', 'volume']
        df.index = pd.to_datetime(df.index)
        df = df.astype(float)
        df = df.sort_index()
        
        # Filter by date range
        start_dt = datetime.strptime(start_date, "%Y%m%d")
        end_dt = datetime.strptime(end_date, "%Y%m%d")
        df = df[(df.index >= start_dt) & (df.index <= end_dt)]
        
        # Add date column
        df['date'] = df.index
        
        return df
        
    except Exception as e:
        print(f"Failed to fetch K-line data for {symbol}: {e}")
        return pd.DataFrame()


def _compute_indicators(df: pd.DataFrame) -> dict:
    """Compute technical indicators similar to akshare version."""
    out = {}
    if df.empty:
        return out
    
    closes = df["close"].astype(float)
    out["ma5"] = closes.rolling(5).mean().iloc[-1]
    out["ma10"] = closes.rolling(10).mean().iloc[-1]
    out["ma20"] = closes.rolling(20).mean().iloc[-1]
    
    # RSI(14)
    delta = closes.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    roll_up = pd.Series(gain).rolling(14).mean()
    roll_down = pd.Series(loss).rolling(14).mean()
    rs = roll_up / (roll_down.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    out["rsi14"] = float(rsi.iloc[-1]) if not np.isnan(rsi.iloc[-1]) else None
    
    # MACD (12,26,9)
    ema12 = closes.ewm(span=12, adjust=False).mean()
    ema26 = closes.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    macd = (dif - dea) * 2
    out["macd_dif"] = float(dif.iloc[-1])
    out["macd_dea"] = float(dea.iloc[-1])
    out["macd_hist"] = float(macd.iloc[-1])
    
    # Bollinger (20, 2 std)
    ma20 = closes.rolling(20).mean()
    std20 = closes.rolling(20).std()
    out["boll_mid"] = float(ma20.iloc[-1]) if not np.isnan(ma20.iloc[-1]) else None
    out["boll_up"] = float((ma20 + 2*std20).iloc[-1]) if not np.isnan(std20.iloc[-1]) else None
    out["boll_dn"] = float((ma20 - 2*std20).iloc[-1]) if not np.isnan(std20.iloc[-1]) else None
    
    return out


def _describe_kline(df: pd.DataFrame, indicators: dict) -> str:
    """Describe K-line data similar to akshare version."""
    if df.empty:
        return "无可用K线数据"
    
    last = df.iloc[-1]
    parts = [
        f"最新交易日: {last['date'].strftime('%Y-%m-%d') if 'date' in df.columns else ''}",
        f"收盘: {last['close']} 开盘: {last['open']} 最高: {last['high']} 最低: {last['low']}",
        f"5/10/20日均线: {_fmt_value(indicators.get('ma5'))} / {_fmt_value(indicators.get('ma10'))} / {_fmt_value(indicators.get('ma20'))}",
        f"RSI14: {_fmt_value(indicators.get('rsi14'))}",
        f"MACD: DIF {_fmt_value(indicators.get('macd_dif'), '.3f')}, DEA {_fmt_value(indicators.get('macd_dea'), '.3f')}, Hist {_fmt_value(indicators.get('macd_hist'), '.3f')}",
        f"布林带: 上 {_fmt_value(indicators.get('boll_up'))}, 中 {_fmt_value(indicators.get('boll_mid'))}, 下 {_fmt_value(indicators.get('boll_dn'))}",
    ]
    return "\n".join([p for p in parts if p])


async def _get_financial_summary_async(symbol: str, trigger_time: str) -> str:
    """Get financial summary using corp_info_us functions."""
    try:
        # Try to get recent earnings data
        earnings_result = await company_earnings.ainvoke({
            "market": "US-Stock",
            "symbol": symbol,
            "trigger_time": trigger_time
        })
        
        if "error" not in earnings_result and "annual_earnings" in earnings_result:
            annual_earnings = earnings_result["annual_earnings"]
            if annual_earnings and len(annual_earnings) > 0:
                latest = annual_earnings[0]
                return f"财务摘要: 年度收益 {latest.get('reportedEPS', 'N/A')} EPS"
        
        return "财务摘要: 数据获取失败或无可用数据"
        
    except Exception as e:
        return f"财务摘要获取失败: {str(e)}"


def _get_sector_analysis() -> str:
    """Get sector performance using Alpha Vantage."""
    try:
        params = {'function': 'SECTOR'}
        result = alpha_vantage_cached.run(params, verbose=False)
        
        if result and 'Rank A: Real-Time Performance' in result:
            sector_data = result['Rank A: Real-Time Performance']
            return f"板块分析: 实时板块表现数据可用，包含 {len(sector_data)} 个板块"
        
        return "板块分析: 无可用数据"
        
    except Exception as e:
        return f"板块分析获取失败: {str(e)}"


async def get_all_stock_data(market: str, symbol: str, stock_name: str, trigger_time: str) -> dict:
    """Main data gathering function similar to akshare version."""
    if market != "US-Stock":
        return {
            "kline_description": "暂不支持该市场",
            "technical_analysis": "",
            "financial_summary": "",
            "sector_analysis": "",
            "stock_moneyflow_analysis": "",
            "intraday_chart_base64": None,
            "kline_chart_base64": None,
        }
    
    trigger_date = trigger_time.split(" ")[0]
    trigger_dt = datetime.strptime(trigger_date, "%Y-%m-%d")
    end_date = (trigger_dt - timedelta(days=1)).strftime("%Y%m%d")
    start_date = (trigger_dt - timedelta(days=90)).strftime("%Y%m%d")
    
    df = _fetch_us_kline(symbol, start_date, end_date)
    indicators = _compute_indicators(df) if not df.empty else {}
    kline_desc = _describe_kline(df, indicators)
    
    tech_desc = (
        "技术指标摘要: MA5/10/20="
        f"{_fmt_value(indicators.get('ma5'))}/"
        f"{_fmt_value(indicators.get('ma10'))}/"
        f"{_fmt_value(indicators.get('ma20'))}; "
        f"RSI14={_fmt_value(indicators.get('rsi14'))}; "
        f"MACD(DIF/DEA/Hist)={_fmt_value(indicators.get('macd_dif'), '.3f')}/"
        f"{_fmt_value(indicators.get('macd_dea'), '.3f')}/"
        f"{_fmt_value(indicators.get('macd_hist'), '.3f')}; "
        f"布林(上/中/下)={_fmt_value(indicators.get('boll_up'))}/"
        f"{_fmt_value(indicators.get('boll_mid'))}/"
        f"{_fmt_value(indicators.get('boll_dn'))}"
    ) if indicators else "技术指标不足，无法计算"
    
    # Get financial and sector data
    financial_summary = await _get_financial_summary_async(symbol, trigger_time)
    sector_analysis = _get_sector_analysis()
    
    return {
        "kline_description": kline_desc,
        "technical_analysis": tech_desc,
        "financial_summary": financial_summary,
        "sector_analysis": sector_analysis,
        "stock_moneyflow_analysis": "资金流向分析: Alpha Vantage暂无直接资金流数据",
        "intraday_chart_base64": None,
        "kline_chart_base64": None,
    }


async def call_llm_for_comprehensive_analysis(prompt, intraday_chart_base64=None, kline_chart_base64=None):
    """Calls the LLM for a comprehensive analysis with text and images."""
    
    content = [{"type": "text", "text": prompt}]
    if intraday_chart_base64:
        content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{intraday_chart_base64}"}})
    if kline_chart_base64:
        content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{kline_chart_base64}"}})
    
    messages = [{"role": "user", "content": content}]
    
    # Fallback to GLOBAL_LLM if GLOBAL_VISION_LLM is not available
    llm = GLOBAL_VISION_LLM if GLOBAL_VISION_LLM else GLOBAL_LLM

    response = await llm.a_run(messages, temperature=0.3, max_tokens=4000, verbose=False, thinking=False)
    return response.content


class StockSummaryInput(BaseModel):
    market: str = Field(description="The market of the company.")
    symbol: str = Field(description="The symbol of the company. For US-Stock use format like 'AAPL'.")
    trigger_time: str = Field(description="The trigger time of the financial data. Format: YYYY-MM-DD HH:MM:SS.")


async def analyze_stock_basic_info(market, symbol, stock_name, trigger_time):
    """Main analysis function, redesigned for Alpha Vantage-available dimensions."""
    print("📊  Fetching K-line & indicators via Alpha Vantage...")
    all_data = await get_all_stock_data(market, symbol, stock_name, trigger_time)
    print("✅  Data fetching complete.")

    # News data (optional)
    try:
        news_result = await search_web.ainvoke({"query": f"{stock_name}", "topk": 10, "trigger_time": trigger_time})
        news_analysis = '\n'.join(map(str, news_result)) if isinstance(news_result, list) else str(news_result)
        print("✅  News fetching complete.")
    except Exception as e:
        news_analysis = f"相关新闻获取失败: {e}"
        print(f"❌  News fetching failed: {e}")

    prompt_template = f"""请为{stock_name}({symbol})生成一份基于可用数据的股票技术分析报告。
分析时间: {trigger_time}

=== 数据输入（基于Alpha Vantage可用性精简）===
【K线数据与关键指标】
{all_data['kline_description']}
{all_data['technical_analysis']}

【财务基本面数据】
{all_data['financial_summary']}

【板块分析数据】
{all_data['sector_analysis']}

【资金流向数据】
{all_data['stock_moneyflow_analysis']}

【新闻事件数据】
{news_analysis}

=== 分析要求 ===
1. 概述近期价格走势与波动特征
2. 结合均线、RSI、MACD、布林带等指标给出技术判断
3. 标注关键支撑/阻力位与潜在风险
4. 说明数据局限性（基于Alpha Vantage可用数据）
"""
    if market == "US-Stock":
        prompt_template += "\n\n请用英文输出美股分析报告"

    print("🤖  Starting LLM technical analysis...")
    try:
        analysis_result = await call_llm_for_comprehensive_analysis(
            prompt_template, 
            all_data['intraday_chart_base64'], 
            all_data['kline_chart_base64']
        )
        return analysis_result
    except Exception as e:
        print(f"❌  LLM analysis failed: {e}")
        return f'LLM分析失败: {e}'


@smart_tool(
    description="""Get stock summarized technical info (Alpha Vantage-based). 仅支持美股，返回基于K线与技术指标的分析报告。""",
    args_schema=StockSummaryInput,
    max_output_len=4000,
    timeout_seconds=120.0
)
async def stock_summary(market: str, symbol: str, trigger_time: str) -> str:
    """Alpha Vantage-based stock summary tool, focusing on K-line and technical indicators."""
    if market not in ["US-Stock"]:
        return f"错误：当前Alpha Vantage模式仅支持美股(US-Stock)，传入: '{market}'。"

    if not TOOL_CACHE.exists():
        TOOL_CACHE.mkdir(parents=True, exist_ok=True)
    
    cache_key = f"{market}_{symbol}_{trigger_time.split(' ')[0]}"
    cache_file = TOOL_CACHE / f"{hashlib.md5(cache_key.encode()).hexdigest()}.txt"

    if cache_file.exists():
        return cache_file.read_text()
    else:
        stock_name = get_stock_name_by_code(symbol, market)
        result = await analyze_stock_basic_info(market, symbol, stock_name, trigger_time)
        cache_file.write_text(result)
        return result


if __name__ == "__main__":
    
    result = asyncio.run(stock_summary.ainvoke(
        { "market": "US-Stock", 
         "symbol": "TSLA", 
         "trigger_time": "2025-08-09 15:00:00"}))
         
    print(result)