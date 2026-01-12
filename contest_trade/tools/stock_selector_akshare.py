from loguru import logger
"""
Stock Selector Tool (Akshare Version)
Based on natural language query and akshare stock basic data.
"""
import re
import json
import asyncio
import pandas as pd
import numpy as np
from pydantic import BaseModel, Field

from tools.tool_utils import smart_tool
from utils.akshare_utils import akshare_cached
from tools.tool_prompts import STOCK_FILTER_PROMPT_AKSHARE
from models.llm_model import GLOBAL_LLM

def get_basic_stock_df_akshare(trigger_time: str):    
    try:
        df1 = akshare_cached.run(
            func_name="stock_zh_a_spot_em",
            func_kwargs={},
            verbose=False
        )
        
        if df1 is None or df1.empty:
            raise Exception("Failed to fetch stock spot data from akshare")
        
        columns_mapping = {
            '代码': 'ts_code',
            '名称': 'name', 
            '最新价': 'close',
            '涨跌幅': 'pct_chg',
            '涨跌额': 'change',
            '成交量': 'vol',
            '成交额': 'amount',
            '振幅': 'amplitude',
            '最高': 'high',
            '最低': 'low',
            '今开': 'open',
            '昨收': 'pre_close',
            '量比': 'volume_ratio',
            '换手率': 'turnover_rate',
            '市盈率-动态': 'pe',
            '市净率': 'pb',
            '总市值': 'total_mv',
            '流通市值': 'circ_mv'
        }
        
        existing_mapping = {k: v for k, v in columns_mapping.items() if k in df1.columns}
        df1 = df1.rename(columns=existing_mapping)
        
        numeric_columns = ['close', 'pct_chg', 'change', 'vol', 'amount', 'amplitude', 
                          'high', 'low', 'open', 'pre_close', 'volume_ratio', 'turnover_rate', 
                          'pe', 'pb', 'total_mv', 'circ_mv']
        
        for col in numeric_columns:
            if col in df1.columns:
                df1[col] = pd.to_numeric(df1[col], errors='coerce')
        
        if 'name' in df1.columns:
            df1 = df1[~df1['name'].str.contains('ST', na=False)]
        if 'ts_code' in df1.columns:
            df1 = df1[~df1['ts_code'].str.contains('.BJ', na=False)]
        
        if 'total_mv' in df1.columns:
            df1['total_mv'] = df1['total_mv'] / 10000
        if 'circ_mv' in df1.columns:
            df1['circ_mv'] = df1['circ_mv'] / 10000
        
        important_stocks = df1.nlargest(200, 'total_mv')['ts_code'].tolist()
        
        stock_details = {}
        for ts_code in important_stocks:
            try:
                detail_info = akshare_cached.run(
                    func_name='stock_individual_info_em',
                    func_kwargs={'symbol': ts_code},
                    verbose=False
                )
                
                if detail_info is not None and not detail_info.empty:
                    info_dict = dict(zip(detail_info['item'], detail_info['value']))
                    stock_details[ts_code] = info_dict
                    
            except Exception:
                continue
        
        df1['industry'] = df1['ts_code'].apply(
            lambda x: stock_details.get(x, {}).get('行业', '未知')
        )
        
        df1['total_share'] = df1['ts_code'].apply(
            lambda x: pd.to_numeric(stock_details.get(x, {}).get('总股本', np.nan), errors='coerce') / 10000 if pd.notna(stock_details.get(x, {}).get('总股本', np.nan)) else np.nan
        )
        
        df1['float_share'] = df1['ts_code'].apply(
            lambda x: pd.to_numeric(stock_details.get(x, {}).get('流通股', np.nan), errors='coerce') / 10000 if pd.notna(stock_details.get(x, {}).get('流通股', np.nan)) else np.nan
        )
        
        return df1
        
    except Exception as e:
        return pd.DataFrame()


class StockSelectorInput(BaseModel):
    market: str = Field(description="目标市场，当前仅支持 CN-Stock")
    query: str = Field(description="股票查询query, 自然语言的形式。")
    trigger_time: str = Field(description="触发时间，格式：YYYY-MM-DD HH:MM:SS")
    limit: int = Field(default=10, description="返回结果数量，默认10个, 最多20个。")


@smart_tool(
    description="""
    选股工具（Akshare版本），基于自然语言查询筛选A股股票，支持通过财务指标等组合筛选股票。不可用于查询具体股票信息。
    例如
    - 行业 + 财务指标筛选
    - 示例："银行市值最大的3家"、"市值超过1000亿的银行股"、"PE小于20银行股" 
    - 注意：行业industry目前只支持：['未知', '半导体', '消费电子', '生物制品', '光伏设备', '医疗器械', '交运设备', '家电行业', '软件开发', 
    '食品饮料', '小金属', '能源金属', '酿酒行业', '非金属材料', '医疗服务', '电子元件', '家用轻工', '计算机设备', '银行', '证券', '电力行业', '船舶制造',
      '航运港口', '有色金属', '煤炭行业', '旅游酒店', '电网设备', '工程建设', '石油行业', '铁路公路', '采掘行业', '通信服务', '电源设备', '汽车零部件', '汽车整车', 
      '保险', '医药商业', '航空机场', '工程机械', '物流行业', '化学原料', '房地产开发', '航天航空', '互联网服务', '燃气', '光学光电子', '综合行业', '玻璃玻纤', '水泥建材',
        '电机', '贵金属', '通信设备', '中药', '商业百货', '化纤行业', '化学制品', '化学制药', '钢铁行业']
    - 如果行业不在上述列表中，则直接返回空结果。

    具体可以筛选的字段如下：             
    字段名              类型      含义                example
    ts_code            str      股票代码              000001.SZ
    name               str      股票名称              平安银行
    industry           str      行业                  银行
    close              float    最新价                12.07
    pct_chg            float    涨跌幅（%）           2.06
    turnover_rate      float    换手率（%）           2.06
    volume_ratio       float    量比                  1.08
    pe                 float    市盈率（动态）        21.22
    pb                 float    市净率                2.16
    total_share        float    总股本（万股）        34813.69
    float_share        float    流通股本（万股）      32001.01
    total_mv           float    总市值（万元）        470332.97
    circ_mv            float    流通市值（万元）      432333.64
    vol                float    成交量                3315206
    amount             float    成交额                2515293334
    amplitude          float    振幅                  0.79
    high               float    最高价                7.62
    low                float    最低价                7.56
    open               float    开盘价                7.58
    pre_close          float    昨收价                7.57
    change             float    涨跌额                0.0
    """,
    args_schema=StockSelectorInput,
    max_output_len=2000,
    timeout_seconds=30.0
)
async def stock_selector(market: str, query: str, trigger_time: str, limit: int = 10) -> str:
    if market != "CN-Stock":
        return f"Error: Currently only CN-Stock is supported for Akshare version."
    
    try:
        stock_df = get_basic_stock_df_akshare(trigger_time)
        logger.debug(stock_df['industry'].unique().tolist())
        
        if stock_df.empty:
            return f"Error: Failed to fetch stock data from akshare."

        prompt = STOCK_FILTER_PROMPT_AKSHARE.format(query=query)
        messages = [{"role": "user", "content": prompt}]
        response = await GLOBAL_LLM.a_run(messages, verbose=False, thinking=False)
        logger.debug(response.content)

        code_match = re.search(r"```python(.*?)```", response.content, re.DOTALL)
        if not code_match:
            return f"Error: Failed to parse filter code from LLM response."
            
        code_str = code_match.group(1)
        exec_globals = {}
        code_str = f'import pandas as pd\nimport numpy as np\n{code_str}'
        exec(code_str, exec_globals)
        
        if 'filter_stock' not in exec_globals:
            return f"Error: LLM did not generate a valid filter_stock function."
            
        filter_stock_func = exec_globals['filter_stock']
        filtered_result = filter_stock_func(stock_df)
        
        if isinstance(filtered_result, pd.DataFrame):
            filter_stock_df = filtered_result.iloc[:min(20, limit)]
        else:
            return f"Error: Filter function did not return a DataFrame."
        
        result_context = json.dumps(filter_stock_df.to_dict(orient='records'), ensure_ascii=False)
        return result_context
        
    except Exception as e:
        return f"Stock selector error: {str(e)}"


if __name__ == "__main__":
    result = asyncio.run(stock_selector.ainvoke({
        "market": "CN-Stock",
        "query": "市值最大的5只光刻机股",
        "trigger_time": "2025-08-20 09:00:00",
        "limit": 5
    }))
    logger.debug(result)
