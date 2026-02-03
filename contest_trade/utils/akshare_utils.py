"""
akshare 的工具函数

"""
import json
import hashlib
import pickle
import pandas as pd
import requests
import concurrent.futures
from akshare.utils import demjson
from pathlib import Path
from datetime import datetime
from config.config import cfg, WORKSPACE_ROOT

import akshare as ak
from loguru import logger
from .tencent_utils import TencentUtils

DEFAULT_AKSHARE_CACHE_DIR = WORKSPACE_ROOT / "agents_workspace" / "akshare_cache"

def execute_with_timeout(func, args=(), kwargs={}, timeout=10):
    """
    带超时限制的函数执行
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func, *args, **kwargs)
        try:
            return future.result(timeout=timeout)
        except concurrent.futures.TimeoutError:
            print(f"Warning: Execution returned Timeout (>{timeout}s) for {func.__name__}")
            raise TimeoutError(f"Execution timed out after {timeout}s")
        except Exception as e:
            raise e

class CachedAksharePro:
    def __init__(self, cache_dir=None):
        if not cache_dir:
            self.cache_dir = DEFAULT_AKSHARE_CACHE_DIR
        else:
            self.cache_dir = Path(cache_dir)
        if not self.cache_dir.exists():
            self.cache_dir.mkdir(parents=True, exist_ok=True)

    def run(self, func_name: str, func_kwargs: dict, verbose: bool = False):
        func_kwargs_str = json.dumps(func_kwargs)
        return self.run_with_cache(func_name, func_kwargs_str, verbose)

    def run_with_cache(self, func_name: str, func_kwargs: str, verbose: bool = False):
        func_kwargs_dict = json.loads(func_kwargs)
        args_hash = hashlib.md5(str(func_kwargs_dict).encode()).hexdigest()
        trigger_time = datetime.now().strftime("%Y%m%d%H")
        args_hash = f"{args_hash}_{trigger_time}"
        func_cache_dir = self.cache_dir / func_name
        if not func_cache_dir.exists():
            func_cache_dir.mkdir(parents=True, exist_ok=True)
        func_cache_file = func_cache_dir / f"{args_hash}.pkl"
        if func_cache_file.exists():
            if verbose:
                print(f"load result from {func_cache_file}")
            with open(func_cache_file, "rb") as f:
                return pickle.load(f)
        else:
            if verbose:
                print(f"cache miss for {func_name} with args: {func_kwargs_dict}")
            
            # --- 增强：容错逻辑 ---
            try:
                if func_name == "stock_zh_a_spot_em":
                    result = get_stock_zh_a_spot_safe()
                elif func_name == "stock_board_industry_name_em":
                    result = get_stock_board_industry_name_em_safe()
                elif func_name == "stock_hsgt_north_net_flow_in_em":
                    result = get_stock_hsgt_north_net_flow_in_em_safe()
                elif func_name in ["stock_zt_pool_em", "stock_zt_pool_dtgc_em"]:
                    # These specific Eastmoney functions for limit-up/down often timeout or fail.
                    # We increase timeout slightly and allow retry
                    import time
                    import random
                    max_retries = 2
                    for attempt in range(max_retries):
                         try:
                             target_func = getattr(ak, func_name)
                             result = execute_with_timeout(target_func, kwargs=func_kwargs_dict, timeout=8)
                             if not result.empty:
                                 break
                         except Exception as retry_e:
                             if attempt < max_retries - 1:
                                 time.sleep(1)
                                 continue
                             else:
                                 raise retry_e
                else:
                    # 针对其他所有 Akshare 调用，增加超时保护
                    # 避免东财接口阻塞导致整个程序卡死
                    target_func = getattr(ak, func_name)
                    # 缩短超时时间到 5 秒（因为非核心数据可以接受缺失）
                    result = execute_with_timeout(target_func, kwargs=func_kwargs_dict, timeout=5)
            except Exception as e:
                print(f"Error calling {func_name}: {e}")
                # 最后的兜底逻辑
                if func_name == "stock_zh_a_spot_em":
                    result = get_stock_zh_a_spot_safe()
                else:
                    # 对于非核心接口调用失败，返回空 DataFrame 以防止程序崩溃
                    print(f"Returning empty DataFrame for {func_name} due to failure.")
                    result = pd.DataFrame()
            # --- 结束增强 ---

            if verbose:
                print(f"save result to {func_cache_file}")
            # 只有获取到数据才缓存（防止缓存空结果导致一直无数据）
            if isinstance(result, pd.DataFrame) and not result.empty:
                with open(func_cache_file, "wb") as f:
                    pickle.dump(result, f)
            elif isinstance(result, (list, dict)) and result: # 针对非DF返回
                with open(func_cache_file, "wb") as f:
                    pickle.dump(result, f)
            
            return result

akshare_cached = CachedAksharePro()

def fetch_sina_market_data_parallel():
    """
    并行获取新浪行情数据，解决单线程分页慢的问题。
    """
    url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
    max_pages = 80  # 预估 6000+支股票 / 100 = 60页，设置80冗余
    
    def _fetch_page(page):
        params = {
            "page": str(page),
            "num": "100",  # 接口似乎限制单页最大100
            "sort": "symbol",
            "asc": "1",
            "node": "hs_a",
            "symbol": "",
            "_s_r_a": "page",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Referer": "http://vip.stock.finance.sina.com.cn/"
        }
        # 重试逻辑
        for attempt in range(3):
            try:
                r = requests.get(url, params=params, headers=headers, timeout=5)
                if r.status_code == 200:
                    data = demjson.decode(r.text)
                    if isinstance(data, list):
                        return data
                    return []
            except Exception as e:
                time.sleep(0.5)  # 增加一点重试间隔
        return []

    all_data = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(_fetch_page, p): p for p in range(1, max_pages + 1)}
        for future in concurrent.futures.as_completed(futures):
            try:
                data = future.result()
                if data:
                    all_data.extend(data)
            except Exception:
                pass

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    # 字段映射 (新浪API -> Akshare标准)
    # 使用 'code' (如 600000) 而非 'symbol' (如 sh600000)
    rename_map = {
        'code': '代码',
        'name': '名称',
        'trade': '最新价',
        'changepercent': '涨跌幅',
        'settlement': '昨收',
        'volume': '成交量',
        'amount': '成交额',
        'high': '最高',
        'low': '最低',
        'open': '今开',
    }
    df = df.rename(columns=rename_map)
    
    # 类型转换
    numeric_cols = ['最新价', '涨跌幅', '昨收', '成交量', '成交额', '最高', '最低', '今开']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
    return df

def get_stock_zh_a_spot_safe() -> pd.DataFrame:
    """
    安全获取 A 股实时行情，带容错平替逻辑。
    优先使用腾讯接口（速度快且稳定），使用新浪接口并行获取代码列表。
    """
    # 定义期望包含的最基础列，确保即使失败也不会因为 KeyError 导致程序中断
    expected_columns = ['代码', '名称', '最新价', '涨跌幅', '昨收', '成交量', '成交额']
    
    try:
        # 1. 优先尝试腾讯接口
        df_tencent = TencentUtils.get_stock_zh_a_spot_tencent()
        if df_tencent is not None and not df_tencent.empty:
            logger.info(f"Successfully fetched {len(df_tencent)} spot data from Tencent.")
            return df_tencent
    except Exception as e:
        logger.error(f"Tencent fetch failed: {e}")

    # 2. 只有当腾讯完全失败（包括缓存代码也没拿到）时，才尝试旧的并行新浪策略
    logger.warning("Falling back to Sina parallel fetch...")
    try:
        df_sina = fetch_sina_market_data_parallel()
        if df_sina is not None and not df_sina.empty:
            return df_sina
    except Exception as e:
        print(f"Warning: Sina parallel fetch failed: {e}. Falling back to Eastmoney.")

    try:
        # 3. 最后的保底：东财接口
        # 增加重试逻辑
        import random
        import time
        retries = 3
        for i in range(retries):
            try:
                if i > 0:
                     time.sleep(random.uniform(2, 5))
                
                # 增加超时时间到 120 秒 (Eastmoney full fetch takes ~60s)
                # 使用 execute_with_timeout 包装防止卡死
                df = execute_with_timeout(ak.stock_zh_a_spot_em, timeout=120)
                if df is not None and not df.empty and '代码' in df.columns:
                    return df
            except Exception as inner_e:
                print(f"Eastmoney fallback attempt {i+1} failed: {inner_e}")
                
    except Exception as e:
        print(f"Error: All spot data sources failed (EM fallback also failed): {e}")
    
    return pd.DataFrame(columns=expected_columns)

def get_stock_board_industry_name_em_safe() -> pd.DataFrame:
    """
    安全获取板块数据。
    """
    try:
        return execute_with_timeout(ak.stock_board_industry_name_em, timeout=5)
    except Exception as e:
        print(f"Warning: stock_board_industry_name_em failed: {e}")
        return pd.DataFrame()

def get_stock_hsgt_north_net_flow_in_em_safe() -> pd.DataFrame:
    """
    安全获取北向资金流向。
    """
    try:
        # 尝试汇总接口
        df = ak.stock_hsgt_fund_flow_summary_em()
        if df is not None and not df.empty:
            df_north = df[df['资金方向'] == '北向'].copy()
            # 按照日期分组汇总（沪股通 + 深股通）
            summary = df_north.groupby('交易日')['资金净流入'].sum().reset_index()
            summary.columns = ['date', '当日净流入']
            return summary
    except Exception as e:
        print(f"Warning: hsgt net flow failed: {e}")
    
    return pd.DataFrame()

if __name__ == "__main__":
    stock_sse_summary_df = akshare_cached.run(
        func_name="stock_sse_summary", 
        func_kwargs={},
        verbose=True
    )
    print(stock_sse_summary_df)