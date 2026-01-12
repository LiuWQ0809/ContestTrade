from loguru import logger
"""
Stock Symbol Search Tool (AKShare Version)
Search for stock symbols by company names or partial symbols using AKShare data.
Replaces the tushare-dependent version with better reliability.
"""
import re
import json
import asyncio
import pandas as pd
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from functools import lru_cache

from tools.tool_utils import smart_tool
from utils.akshare_utils import akshare_cached

class StockSymbolSearchAkshareInput(BaseModel):
    market: str = Field(description="The target market. Currently supports: CN-Stock")
    queries: List[str] = Field(description="List of search queries: company names or stock symbols (partial match supported)")
    trigger_time: str = Field(description="The trigger time. Format: YYYY-MM-DD HH:MM:SS")
    limit_per_query: int = Field(default=5, description="Maximum number of results per query")
    match_mode: str = Field(default="best", description="Match mode: 'best' (top match), 'all' (all matches), 'exact' (exact only)")

@lru_cache(maxsize=1)
def get_stock_basic_akshare():
    """Get basic stock information from AKShare with caching"""
    try:
        # 获取A股基本信息
        df = akshare_cached.run(
            func_name="stock_zh_a_spot_em",
            func_kwargs={},
            verbose=False
        )
        
        if df is None or df.empty:
            raise Exception("Failed to fetch stock basic data from akshare")
        
        # 标准化列名
        columns_mapping = {
            '代码': 'ts_code',
            '名称': 'name', 
            '最新价': 'close',
            '涨跌幅': 'pct_chg',
            '总市值': 'total_mv',
            '流通市值': 'circ_mv'
        }
        
        # 只重命名存在的列
        existing_mapping = {k: v for k, v in columns_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_mapping)
        
        # 确保必需的列存在
        required_cols = ['ts_code', 'name']
        for col in required_cols:
            if col not in df.columns:
                raise Exception(f"Required column {col} not found in akshare data")
        
        return df
        
    except Exception as e:
        logger.error(f"Error fetching stock basic data: {e}")
        return pd.DataFrame()

def calculate_match_score(query: str, ts_code: str, name: str) -> tuple[str, float]:
    """Calculate match score and type - matches original version exactly"""
    query_lower = query.lower()
    ts_code_lower = ts_code.lower()
    name_lower = name.lower()
    
    # Exact match
    if query == ts_code or query == name:
        return "exact", 1.0
    if query_lower == ts_code_lower or query_lower == name_lower:
        return "exact", 1.0
    
    # Prefix match
    if ts_code_lower.startswith(query_lower) or name_lower.startswith(query_lower):
        return "prefix", 0.9
    
    # Contains match
    if query_lower in ts_code_lower or query_lower in name_lower:
        return "contains", 0.8
    
    # Fuzzy match for partial code
    if re.search(re.escape(query_lower), ts_code_lower) or re.search(re.escape(query_lower), name_lower):
        return "fuzzy", 0.7
    
    return "none", 0.0

def search_single_query(symbols_df: pd.DataFrame, query: str, limit: int, match_mode: str, market: str) -> List[Dict[str, Any]]:
    """Search for a single query in the symbols dataframe"""
    results = []
    
    if symbols_df.empty:
        return results
    
    for _, row in symbols_df.iterrows():
        ts_code = str(row.get('ts_code', ''))
        name = str(row.get('name', ''))
        
        if not ts_code or not name:
            continue
            
        match_type, score = calculate_match_score(query, ts_code, name)
        
        if match_mode == "exact" and match_type != "exact":
            continue
        
        if score > 0:
            results.append({
                "ts_code": ts_code,
                "name": name,
                "market": market,
                "match_type": match_type,
                "match_score": score
            })
    
    # Sort by score and match type priority (matches original version)
    results.sort(key=lambda x: (x['match_score'], x['match_type'] == 'exact'), reverse=True)
    
    if match_mode == "best":
        return results[:1]
    else:
        return results[:limit]

@smart_tool(
    description="Search for stock symbols by company names or partial symbols using AKShare data. Supports Chinese company names and stock codes.",
    args_schema=StockSymbolSearchAkshareInput,
    max_output_len=4000,
    timeout_seconds=5.0
)
async def stock_symbol_search(
    market: str, 
    queries: List[str], 
    trigger_time: str,
    limit_per_query: int = 5,
    match_mode: str = "best"
) -> Dict[str, Any]:
    """
    Search for stock symbols using AKShare data.
    
    Args:
        market: Target market (currently CN-Stock)
        queries: List of search queries (company names or stock codes)
        trigger_time: Trigger time (for compatibility)
        limit_per_query: Max results per query
        match_mode: Match mode (best/all/exact)
    
    Returns:
        Dict with search results, summary, and any failed queries
    """
    
    try:
        # Validate market
        if market != "CN-Stock":
            return {
                "error": f"Market {market} not supported. Only CN-Stock is currently supported.",
                "results": {},
                "summary": {"total_queries": len(queries), "successful_matches": 0, "failed_matches": len(queries)},
                "failed_queries": [{"query": q, "error": f"Unsupported market: {market}"} for q in queries]
            }
        
        # Get stock basic data
        symbols_df = get_stock_basic_akshare()
        
        if symbols_df.empty:
            return {
                "error": f"No stock data available for market {market}",
                "results": {},
                "summary": {"total_queries": len(queries), "successful_matches": 0, "failed_matches": len(queries)},
                "failed_queries": [{"query": q, "error": "No stock data available"} for q in queries]
            }
        
        logger.debug(f"Loaded {len(symbols_df)} stocks for search")
        
        # Process each query
        results = {}
        failed_queries = []
        
        for query in queries:
            if not query or not query.strip():
                failed_queries.append({"query": query, "error": "Empty query"})
                continue
                
            try:
                matches = search_single_query(symbols_df, query.strip(), limit_per_query, match_mode, market)
                if matches:
                    results[query] = matches
                    logger.debug(f"Query '{query}': {len(matches)} matches found")
                else:
                    failed_queries.append({"query": query, "error": "No matches found"})
                    logger.debug(f"Query '{query}': No matches found")
            except Exception as e:
                failed_queries.append({"query": query, "error": str(e)})
                logger.debug(f"Query '{query}': Search error - {e}")
        
        # Generate summary (matches original version)
        summary = {
            "total_queries": len(queries),
            "successful_matches": len(results),
            "failed_matches": len(failed_queries),
            "total_results": sum(len(matches) for matches in results.values())
        }
        
        return {
            "results": results,
            "summary": summary,
            "failed_queries": failed_queries,
            "market": market,
            "trigger_time": trigger_time
        }
        
    except Exception as e:
        error_msg = f"Stock symbol search failed: {str(e)}"
        logger.error(error_msg)
        return {
            "error": error_msg,
            "results": {},
            "summary": {"total_queries": len(queries), "successful_matches": 0, "failed_matches": len(queries)},
            "failed_queries": [{"query": q, "error": error_msg} for q in queries]
        }

if __name__ == "__main__":
    # Test the tool
    async def test():
        result = await stock_symbol_search.ainvoke({
            "market": "CN-Stock",
            "queries": ["茅台", "平安银行", "000001", "腾讯"],
            "trigger_time": "2025-08-21 12:00:00",
            "limit_per_query": 3,
            "match_mode": "best"
        })
        logger.debug(json.dumps(result, indent=2, ensure_ascii=False))
    
    asyncio.run(test())