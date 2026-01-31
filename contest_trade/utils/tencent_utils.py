
import json
import requests
import pandas as pd
import concurrent.futures
import time
from datetime import datetime
from akshare.utils import demjson
import akshare as ak
from loguru import logger
import math
from pathlib import Path
from typing import List, Dict

class TencentUtils:
    _code_cache = []
    _code_cache_time = 0
    _market_open_hour = 9
    _market_open_minute = 30
    _cache_dir = Path(__file__).parent / "cache"
    _cache_file = _cache_dir / "tencent_code_cache.json"

    @staticmethod
    def _is_cache_valid(now_ts: float, cache_ts: float) -> bool:
        if not cache_ts:
            return False
        now_dt = datetime.fromtimestamp(now_ts)
        cache_dt = datetime.fromtimestamp(cache_ts)
        open_dt = now_dt.replace(
            hour=TencentUtils._market_open_hour,
            minute=TencentUtils._market_open_minute,
            second=0,
            microsecond=0,
        )
        # Before market open: keep existing cache (no forced refresh)
        if now_dt < open_dt:
            return True
        # After market open: refresh once per day (cache must be from today after open)
        return cache_dt >= open_dt

    @staticmethod
    def _load_code_cache(now_ts: float) -> List[str]:
        try:
            if TencentUtils._cache_file.exists():
                with open(TencentUtils._cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                ts = data.get("ts")
                codes = data.get("codes")
                if isinstance(ts, (int, float)) and isinstance(codes, list):
                    if TencentUtils._is_cache_valid(now_ts, ts):
                        TencentUtils._code_cache = codes
                        TencentUtils._code_cache_time = ts
                        logger.info(f"Loaded {len(codes)} codes from disk cache.")
                        return codes
        except Exception as e:
            logger.warning(f"Failed to load code cache from disk: {e}")
        return []

    @staticmethod
    def _save_code_cache(codes: List[str], ts: float) -> None:
        try:
            TencentUtils._cache_dir.mkdir(parents=True, exist_ok=True)
            tmp_path = TencentUtils._cache_file.with_suffix(".tmp")
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump({"ts": ts, "codes": codes}, f)
            tmp_path.replace(TencentUtils._cache_file)
        except Exception as e:
            logger.warning(f"Failed to save code cache to disk: {e}")

    @staticmethod
    def get_stock_zh_a_spot_tencent() -> pd.DataFrame:
        """
        Get real-time spot data using Tencent (gtimg) as primary source for prices,
        and Sina as the source for stock codes discovery.
        """
        # 1. Get Code List (Buffered)
        codes = TencentUtils._get_all_codes()
        if not codes:
            logger.error("Failed to retrieve stock codes from Sina fallback.")
            return pd.DataFrame()

        # 2. Fetch Prices from Tencent
        return TencentUtils._fetch_qt_prices(codes)

    @staticmethod
    def _get_all_codes() -> List[str]:
        """
        Get all A-share codes. 
        Uses internal cache or fetches from Eastmoney/Sina.
        """
        now = time.time()
        if TencentUtils._code_cache and TencentUtils._is_cache_valid(now, TencentUtils._code_cache_time):
            return TencentUtils._code_cache
        # Disk cache for restart resilience
        disk_codes = TencentUtils._load_code_cache(now)
        if disk_codes:
            return disk_codes

        logger.info("Refresing stock code list from Eastmoney (stable source)...")
        # Try Eastmoney first
        codes = TencentUtils._fetch_codes_from_eastmoney()
        if not codes:
            logger.warning("Eastmoney code fetch failed, falling back to Sina...")
            codes = TencentUtils._fetch_codes_from_sina()
            
        if codes:
            # Filter valid codes (sh6..., sz0..., sz3..., sh688...)
            # Sina format is usually "sh600000" or "sz000001"
            # We ensure they are 8 chars and start with sh/sz
            valid_codes = [c for c in codes if len(c) == 8 and (c.startswith('sh') or c.startswith('sz') or c.startswith('bj'))]
            TencentUtils._code_cache = valid_codes
            TencentUtils._code_cache_time = now
            TencentUtils._save_code_cache(valid_codes, now)
            logger.info(f"Refreshed {len(valid_codes)} codes.")
            return valid_codes
        return []

    @staticmethod
    def _fetch_codes_from_eastmoney() -> List[str]:
        """
        Fetch all codes using AkShare Eastmoney interface.
        Stable but takes ~60s due to large data. cache handles it.
        No retries: if the first attempt fails, fall back to Sina immediately.
        """
        retries = 1

        for i in range(retries):
            try:
                # timeout set to density of network
                # Since akshare doesn't allow passing timeout to this func easily (unless we wrap it),
                # we rely on its internal requests or just blocking.
                # But we can assume if it fails it throws.
                df = ak.stock_zh_a_spot_em() 
                
                if df.empty:
                    logger.warning(f"Eastmoney returned empty dataframe on attempt {i+1}")
                    continue
                
                # Convert to sh/sz format
                # Eastmoney columns: 序号, 代码, 名称, ...
                codes_list = []
                # Ensure '代码' column exists
                if '代码' not in df.columns:
                     logger.warning("Eastmoney data missing '代码' column")
                     continue

                for code in df['代码'].astype(str).tolist():
                    # Add prefix based on rules
                    if code.startswith('6'):
                        codes_list.append(f"sh{code}")
                    elif code.startswith('0') or code.startswith('3'):
                        codes_list.append(f"sz{code}")
                    elif code.startswith('8') or code.startswith('4') or code.startswith('9'):
                        codes_list.append(f"bj{code}")
                    else:
                        pass
                
                if codes_list:
                    return codes_list
                
            except Exception as e:
                logger.error(f"Error fetching codes from Eastmoney (Attempt {i+1}/{retries}): {e}")
                
        return []

    @staticmethod
    def _fetch_codes_from_sina() -> List[str]:
        """
        Fetch all codes using Sina paging. 
        Only needed to get the 'symbol' list.
        """
        url = "http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQNodeData"
        max_pages = 80 # Covers ~8000 stocks
        all_codes = []
        
        def _fetch_page(page):
            params = {
                "page": str(page),
                "num": "100",
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
            try:
                # Use short timeout, retry will handle
                r = requests.get(url, params=params, headers=headers, timeout=3)
                if r.status_code == 200:
                    data = demjson.decode(r.text)
                    if isinstance(data, list):
                        return [item['symbol'] for item in data if 'symbol' in item]
                else:
                    return None # Non-200 is failure
            except Exception:
                return None
            return None # Fallback failure if decode fails or empty text?

        # We must ensure we don't miss pages, so we might need to be sequential or careful parallel
        # Parallel is fine if we retry failures.
        
        pages_to_fetch = list(range(1, max_pages + 1))
        max_retries = 3
        
        for attempt in range(max_retries):
            if not pages_to_fetch:
                break
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                future_to_page = {executor.submit(_fetch_page, p): p for p in pages_to_fetch}
                
                rows_this_round = []
                failed_pages = []
                
                for future in concurrent.futures.as_completed(future_to_page):
                    page = future_to_page[future]
                    try:
                        res = future.result()
                        if res is not None:
                            if res: # Not empty list
                                rows_this_round.extend(res)
                            else:
                                # Empty list means end of data likely (or failure?)
                                # Usually Sina returns [] if out of range.
                                # So if page 60 returns [], page 61 will too.
                                pass 
                        else:
                            failed_pages.append(page)
                    except Exception:
                        failed_pages.append(page)
                
                all_codes.extend(rows_this_round)
                pages_to_fetch = failed_pages
                if failed_pages:
                    logger.warning(f"Sina code fetch attempt {attempt+1} failed for {len(failed_pages)} pages. Retrying...")
                    time.sleep(1)

        return list(set(all_codes)) # Dedup just in case

    @staticmethod
    def _fetch_qt_prices(codes: List[str]) -> pd.DataFrame:
        """
        Fetch real-time data from Tencent (qt.gtimg.cn)
        """
        if not codes:
            return pd.DataFrame()
        
        # Split codes into chunks of 80
        chunk_size = 80
        chunks = [codes[i:i + chunk_size] for i in range(0, len(codes), chunk_size)]
        
        all_lines = []
        
        def _fetch_chunk(chunk_codes):
            url_codes = ",".join(chunk_codes)
            url = f"http://qt.gtimg.cn/q={url_codes}"
            try:
                r = requests.get(url, timeout=3) # Fast timeout
                if r.status_code == 200:
                    return r.text
            except Exception:
                pass
            return ""

        # Parallel fetch
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            chunk_futures = [executor.submit(_fetch_chunk, ch) for ch in chunks]
            for future in concurrent.futures.as_completed(chunk_futures):
                try:
                    text = future.result()
                    if text:
                        lines = text.strip().split(';')
                        all_lines.extend([l for l in lines if l.strip()])
                except Exception:
                    pass
        
        # Parse result
        data_list = []
        for line in all_lines:
            # Format: v_sh600000="1~浦发银行~600000~10.52~..."
            if '="' not in line:
                continue
            try:
                left, right = line.split('="')
                content = right.strip('"')
                parts = content.split('~')
                if len(parts) < 40:
                    continue
                
                # Tencent format mapping:
                # 1: Name, 2: Code, 3: Current Price, 4: Close, 5: Open
                # 30: Date (YYYYMMDDHHMMSS) or similar? 
                # Actually index 30 is usually date like 20230101150000
                
                name = parts[1]
                code = parts[2]
                price = float(parts[3])
                prev_close = float(parts[4])
                open_price = float(parts[5])
                volume = float(parts[6]) # lots?
                # amount seems to be parts[37]?
                amount = float(parts[37]) * 10000 if parts[37] else 0 # 37 is amount in wan usually?
                # Let's verify amount later if needed. For now basic fields.
                
                high = float(parts[33]) if parts[33] else 0
                low = float(parts[34]) if parts[34] else 0
                
                pct_chg = float(parts[32]) if parts[32] else 0 # 32 is pct change?
                
                # Re-verify fields with sample:
                # v_sh600000="1~浦发银行~600000~10.52~10.63~10.63~435866~152752~282855~10.51~32...
                # 3: 10.52 (Price)
                # 31: 0.11 (Change amount?)
                # 32: -1.03 (Pct Change)
                # 33: 10.66 (High)
                # 34: 10.51 (Low)
                # 36: 435866 (Volume again?)
                # 37: 46146 (Amount in Wan?)
                
                # Calculate change manually if needed or use index 32
                change_rate = float(parts[32])
                
                data_list.append({
                    "代码": code,
                    "名称": name,
                    "最新价": price,
                    "涨跌幅": change_rate,
                    "昨收": prev_close,
                    "成交量": volume, # Int or float
                    "成交额": amount,
                    "最高": high,
                    "最低": low,
                    "今开": open_price
                })
            except Exception:
                continue

        df = pd.DataFrame(data_list)
        return df
