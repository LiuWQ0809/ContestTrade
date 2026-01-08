import json
import os
from datetime import datetime
from pathlib import Path
from loguru import logger

class VirtualPortfolio:
    def __init__(self, storage_path: str = "agents_workspace/portfolio.json"):
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.data = self._load()

    def _load(self):
        if self.storage_path.exists():
            try:
                with open(self.storage_path, "r", encoding="utf-8") as f:
                    content = f.read().strip()
                    if content:
                        return json.loads(content)
            except Exception as e:
                logger.error(f"加载账户数据失败，将重新创建: {e}")
        
        return {
            "cash": 20000.0,  # 初始资金 2万
            "holdings": {},     # {symbol: {quantity, avg_price, buy_time}}
            "history": [],      # [{type, symbol, price, quantity, time, pnl}]
            "daily_stats": []   # [{date, total_value, day_return}]
        }

    def _save(self):
        with open(self.storage_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=4, ensure_ascii=False)

    def buy(self, symbol, price, time_str, name="N/A", amount=5000):
        if symbol in self.data["holdings"]:
            return False
        
        if self.data["cash"] < amount:
            return False

        quantity = int(amount / price)
        cost = quantity * price
        
        self.data["cash"] -= cost
        self.data["holdings"][symbol] = {
            "name": name,
            "quantity": quantity,
            "buy_price": price,
            "buy_time": time_str,
            "current_price": price
        }
        
        self.data["history"].append({
            "type": "BUY",
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "time": time_str
        })
        
        logger.info(f"Virtual Buy: {symbol} @ {price}, quantity: {quantity}")
        self._save()
        return True

    def sell(self, symbol, price, time_str):
        if symbol not in self.data["holdings"]:
            logger.info(f"Not holding {symbol}, cannot sell.")
            return False
        
        holding = self.data["holdings"].pop(symbol)
        quantity = holding["quantity"]
        revenue = quantity * price
        buy_price = holding["buy_price"]
        pnl = revenue - (quantity * buy_price)
        pnl_rate = pnl / (quantity * buy_price)
        
        self.data["cash"] += revenue
        self.data["history"].append({
            "type": "SELL",
            "symbol": symbol,
            "buy_price": buy_price,
            "sell_price": price,
            "quantity": quantity,
            "time": time_str,
            "pnl": round(pnl, 2),
            "pnl_rate": f"{pnl_rate:.2%}"
        })
        
        logger.info(f"Virtual Sell: {symbol} @ {price}, PnL: {pnl:.2f} ({pnl_rate:.2%})")
        self._save()
        return True

    def update_performance(self, current_prices: dict, date_str: str):
        """
        current_prices: {symbol: price}
        """
        total_holdings_value = 0
        stock_details = []
        for symbol, info in self.data["holdings"].items():
            if symbol in current_prices:
                info["current_price"] = current_prices[symbol]
                
            cur_price = info["current_price"]
            buy_price = info["buy_price"]
            qty = info["quantity"]
            
            total_pnl = (cur_price - buy_price) * qty
            total_pnl_rate = (cur_price - buy_price) / buy_price
            
            total_holdings_value += qty * cur_price
            
            stock_details.append({
                "symbol": symbol,
                "current_price": cur_price,
                "total_pnl": round(total_pnl, 2),
                "total_pnl_rate": f"{total_pnl_rate:.2%}"
            })
        
        total_value = self.data["cash"] + total_holdings_value
        
        # Calculate daily return if possible
        day_return = 0
        if self.data["daily_stats"]:
            prev_value = self.data["daily_stats"][-1]["total_value"]
            day_return = (total_value - prev_value) / prev_value
            
        stat = {
            "date": date_str,
            "total_value": round(total_value, 2),
            "total_pnl": round(total_value - 20000.0, 2),
            "day_return": f"{day_return:.2%}",
            "holdings": stock_details
        }
        self.data["daily_stats"].append(stat)
        
        logger.info(f"Performance Update [{date_str}]: Total Value: {total_value:.2f}, Daily Return: {day_return:.2%}")
        logger.info(f"Holdings Details: {json.dumps(stock_details, ensure_ascii=False)}")
        self._save()
