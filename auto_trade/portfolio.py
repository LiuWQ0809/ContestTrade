import json
import os
from datetime import datetime
from pathlib import Path
from loguru import logger

class VirtualPortfolio:
    # 模拟 A 股交易费用 (标准设置)
    COMMISSION_RATE = 0.0003  # 佣金 万分之三
    MIN_COMMISSION = 5.0      # 最低佣金 5元
    STAMP_DUTY_RATE = 0.0005  # 印花税 千分之零点五 (仅卖出时收取)
    TRANSFER_FEE_RATE = 0.00001 # 过户费 (双向)

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
            "daily_stats": [],  # [{date, total_value, day_return}]
            "total_fees": 0.0   # 累计产生的费用
        }

    def _save(self):
        with open(self.storage_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=4, ensure_ascii=False)

    def _calculate_buy_fee(self, cost):
        commission = max(self.MIN_COMMISSION, cost * self.COMMISSION_RATE)
        transfer = cost * self.TRANSFER_FEE_RATE
        return round(commission + transfer, 2)

    def _calculate_sell_fee(self, revenue):
        commission = max(self.MIN_COMMISSION, revenue * self.COMMISSION_RATE)
        stamp_duty = revenue * self.STAMP_DUTY_RATE
        transfer = revenue * self.TRANSFER_FEE_RATE
        return round(commission + stamp_duty + transfer, 2)

    def buy(self, symbol, price, time_str, name="N/A", amount=10000):
        if symbol in self.data["holdings"]:
            return False
        
        # A 股买入单位为 100 股 (手)
        quantity = (int(amount / price) // 100) * 100
        if quantity < 100:
            logger.warning(f"买入金额不足以购买一手 (100股): {symbol} @ {price}")
            return False

        cost = quantity * price
        fee = self._calculate_buy_fee(cost)
        
        if self.data["cash"] < (cost + fee):
            logger.warning(f"资金不足以支付股票及费用: Cash {self.data['cash']}, 需要 {cost + fee}")
            return False

        self.data["cash"] -= (cost + fee)
        # 记录累计税费
        self.data["total_fees"] = round(self.data.get("total_fees", 0.0) + fee, 2)

        self.data["holdings"][symbol] = {
            "name": name,
            "quantity": quantity,
            "buy_price": price,
            "buy_time": time_str,
            "current_price": price,
            "buy_fee": fee
        }
        
        self.data["history"].append({
            "type": "BUY",
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "fee": fee,
            "time": time_str
        })
        
        logger.info(f"Virtual Buy: {symbol} @ {price}, quantity: {quantity}, Fee: {fee}")
        self._save()
        return True

    def sell(self, symbol, price, time_str, reason="AI Signal"):
        if symbol not in self.data["holdings"]:
            logger.info(f"Not holding {symbol}, cannot sell.")
            return False
        
        holding = self.data["holdings"][symbol]
        name = holding.get("name", "N/A")

        
        # T+1 规则检查: 检查买入日期是否是今天
        buy_date = holding["buy_time"].split(' ')[0]
        cur_date = time_str.split(' ')[0]
        if buy_date == cur_date:
            logger.warning(f"T+1 限制: {symbol} 是今日买入的，今日不可卖出。")
            return False

        holding = self.data["holdings"].pop(symbol)
        quantity = holding["quantity"]
        revenue = quantity * price
        fee = self._calculate_sell_fee(revenue)
        
        buy_price = holding["buy_price"]
        buy_fee = holding.get("buy_fee", 0.0)
        
        # 净盈亏 = 卖收 - 买入成本 - 卖出费用 - 买入费用
        pnl = revenue - (quantity * buy_price) - fee - buy_fee
        pnl_rate = pnl / (quantity * buy_price + buy_fee)
        
        self.data["cash"] += (revenue - fee)
        # 记录累计税费
        self.data["total_fees"] = round(self.data.get("total_fees", 0.0) + fee, 2)

        self.data["history"].append({
            "type": "SELL",
            "symbol": symbol,
            "name": name,
            "buy_price": buy_price,
            "sell_price": price,
            "quantity": quantity,
            "buy_fee": buy_fee,
            "sell_fee": fee,
            "time": time_str,
            "pnl": round(pnl, 2),
            "pnl_rate": f"{pnl_rate:.2%}",
            "reason": reason
        })
        
        logger.info(f"Virtual Sell: {symbol} @ {price}, Fee: {fee}, Net PnL: {pnl:.2f} ({pnl_rate:.2%}), Reason: {reason}")
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
            buy_fee = info.get("buy_fee", 0.0)
            
            # 预估卖出费用 (用于计算净盈亏)
            est_revenue = qty * cur_price
            est_sell_fee = self._calculate_sell_fee(est_revenue)
            
            # 净盈亏 = (现价 - 买价) * 数量 - 买费 - 预估卖费
            net_pnl = (cur_price - buy_price) * qty - buy_fee - est_sell_fee
            net_pnl_rate = net_pnl / (qty * buy_price + buy_fee)
            
            total_holdings_value += (qty * cur_price - est_sell_fee) # 估算的清缴后价值
            
            stock_details.append({
                "symbol": symbol,
                "current_price": cur_price,
                "net_pnl": round(net_pnl, 2),
                "net_pnl_rate": f"{net_pnl_rate:.2%}"
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
            "total_fees": self.data.get("total_fees", 0.0),
            "day_return": f"{day_return:.2%}",
            "holdings": stock_details
        }
        self.data["daily_stats"].append(stat)
        
        logger.info(f"Performance Update [{date_str}]: Total Value: {total_value:.2f}, Daily Return: {day_return:.2%}, Total Fees: {self.data.get('total_fees', 0.0)}")
        logger.info(f"Holdings Details: {json.dumps(stock_details, ensure_ascii=False)}")
        self._save()
