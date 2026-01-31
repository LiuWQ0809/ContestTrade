import json
import sys
from datetime import datetime
from pathlib import Path

def _init_portfolio_data():
    return {
        "cash": 20000.0,    # 初始资金 2万
        "holdings": {},
        "history": [],
        "daily_stats": [],
        "total_fees": 0.0
    }

def _ensure_portfolio(portfolio_path: Path) -> bool:
    if portfolio_path.exists():
        return True
    try:
        portfolio_path.parent.mkdir(parents=True, exist_ok=True)
        with open(portfolio_path, "w", encoding="utf-8") as f:
            json.dump(_init_portfolio_data(), f, ensure_ascii=False, indent=4)
        print(f"ℹ️ 未找到账户文件，已初始化: {portfolio_path}")
        return True
    except Exception as e:
        print(f"Error: 无法初始化账户文件 {portfolio_path}: {e}")
        return False

def add_holding(symbol: str, price: float, quantity: int, name: str = None):
    # 定位 portfolio.json 路径
    project_root = Path(__file__).parent.parent.resolve()
    portfolio_path = project_root / "agents_workspace" / "portfolio.json"
    
    if not _ensure_portfolio(portfolio_path):
        return

    try:
        with open(portfolio_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # 1. 检查资金是否足够 (可选逻辑，手动添加通常可以强制执行)
        cost = price * quantity
        if data.get("cash", 0) < cost:
            print(f"⚠️ 警告: 现金不足 ({data.get('cash', 0):.2f} < {cost:.2f})")
            confirm = input("是否仍要强行添加? (y/n): ")
            if confirm.lower() != 'y': return

        # 2. 更新持仓
        if "holdings" not in data: data["holdings"] = {}
        
        if symbol in data["holdings"]:
            # 补仓逻辑：计算加权平均价
            old_qty = data["holdings"][symbol]["quantity"]
            old_price = data["holdings"][symbol]["buy_price"]
            new_total_qty = old_qty + quantity
            new_avg_price = (old_price * old_qty + price * quantity) / new_total_qty
            
            data["holdings"][symbol]["quantity"] = new_total_qty
            data["holdings"][symbol]["buy_price"] = round(new_avg_price, 3)
            print(f"🔄 更新持仓: {symbol} 数量 {old_qty}->{new_total_qty}, 成本价 {old_price}->{new_avg_price:.3f}")
        else:
            # 新开仓
            data["holdings"][symbol] = {
                "name": name or symbol,
                "quantity": quantity,
                "buy_price": price,
                "buy_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "current_price": price
            }
            print(f"✨ 新增持仓: {name or symbol}({symbol}) 价格: {price}, 数量: {quantity}")

        # 3. 扣除现金
        data["cash"] -= cost
        
        # 4. 记录历史
        if "history" not in data: data["history"] = []
        data["history"].append({
            "type": "BUY_MANUAL",
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "notes": "Manual position add"
        })
        
        with open(portfolio_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"✅ 持仓更新成功！剩余现金: {data['cash']:.2f}")
        
    except Exception as e:
        print(f"❌ 操作失败: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("用法: python add_holding.py <代码> <单价> <数量> [名称]")
        print("示例: python add_holding.py 600519 1800 100 贵州茅台")
    else:
        symbol = sys.argv[1]
        price = float(sys.argv[2])
        quantity = int(sys.argv[3])
        name = sys.argv[4] if len(sys.argv) > 4 else None
        add_holding(symbol, price, quantity, name)
