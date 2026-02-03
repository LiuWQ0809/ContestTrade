import json
import sys
from datetime import datetime
from pathlib import Path

def _ensure_portfolio(portfolio_path: Path) -> bool:
    if portfolio_path.exists():
        return True
    print(f"Error: 账户文件不存在 {portfolio_path}")
    return False

def sell_holding(symbol: str, price: float, quantity: int):
    # 定位 portfolio.json 路径
    project_root = Path(__file__).parent.parent.resolve()
    portfolio_path = project_root / "agents_workspace" / "portfolio.json"
    
    if not _ensure_portfolio(portfolio_path):
        return

    try:
        with open(portfolio_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # 1. 检查持仓是否存在
        if "holdings" not in data or symbol not in data["holdings"]:
            print(f"❌ 错误: 未找到持仓 {symbol}")
            return

        holding = data["holdings"][symbol]
        current_qty = holding["quantity"]
        name = holding.get("name", symbol)
        
        # 2. 检查数量是否足够
        if current_qty < quantity:
            print(f"❌ 错误: 持仓不足 (持有 {current_qty}, 试图卖出 {quantity})")
            return

        # 3. 计算收益
        revenue = price * quantity
        
        # 4. 更新持仓
        new_qty = current_qty - quantity
        if new_qty > 0:
            data["holdings"][symbol]["quantity"] = new_qty
            print(f"🔄 更新持仓: {name}({symbol}) 数量 {current_qty}->{new_qty}")
        else:
            del data["holdings"][symbol]
            print(f"🗑️ 平仓完成: {name}({symbol}) 已从持仓中移除")

        # 5. 增加现金
        old_cash = data.get("cash", 0.0)
        data["cash"] = old_cash + revenue
        
        # 6. 记录历史
        if "history" not in data: data["history"] = []
        data["history"].append({
            "type": "SELL_MANUAL",
            "symbol": symbol,
            "price": price,
            "quantity": quantity,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "notes": "Manual position sell"
        })
        
        with open(portfolio_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"✅ 卖出成功！获得资金: {revenue:.2f}, 当前现金: {data['cash']:.2f}")
        
    except Exception as e:
        print(f"❌ 操作失败: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("用法: python sell_holding.py <代码> <卖出单价> <卖出数量>")
        print("示例: python sell_holding.py 600519 1850 50")
    else:
        symbol = sys.argv[1]
        price = float(sys.argv[2])
        quantity = int(sys.argv[3])
        sell_holding(symbol, price, quantity)
