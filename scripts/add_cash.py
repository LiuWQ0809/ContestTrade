import json
import os
import sys
from datetime import datetime
from pathlib import Path

def add_cash(amount: float):
    # 定位 portfolio.json 路径
    project_root = Path(__file__).parent.parent.resolve()
    portfolio_path = project_root / "agents_workspace" / "portfolio.json"
    
    if not portfolio_path.exists():
        print(f"Error: 找不到账户文件 {portfolio_path}")
        return

    try:
        with open(portfolio_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        old_cash = data.get("cash", 0)
        data["cash"] = old_cash + amount
        
        # 记录充值历史，保持账目清晰
        if "history" not in data:
            data["history"] = []
            
        data["history"].append({
            "type": "DEPOSIT",
            "amount": amount,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "notes": "Manual cash injection"
        })
        
        with open(portfolio_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            
        print(f"✅ 成功充值: {amount:.2f}")
        print(f"💰 账户余额: {old_cash:.2f} -> {data['cash']:.2f}")
        
    except Exception as e:
        print(f"❌ 操作失败: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python add_cash.py <金额>")
        print("示例: python add_cash.py 10000")
    else:
        try:
            amount_to_add = float(sys.argv[1])
            add_cash(amount_to_add)
        except ValueError:
            print("错误: 请输入有效的数字金额")
