import sys
import yfinance as yf
import pandas as pd
import numpy as np

def calculate_rsi(data, periods=14):
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def analyze_stock(symbol):
    print(f"🔍 正在分析 {symbol} ...")
    
    # yfinance suffix for Shanghai is .SS, Shenzhen is .SZ
    # "白银有色" 601212 is Shanghai
    yf_symbol = symbol
    if symbol.isdigit():
        if symbol.startswith('6'):
            yf_symbol = f"{symbol}.SS"
        else:
            yf_symbol = f"{symbol}.SZ"
            
    try:
        # 获取最近3个月的数据
        stock = yf.Ticker(yf_symbol)
        hist = stock.history(period="3mo")
        
        if hist.empty:
            print(f"❌ 无法获取 {symbol} 的数据。请检查代码或网络连接。")
            return

        current_price = hist['Close'].iloc[-1]
        prev_price = hist['Close'].iloc[-2]
        change = (current_price - prev_price) / prev_price * 100
        
        # 计算技术指标
        hist['MA5'] = hist['Close'].rolling(window=5).mean()
        hist['MA20'] = hist['Close'].rolling(window=20).mean()
        hist['RSI'] = calculate_rsi(hist['Close'])
        
        rsi_val = hist['RSI'].iloc[-1]
        ma5_val = hist['MA5'].iloc[-1]
        ma20_val = hist['MA20'].iloc[-1]
        
        print("\n" + "="*40)
        print(f"📊 股票分析报告: {stock.info.get('longName', symbol)}")
        print(f"当前价格: {current_price:.2f} ({change:+.2f}%)")
        print("="*40)
        
        print(f"\n📈 技术指标:")
        print(f"- RSI (14): {rsi_val:.2f}")
        print(f"- MA5     : {ma5_val:.2f}")
        print(f"- MA20    : {ma20_val:.2f}")
        
        print("\n💡 抄底参考信号:")
        
        # 简单的抄底判断逻辑
        signals = []
        
        # 1. RSI 超卖
        if rsi_val < 30:
            signals.append("✅ RSI低于30，处于超卖区间，可能存在反弹机会")
        elif rsi_val > 70:
            signals.append("⚠️ RSI高于70，处于超买区间，风险较高")
        else:
            signals.append(f"ℹ️ RSI为{rsi_val:.0f}，处于中性区间")
            
        # 2. 均线乖离率 (当前价格远离均线)
        bias = (current_price - ma20_val) / ma20_val * 100
        if bias < -10:
            signals.append(f"✅ 股价低于20日均线 {abs(bias):.1f}%，超跌明显")
        elif bias > 10:
            signals.append(f"⚠️ 股价高于20日均线 {bias:.1f}%，短期涨幅过大")
        
        # 3. 价格位置
        high_3m = hist['High'].max()
        low_3m = hist['Low'].min()
        position = (current_price - low_3m) / (high_3m - low_3m) * 100
        signals.append(f"ℹ️ 当前价格处于近3个月的 {position:.0f}% 位置 (0%=最低, 100%=最高)")
        
        if current_price < low_3m * 1.05:
            signals.append("✅ 接近近3个月最低价，具有一定支撑")

        for s in signals:
            print(s)
            
        print("\n⚠️ 免责声明: 以上仅为技术指标计算结果，不构成投资建议。请结合基本面谨慎决策。")
        print("="*40)

    except Exception as e:
        print(f"❌ 分析失败: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python analyze_stock.py <代码>")
        print("示例: python analyze_stock.py 601212")
    else:
        analyze_stock(sys.argv[1])
