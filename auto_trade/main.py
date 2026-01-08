import asyncio
import os
import sys
import datetime
from pathlib import Path
from loguru import logger
import akshare as ak
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout
from rich.text import Text
from rich import box

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.append(str(PROJECT_ROOT))
sys.path.append(str(PROJECT_ROOT / "contest_trade"))

from contest_trade.main import SimpleTradeCompany
from auto_trade.portfolio import VirtualPortfolio

# Setup logging
LOG_PATH = PROJECT_ROOT / "agents_workspace" / "logs" / "auto_trade.log"
logger.add(LOG_PATH, rotation="10 MB", level="INFO")

console = Console()

class AutoTrader:
    def __init__(self, market="CN-Stock"):
        self.market = market
        os.environ['CONTEST_TRADE_MARKET'] = market
        self.company = SimpleTradeCompany()
        self.portfolio = VirtualPortfolio(PROJECT_ROOT / "agents_workspace" / "portfolio.json")
        self.last_run_status = "等待运行..."
        self.last_run_time = "无"

    def get_realtime_price_and_name(self, symbol_or_name):
        """获取实时价格和股票名称"""
        try:
            # 兼容代码或名称输入
            base_symbol = symbol_or_name.split('.')[0]
            df = ak.stock_zh_a_spot_em()
            
            row = df[df['代码'] == base_symbol]
            if row.empty:
                row = df[df['名称'] == symbol_or_name]
            
            if not row.empty:
                return float(row.iloc[0]['最新价']), row.iloc[0]['名称'], row.iloc[0]['代码']
        except Exception as e:
            logger.error(f"Error getting price for {symbol_or_name}: {e}")
        return None, None, None

    def display_portfolio(self):
        """使用 Rich 打印专业的持仓报告"""
        table = Table(title="📊 虚拟交易账户持仓明细", box=box.ROUNDED, header_style="bold magenta", expand=True)
        table.add_column("股票名称", justify="center")
        table.add_column("代码", justify="center")
        table.add_column("持仓数量", justify="right")
        table.add_column("买入均价", justify="right")
        table.add_column("当前价格", justify="right")
        table.add_column("浮动盈亏", justify="right")
        table.add_column("收益率", justify="right")

        total_holdings_value = 0
        for symbol, info in self.portfolio.data["holdings"].items():
            name = info.get("name", "未知")
            qty = info["quantity"]
            buy_price = info["buy_price"]
            cur_price = info.get("current_price", buy_price)
            pnl = (cur_price - buy_price) * qty
            pnl_rate = (cur_price - buy_price) / buy_price
            
            total_holdings_value += qty * cur_price
            
            color = "green" if pnl >= 0 else "red"
            table.add_row(
                name, symbol, str(qty), f"{buy_price:.2f}", f"{cur_price:.2f}",
                f"[{color}]{pnl:.2f}[/{color}]", f"[{color}]{pnl_rate:.2%}[/{color}]"
            )

        total_value = self.portfolio.data["cash"] + total_holdings_value
        total_pnl = total_value - 20000.0
        pnl_color = "bold green" if total_pnl >= 0 else "bold red"

        summary = Text.assemble(
            ("账户总资产: ", "bold"), (f"{total_value:.2f}", "yellow"), (" | "),
            ("可用现金: ", "bold"), (f"{self.portfolio.data['cash']:.2f}", "cyan"), (" | "),
            ("累计盈亏: ", "bold"), (f"{total_pnl:.2f}", pnl_color)
        )
        
        console.print(Panel(summary, title="💰 账户概览", border_style="blue"))
        if self.portfolio.data["holdings"]:
            console.print(table)
        else:
            console.print("[dim]当前暂无持仓[/dim]")

    async def run_once(self):
        now_dt = datetime.datetime.now()
        rounded_minute = (now_dt.minute // 5) * 5
        trigger_time = now_dt.replace(minute=rounded_minute, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        
        self.last_run_time = trigger_time
        self.last_run_status = "🔄 正在分析中..."
        
        console.print(f"\n[bold blue]🚀 开启新一轮市场评估 - 触发时间: {trigger_time}[/bold blue]")
        
        try:
            # 1. 执行 AI 分析
            final_state = await self.company.run_company(trigger_time)
            best_signals = final_state.get('step_results', {}).get('contest', {}).get('best_signals', [])
            
            # 2. 处理信号
            sig_table = Table(title="🔍 AI 交易信号汇总", box=box.SIMPLE, header_style="bold cyan")
            sig_table.add_column("股票", justify="left")
            sig_table.add_column("建议", justify="center")
            sig_table.add_column("确定性", justify="right")
            sig_table.add_column("执行状态", justify="left")

            current_prices = {}
            for signal in best_signals:
                raw_symbol = signal.get('symbol_name')
                action = signal.get('action', '').lower()
                has_opp = signal.get('has_opportunity', 'no')
                score = signal.get('probability', 'N/A')
                
                if not raw_symbol or has_opp != 'yes':
                    continue
                
                price, name, code = self.get_realtime_price_and_name(raw_symbol)
                status = "[yellow]等待[/yellow]"
                
                if price:
                    current_prices[code] = price
                    if action == 'buy':
                        if self.portfolio.buy(code, price, trigger_time, name=name):
                            status = "[green]✅ 已买入[/green]"
                        else:
                            status = "[dim]跳过 (已持仓或资金不足)[/dim]"
                    elif action == 'sell':
                        if self.portfolio.sell(code, price, trigger_time):
                            status = "[red]成交量 (已卖出)[/red]"
                        else:
                            status = "[dim]跳过 (未持仓)[/dim]"
                else:
                    status = "[red]❌ 获取价格失败[/red]"

                sig_table.add_row(f"{name or raw_symbol}({code or '?'})", action.upper(), f"{score}%", status)

            if best_signals:
                console.print(sig_table)
            else:
                console.print("[yellow]本次评估未发现明确交易机会[/yellow]")

            # 3. 更新收益
            for held_code in list(self.portfolio.data["holdings"].keys()):
                if held_code not in current_prices:
                    price, _, _ = self.get_realtime_price_and_name(held_code)
                    if price:
                        current_prices[held_code] = price
            
            self.portfolio.update_performance(current_prices, trigger_time.split(' ')[0])
            self.display_portfolio()
            self.last_run_status = "✅ 分析完成"
            
        except Exception as e:
            self.last_run_status = "❌ 运行出错"
            logger.exception(f"Error during run_once: {e}")
            console.print(f"[bold red]运行异常: {e}[/bold red]")

    async def scheduler(self):
        """改进的调度器，提供动态监控界面"""
        # A股交易时间：09:30-11:30, 13:00-15:00
        # 每隔半小时查询一次
        target_times = [
            "09:30", "10:00", "10:30", "11:00", "11:30", 
            "13:05", "13:30", "14:00", "14:30", "15:05"
        ]
        last_heartbeat = None
        
        console.print(Panel(
            f"[bold green]AutoTrader 智能交易系统已启动[/bold green]\n"
            f"当前市场: [bold]{self.market}[/bold]\n"
            f"监控频率: [bold]交易时间内每 30 分钟[/bold]\n"
            f"时间点: {', '.join(target_times)}\n"
            f"日志路径: {LOG_PATH}",
            title="系统状态", border_style="green"
        ))

        # 启动时立即运行一次初始评估
        weekday = datetime.datetime.now().weekday()
        if weekday < 5:
            console.print("[bold yellow]🚀 启动完成，正在执行首次市场评估...[/bold yellow]")
            await self.run_once()

        while True:
            now = datetime.datetime.now()
            now_str = now.strftime("%H:%M")
            weekday = now.weekday() # 0-4 is Mon-Fri
            
            # 只有交易日（周一至周五）才执行交易逻辑
            if weekday < 5:
                if now_str in target_times:
                    await self.run_once()
                    await asyncio.sleep(60)
            
            # 每 30 分钟在终端显示一次状态心跳
            if last_heartbeat is None or (now - last_heartbeat).total_seconds() >= 1800:
                status_msg = "系统正常运行中" if weekday < 5 else "周末休市中"
                console.print(f"[dim][{now.strftime('%H:%M:%S')}] ⏳ {status_msg}，正在等待交易窗口...[/dim]")
                last_heartbeat = now

            # 保持终端活性输出
            market_status = "开盘" if weekday < 5 else "休市"
            print(f"\r[ 🕒 时间: {now.strftime('%H:%M:%S')} | 市场: {market_status} | 上次运行: {self.last_run_time or '无'} | 状态: {self.last_run_status} ]", end="")
            await asyncio.sleep(1)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="立即运行一次分析并退出")
    args = parser.parse_args()

    trader = AutoTrader()
    if args.once:
        asyncio.run(trader.run_once())
    else:
        try:
            asyncio.run(trader.scheduler())
        except KeyboardInterrupt:
            console.print("\n[bold yellow]👋 用户中断，系统正在安全退出...[/bold yellow]")
