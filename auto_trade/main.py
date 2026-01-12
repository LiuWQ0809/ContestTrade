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
LOG_DIR = PROJECT_ROOT / "agents_workspace" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "auto_trade.log"

def is_market_open(dt=None):
    """判断是否在 A 股交易时间内 (9:30-11:30, 13:00-15:00)"""
    if dt is None:
        dt = datetime.datetime.now()
    
    # 检查周六周日
    if dt.weekday() >= 5:
        return False
    
    current_time = dt.time()
    am_start = datetime.time(9, 30)
    am_end = datetime.time(11, 31) # 略微宽松
    pm_start = datetime.time(13, 0)
    pm_end = datetime.time(15, 1)
    
    return (am_start <= current_time <= am_end) or (pm_start <= current_time <= pm_end)

# 清理现有的 loguru 配置
logger.remove()

class RichConsoleLogger:
    """包装 Console 使得输出同时记录到 loguru，确保日志文件纯净无乱码"""
    def __init__(self):
        # 终端显示 Console (带颜色)
        self.console = Console(width=120)
        # 纯文本捕获 Console (无颜色，用于写入日志文件，避免出现 [32m 等乱码)
        self.file_console = Console(width=120, force_terminal=False, no_color=True, highlight=False)
    
    def print(self, *args, **kwargs):
        # 1. 打印到终端
        self.console.print(*args, **kwargs)
        
        # 2. 捕获纯文本发送给 logger
        with self.file_console.capture() as capture:
            self.file_console.print(*args, **kwargs)
        text_output = capture.get().strip()
        if text_output:
            # 恢复 INFO 级别，确保能写入日志文件（配合 log_file_filter）
            # 注意：log_file_filter 会保留 'auto_trade' 模块的 INFO 日志
            logger.info(f"\n[REPORT]\n{text_output}\n")

    def rule(self, *args, **kwargs):
        self.console.rule(*args, **kwargs)
        with self.file_console.capture() as capture:
            self.file_console.rule(*args, **kwargs)
        text_output = capture.get().strip()
        if text_output:
             logger.info(f"\n{text_output}\n")

# 初始化增强版 Console
console = RichConsoleLogger()

def log_file_filter(record):
    """文件日志过滤器：排除冗余的 Agent 执行过程，仅保留核心交易和报表"""
    # 排除名单：这些模块的 INFO 日志太频繁，不存入文件
    exclude_modules = ["agents", "contest_trade", "data_source", "tools"]
    if record["level"].name == "INFO":
        for mod in exclude_modules:
            if record["name"].startswith(mod):
                return False
    return True

# 配置 loguru 输出
# 终端：使用带颜色的简洁格式（不包含多余的前缀，适合作为 CLI 界面的一部分）
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | <cyan>{message}</cyan>", colorize=True)
# 文件：记录纯文本信息，确保编码为 utf-8，使用过滤器排除过程日志
# 修改 format 以移除 __main__:print: 前缀，使日志更清爽
logger.add(LOG_PATH, rotation="10 MB", level="INFO", format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}", filter=log_file_filter, encoding="utf-8", enqueue=True, colorize=False)

class AutoTrader:
    def __init__(self, market="CN-Stock"):
        self.market = market
        os.environ['CONTEST_TRADE_MARKET'] = market
        self.company = SimpleTradeCompany()
        self.portfolio = VirtualPortfolio(PROJECT_ROOT / "agents_workspace" / "portfolio.json")
        self.last_run_status = "等待运行..."
        self.last_run_time = "无"

    def get_realtime_price_and_name(self, symbol_or_name):
        """获取实时价格、股票名称、代码以及昨收价"""
        try:
            # 兼容代码或名称输入
            base_symbol = symbol_or_name.split('.')[0]
            df = ak.stock_zh_a_spot_em()
            
            row = df[df['代码'] == base_symbol]
            if row.empty:
                row = df[df['名称'] == symbol_or_name]
            
            if not row.empty:
                return (
                    float(row.iloc[0]['最新价']), 
                    row.iloc[0]['名称'], 
                    row.iloc[0]['代码'], 
                    float(row.iloc[0]['昨收']),
                    float(row.iloc[0].get('涨跌幅', 0))
                )
        except Exception as e:
            logger.error(f"Error getting price for {symbol_or_name}: {e}")
        return None, None, None, None, 0

    def display_portfolio(self):
        """使用 Rich 打印专业的持仓报告，包含今日已卖出"""
        
        # 1. 准备数据
        total_holdings_value = 0
        total_day_pnl = 0
        
        # 批量获取行情
        try:
            spot_df = ak.stock_zh_a_spot_em()
        except:
            spot_df = None
            
        # 2. 持仓表格
        holdings_table = Table(title="📊 虚拟交易账户持仓明细", box=box.ROUNDED, header_style="bold magenta", expand=True)
        holdings_table.add_column("股票名称", justify="center")
        holdings_table.add_column("代码", justify="center")
        holdings_table.add_column("持仓数量", justify="right")
        holdings_table.add_column("买入均价", justify="right")
        holdings_table.add_column("当前价格", justify="right")
        holdings_table.add_column("当日盈亏", justify="right")
        holdings_table.add_column("当日收益率", justify="right")
        holdings_table.add_column("浮动盈亏", justify="right")
        holdings_table.add_column("累计收益率", justify="right")

        for symbol, info in self.portfolio.data["holdings"].items():
            name = info.get("name", "未知")
            qty = info["quantity"]
            buy_price = info["buy_price"]
            
            # 从实时行情获取最新价和昨收
            cur_price = info.get("current_price", buy_price)
            pre_close = cur_price # 默认值
            
            if spot_df is not None:
                row = spot_df[spot_df['代码'] == symbol.split('.')[0]]
                if not row.empty:
                    cur_price = float(row.iloc[0]['最新价'])
                    pre_close = float(row.iloc[0]['昨收'])

            # 判断是否为今日买入
            buy_date = info["buy_time"].split(' ')[0]
            cur_date = datetime.datetime.now().strftime("%Y-%m-%d")
            is_new_buy = (buy_date == cur_date)

            buy_fee = info.get("buy_fee", 0.0)
            # 准确计算预估卖出费 (使用 portfolio 中的逻辑)
            holding_revenue = qty * cur_price
            est_sell_fee = self.portfolio._calculate_sell_fee(holding_revenue)

            # 计算当日盈亏 (Holdings Part)
            if is_new_buy:
                # 今日买入：当日盈亏 = (现价 - 买价) * 数量 - 买入费
                # 解释：资产从现金变成了股票，这部分变动在买入瞬间是 (-费用)。
                holding_day_pnl = (cur_price - buy_price) * qty - buy_fee
                # 对于今日买入，分母使用初始投入成本(含费)
                initial_cost = buy_price * qty + buy_fee
                holding_day_pnl_rate = holding_day_pnl / initial_cost if initial_cost > 0 else 0
            else:
                # 非今日买入：当日盈亏 = (现价 - 昨收) * 数量
                holding_day_pnl = (cur_price - pre_close) * qty
                market_val_pre = pre_close * qty
                holding_day_pnl_rate = holding_day_pnl / market_val_pre if market_val_pre > 0 else 0
            
            # 计算累计浮动盈亏 (扣除买入费和预估卖出费，即净清算价值 - 投入本金)
            total_cost = qty * buy_price + buy_fee
            net_liquidation_value = qty * cur_price - est_sell_fee
            total_pnl = net_liquidation_value - total_cost
            total_pnl_rate = total_pnl / total_cost if total_cost > 0 else 0

            # 累加
            total_holdings_value += net_liquidation_value
            total_day_pnl += holding_day_pnl
            
            day_color = "red" if holding_day_pnl < 0 else "green"
            total_color = "red" if total_pnl < 0 else "green"
            
            holdings_table.add_row(
                name, symbol, str(qty), f"{buy_price:.2f}", f"{cur_price:.2f}",
                f"[{day_color}]{holding_day_pnl:+.2f}[/{day_color}]", f"[{day_color}]{holding_day_pnl_rate:+.2%}[/{day_color}]",
                f"[{total_color}]{total_pnl:+.2f}[/{total_color}]", f"[{total_color}]{total_pnl_rate:+.2%}[/{total_color}]"
            )

        # 3. 今日已卖出表格 & 修正当日总盈亏
        sold_table = Table(title="📉 今日已卖出交易明细", box=box.ROUNDED, header_style="bold yellow", expand=True)
        sold_table.add_column("股票名称", justify="center")
        sold_table.add_column("代码", justify="center")
        sold_table.add_column("买入价", justify="right")
        sold_table.add_column("卖出价", justify="right")
        sold_table.add_column("数量", justify="right")
        sold_table.add_column("实 盈亏", justify="right") # Realized PnL
        sold_table.add_column("交易税费", justify="right")
        sold_table.add_column("卖出原因", justify="center")

        cur_date_str = datetime.datetime.now().strftime("%Y-%m-%d")
        history = self.portfolio.data.get("history", [])
        
        has_sold_today = False
        today_realized_pnl = 0.0

        for trade in history:
            if trade['type'] == 'SELL' and trade['time'].startswith(cur_date_str):
                has_sold_today = True
                
                # 获取数据
                s_name = trade.get('name', 'N/A')
                s_code = trade.get('symbol', '')
                
                # 补充名称查找逻辑：如果是旧数据没有存 name，尝试从 spot_df 或 holdings 缓存里找
                if s_name == 'N/A' and spot_df is not None:
                     match_row = spot_df[spot_df['代码'] == s_code.split('.')[0]]
                     if not match_row.empty:
                         s_name = match_row.iloc[0]['名称']
                
                s_buy_price = trade.get('buy_price', 0.0)
                # 兼容旧数据：如果没有记录 reason，显示默认文案
                s_reason = trade.get('reason', 'AI模型决策')
                s_sell_price = trade.get('sell_price', 0.0)
                s_qty = trade.get('quantity', 0)
                s_pnl = trade.get('pnl', 0.0)
                s_fee = trade.get('sell_fee', 0.0)

                # 累加到当日总盈亏 (Realized Part)
                # 注意：当日卖出的，如果昨天持仓，那么今日的这部分变动也应该算入当日盈亏。
                # 但这里的 trade['pnl'] 是"累计实现盈亏" (Total Realized PnL vs Buy Cost)。
                # 为了计算准确的"当日"盈亏，我们需要拆分：
                #   当日卖出盈亏贡献 = (卖出价 - 昨收价) * 数量 - 卖出费用
                #   如果是今日买今日卖（T+0不可能，但按逻辑说）：(卖出价 - 买入价) * 数量 - 买卖费用
                # 由于 A 股 T+1，且假设一定非今日买入：
                #   Realized Day PnL = (SellPrice - PrevClose) * Qty - SellFee
                # 但是 historical trade record 并没有存 PrevClose。
                # 方案 B：简单处理，将今日卖出的"落袋盈亏"直接算入"当日盈亏"展示可能有歧义（混淆了过去几天的），
                # 但为了财务报表的"净资产变动"视角：
                #   今日净资产变动 = (今日持仓市值 - 昨日持仓市值) + (今日现金 - 昨日现金)
                #   这等价于：Holdings Day PnL + Realized Day PnL - Withdrawals.
                
                # 我们尝试重新获取昨收价来计算精确的 Day PnL Contribution
                r_pre_close = s_buy_price # Fallback
                if spot_df is not None:
                     r_row = spot_df[spot_df['代码'] == s_code.split('.')[0]]
                     if not r_row.empty:
                        r_pre_close = float(r_row.iloc[0]['昨收'])

                # 估算当日该笔交易的贡献 (T+1假设)
                # 贡献 = (卖出价 - 昨收) * 数量 - 卖出费
                # 验证：如果昨收 100，卖出 110，盈 10。资产增加了 10 (忽略费)。正确。
                trade_day_pnl_contribution = (s_sell_price - r_pre_close) * s_qty - s_fee
                total_day_pnl += trade_day_pnl_contribution

                pnl_color = "red" if s_pnl < 0 else "green"
                sold_table.add_row(
                    s_name, s_code, f"{s_buy_price:.2f}", f"{s_sell_price:.2f}", str(s_qty),
                    f"[{pnl_color}]{s_pnl:+.2f}[/{pnl_color}]", f"{s_fee:.2f}", s_reason
                )

        # 4. 汇总计算
        total_value = self.portfolio.data["cash"] + total_holdings_value
        cumulative_pnl = total_value - 20000.0
        cumulative_pnl_rate = cumulative_pnl / 20000.0
        
        # 当日账户收益率 = 当日总盈亏 / 昨日总资产
        yesterday_value = total_value - total_day_pnl # 反推
        day_pnl_rate = total_day_pnl / yesterday_value if yesterday_value != 0 else 0

        pnl_color = "bold green" if cumulative_pnl >= 0 else "bold red"
        day_pnl_color = "bold green" if total_day_pnl >= 0 else "bold red"

        summary = Text.assemble(
            ("账户总资产: ", "bold"), (f"{total_value:.2f}", "yellow"), (" | "),
            ("可用现金: ", "bold"), (f"{self.portfolio.data['cash']:.2f}", "cyan"), (" | "),
            ("当日盈亏: ", "bold"), (f"{total_day_pnl:+.2f}", day_pnl_color), 
            (" (", day_pnl_color), (f"{day_pnl_rate:+.2%}", day_pnl_color), (") | ", day_pnl_color),
            ("累计盈亏: ", "bold"), (f"{cumulative_pnl:+.2f}", pnl_color),
            (" (", pnl_color), (f"{cumulative_pnl_rate:+.2%}", pnl_color), (") | "),
            ("交易税费: ", "bold"), (f"{self.portfolio.data.get('total_fees', 0):.2f}", "magenta")
        )
        
        console.print(Panel(summary, title="💰 账户概览 (T+0 实时估算)", border_style="blue"))
        if self.portfolio.data["holdings"]:
            console.print(holdings_table)
        else:
            console.print("[dim]当前暂无持仓[/dim]")
            
        if has_sold_today:
             console.print(sold_table)

    async def run_once(self):
        now_dt = datetime.datetime.now()
        rounded_minute = (now_dt.minute // 5) * 5
        trigger_time = now_dt.replace(minute=rounded_minute, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        
        self.last_run_time = trigger_time
        self.last_run_status = "🔄 正在分析中..."
        
        # 交易时间强制检查
        if not is_market_open(now_dt):
            console.print(f"[yellow]非交易时段 ({now_dt.strftime('%H:%M:%S')})，仅展示账户概览，不执行交易决策。[/yellow]")
            self.display_portfolio()
            self.last_run_status = "💤 非交易时段"
            return

        console.print(f"\n[bold blue]🚀 开启新一轮市场评估 - 触发时间: {trigger_time}[/bold blue]")
        
        try:
            # 资金前置检查
            available_cash = self.portfolio.data.get('cash', 0)
            has_holdings = len(self.portfolio.data.get('holdings', {})) > 0
            
            if available_cash < 1000 and not has_holdings:
                console.print("[yellow]⚠️ 账户余额不足 1000 且无持仓，跳过本轮分析以节省 API 消耗。[/yellow]")
                self.last_run_status = "💤 资金不足跳过"
                return

            # 1. 执行 AI 分析 (将账户信息传入以便 Agent 决策)
            final_state = await self.company.run_company(trigger_time, portfolio_info=self.portfolio.data)
            best_signals = final_state.get('step_results', {}).get('contest', {}).get('best_signals', [])
            
            # 2. 处理信号
            sig_table = Table(title="🔍 AI 交易信号汇总", box=box.SIMPLE, header_style="bold cyan")
            sig_table.add_column("股票", justify="left")
            sig_table.add_column("建议", justify="center")
            sig_table.add_column("确定性", justify="right")
            sig_table.add_column("执行状态", justify="left")

            # 优化：先处理卖出信号释放资金，再处理买入信号
            sorted_signals = sorted(best_signals, key=lambda x: 0 if x.get('action', '').lower() == 'sell' else 1)

            current_prices = {}
            for signal in sorted_signals:
                raw_symbol = signal.get('symbol_name')
                action = signal.get('action', '').lower()
                has_opp = signal.get('has_opportunity', 'no')
                score = signal.get('probability', 'N/A')
                
                if not raw_symbol or has_opp != 'yes':
                    continue
                
                price, name, code, _, pct_chg = self.get_realtime_price_and_name(raw_symbol)
                status = "[yellow]等待[/yellow]"
                
                if price:
                    current_prices[code] = price
                    if action == 'buy':
                        # 涨停板规则: 涨幅超过 9.9% 且非创业板/科创板，通常很难买入
                        if pct_chg > 9.9 and not (code.startswith('300') or code.startswith('688')):
                             status = "[dim]跳过 (涨停无法买入)[/dim]"
                        elif pct_chg > 19.9: # 创业板/科创板涨停
                             status = "[dim]跳过 (涨停无法买入)[/dim]"
                        elif self.portfolio.buy(code, price, trigger_time, name=name):
                            status = "[green]✅ 已买入[/green]"
                        else:
                            status = "[dim]跳过 (已持仓或资金不足)[/dim]"
                    elif action == 'sell':
                        # 跌停板规则
                        if pct_chg < -9.9 and not (code.startswith('300') or code.startswith('688')):
                             status = "[dim]跳过 (跌停无法卖出)[/dim]"
                        elif pct_chg < -19.9:
                             status = "[dim]跳过 (跌停无法卖出)[/dim]"
                        elif self.portfolio.sell(code, price, trigger_time, reason=signal.get('reason', 'AI Signal')):
                            status = "[red]成交量 (已卖出)[/red]"
                        else:
                            status = "[dim]跳过 (未持仓或T+1限制)[/dim]"
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
                    price, _, _, _, _ = self.get_realtime_price_and_name(held_code)
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
        # 优化后的监控时间点：
        # 09:35 - 避开开盘集合竞价后的剧烈波动，等待价格稳定
        # 11:25 - 上午收盘前最后的交易机会
        # 13:05 - 午盘开盘后，给予5分钟数据稳定期
        # 14:50 - 尾盘黄金10分钟，捕捉日内趋势或进行调仓 (避开14:57的集合竞价)
        # 15:05 - 盘后总结 (只读，不交易)
        target_times = [
            "09:35", "10:00", "10:30", "11:00", "11:25", 
            "13:05", "13:30", "14:00", "14:30", "14:50", "15:05"
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

            # 移除 \r 打印，避免在高频日志输出时导致终端显示错乱
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
