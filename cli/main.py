"""
ContestTrade: 基于内部竞赛机制的Multi-Agent交易系统
"""
import asyncio
import sys
import json
import re
import os
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime
from collections import deque

import typer
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout
from rich.text import Text
from rich.align import Align
from rich import box

from .utils import get_trigger_time, validate_required_services
from .static.report_template import display_final_report_interactive
from .utils import get_trigger_time_for_market, get_market_selection

console = Console()

def get_text(cn_text: str, en_text: str) -> str:
    """根据市场类型返回对应语言的文本"""
    market_type = os.environ.get('CONTEST_TRADE_MARKET', 'CN-Stock')
    return en_text if market_type == 'US-Stock' else cn_text

app = typer.Typer(
    name="contesttrade",
    help="ContestTrade: 基于内部竞赛机制的Multi-Agent交易系统 (支持A股和美股)",
    add_completion=True,
)

def _get_agent_config():
    """从配置文件动态获取代理配置"""
    # Import config after environment variable is set
    from contest_trade.config.config import cfg, PROJECT_ROOT
    import sys
    sys.path.append(str(PROJECT_ROOT))
    
    agent_status = {}
    
    # 从配置文件获取数据代理
    data_agents_config = cfg.data_agents_config
    for agent_config in data_agents_config:
        agent_name = agent_config.get('agent_name', '')
        if agent_name:
            agent_status[agent_name] = "pending"
    
    # 从belief_list.json获取研究代理数量
    belief_list_path = PROJECT_ROOT / "config" / "belief_list.json"

    with open(belief_list_path, 'r', encoding='utf-8') as f:
        belief_list = json.load(f)
    # 根据belief数量创建研究代理
    for i in range(len(belief_list)):
        agent_status[f"agent_{i}"] = "pending"
    
    return agent_status
class ContestTradeDisplay:
    """ContestTrade显示管理器"""
    
    def __init__(self):
        self.market_type = os.environ.get('CONTEST_TRADE_MARKET', 'CN-Stock')
        self.messages = deque(maxlen=200)  # 增加消息队列容量
        self.agent_status = _get_agent_config()
        self.current_task = get_text("初始化系统...", "Initializing system...")
        self.progress_info = ""
        self.final_state = None
        self.analysis_completed = False
        self.step_counts = {"data": 0, "research": 0, "contest": 0, "finalize": 0}
        self._last_update_hash = None  # 用于检测内容是否真正发生变化
        self._last_console_size = None  # 用于检测控制台大小变化
        
        # 日志监控相关 - Import PROJECT_ROOT when needed
        from contest_trade.config.config import WORKSPACE_ROOT
        self.logs_dir = Path(WORKSPACE_ROOT) / "agents_workspace" / "logs"
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
    def create_log_file(self, trigger_time: str):
        """创建本次运行的日志文件"""
        timestamp = trigger_time.replace(":", "-").replace(" ", "_")
        self.log_file = self.logs_dir / f"run_{timestamp}.log"
        with open(self.log_file, "w", encoding="utf-8") as f:
            f.write(f"ContestTrade Run Log - {trigger_time}\n")
            f.write("=" * 50 + "\n")
        
    def check_agent_status_from_events_and_files(self, trigger_time: str):
        """基于事件和文件系统更新agent状态"""
        # Import WORKSPACE_ROOT when needed
        from contest_trade.config.config import WORKSPACE_ROOT
        
        # 格式化时间戳用于文件匹配
        timestamp_str = trigger_time.replace("-", "-").replace(":", "-").replace(" ", "_")
        
        # 检查factors目录
        factors_dir = Path(WORKSPACE_ROOT) / "agents_workspace" / "factors"
        if factors_dir.exists():
            for agent_name in self.agent_status:
                if not agent_name.startswith("agent_"):  # Data agents
                    agent_dir = factors_dir / agent_name
                    if agent_dir.exists():
                        # 查找对应时间戳的文件
                        pattern = f"{timestamp_str}*.json"
                        files = list(agent_dir.glob(pattern))
                        if files and self.agent_status[agent_name] != "completed":
                            self.update_agent_status(agent_name, "completed")
                            self.add_message(get_text("Data Analysis Agent", "Data Analysis Agent"), get_text(f"✅ {agent_name} 完成数据分析", f"✅ {agent_name} completed data analysis"))
        
        # 检查reports目录（Research Agent结果）
        reports_dir = Path(WORKSPACE_ROOT) / "agents_workspace" / "reports"
        if reports_dir.exists():
            for agent_name in self.agent_status:
                if agent_name.startswith("agent_"):  # Research agents
                    agent_dir = reports_dir / agent_name
                    if agent_dir.exists():
                        # 查找对应时间戳的文件
                        pattern = f"{timestamp_str}*.json"
                        files = list(agent_dir.glob(pattern))
                        if files and self.agent_status[agent_name] != "completed":
                            self.update_agent_status(agent_name, "completed")
                            self.add_message(get_text("Research Agent", "Research Agent"), get_text(f"✅ {agent_name} 完成研究分析", f"✅ {agent_name} completed research analysis"))
    
    def start_data_agents(self):
        """开始所有Data Analysis Agent"""
        for agent_name in self.agent_status:
            if not agent_name.startswith("agent_"):  # Data agents
                self.update_agent_status(agent_name, "running")
        self.add_message(get_text("系统", "System"), get_text("🚀 开始运行所有Data Analysis Agent", "🚀 Starting all Data Analysis Agents"))
    
    def start_research_agents(self):
        """开始所有Research Agent"""
        for agent_name in self.agent_status:
            if agent_name.startswith("agent_"):  # Research agents
                self.update_agent_status(agent_name, "running")
        self.add_message(get_text("系统", "System"), get_text("🚀 开始运行所有Research Agent", "🚀 Starting all Research Agents"))
        
    def add_message(self, message_type: str, content: str):
        """添加消息"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        new_message = f"[{timestamp}] {message_type}: {content}"
        self.messages.append(new_message)
        
    def should_update_display(self) -> bool:
        """检查是否需要更新显示（内容是否发生变化）"""
        current_hash = hash(str(self.messages) + self.current_task + self.progress_info + str(self.agent_status))
        if current_hash != self._last_update_hash:
            self._last_update_hash = current_hash
            return True
        return False
    
    def console_size_changed(self) -> bool:
        """检查控制台大小是否发生变化"""
        current_size = console.size
        if current_size != self._last_console_size:
            self._last_console_size = current_size
            return True
        return False
        
    def update_agent_status(self, agent_name: str, status: str):
        """更新Agent状态"""
        if agent_name not in self.agent_status:
            self.agent_status[agent_name] = "pending"
            
        self.agent_status[agent_name] = status
        
    def set_current_task(self, task: str):
        """设置当前任务"""
        self.current_task = task
        
    def set_progress_info(self, info: str):
        """设置进度信息"""
        self.progress_info = info
        
    def set_analysis_completed(self, completed: bool = True):
        """设置分析完成状态"""
        self.analysis_completed = completed
        
    def create_layout(self, trigger_time: str) -> Layout:
        """创建自适应布局"""
        layout = Layout()
        
        # 获取终端大小
        console_size = console.size
        
        # 根据终端高度调整header大小
        header_size = min(10, max(9, console_size.height // 6))
        
        layout.split_column(
            Layout(name="header", size=header_size),
            Layout(name="main_content")
        )
        
        # 根据终端宽度调整左右面板比例
        if console_size.width < 120:
            left_ratio, right_ratio = 2, 3  # 窄屏时调整比例
        else:
            left_ratio, right_ratio = 4, 7  # 宽屏时的比例
            
        layout["main_content"].split_row(
            Layout(name="left_panel", ratio=left_ratio),
            Layout(name="right_panel", ratio=right_ratio)
        )
        layout["left_panel"].split_column(
            Layout(name="status", ratio=3),
            Layout(name="progress", ratio=2)
        )
        layout["right_panel"].split_column(
            Layout(name="content", ratio=3),
            Layout(name="footer", ratio=2)
        )
        
        return layout
        
    def update_display(self, layout: Layout, trigger_time: str):
        """更新显示"""
        welcome_text = Path(__file__).parent / "static" / "welcome.txt"
        if welcome_text.exists():
            with open(welcome_text, "r", encoding="utf-8") as f:
                welcome = f.read()
        else:
            welcome = get_text("ContestTrade: 基于内部竞赛机制的Multi-Agent交易系统", "ContestTrade: Multi-Agent Trading System Based on Internal Contest Mechanism")
        
        header_panel = Panel(
            Align.center(welcome),
            title=get_text("🎯 ContestTrade - 基于内部竞赛机制的Multi-Agent交易系统", "🎯 ContestTrade - Multi-Agent Trading System Based on Internal Contest Mechanism"),
            border_style="blue",
            padding=(0, 1),
            expand=True  # 自适应宽度
        )
        layout["header"].update(header_panel)
        
        # 更新Agent状态面板
        status_text = Text()
        
        # Data Analysis Agent状态
        data_agents = {k: v for k, v in self.agent_status.items() if not k.startswith("agent_")}
        if data_agents:
            status_text.append(get_text("📊 Data Analysis Agent\n", "📊 Data Analysis Agent\n"), style="bold cyan")
            for agent_name, status in data_agents.items():
                status_icon = {
                    "pending": get_text("⏳等待中...", "⏳ Pending..."),
                    "running": get_text("🔄分析中...", "🔄 Analyzing..."), 
                    "completed": get_text("✅分析完成", "✅ Analysis Complete")
                }.get(status, "❓")
                
                agent_display = agent_name[:20].ljust(20)
                status_text.append(f"{agent_display} {status_icon}\n")
        
        # Research Agent状态
        research_agents = {k: v for k, v in self.agent_status.items() if k.startswith("agent_")}
        if research_agents:
            status_text.append(get_text("\n🔍 Research Agent\n", "\n🔍 Research Agent\n"), style="bold green")
            for agent_name, status in research_agents.items():
                status_icon = {
                    "pending": get_text("⏳等待中...", "⏳ Pending..."),
                    "running": get_text("🔄分析中...", "🔄 Analyzing..."), 
                    "completed": get_text("✅分析完成", "✅ Analysis Complete")
                }.get(status, "❓")
                
                agent_display = agent_name[:20].ljust(20)
                status_text.append(f"{agent_display} {status_icon}\n")
        
        status_panel = Panel(
            status_text,
            title=get_text("🤖 Agent状态", "🤖 Agent Status"),
            border_style="yellow",
            padding=(0, 1),
            expand=True  # 自适应宽度
        )
        layout["status"].update(status_panel)
        
        # 更新进度面板
        progress_text = Text()
        progress_text.append(get_text(f"📅 触发时间: {trigger_time}\n", f"📅 Trigger Time: {trigger_time}\n"), style="cyan")
        progress_text.append(get_text(f"🎯 当前任务: {self.current_task}\n", f"🎯 Current Task: {self.current_task}\n"), style="yellow")
        if self.progress_info:
            progress_text.append(get_text(f"📈 进度: {self.progress_info}\n", f"📈 Progress: {self.progress_info}\n"), style="green")
        
        # 显示步骤计数
        progress_text.append(get_text(f"\n📊 步骤统计:\n", f"\n📊 Step Statistics:\n"), style="bold blue")
        progress_text.append(get_text(f"  Data Analysis Agent事件: {self.step_counts['data']}\n", f"  Data Analysis Agent Events: {self.step_counts['data']}\n"))
        progress_text.append(get_text(f"  Research Agent事件: {self.step_counts['research']}\n", f"  Research Agent Events: {self.step_counts['research']}\n"))
        # progress_text.append(f"  竞赛事件: {self.step_counts['contest']}\n")
        # progress_text.append(f"  完成事件: {self.step_counts['finalize']}\n")
        
        progress_panel = Panel(
            progress_text,
            title=get_text("📊 进度信息", "📊 Progress Information"),
            border_style="blue",
            padding=(0, 1),
            expand=True  # 自适应宽度
        )
        layout["progress"].update(progress_panel)
        
        # 更新主内容区域
        content_text = Text()
        content_text.append(get_text("🔄 实时事件日志\n", "🔄 Real-time Event Log\n"), style="bold blue")
        
        if self.messages:
            for msg in list(self.messages)[-8:]:
                content_text.append(f"{msg}\n")
        else:
            content_text.append(get_text("  ⏳ 等待事件...\n", "  ⏳ Waiting for events...\n"))
        
        content_panel = Panel(
            content_text,
            title=get_text("📄 事件流", "📄 Event Stream"),
            border_style="blue",
            padding=(1, 2),
            expand=True  # 自适应宽度
        )
        layout["content"].update(content_panel)
        
        # 更新底部
        if self.analysis_completed and self.final_state:
            footer_text = self._create_result_summary()
            footer_title = get_text("🏆 结果摘要", "🏆 Result Summary")
        else:
            footer_text = Text()
            footer_text.append(get_text("🔄 分析进行中...预计等待10分钟...", "🔄 Analysis in progress... Expected wait time: 10 minutes..."), style="bold yellow")
            if self.analysis_completed:
                footer_text.append(get_text("\n✅ 分析完成！请按回车键(↵)退出运行界面...", "\n✅ Analysis completed! Press Enter (↵) to exit the interface..."), style="bold green")
            footer_title = get_text("📊 状态信息", "📊 Status Information")
        
        footer_panel = Panel(
            footer_text,
            title=footer_title,
            border_style="green",
            padding=(0, 1),
            expand=True  # 自适应宽度
        )
        layout["footer"].update(footer_panel)
    
    def _create_result_summary(self) -> Text:
        """创建结果摘要"""
        summary_text = Text()
        
        if self.final_state:
            # 从step_results中获取统计信息
            step_results = self.final_state.get('step_results', {})
            data_team_results = step_results.get('data_team', {})
            research_team_results = step_results.get('research_team', {})
            
            data_factors_count = data_team_results.get('factors_count', 0)
            research_signals_count = research_team_results.get('signals_count', 0)
            
            summary_text.append(get_text(f"📊 数据源: {data_factors_count} | ", f"📊 Data Sources: {data_factors_count} | "), style="green")
            summary_text.append(get_text(f"🔍 研究信号: {research_signals_count} | ", f"🔍 Research Signals: {research_signals_count} | "), style="blue")
            
            # 获取所有信号并筛选有机会的信号
            best_signals = step_results.get('contest', {}).get('best_signals', [])
            
            # 筛选 has_opportunity 为 yes 的信号
            valid_signals = []
            for signal in best_signals:
                has_opportunity = signal.get('has_opportunity', 'no')
                if has_opportunity == 'yes':
                    valid_signals.append(signal)
            
            if valid_signals:
                summary_text.append(get_text(f"🎯 有效信号: {len(valid_signals)}", f"🎯 Valid Signals: {len(valid_signals)}"), style="bold red")
                
                for i, signal in enumerate(valid_signals):
                    symbol_name = signal.get('symbol_name', 'N/A')
                    action = signal.get('action', 'N/A')
                    agent_id = signal.get('agent_id', 'N/A')
                    
                    summary_text.append(get_text(f"\n  {i+1}. Research Agent{agent_id}：", f"\n  {i+1}. Research Agent{agent_id}: "), style="yellow")
                    summary_text.append(f"{symbol_name}({action}) ", style="cyan")
                    
            else:
                summary_text.append(get_text("🎯 有效信号: 0", "🎯 Valid Signals: 0"), style="bold red")     
                summary_text.append(get_text(" 无有效信号", " No valid signals"))

            summary_text.append(get_text("\n💡分析完成，按回车退出运行界面...", "\n💡Analysis completed, press Enter to exit the interface..."))
        else:
            summary_text.append(get_text("❌ 分析失败", "❌ Analysis Failed"), style="red")
        
        return summary_text


def run_contest_analysis_interactive(trigger_time: str, market: str):
    """在交互界面中运行竞赛分析"""
    try:
        # 创建显示管理器
        display = ContestTradeDisplay()
        
        # 在显示中添加市场信息
        display.set_current_task(get_text(f"初始化ContestTrade系统... (市场: {market})", f"Initializing ContestTrade system... (Market: {market})"))
        
        # 创建初始布局
        layout = display.create_layout(trigger_time)
        
        # 使用Live界面运行 - 提高刷新频率以更好响应窗口大小变化
        with Live(layout, refresh_per_second=4, screen=True, auto_refresh=True, console=console) as live:
            # 初始显示
            display.update_display(layout, trigger_time)
            
            # 添加初始消息
            display.add_message(get_text("系统", "System"), get_text(f"开始分析 - 市场: {market}, 时间: {trigger_time}", f"Starting analysis - Market: {market}, Time: {trigger_time}"))
            display.update_display(layout, trigger_time)
            
            # 检查模块导入 - Import when needed
            try:
                from contest_trade.main import SimpleTradeCompany
                if SimpleTradeCompany is None:
                    raise ImportError("SimpleTradeCompany模块导入失败")
                    
                display.add_message(get_text("系统", "System"), get_text("✅ 成功导入SimpleTradeCompany模块", "✅ Successfully imported SimpleTradeCompany module"))
                display.update_display(layout, trigger_time)
                
                # 创建公司实例
                company = SimpleTradeCompany()
                display.add_message(get_text("系统", "System"), get_text("✅ 成功创建SimpleTradeCompany实例", "✅ Successfully created SimpleTradeCompany instance"))
                display.update_display(layout, trigger_time)
                
            except Exception as e:
                display.add_message("错误", f"❌ 模块导入失败: {str(e)}")
                display.update_display(layout, trigger_time)
                return None, display
            
            # 运行工作流并捕获输出
            final_state = asyncio.run(run_with_events_capture(company, trigger_time, display, layout, live))
            
            # 运行结束后
            if final_state:
                display.add_message(get_text("完成", "Completed"), get_text("✅ 分析完成！", "✅ Analysis completed!"))
                display.set_current_task(get_text("分析完成，生成报告...", "Analysis completed, generating report..."))
                display.set_analysis_completed(True)
                display.final_state = final_state
                display.update_display(layout, trigger_time)
                
                # 自动生成MD报告
                try:
                    from contest_trade.config.config import WORKSPACE_ROOT
                    results_dir = Path(WORKSPACE_ROOT) / "agents_workspace" / "results"
                    from .static.report_template import generate_final_report, generate_data_report
                    
                    # 生成研究报告
                    markdown_content, report_path = generate_final_report(final_state, results_dir)
                    display.add_message(get_text("报告", "Report"), get_text(f"✅ 研究报告已生成: {report_path.name}", f"✅ Research report generated: {report_path.name}"))
                    
                    # 生成数据报告
                    factors_data = load_factors_data(trigger_time)
                    if factors_data and factors_data.get('agents'):
                        data_markdown_content, data_report_path = generate_data_report(factors_data, results_dir)
                        display.add_message(get_text("报告", "Report"), get_text(f"✅ 数据报告已生成: {data_report_path.name}", f"✅ Data report generated: {data_report_path.name}"))
                    else:
                        display.add_message(get_text("报告", "Report"), get_text(f"⚠️ 未找到数据源，跳过数据报告生成", f"⚠️ No data sources found, skipping data report generation"))
                    
                    display.update_display(layout, trigger_time)
                except Exception as e:
                    display.add_message("报告", f"⚠️ MD报告生成失败: {str(e)}")
                    display.update_display(layout, trigger_time)
                
                # 等待用户手动退出
                console.print(get_text("\n[green]✅ 分析完成！[/green]", "\n[green]✅ Analysis completed![/green]"))
                console.print(get_text("[dim]按任意键退出运行界面...[/dim]", "[dim]Press any key to exit the interface...[/dim]"))
                input()
                
            else:
                display.add_message("错误", "❌ 分析失败")
                display.set_current_task("分析失败")
                display.update_display(layout, trigger_time)
                console.print("\n[red]❌ 分析失败！[/red]")
                console.print("[dim]按任意键退出运行界面...[/dim]")
                input()
                return None, display
                
    except Exception as e:
        console.print(f"[red]运行失败: {e}[/red]")
        return None, None
    
    # Live界面结束后，处理用户输入
    if final_state:
        return ask_user_for_next_action(final_state)
    
    return final_state, display


async def run_with_events_capture(company, trigger_time: str, display: ContestTradeDisplay, layout, live):
    """运行公司工作流并捕获事件流"""
    try:
        display.add_message(get_text("开始", "Start"), get_text("🚀 开始运行工作流...", "🚀 Starting workflow..."))
        display.set_current_task(get_text("🔄 启动工作流...", "🔄 Starting workflow..."))
        display.create_log_file(trigger_time)
        display.update_display(layout, trigger_time)
        
        # 启动定期检查文件状态的任务
        async def periodic_status_check():
            while not display.analysis_completed:
                display.check_agent_status_from_events_and_files(trigger_time)
                
                # 检查控制台大小是否变化，如果变化则重新创建布局
                if display.console_size_changed():
                    new_layout = display.create_layout(trigger_time)
                    # 将新布局的内容复制到当前布局中
                    layout.update(new_layout)
                    display.update_display(layout, trigger_time)
                else:
                    display.update_display(layout, trigger_time)
                    
                await asyncio.sleep(1)  # 每1秒检查一次，提高响应性
        
        # 启动状态检查任务
        status_check_task = asyncio.create_task(periodic_status_check())
        
        # 运行公司工作流并处理事件
        final_state = None
        async for event in company.run_company_with_events(trigger_time):
            event_name = event.get("name", "")
            event_type = event.get("event", "")
            event_data = event.get("data", {})
            
            # 记录重要事件到日志
            if event_type in ["on_chain_start", "on_chain_end"]:
                log_msg = f"[{datetime.now().strftime('%H:%M:%S')}] {event_type}: {event_name}\n"
                with open(display.log_file, "a", encoding="utf-8") as f:
                    f.write(log_msg)
                # # 同时显示到界面事件流
                # display.add_message("事件", f"{event_type}: {event_name}")
            
            # 记录自定义事件到日志和界面
            if event_type == "on_custom":
                custom_event_name = event_name
                custom_data = event_data
                log_msg = f"[{datetime.now().strftime('%H:%M:%S')}] CUSTOM: {custom_event_name} - {custom_data}\n"
                with open(display.log_file, "a", encoding="utf-8") as f:
                    f.write(log_msg)
                # 显示到界面
                display.add_message("自定义事件", f"{custom_event_name}")
            
            # 处理stdout输出（记录到日志和界面）
            if event_type == "on_stdout":
                stdout_content = event_data.get("chunk", "")
                if stdout_content.strip():
                    log_msg = f"[{datetime.now().strftime('%H:%M:%S')}] STDOUT: {stdout_content.strip()}\n"
                    with open(display.log_file, "a", encoding="utf-8") as f:
                        f.write(log_msg)
                    # 显示所有stdout到界面
                    display.add_message("输出", stdout_content.strip())
            
            # 处理关键阶段事件
            if event_type == "on_chain_start":
                stage_config = {
                    "run_data_agents": {
                        "action": display.start_data_agents,
                        "task": get_text("🔄 Data Analysis Agent 数据收集阶段", "🔄 Data Analysis Agent Data Collection Phase"),
                        "progress": get_text("数据收集阶段 1/4", "Data Collection Phase 1/4")
                    },
                    "run_research_agents": {
                        "action": display.start_research_agents,
                        "task": get_text("🔄 Research Agent 研究分析阶段", "🔄 Research Agent Analysis Phase"), 
                        "progress": get_text("研究分析阶段 2/4", "Research Analysis Phase 2/4")
                    },
                    "run_contest": {
                        "action": lambda: None,
                        "task": get_text("🔄 竞赛评选阶段", "🔄 Contest Evaluation Phase"),
                        "progress": get_text("竞赛评选阶段 3/4", "Contest Evaluation Phase 3/4")
                    },
                    "finalize": {
                        "action": lambda: None,
                        "task": get_text("🔄 结果生成阶段", "🔄 Result Generation Phase"),
                        "progress": get_text("结果生成阶段 4/4", "Result Generation Phase 4/4")
                    }
                }
                
                if event_name in stage_config:
                    config = stage_config[event_name]
                    config["action"]()
                    display.set_current_task(config["task"])
                    display.set_progress_info(config["progress"])
            
            # 处理完成事件
            elif event_type == "on_chain_end":
                completion_config = {
                    "run_data_agents": {
                        "task": get_text("✅ Data Analysis Agent 完成", "✅ Data Analysis Agent Completed"),
                        "message": get_text("✅ 所有Data Analysis Agent完成", "✅ All Data Analysis Agents Completed")
                    },
                    "run_research_agents": {
                        "task": get_text("✅ Research Agent 完成", "✅ Research Agent Completed"), 
                        "message": get_text("✅ 所有Research Agent完成", "✅ All Research Agents Completed")
                    },
                    "run_contest": {
                        "task": get_text("✅ 竞赛评选完成", "✅ Contest Evaluation Completed"),
                        "message": None
                    },
                    "finalize": {
                        "task": get_text("✅ 结果生成完成", "✅ Result Generation Completed"),
                        "message": None,
                        "special": True
                    }
                }
                
                if event_name in completion_config:
                    config = completion_config[event_name]
                    display.set_current_task(config["task"])
                    if config.get("message"):
                        display.add_message(get_text("系统", "System"), config["message"])
                    
                    if config.get("special"):  # finalize阶段的特殊处理
                        final_state = event_data.get("output", {})
                        if 'trigger_time' not in final_state:
                            final_state['trigger_time'] = trigger_time
                        display.set_analysis_completed(True)
            
            # 处理具体的节点事件（用于步骤统计）
            if event_type == "on_chain_start":
                step_mapping = {
                    "data": ["init_factor", "recompute_factor", "submit_result", "preprocess", "batch_process", "final_summary"],
                    "research": ["init_signal", "recompute_signal", "init_data", "plan", "tool_selection", "call_tool", "write_result"],
                    "contest": ["run_contest", "run_judger_critic"],
                    "finalize": ["finalize"]
                }
                
                for step_type, keywords in step_mapping.items():
                    if any(keyword in event_name.lower() for keyword in keywords):
                        display.step_counts[step_type] += 1
                        break
            
            # 更新显示 - 由于启用了自动刷新，不需要手动refresh
            display.update_display(layout, trigger_time)
        
        # 停止状态检查任务并设置最终状态
        if 'status_check_task' in locals():
            status_check_task.cancel()
            try:
                await status_check_task
            except asyncio.CancelledError:
                pass
        
        # 设置所有Agent为完成状态
        for agent_name in display.agent_status:
            display.update_agent_status(agent_name, "completed")
        
        # 确保final_state包含trigger_time
        if final_state is not None and 'trigger_time' not in final_state:
            final_state['trigger_time'] = trigger_time
        
        return final_state
        
    except Exception as e:
        # 停止状态检查任务
        if 'status_check_task' in locals():
            status_check_task.cancel()
            try:
                await status_check_task
            except asyncio.CancelledError:
                pass
        
        display.add_message("错误", f"❌ 运行失败: {str(e)}")
        console.print(f"[red]详细错误: {e}[/red]")
        return None


def ask_user_for_next_action(final_state):
    """询问用户下一步操作"""
    console.print(get_text("\n[green]✅ 分析完成！[/green]", "\n[green]✅ Analysis completed![/green]"))
    console.print(get_text("[dim]输入 'rr' 查看研究报告 | 'dr' 查看数据报告 | 'n' 运行新分析 | 'q' 退出[/dim]", "[dim]Enter 'rr' to view research report | 'dr' to view data report | 'n' for new analysis | 'q' to quit[/dim]"))
    
    while True:
        try:
            user_input = input(get_text("请选择操作 (rr/dr/n/q): ", "Choose action (rr/dr/n/q): ")).strip().lower()
            if user_input == 'rr':
                display_detailed_report(final_state)
                console.print(get_text("[dim]输入 'rr' 查看研究报告 | 'dr' 查看数据报告 | 'n' 运行新分析 | 'q' 退出[/dim]", "[dim]Enter 'rr' to view research report | 'dr' to view data report | 'n' for new analysis | 'q' to quit[/dim]"))
            elif user_input == 'dr':
                display_data_report(final_state)
                console.print(get_text("[dim]输入 'rr' 查看研究报告 | 'dr' 查看数据报告 | 'n' 运行新分析 | 'q' 退出[/dim]", "[dim]Enter 'rr' to view research report | 'dr' to view data report | 'n' for new analysis | 'q' to quit[/dim]"))
            elif user_input == 'n':
                return final_state, "new_analysis"
            elif user_input == 'q':
                return final_state, "quit"
            else:
                console.print(get_text("[yellow]无效输入，请输入 'rr', 'dr', 'n' 或 'q'[/yellow]", "[yellow]Invalid input, please enter 'rr', 'dr', 'n' or 'q'[/yellow]"))
        except KeyboardInterrupt:
            console.print(get_text("\n[yellow]用户中断，退出...[/yellow]", "\n[yellow]User interrupted, exiting...[/yellow]"))
            return final_state, "quit"

def display_data_report(final_state: Dict):
    """显示数据分析报告"""
    if not final_state:
        console.print("[red]无结果可显示[/red]")
        return
    
    try:
        from .static.report_template import DataReportGenerator
        
        # 从final_state获取trigger_time，然后读取factors数据
        trigger_time = final_state.get('trigger_time', 'N/A')
        
        # 读取factors文件夹中的数据
        factors_data = load_factors_data(trigger_time)
        
        if not factors_data or not factors_data.get('agents'):
            console.print("[yellow]未找到数据分析结果[/yellow]")
            return
        
        generator = DataReportGenerator(factors_data)
        
        # 生成报告内容
        total_agents = len(factors_data.get('agents', {}))
        
        markdown_content = f"""# ContestTrade {get_text('数据分析报告', 'Data Analysis Report')}

## 📊 {get_text('数据摘要', 'Data Summary')}

**{get_text('分析时间', 'Analysis Time')}**: {trigger_time}  
**{get_text('分析状态', 'Analysis Status')}**: ✅ {get_text('完成', 'Completed')}  
**{get_text('数据代理数量', 'Data Agent Count')}**: {total_agents}  

---

## 🔍 {get_text('数据源分析详情', 'Data Source Analysis Details')}

"""
        
        # 遍历每个代理的数据
        for agent_name, agent_data in factors_data.get('agents', {}).items():
            markdown_content += f"### 📈 {agent_name.replace('_', ' ').title()}\n\n"
            
            # 只获取context_string字段
            context_string = agent_data.get('context_string', '')
            
            if context_string:
                # 清洗掉 [Batch X] 标记
                cleaned_context = re.sub(r'\[Batch \d+\]', '', context_string).strip()
                markdown_content += f"{cleaned_context}\n\n"
            else:
                markdown_content += f"**{get_text('暂无分析内容', 'No analysis content available')}**\n\n"
            
            markdown_content += "---\n\n"
        
        generator.display_terminal_interactive_report(markdown_content)
        
    except Exception as e:
        console.print(f"[red]数据报告显示失败: {e}[/red]")
        console.print("[yellow]正在显示简化版数据报告...[/yellow]")
        
        # 显示简化版数据报告
        try:
            factors_data = load_factors_data(final_state.get('trigger_time', 'N/A'))
            if factors_data and factors_data.get('agents'):
                console.print(f"\n[bold]{get_text('数据分析摘要', 'Data Analysis Summary')}:[/bold]")
                console.print(f"{get_text('数据代理数量', 'Data Agent Count')}: {len(factors_data.get('agents', {}))}")
                
                for agent_name in factors_data.get('agents', {}):
                    console.print(f"- {agent_name}")
            else:
                console.print(f"[yellow]{get_text('未找到数据分析结果', 'No data analysis results found')}[/yellow]")
        except Exception as inner_e:
            console.print(f"[red]{get_text('简化版数据报告也显示失败', 'Simplified data report display also failed')}: {inner_e}[/red]")


def load_factors_data(trigger_time: str) -> Dict:
    """加载factors文件夹中的数据"""
    from contest_trade.config.config import WORKSPACE_ROOT
    
    factors_data = {
        'trigger_time': trigger_time,
        'agents': {}
    }
    
    # 格式化时间戳用于文件匹配
    if trigger_time and trigger_time != 'N/A':
        timestamp_str = trigger_time.replace("-", "-").replace(":", "-").replace(" ", "_")
    else:
        return factors_data
    
    # 读取factors目录
    factors_dir = Path(WORKSPACE_ROOT) / "agents_workspace" / "factors"
    if not factors_dir.exists():
        return factors_data
    
    try:
        for agent_dir in factors_dir.iterdir():
            if agent_dir.is_dir():
                agent_name = agent_dir.name
                
                # 查找对应时间戳的JSON文件
                pattern = f"{timestamp_str}*.json"
                files = list(agent_dir.glob(pattern))
                
                if files:
                    # 读取第一个匹配的文件
                    with open(files[0], 'r', encoding='utf-8') as f:
                        agent_data = json.load(f)
                        factors_data['agents'][agent_name] = agent_data
    except Exception as e:
        console.print(f"[yellow]加载factors数据时出错: {e}[/yellow]")
    
    return factors_data


def display_detailed_report(final_state: Dict):
    """显示详细的可滚动终端报告（使用Rich交互式显示）"""
    if not final_state:
        console.print("[red]无结果可显示[/red]")
        return
    
    try:
        from .static.report_template import FinalReportGenerator
        generator = FinalReportGenerator(final_state)
        step_results = final_state.get('step_results', {})
        data_team_results = step_results.get('data_team', {})
        research_team_results = step_results.get('research_team', {})
        contest_results = step_results.get('contest', {})
        
        trigger_time = final_state.get('trigger_time', 'N/A')
        data_factors_count = data_team_results.get('factors_count', 0)
        research_signals_count = research_team_results.get('signals_count', 0)
        best_signals = contest_results.get('best_signals', [])
        
        valid_signals = [s for s in best_signals if s.get('has_opportunity', 'no') == 'yes']
        invalid_signals = [s for s in best_signals if s.get('has_opportunity', 'no') != 'yes']
        
        signal_rate = f"{len(valid_signals)/len(best_signals)*100:.1f}% ({len(valid_signals)}/{len(best_signals)})" if len(best_signals) > 0 else "0% (0/0)"
        
        markdown_content = f"""# ContestTrade {get_text('详细分析报告', 'Detailed Analysis Report')}

## 📊 {get_text('执行摘要', 'Executive Summary')}

**{get_text('分析时间', 'Analysis Time')}**: {trigger_time}  
**{get_text('数据源数量', 'Data Sources Count')}**: {data_factors_count}  
**{get_text('研究信号数量', 'Research Signals Count')}**: {research_signals_count}  
**{get_text('有效投资信号', 'Valid Investment Signals')}**: {len(valid_signals)}  
**{get_text('信号有效率', 'Signal Effectiveness Rate')}**: {signal_rate}

---

## 🎯 {get_text('投资信号详情', 'Investment Signals Details')}
"""
        
        if valid_signals:
            markdown_content += f"\n### ✅ {get_text('推荐投资信号', 'Recommended Investment Signals')} ({len(valid_signals)}{get_text('个', '')})\n\n"
            
            for i, signal in enumerate(valid_signals, 1):
                symbol_name = signal.get('symbol_name', 'N/A')
                symbol_code = signal.get('symbol_code', 'N/A')
                action = signal.get('action', 'N/A')
                probability = signal.get('probability', 'N/A')
                agent_id = signal.get('agent_id', 'N/A')
                
                markdown_content += f"#### {i}. {symbol_name} ({symbol_code})\n\n"
                markdown_content += f"- **{get_text('投资动作', 'Investment Action')}**: {action}\n"
                markdown_content += f"- **{get_text('分析来源', 'Analysis Source')}**: Research Agent {agent_id}\n\n"
                
                evidence_list = signal.get('evidence_list', [])
                if evidence_list:
                    markdown_content += f"**📋 {get_text('支撑证据', 'Supporting Evidence')} ({len(evidence_list)}{get_text('项', '')}):**\n\n"
                    for j, evidence in enumerate(evidence_list, 1):
                        desc = evidence.get('description', 'N/A')
                        source = evidence.get('from_source', 'N/A')
                        time = evidence.get('time', 'N/A')
                        markdown_content += f"{j}. **{desc}**\n"
                        markdown_content += f"   - {get_text('时间', 'Time')}: {time}\n"
                        markdown_content += f"   - {get_text('来源', 'Source')}: {source}\n\n"
                
                # 风险提示
                limitations = signal.get('limitations', [])
                if limitations:
                    markdown_content += f"**⚠️ {get_text('潜在风险', 'Potential Risks')}:**\n\n"
                    for limitation in limitations:
                        markdown_content += f"- {limitation}\n"
                    markdown_content += "\n"
                
                markdown_content += "---\n"
        else:
            markdown_content += f"\n### ❌ {get_text('暂无推荐投资信号', 'No Recommended Investment Signals')}\n\n"
            markdown_content += get_text("本次分析未发现具有明确投资机会的信号。\n\n", "No signals with clear investment opportunities were found in this analysis.\n\n")
        
        # 无效信号统计
        if invalid_signals:
            markdown_content += f"### ⚠️ {get_text('排除信号', 'Excluded Signals')} ({len(invalid_signals)}{get_text('个', '')})\n"
            markdown_content += get_text("以下信号经分析后认为不具备投资机会：\n\n", "The following signals were analyzed and deemed not to have investment opportunities:\n\n")
            
            for i, signal in enumerate(invalid_signals, 1):
                agent_id = signal.get('agent_id', 'N/A')
                markdown_content += f"{i}. Research Agent {agent_id} - {get_text('无明确投资机会', 'No clear investment opportunity')}\n"
            
            markdown_content += "\n"
        generator.display_terminal_interactive_report(markdown_content)
        
    except Exception as e:
        console.print(get_text(f"[red]交互式报告显示失败: {e}[/red]", f"[red]Interactive report display failed: {e}[/red]"))
        console.print(get_text("[yellow]正在显示简化版报告...[/yellow]", "[yellow]Displaying simplified report...[/yellow]"))
        
        # 显示简化版报告
        step_results = final_state.get('step_results', {})
        best_signals = step_results.get('contest', {}).get('best_signals', [])
        valid_signals = [s for s in best_signals if s.get('has_opportunity', 'no') == 'yes']
        
        console.print(f"\n[bold]{get_text('分析摘要', 'Analysis Summary')}:[/bold]")
        console.print(get_text(f"总信号: {len(best_signals)}, 有效信号: {len(valid_signals)}", f"Total signals: {len(best_signals)}, Valid signals: {len(valid_signals)}"))
        
        for i, signal in enumerate(valid_signals, 1):
            console.print(f"{i}. {signal.get('symbol_name', 'N/A')} - {signal.get('action', 'N/A')}")

@app.command()
def run(
    market: Optional[str] = typer.Option(None, "--market", "-m", help="选择市场 (CN-Stock/US-Stock)"),
):
    """运行ContestTrade分析"""

    # 获取市场选择
    if not market:
        market = get_market_selection()
    
    # 验证市场选择
    if not market:
        console.print("[red]未提供市场选择[/red]")
        raise typer.Exit(1)
    
    if market not in ["CN-Stock", "US-Stock"]:
        console.print("[red]市场选择错误，请选择 CN-Stock 或 US-Stock[/red]")
        raise typer.Exit(1)
    
    # 设置环境变量 - 这样全局的 cfg 就会读取对应的配置
    os.environ['CONTEST_TRADE_MARKET'] = market
    
    # 根据市场获取对应的触发时间
    trigger_time = get_trigger_time_for_market(market)
    
    # 验证触发时间
    if not trigger_time:
        console.print("[red]无法获取对应市场的触发时间[/red]")
        raise typer.Exit(1)
    
    console.print(f"[green]已选择市场: {market}[/green]")
    console.print(f"[green]触发时间: {trigger_time}[/green]")
    
    # 验证必需的服务连接
    if not validate_required_services():
        console.print("[red]系统验证失败，无法启动分析[/red]")
        raise typer.Exit(1)
    
    # 主循环
    while True:
        try:
            result = run_contest_analysis_interactive(trigger_time, market)
        except Exception as e:
            console.print(f"[red]运行分析时发生错误: {e}[/red]")
            break
        
        if result is None or (isinstance(result, tuple) and result[0] is None):
            console.print("[red]❌ 分析失败[/red]")
            break
            
        if isinstance(result, tuple):
            final_state, action = result
            if action == "new_analysis":
                # 重新选择市场
                market = get_market_selection()
                if not market:
                    break
                
                # 设置环境变量
                os.environ['CONTEST_TRADE_MARKET'] = market
                
                # 获取新的触发时间
                trigger_time = get_trigger_time_for_market(market)
                if not trigger_time:
                    console.print("[red]无法获取对应市场的触发时间[/red]")
                    break
                
                # 验证服务连接
                if not validate_required_services():
                    console.print("[red]系统验证失败，无法启动分析[/red]")
                    break
                
                console.print(f"[green]已切换到市场: {market}[/green]")
                console.print(f"[green]新触发时间: {trigger_time}[/green]")
                continue
            elif action == "quit":
                break
        else:
            final_state = result
            display = None

        break
    
    console.print(get_text(f"[green]感谢使用ContestTrade![/green]", f"[green]Thank you for using ContestTrade![/green]"))

@app.command()
def config():
    """显示当前配置"""
    try:
        # Import config when needed
        from contest_trade.config.config import cfg
        
        if cfg is None:
            console.print("[red]配置模块导入失败[/red]")
            raise typer.Exit(1)
            
        console.print("[bold blue]ContestTrade 配置信息[/bold blue]")
        console.print("="*50)
        
        console.print(f"\n[bold]LLM配置:[/bold]")
        console.print(f"  模型: {cfg.llm.get('model_name', 'N/A')}")
        console.print(f"  基础URL: {cfg.llm.get('base_url', 'N/A')}")
        
        # Data Analysis Agent配置
        console.print(f"\n[bold]Data Analysis Agent配置:[/bold]")
        for i, agent_config in enumerate(cfg.data_agents_config, 1):
            console.print(f"  {i}. {agent_config.get('agent_name', 'N/A')}")
            console.print(f"     数据源: {', '.join(agent_config.get('data_source_list', []))}")
        
        # Research Agent配置
        console.print(f"\n[bold]Research Agent配置:[/bold]")
        console.print(f"  最大反应步骤: {cfg.research_agent_config.get('max_react_step', 'N/A')}")
        console.print(f"  输出语言: {cfg.research_agent_config.get('output_language', 'N/A')}")
        console.print(f"  工具数量: {len(cfg.research_agent_config.get('tools', []))}")
        
    except Exception as e:
        console.print(f"[red]配置加载失败: {e}[/red]")
        raise typer.Exit(1)

@app.command()
def version():
    """显示版本信息"""
    console.print("[bold blue]ContestTrade[/bold blue]")
    console.print("基于内部竞赛机制的Multi-Agent交易系统")
    console.print("Multi-Agent Trading System Based on Internal Contest Mechanism")
    console.print(f"版本: 1.1")

if __name__ == "__main__":
    app()