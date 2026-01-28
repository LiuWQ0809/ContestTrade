"""
Simplified Trade Company - 合并所有代码，包装成LangGraph工作流
"""
import re
import json
import asyncio
from loguru import logger
from datetime import datetime
from typing import List, Dict, TypedDict
from langgraph.graph import END, StateGraph
from langchain_core.runnables import RunnableConfig
from langchain_core.callbacks import dispatch_custom_event
from config.config import cfg, PROJECT_ROOT, WORKSPACE_ROOT
from agents.data_analysis_agent import DataAnalysisAgent, DataAnalysisAgentConfig, DataAnalysisAgentInput
from agents.research_agent import ResearchAgent, ResearchAgentConfig, ResearchAgentInput
from utils.market_manager import GLOBAL_MARKET_MANAGER

# 统一的状态定义
class CompanyState(TypedDict):
    trigger_time: str
    data_factors: List[Dict]
    research_signals: List[Dict]
    all_events: List[Dict]
    step_results: Dict
    portfolio_info: Dict  # 新增：账户资金和持仓信息

class SimpleTradeCompany:
    def __init__(self):
        # 设置工作目录
        self.workspace_dir = str(WORKSPACE_ROOT / "agents_workspace")
        
        # 初始化Data Agents
        self.data_agents = {}
        for agent_config_idx, agent_config in enumerate(cfg.data_agents_config):
            custom_config = DataAnalysisAgentConfig(
                source_list=agent_config["data_source_list"],
                agent_name=agent_config["agent_name"],
                final_target_tokens=agent_config.get("final_target_tokens", 4000),
                bias_goal=agent_config.get("bias_goal", ""),
            )
            self.data_agents[agent_config_idx] = DataAnalysisAgent(custom_config)
        
        # 初始化Research Agents
        self.research_agents = {}

        # 从belief_list.json读取belief配置
        belief_list_path = PROJECT_ROOT / cfg.research_agent_config["belief_list_path"]
        with open(belief_list_path, 'r', encoding='utf-8') as f:
            belief_list = json.load(f)

        for agent_config_idx, belief in enumerate(belief_list):
            custom_config = ResearchAgentConfig(
                agent_name=f"agent_{agent_config_idx}",
                belief=belief,
            )
            self.research_agents[agent_config_idx] = ResearchAgent(custom_config)

    # LangGraph节点函数
    async def run_data_agents_step(self, state: CompanyState, config: RunnableConfig) -> CompanyState:
        """运行Data Agents步骤"""
        trigger_time = state["trigger_time"]
        
        logger.info("🚀 开始并发运行Data Agents...")
        
        # 创建并发任务
        agent_tasks = []
        for agent_id, agent in self.data_agents.items():
            task = self._run_single_data_agent(agent_id, agent, trigger_time, config)
            agent_tasks.append(task)
        
        # 并发执行
        results = await asyncio.gather(*agent_tasks)
        
        # 收集结果
        all_factors = []
        all_events = []
        for result in results:
            if result:
                all_factors.append(result["factor"])
                all_events.extend(result["events"])
        
        logger.info(f"✅ Data Agents完成，有效结果: {len(all_factors)}")
        
        # 更新状态
        all_events_state = state["all_events"].copy()
        all_events_state.extend(all_events)
        
        step_results = state["step_results"].copy()
        step_results["data_team"] = {"factors_count": len(all_factors), "events_count": len(all_events)}
        
        return {
            "data_factors": all_factors,
            "all_events": all_events_state,
            "step_results": step_results
        }

    async def run_research_agents_step(self, state: CompanyState, config: RunnableConfig) -> CompanyState:
        """运行Research Agents步骤 - 并行化优化版"""
        trigger_time = state["trigger_time"]
        data_factors = state["data_factors"]
        portfolio_info = state.get("portfolio_info", {})
        
        if not data_factors:
            logger.warning("No data factors found, skipping research step.")
            return state

        # 优化：并行化处理逻辑
        # 我们将 data_factors 进行分块，每个 Agent 负责处理一小块资讯，
        # 从而实现“并行分析多个候选票”，显著降低 Qwen 思考模式的串行等待时间。
        num_factors = len(data_factors)
        num_chunks = 2 if num_factors > 1 else 1 # 按照 2 个分块进行初步拆分，可根据资源调整
        
        # 这种分块方式可以确保不同的资讯块被不同的 Agent 实例并发处理
        factor_chunks = []
        chunk_size = (num_factors + num_chunks - 1) // num_chunks
        for i in range(0, num_factors, chunk_size):
            factor_chunks.append(data_factors[i:i + chunk_size])

        logger.info(f"🚀 正在并发运行 Research Agents (分块并行化: {len(self.research_agents)} 策略 x {len(factor_chunks)} 数据块)...")
        
        # 创建并发任务
        agent_tasks = []
        for agent_id, agent in self.research_agents.items():
            for chunk_id, chunk_data in enumerate(factor_chunks):
                # 唯一的子任务 ID
                sub_task_id = f"{agent_id}_{chunk_id}"
                task = self._run_single_research_agent(sub_task_id, agent, trigger_time, chunk_data, config, portfolio_info)
                agent_tasks.append(task)
        
        # 并发执行所有子任务
        results = await asyncio.gather(*agent_tasks)
        
        # 收集结果
        all_signals = []
        all_events = []
        for result in results:
            if result and result["signals"]:
                all_signals.extend(result["signals"])
                all_events.extend(result["events"])
        
        # 对信号进行去重（可能多块数据提到了同一个好机会）
        unique_signals = []
        seen_symbols = set()
        for sig in sorted(all_signals, key=lambda x: x.get('probability', 0), reverse=True):
            sym = sig.get('symbol_code')
            if sym not in seen_symbols:
                unique_signals.append(sig)
                seen_symbols.add(sym)
        
        logger.info(f"✅ Research Agents并行完成，原始信号: {len(all_signals)}, 去重后信号: {len(unique_signals)}")
        
        # 更新状态
        all_events_state = state["all_events"].copy()
        all_events_state.extend(all_events)
        
        step_results = state["step_results"].copy()
        step_results["research_team"] = {"signals_count": len(unique_signals), "events_count": len(all_events)}
        
        return {
            "research_signals": unique_signals,
            "all_events": all_events_state,
            "step_results": step_results
        }

    async def finalize_step(self, state: CompanyState, config: RunnableConfig) -> CompanyState:
        """最终结果步骤"""
        trigger_time = state["trigger_time"]
        data_factors = state["data_factors"]
        research_signals = state["research_signals"]
        all_events = state["all_events"]
        step_results = state["step_results"]
        
        logger.info("🚀 开始最终结果步骤...")
        # 优先使用research产生的信号作为最终最佳信号
        best_signals = research_signals if research_signals else []

        # 生成最终结果（保留但不额外输出）
        final_result = {
            "trigger_time": trigger_time,
            "data_factors_count": len(data_factors),
            "research_signals_count": len(research_signals),
            "total_events_count": len(all_events),
            "best_signals": best_signals,
            "step_results": step_results
        }

        logger.info("✅ 最终结果步骤完成")

        step_results = state["step_results"]
        step_results["contest"] = {
            "best_signals": best_signals
        }
        return {
            "step_results": step_results
        }

    # 辅助函数
    async def _run_single_data_agent(self, agent_id: int, agent, trigger_time: str, config: RunnableConfig):
        """运行单个data agent"""
        logger.info(f"🔍 开始运行Data Agent {agent_id} ({agent.config.agent_name})...")
        
        agent_input = DataAnalysisAgentInput(trigger_time=trigger_time)
        agent_events = []
        agent_output = None
        
        # 运行agent并收集事件
        async for event in agent.run_with_monitoring_events(agent_input, config):
            # 转发事件
            if event["event"] == "on_custom":
                dispatch_custom_event(
                    name=f"data_agent_{agent_id}_{event['name']}", 
                    data={**event.get('data', {}), "agent_id": agent_id, "agent_name": agent.config.agent_name},
                    config=config
                )
            else:
                dispatch_custom_event(
                    name=f"data_agent_{agent_id}_{event['event']}", 
                    data={"agent_id": agent_id, "agent_name": agent.config.agent_name, "sub_node": event.get('name', 'unknown')},
                    config=config
                )
            
            agent_events.append({**event, "agent_id": agent_id, "agent_name": agent.config.agent_name})
            
            # 获取最终结果
            if event["event"] == "on_chain_end" and event.get("name") == "submit_result":
                agent_output = event.get("data", {}).get("output", {})
        
        # 处理结果
        factor = None
        if agent_output:
            factor = agent_output['result']
        return {"factor": factor, "events": agent_events} if factor else None

    async def _run_single_research_agent(self, agent_id: int, agent, trigger_time: str, factors: List, config: RunnableConfig, portfolio_info: Dict = None):
        """运行单个research agent"""
        logger.info(f"🔍 开始运行Research Agent {agent_id} ({agent.config.agent_name})...")
        
        # 构建背景信息，加入账户信息
        background_information = agent.build_background_information(trigger_time, agent.config.belief, factors)
        
        if portfolio_info:
            cash = portfolio_info.get("cash", 0)
            holdings = portfolio_info.get("holdings", {})
            total_fees = portfolio_info.get("total_fees", 0)
            holdings_str = ", ".join([f"{h.get('name', k)}({k})" for k, h in holdings.items()]) if holdings else "无"
            account_context = f"\n<account_info>\n当前可用现金: {cash:.2f}\n当前持仓股票: {holdings_str}\n累计已支付交易费: {total_fees:.2f}\n"
            account_context += "交易费率提示: A股交易存在成本 (佣金0.03%[最低5元], 卖出额外印花税0.05%, 过户费等)。单笔买入5000元约产生6.5元费用，卖出约产生9元费用。请避免买入预期涨幅无法覆盖交易成本的股票。\n"
            account_context += "任务指令: 请务必审视当前持仓。如果持仓逻辑依然成立且表现较好，建议 HOLD；如果逻辑失效或有明显更好的替代机会，建议 SELL。\n"
            if cash < 1000: # 假设 1000 为起投金额
                account_context += "提示: 当前可用资金极低。如果你发现必须买入的绝佳机会，你必须同时识别并建议卖出（SELL）当前持仓中表现较差的股票以释放资金，否则买入动作指令将会因资金不足而失败。\n"
            account_context += "</account_info>\n"
            background_information = account_context + background_information

        agent_input = ResearchAgentInput(
            trigger_time=trigger_time,
            background_information=background_information
        )
        
        agent_events = []
        agent_output = None

        # 运行agent并收集事件
        async for event in agent.run_with_monitoring_events(agent_input, config):
            # 转发事件
            if event["event"] == "on_custom":
                dispatch_custom_event(
                    name=f"research_agent_{agent_id}_{event['name']}", 
                    data={**event.get('data', {}), "agent_id": agent_id, "agent_name": agent.config.agent_name},
                    config=config
                )
            else:
                dispatch_custom_event(
                    name=f"research_agent_{agent_id}_{event['event']}", 
                    data={"agent_id": agent_id, "agent_name": agent.config.agent_name, "sub_node": event.get('name', 'unknown')},
                    config=config
                )
            
            agent_events.append({**event, "agent_id": agent_id, "agent_name": agent.config.agent_name})
            
            # 获取最终结果
            if event["event"] == "on_chain_end" and event.get("name") == "submit_result":
                agent_output = event.get("data", {}).get("output", {})
        
        # 处理结果 - 解析多个信号
        signals = []
        if agent_output:
            if "result" in agent_output and agent_output["result"]:
                result_obj = agent_output["result"]
                signals = self._parse_multiple_results(result_obj.final_result_thinking, result_obj.final_result)
            else:
                signals = self._parse_multiple_results(agent_output.get("final_result_thinking", ""), agent_output.get("final_result", ""))
            
            # 为每个信号添加agent信息，最多取5个信号
            valid_signals = []
            for i, signal in enumerate(signals[:5]):
                if signal:
                    signal["agent_id"] = agent_id
                    signal["agent_name"] = agent.config.agent_name
                    signal["signal_index"] = i + 1
                    valid_signals.append(signal)
            signals = valid_signals
        
        return {"signals": signals, "events": agent_events} if signals else None

    def _parse_multiple_results(self, thinking_result: str, output_result: str):
        """解析多个信号结果"""
        thinking = thinking_result.split("<Output>")[0].strip('\n').strip()
        output = output_result.split("<Output>")[-1].strip('\n').strip()
        
        signals = []
        try:
            # 查找所有signal块
            signal_blocks = re.findall(r'<signal>(.*?)</signal>', output, flags=re.DOTALL)
            
            for signal_block in signal_blocks:
                try:
                    signal = self._parse_single_signal_block(signal_block, thinking)
                    if signal:
                        signals.append(signal)
                except Exception as e:
                    logger.error(f"Error parsing individual signal: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"Error parsing multiple results: {e}")
        
        return signals

    def _parse_single_signal_block(self, signal_block: str, thinking: str):
        """解析单个信号块"""
        def extract_tag(tag, text, default=""):
            match = re.search(f"<{tag}>(.*?)</{tag}>", text, flags=re.DOTALL)
            return match.group(1).strip() if match else default

        try:
            has_opportunity = extract_tag("has_opportunity", signal_block, "no")
            action = extract_tag("action", signal_block, "hold")
            symbol_code = extract_tag("symbol_code", signal_block, "N/A")
            symbol_name = extract_tag("symbol_name", signal_block, "N/A")
            
            # 解析evidence_list
            evidence_list = []
            evidence_list_match = re.search(r"<evidence_list>(.*?)</evidence_list>", signal_block, flags=re.DOTALL)
            if evidence_list_match:
                evidence_list_str = evidence_list_match.group(1)
                for item in evidence_list_str.split("<evidence>"):
                    if '</evidence>' not in item:
                        continue
                    evidence_description = item.split("</evidence>")[0].strip()
                    evidence_time = extract_tag("time", item, "N/A")
                    evidence_from_source = extract_tag("from_source", item, "N/A")
                        
                    evidence_list.append({
                        "description": evidence_description,
                        "time": evidence_time,
                        "from_source": evidence_from_source,
                    })

            # 解析limitations
            limitations = []
            limitations_match = re.search(r"<limitations>(.*?)</limitations>", signal_block, flags=re.DOTALL)
            if limitations_match:
                limitations_str = limitations_match.group(1)
                limitations = re.findall(r"<limitation>(.*?)</limitation>", limitations_str, flags=re.DOTALL)
                limitations = [l.strip() for l in limitations]
            
            # 解析probability
            probability = extract_tag("probability", signal_block, "0%")
            
            # 解析hold_period
            hold_period = extract_tag("hold_period", signal_block, "1D")
            
            # 修正symbol信息
            if symbol_name != "N/A" or symbol_code != "N/A":
                symbol_name, symbol_code = GLOBAL_MARKET_MANAGER.fix_symbol_code("CN-Stock", symbol_name, symbol_code)
            
            return {
                "thinking": thinking,
                "has_opportunity": has_opportunity,
                "action": action,   
                "symbol_code": symbol_code,
                "symbol_name": symbol_name,
                "evidence_list": evidence_list,
                "limitations": limitations,
                "probability": probability,
                "hold_period": hold_period
            }
        except Exception as e:
            logger.error(f"Error parsing single signal block: {e}")
            return None

    # LangGraph工作流创建
    def create_company_workflow(self):
        """创建公司工作流"""
        workflow = StateGraph(CompanyState)

        # 添加节点
        workflow.add_node("run_data_agents", self.run_data_agents_step)
        workflow.add_node("run_research_agents", self.run_research_agents_step)
        workflow.add_node("finalize", self.finalize_step)

        # 设置入口点
        workflow.set_entry_point("run_data_agents")

        # 定义边（data -> research -> finalize）
        workflow.add_edge("run_data_agents", "run_research_agents")
        workflow.add_edge("run_research_agents", "finalize")
        workflow.add_edge("finalize", END)

        return workflow.compile()

    async def run_company(self, trigger_time: str, config: RunnableConfig = None, portfolio_info: Dict = None):
        """运行整个公司流程"""
        logger.info("🚀 开始运行Simplified TradeCompany...")
        
        if config is None:
            config = RunnableConfig(recursion_limit=50)
        
        # 创建初始状态
        initial_state = CompanyState(
            trigger_time=trigger_time,
            data_factors=[],
            research_signals=[],
            all_events=[],
            step_results={},
            portfolio_info=portfolio_info or {}
        )
        
        # 运行工作流
        workflow = self.create_company_workflow()
        final_state = await workflow.ainvoke(initial_state, config=config)
        
        logger.info("✅ Simplified TradeCompany完成")
        logger.info(f"📊 最终结果:")
        
        # 从step_results中获取更准确的统计信息
        step_results = final_state.get('step_results', {})
        data_team_results = step_results.get("data_team", {})
        research_team_results = step_results.get("research_team", {})
        
        data_factors_count = data_team_results.get("factors_count", len(final_state.get('data_factors', [])))
        research_signals_count = research_team_results.get("signals_count", len(final_state.get('research_signals', [])))
        total_events_count = len(final_state.get('all_events', []))
        
        logger.info(f"   数据因子: {data_factors_count}")
        logger.info(f"   研究信号: {research_signals_count}")
        logger.info(f"   总事件: {total_events_count}")
        
        return final_state

    async def run_company_with_events(self, trigger_time: str, config: RunnableConfig = None):
        """使用事件流运行公司"""
        if config is None:
            config = RunnableConfig(recursion_limit=50)
        
        # 创建初始状态
        initial_state = CompanyState(
            trigger_time=trigger_time,
            data_factors=[],
            research_signals=[],
            all_events=[],
            step_results={}
        )
        
        # 运行工作流并返回事件流
        workflow = self.create_company_workflow()
        async for event in workflow.astream_events(initial_state, version="v2", config=config):
            yield event

if __name__ == "__main__":
    async def main():
        company = SimpleTradeCompany()
        
        # 使用事件流运行
        logger.info("🚀 开始测试Simplified TradeCompany事件流...")
        logger.info("=" * 60)

        trigger_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        company_events = []
        final_state = None
        
        async for event in company.run_company_with_events(trigger_time):
            company_events.append(event)
            
            # 监听并打印事件
            event_type = event.get("event", "unknown")
            event_name = event.get("name", "unknown")
            
            if event_type == "on_chain_start":
                if event_name != "__start__":
                    logger.info(f"🔄 Company开始: {event_name}")
            elif event_type == "on_chain_end":
                if event_name != "__start__":
                    logger.info(f"✅ Company完成: {event_name}")
                    if event_name == "finalize":
                        final_state = event.get("data", {}).get("output", {})
            elif event_type == "on_custom":
                custom_name = event.get("name", "")
                custom_data = event.get("data", {})
                
                if custom_name.startswith("data_agent_"):
                    agent_id = custom_data.get("agent_id", "unknown")
                    logger.info(f"📊 Data Agent {agent_id}: {custom_name}")
                elif custom_name.startswith("research_agent_"):
                    agent_id = custom_data.get("agent_id", "unknown")
                    logger.info(f"🔍 Research Agent {agent_id}: {custom_name}")
                else:
                    logger.info(f"🎯 自定义事件: {custom_name}")
        
        logger.info("=" * 60)
        logger.info(f"✅ 公司工作流完成:")
        if final_state:
            step_results = final_state.get('step_results', {})
            data_team_results = step_results.get("data_team", {})
            research_team_results = step_results.get("research_team", {})
            
            data_factors_count = data_team_results.get("factors_count", len(final_state.get('data_factors', [])))
            research_signals_count = research_team_results.get("signals_count", len(final_state.get('research_signals', [])))
            
            logger.info(f"   数据因子: {data_factors_count}")
            logger.info(f"   研究信号: {research_signals_count}")
        else:
            logger.info(f"   无最终状态数据")
        logger.info(f"   公司事件总数: {len(company_events)}")
        
    asyncio.run(main())