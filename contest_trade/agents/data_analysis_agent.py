""" 

Core Process:
Original Documents → Batch Processing → LLM Intelligent Filtering → Content Deep Summary → Multi-batch Merging → Final Factor

"""
import re
import json
import traceback
import asyncio
import importlib
import pandas as pd
from typing import List, Tuple, Dict, Any, TypedDict
from datetime import datetime
from langgraph.graph import StateGraph, END
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from loguru import logger
from utils.llm_utils import count_tokens
from models.llm_model import GLOBAL_LLM
from langchain_core.runnables import RunnableConfig
from config.config import PROJECT_ROOT, WORKSPACE_ROOT, cfg
from agents.prompts import prompt_for_data_analysis_summary_doc, prompt_for_data_analysis_filter_doc, prompt_for_data_analysis_merge_summary


@dataclass
class DataAnalysisAgentInput:
    """Data Analysis Agent Input"""
    trigger_time: str


@dataclass
class DataAnalysisAgentOutput:
    """Data Analysis Agent Output"""
    agent_name: str
    trigger_time: str
    source_list: List[str]
    bias_goal: str
    context_string: str
    references: List[Dict[str, Any]]
    batch_summaries: List[Dict[str, Any]]

    def to_dict(self):
        return {
            "agent_name": self.agent_name,
            "trigger_time": self.trigger_time,
            "source_list": self.source_list,
            "bias_goal": self.bias_goal,
            "context_string": self.context_string,
            "references": self.references,
            "batch_summaries": self.batch_summaries
        }

@dataclass
class DataAnalysisAgentConfig:
    """Data Analysis Agent Config"""
    agent_name: str
    source_list: List[str]
    max_concurrent_tasks: int
    credits_per_batch: int
    content_cutoff_length: int
    max_llm_context: int
    llm_call_num: int
    final_target_tokens: int
    bias_goal: str = None

    def __init__(
        self,
        agent_name: str = "thx_news_summary",
        source_list: List[str] = [],
        max_concurrent_tasks: int = 6,
        credits_per_batch: int = 10,
        content_cutoff_length: int = 2000,
        max_llm_context: int = 28000,
        llm_call_num: int = 2,
        final_target_tokens: int = 4000, 
        bias_goal: str = None,
    ):
        self.agent_name = agent_name
        self.source_list = source_list
        self.max_concurrent_tasks = max_concurrent_tasks
        self.credits_per_batch = credits_per_batch
        self.content_cutoff_length = content_cutoff_length
        self.max_llm_context = max_llm_context
        self.llm_call_num = llm_call_num
        self.final_target_tokens = final_target_tokens
        self.bias_goal = bias_goal
        # Calculate derived parameters based on configuration
        self.batch_count = self.credits_per_batch // self.llm_call_num + 1
        self.title_selection_per_batch = self.max_llm_context // self.content_cutoff_length
        self.summary_target_tokens = self.max_llm_context // self.batch_count


class DataAnalysisAgentState(TypedDict):
    """Detailed Analysis Result Class"""
    trigger_time: str
    source_list: List[str]
    bias_goal: str
    data_source_list: List[Any]
    batch_info: Dict[str, Any]
    batch_results: List[Dict[str, Any]]
    filtered_docs: List[Dict[str, Any]]
    error_log: List[Dict[str, Any]]
    metadata: Dict[str, Any]
    data_df: pd.DataFrame
    summary: str
    previous_summary: str
    processing_stats: Dict[str, Any]
    batch_details: List[Dict[str, Any]]
    result: DataAnalysisAgentOutput


class DataAnalysisAgent:
    """Data Analysis Agent - Records detailed processing procedures"""
    
    def __init__(self, config: DataAnalysisAgentConfig = None):
        self.config = config or DataAnalysisAgentConfig()
        self.app = self._build_graph()

        self.factor_dir = WORKSPACE_ROOT / "agents_workspace" / "factors" / self.config.agent_name
        if not self.factor_dir.exists():
            self.factor_dir.mkdir(parents=True, exist_ok=True)

        self.set_source_by_config(self.config.source_list)

    def set_source_by_config(self, data_source_list):
        """设置数据源配置"""
        self.data_source_list = []
        
        for source_path in data_source_list:
            try:
                # 解析模块路径和类名
                parts = source_path.split('.')
                class_name = parts[-1]
                module_name = '.'.join(parts[:-1])
                
                # 动态导入模块
                module = importlib.import_module(module_name)
                # 获取类
                data_source_class = getattr(module, class_name)
                
                if not callable(data_source_class):
                    raise ValueError(f"{source_path} is not callable")
                
                # 创建实例
                data_source = data_source_class()
                self.data_source_list.append(data_source)
                logger.debug(f"Successfully loaded data source: {source_path}")
                
            except (ImportError, AttributeError) as e:
                logger.error(f"Error loading data source '{source_path}': {e}")
                continue
            except Exception as e:
                logger.error(f"Unexpected error loading '{source_path}': {e}")
                continue


    def _build_graph(self) -> StateGraph:
        """Build the data analysis graph"""
        workflow = StateGraph(DataAnalysisAgentState)
        workflow.add_node("init_factor_dir", self._init_factor_dir)
        workflow.add_node("recompute_factor", self._recompute_factor)
        workflow.add_node("preprocess", self._preprocess)
        workflow.add_node("batch_process", self._batch_process)
        workflow.add_node("final_summary", self._final_summary)
        workflow.add_node("submit_result", self._submit_result)

        workflow.set_entry_point("init_factor_dir")
        workflow.add_conditional_edges("init_factor_dir",
            self._recompute_factor,
            {
                "yes": "preprocess",
                "no": "submit_result"
            })
        workflow.add_edge("recompute_factor", "preprocess")
        workflow.add_edge("preprocess", "batch_process")
        workflow.add_edge("batch_process", "final_summary")
        workflow.add_edge("final_summary", "submit_result")
        workflow.add_edge("submit_result", END)
        return workflow.compile()
    
    async def _init_factor_dir(self, state: DataAnalysisAgentState) -> DataAnalysisAgentState:
        """try to load factor from file"""
        try:
            factor_file = self.factor_dir / f'{state["trigger_time"].replace(" ", "_").replace(":", "-")}.json'
            if factor_file.exists():
                with open(factor_file, 'r', encoding='utf-8') as f:
                    factor_data = json.load(f)
                state["result"] = DataAnalysisAgentOutput(**factor_data)
        except Exception as e:
            logger.error(f"Error loading factor from file: {e}")
            logger.error(traceback.format_exc())
        return state
    
    async def _recompute_factor(self, state: DataAnalysisAgentState):
        """recompute factor"""
        if state["result"]:
            logger.debug(f"Data already exists for {state['trigger_time']}, skipping recompute")
            return "no"
        else:
            logger.info(f"Data does not exist for {state['trigger_time']}, recomputing factor")
            return "yes"

    def _get_previous_daily_factor(self, current_trigger_time: str) -> Dict[str, Any]:
        """Find the latest factor file from earlier today"""
        try:
            current_dt = datetime.strptime(current_trigger_time, "%Y-%m-%d %H:%M:%S")
            day_str = current_dt.strftime("%Y-%m-%d")
            
            # List all JSON files for today
            files = list(self.factor_dir.glob(f"{day_str}*.json"))
            if not files:
                return None
                
            # Filter and sort files to find the one just before current_trigger_time
            current_file_name = f'{current_trigger_time.replace(" ", "_").replace(":", "-")}.json'
            prev_files = [f for f in files if f.name < current_file_name]
            
            if not prev_files:
                return None
                
            # Sort by name (which is timestamped)
            prev_files.sort()
            latest_prev_file = prev_files[-1]
            
            with open(latest_prev_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error getting previous daily factor: {e}")
            return None

    async def _preprocess(self, state: DataAnalysisAgentState) -> DataAnalysisAgentState:
        """Preprocess the document data with deduplication and incremental filtering"""
        try:
            # 1. Load previous context for the same day
            prev_factor = self._get_previous_daily_factor(state["trigger_time"])
            last_processed_time = "2000-01-01 00:00:00" # Default to far past
            state["previous_summary"] = ""
            
            if prev_factor:
                last_processed_time = prev_factor.get("trigger_time", last_processed_time)
                state["previous_summary"] = prev_factor.get("context_string", "")
                logger.info(f"Incremental processing enabled. Last processed time: {last_processed_time}")

            # 并发获取所有数据源的数据
            logger.info(f"🚀 正在并发获取 {len(self.data_source_list)} 个数据源的数据...")
            
            async def get_source_data(source):
                try:
                    logger.info(f"Getting data from {source.__class__.__name__}...")
                    return await source.get_data(state["trigger_time"])
                except Exception as e:
                    logger.error(f"Error getting data from {source.__class__.__name__}: {e}")
                    return pd.DataFrame()

            tasks = [get_source_data(source) for source in self.data_source_list]
            dfs = await asyncio.gather(*tasks)
            
            data_dfs = []
            required_columns = ['title', 'content', 'pub_time']
            for i, df in enumerate(dfs):
                source_name = self.data_source_list[i].__class__.__name__
                if df.empty:
                    continue
                    
                # Check for missing columns
                missing_cols = [col for col in required_columns if col not in df.columns]
                if missing_cols:
                    logger.warning(f"Source {source_name} missing columns: {missing_cols}")
                    continue

                df = df[df['title'].str.strip() != '']
                df = df[df['content'].str.strip() != '']
                df = df[required_columns]
                data_dfs.append(df)
            
            if not data_dfs:
                state["data_df"] = pd.DataFrame(columns=['title', 'content', 'pub_time', 'id'])
                state["batch_info"] = {'batch_count': 0, 'total_data': 0}
                return state

            data_df = pd.concat(data_dfs, ignore_index=True)
            
            # 2. Deduplication across all sources by title
            original_count = len(data_df)
            data_df = data_df.drop_duplicates(subset=['title'], keep='first')
            dedup_count = original_count - len(data_df)
            if dedup_count > 0:
                logger.debug(f"Deduplicated {dedup_count} documents across sources.")

            # 3. Incremental Filtering: Filter out previously processed news
            # Some sources might have slightly different time formats, try to normalize
            data_df['pub_time_dt'] = pd.to_datetime(data_df['pub_time'], errors='coerce')
            last_processed_dt = pd.to_datetime(last_processed_time)
            
            incremental_df = data_df[data_df['pub_time_dt'] > last_processed_dt].copy()
            new_docs_count = len(incremental_df)
            
            logger.info(f"Summary for {state['trigger_time']}: Total={original_count}, Deduped={len(data_df)}, Increment={new_docs_count}")
            
            incremental_df = incremental_df.drop(columns=['pub_time_dt'])
            incremental_df['id'] = range(1, len(incremental_df) + 1)
            state["data_df"] = incremental_df

            total_docs = len(incremental_df)
            
            # Adjust batch count for incremental data
            max_batch_count = self.config.batch_count
            batch_count = max_batch_count
            
            if total_docs <= self.config.title_selection_per_batch * 2:
                batch_count = 1 if total_docs > 0 else 0
            else:
                suggested_batch_count = total_docs // self.config.title_selection_per_batch
                batch_count = max(1, min(max_batch_count, suggested_batch_count))

            batch_size = total_docs // batch_count if batch_count > 0 else 0
            if batch_count > 0 and total_docs % batch_count:
                batch_size += 1
                
            state["batch_info"] = {
                'batch_count': batch_count,
                'batch_size': batch_size,
                'total_data': total_docs,
                'titles_per_batch': min(self.config.title_selection_per_batch, batch_size if batch_size > 0 else total_docs)
            }
        except Exception as e:
            logger.error(f"Error preprocessing data: {e}")
            logger.error(traceback.format_exc())
            return state
        logger.info("preprocess success")
        return state
    
   
    async def _batch_process(self, state: DataAnalysisAgentState) -> DataAnalysisAgentState:
        """Asynchronously process all document batches, return detailed results"""
        logger.info("begin batch process")
        try:
            batch_tasks = []
            for i in range(state["batch_info"]['batch_count']):
                start_idx = i * state["batch_info"]['batch_size']
                end_idx = min((i + 1) * state["batch_info"]['batch_size'], len(state["data_df"]))
                
                if start_idx >= len(state["data_df"]):
                    break
            
                batch_df = state["data_df"].iloc[start_idx:end_idx]
                if not batch_df.empty:
                    batch_tasks.append((i + 1, state["trigger_time"], batch_df, state["batch_info"]['titles_per_batch'], state["bias_goal"]))
        
            # Use semaphore to control concurrency
            semaphore = asyncio.Semaphore(self.config.max_concurrent_tasks)
            
            async def process_single_batch(task_data: Tuple[int, str, pd.DataFrame, int, str]) -> Dict[str, Any]:
                async with semaphore:
                    return await self._process_batch_detailed(task_data)
            
            # Execute all batch tasks
            tasks = [process_single_batch(task) for task in batch_tasks]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Collect results
            batch_results = []
            for result in results:
                if isinstance(result, Exception):
                    batch_results.append({
                        "batch_id": "unknown",
                        "success": False,
                        "error": str(result),
                        "timestamp": datetime.now().isoformat()
                    })
                else:
                    batch_results.append(result)
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            batch_results = []
        state["batch_results"] = batch_results
        return state
    

    async def _final_summary(self, state: DataAnalysisAgentState) -> DataAnalysisAgentState:
        """Merge multiple batch summaries into final document factor with cumulative updates"""
        try:
            # 1. Handle no new news case
            valid_results = [r for r in state["batch_results"] if r.get("success") and r.get("summary")]
            
            if not valid_results:
                logger.info("No new batch results, using previous summary.")
                state["summary"] = state.get("previous_summary", "No market news available.")
                state["result"] = DataAnalysisAgentOutput(
                    agent_name=self.config.agent_name,
                    trigger_time=state["trigger_time"],
                    source_list=state["source_list"],
                    bias_goal=state["bias_goal"],
                    context_string=state["summary"],
                    references=[],
                    batch_summaries=[]
                )
                return state
            
            # 2. Prepare combined input for merging
            new_combined_summary = "\n\n".join([
                f"Batch {i+1} New Documents:\n{result.get('summary', '')}" 
                for i, result in enumerate(state["batch_results"])
            ])

            # Use LLM to merge previous context with new summaries
            goal_instruction = f"Integrate goal '{state['bias_goal']}'" if state["bias_goal"] else "Objectively integrate market information"
            summary_focus = "Maintain continuity with previous context and highlight NEW important developments"
            
            # Construct the combined context for the merge prompt
            merge_context = new_combined_summary
            if state.get("previous_summary"):
                merge_context = f"--- PREVIOUS SUMMARY ---\n{state['previous_summary']}\n\n--- NEW UPDATES ---\n{new_combined_summary}"

            prompt = prompt_for_data_analysis_merge_summary.format(
                trigger_time=state["trigger_time"],
                goal_instruction=goal_instruction,
                combined_summary=merge_context,
                summary_focus=summary_focus,
                final_description="Cumulative Market Information Summary",
                final_target_tokens=self.config.final_target_tokens,
                language=cfg.system_language
            )
        
            messages = [{"role": "user", "content": prompt}]
            response = await GLOBAL_LLM.a_run(
                    messages, thinking=False, verbose=False, max_tokens=self.config.final_target_tokens)
            final_summary = response.content.strip()
            
            # 3. Collect references
            all_ref_ids = set()
            batch_summaries = []
            
            for batch_result in state["batch_results"]:
                if batch_result.get("success") and batch_result.get("summary"):
                    batch_summaries.append({
                        "batch_id": batch_result["batch_id"],
                        "summary": batch_result["summary"],
                        "references": batch_result.get("references", [])
                    })
                    for ref in batch_result.get("references", []):
                        if isinstance(ref, dict) and "id" in ref:
                            all_ref_ids.add(str(ref["id"]))
            
            final_ref_ids = re.findall(r'\[(\d+)\]', final_summary)
            all_ref_ids.update(final_ref_ids)
            
            try:
                ref_ids_int = [int(ref_id) for ref_id in all_ref_ids if ref_id.isdigit()]
                references_df = state["data_df"][state["data_df"]["id"].isin(ref_ids_int)]
            except:
                references_df = state["data_df"][state["data_df"]["id"].astype(str).isin(all_ref_ids)]
            references = references_df.to_dict(orient="records")
            
            state["summary"] = final_summary
            state["result"] = DataAnalysisAgentOutput(
                agent_name=self.config.agent_name,
                trigger_time=state["trigger_time"],
                source_list=state["source_list"],
                bias_goal=state["bias_goal"],
                context_string=state["summary"],
                references=references,
                batch_summaries=batch_summaries
            )
            return state
        except Exception as e:
            logger.error(f"Error in final summary merge: {e}")
            logger.error(traceback.format_exc())
            return state

    
    async def _process_batch_detailed(self, task_data: Tuple[int, str, pd.DataFrame, int, str]) -> Dict[str, Any]:
        """Asynchronously process single batch, return detailed results"""
        batch_idx, trigger_datetime, batch_df, titles_to_select, bias_goal = task_data
        batch_start_time = datetime.now()
        
        batch_result = {
            "batch_id": batch_idx,
            "success": False,
            "original_count": len(batch_df),
            "start_time": batch_start_time.isoformat(),
            "filtered_docs": [],
            "summary": "",
            "error": None
        }
        
        logger.info(f"Starting to process batch {batch_idx} ({len(batch_df)} documents)...")
        
        try:
            # Filter document titles
            filtered_df = await self._filter_docs_by_title(trigger_datetime, batch_df, titles_to_select)
            
            # Record filtered document details
            batch_result["filtered_count"] = len(filtered_df)
            batch_result["filtered_docs"] = [
                {
                    "id": row.get('id', ''),
                    "original_index": idx,
                    "title": row.get('title', ''),
                    "pub_time": row.get('pub_time', ''),
                    "content_length": len(str(row.get('content', '')))
                }
                for idx, row in filtered_df.iterrows()
            ]
            
            # Generate content summary
            summary = await self._summarize_doc_content(trigger_datetime, filtered_df, bias_goal)
            
            batch_result["summary"] = summary
            batch_result["summary_length"] = count_tokens(summary) if summary else 0
            batch_result["success"] = True
            
            # Collect references from batch summary
            summary_ref_ids = [int(i) for i in re.findall(r'\[(\d+)\]', summary)]
            batch_result["references"] = filtered_df[filtered_df["id"].isin(summary_ref_ids)].to_dict(orient="records")
            
            logger.info(f"Completed processing batch {batch_idx}")
            
        except Exception as e:
            error_msg = f"Error processing batch {batch_idx}: {e}"
            logger.error(error_msg)
            batch_result["error"] = error_msg
        
        # Record batch processing completion time
        batch_end_time = datetime.now()
        batch_result["end_time"] = batch_end_time.isoformat()
        batch_result["processing_duration"] = (batch_end_time - batch_start_time).total_seconds()
        
        return batch_result
    
    
    async def _filter_docs_by_title(self, trigger_datetime: str, batch_df: pd.DataFrame, titles_to_select: int) -> pd.DataFrame:
        """Use LLM to filter most valuable documents based on titles"""
        if batch_df.empty or len(batch_df) <= titles_to_select:
            return batch_df
        
        # Build title context
        titles_context = ""
        for idx, row in batch_df.iterrows():
            doc_id = row.get('id', idx)
            title = row.get('title', '')
            pub_time = row.get('pub_time', '')
            titles_context += f"ID: {doc_id}\nTitle: {title}\nPublish Time: {pub_time}\n\n"
        
        prompt = prompt_for_data_analysis_filter_doc.format(
            trigger_datetime=trigger_datetime,
            titles_to_select=titles_to_select,
            titles_context=titles_context,
            language=cfg.system_language
        )
        
        messages = [{"role": "user", "content": prompt}]
        response = await GLOBAL_LLM.a_run(messages, verbose=False, thinking=False)
        logger.debug(f"Title filter response: {response.content}")
        
        # Parse LLM returned IDs
        try:
            selected_ids_str = [x.strip() for x in response.content.strip().split(',') if x.strip()]
            # Try to convert to numbers, if failed keep as string
            selected_ids = []
            for id_str in selected_ids_str:
                try:
                    selected_ids.append(int(id_str))
                except ValueError:
                    selected_ids.append(id_str)
            
            # Filter by id column
            if 'id' in batch_df.columns:
                valid_df = batch_df[batch_df['id'].isin(selected_ids)]
                if not valid_df.empty:
                    return valid_df
            
            return batch_df.head(titles_to_select)
        except:
            return batch_df.head(titles_to_select)
    
    
    async def _summarize_doc_content(self, trigger_datetime: str, batch_df: pd.DataFrame, bias_goal: str = None) -> str:
        """Summarize filtered document content"""
        if batch_df.empty:
            return "No valid document content"
        
        # Build document content context
        doc_context = ""
        doc_raw_content = ""
        for _, row in batch_df.iterrows():
            doc_id = row.get('id', '')
            title = row.get('title', '')
            content = row.get('content', '')
            pub_time = row.get('pub_time', '')
            
            # Truncate content
            if len(content) > self.config.content_cutoff_length:
                content = content[:self.config.content_cutoff_length] + "..."
            
            if pub_time.endswith("23:59:59"):
                pub_time = pub_time.split(" ")[0]
            doc_context += f"<doc id={doc_id}> Title: {title}\nPublish Time: {pub_time}\nContent: {content}</doc>\n"
            doc_raw_content += f"Title: {title}\nPublish Time: {pub_time}\nContent: {content}\n"
        
        if len(doc_context) <= self.config.summary_target_tokens and not bias_goal:
            return doc_raw_content

        # Adjust prompt based on whether there's a bias goal
        if bias_goal:
            bias_instruction = f"Focus on target '{bias_goal}' for targeted summary, emphasizing information related to this goal"
            summary_style = "Goal-oriented Summary"
        else:
            bias_instruction = "Objectively summarize market dynamics and important events"
            summary_style = "Objective Summary"
        
        prompt = prompt_for_data_analysis_summary_doc.format(
            trigger_datetime=trigger_datetime,
            bias_instruction=bias_instruction,
            summary_style=summary_style,
            doc_context=doc_context,
            summary_target_tokens=self.config.summary_target_tokens,
            language=cfg.system_language
        )
        
        messages = [{"role": "user", "content": prompt}]
        response = await GLOBAL_LLM.a_run(messages, verbose=False, max_tokens=self.config.summary_target_tokens)
        
        return response.content.strip()
    
    async def _submit_result(self, state: DataAnalysisAgentState) -> DataAnalysisAgentState:
        """Write the result to a file"""
        try:
            factor_file = self.factor_dir / f'{state["trigger_time"].replace(" ", "_").replace(":", "-")}.json'
            with open(factor_file, 'w', encoding='utf-8') as f:
                json.dump(state["result"].to_dict(), f, ensure_ascii=False, indent=4)
            logger.info(f"Data analysis result saved to {factor_file}")
        except Exception as e:
            logger.error(f"Error writing result: {e}")
        return state

    async def run_with_monitoring_events(self, input: DataAnalysisAgentInput, config: RunnableConfig = None) -> DataAnalysisAgentOutput:
        """使用事件流监控运行Agent，返回事件流"""
        initial_state = DataAnalysisAgentState(
            trigger_time=input.trigger_time,
            source_list=self.config.source_list,
            bias_goal=self.config.bias_goal or "",
            data_source_list=[],
            batch_info={},
            batch_results=[],
            filtered_docs=[],
            error_log=[],
            metadata={},
            data_df=pd.DataFrame(),
            summary="",
            previous_summary="",
            processing_stats={},
            batch_details=[],
            result=None
        )
        
        logger.info(f"🚀 Data Analysis Agent Starting - {input.trigger_time}")
        
        # 返回事件流
        async for event in self.app.astream_events(initial_state, version="v2", config=config or RunnableConfig(recursion_limit=50)):
            yield event

    async def run_with_monitoring(self, input: DataAnalysisAgentInput) -> DataAnalysisAgentOutput:
        """使用事件流监控运行Agent"""
        events = self.run_with_monitoring_events(input)
        final_result = None
        async for event in events:
            event_type = event["event"]
            if event_type == "on_chain_start":
                node_name = event["name"]
                if node_name != "__start__":  # 忽略开始事件
                    logger.info(f"🔄 Starting: {node_name}")
            elif event_type == "on_chain_end":
                node_name = event["name"]
                if node_name != "__start__":  # 忽略开始事件
                    logger.info(f"✅ Completed: {node_name}")
                    if node_name == "submit_result":
                        final_state = event.get("data", {}).get("output", None)
                        if final_state and "result" in final_state and final_state["result"]:
                            return final_state["result"]
        return final_result
        

if __name__ == "__main__":
    import json
    from data_source.thx_news import ThxNews
    from data_source.sina_news import SinaNews

    data_source_list = [
        # "data_source.thx_news.ThxNews",
        "data_source.sina_news.SinaNews"
        #"data_source.price_market.PriceMarket",
    ]
    
    # Create custom configuration
    custom_config = DataAnalysisAgentConfig(
        agent_name="sina_news_vtest",
        source_list=data_source_list,
        final_target_tokens=4000,
        bias_goal="",
    )
    
    # Run detailed analysis generation
    async def main():
        trigger_datetime = "2024-01-23 09:00:00"
        data_agent = DataAnalysisAgent(custom_config)

        agent_input = DataAnalysisAgentInput(
            trigger_time=trigger_datetime,
        )

        output = await data_agent.run_with_monitoring(agent_input)
        print("=== Detailed Analysis Results ===")
        if output and hasattr(output, 'context_string') and output.context_string:
            print(f"Summary: {output.context_string}")
        else:
            print("❌ No summary available", output)
    
    asyncio.run(main()) 


    pass
