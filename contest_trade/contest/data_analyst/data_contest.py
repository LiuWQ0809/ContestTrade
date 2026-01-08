"""
DataContest - 数据因子竞争系统主控制器

核心功能：
1. Evaluation: 评估历史因子的市场表现，计算reward  
2. Prediction: 基于历史reward预测因子排序
3. Selection: 选择优质因子为研究提供背景信息

职责：协调各个子模块，提供统一的外部接口
"""

import sys
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Optional

# 添加项目根目录到path
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.resolve()
sys.path.append(str(PROJECT_ROOT / "contest_trade"))

from models.llm_model import GLOBAL_LLM
from utils.market_manager import GLOBAL_MARKET_MANAGER

from data_contest_types import FactorData, ContestResult
from data_manager import ContestDataManager
from evaluator import ContestEvaluator
from predictor import ContestPredictor

logger = logging.getLogger(__name__)


class DataContest:
    """数据因子竞争系统主控制器"""
    
    def __init__(self, target_agents: List[str] = None):
        self.history_window_days = 5  # 固定为5天，内部参数
        self.target_agents = target_agents or []
        
        # 初始化各个子模块
        self.data_manager = ContestDataManager(self.history_window_days, PROJECT_ROOT, target_agents)
        self.evaluator = ContestEvaluator(GLOBAL_LLM, GLOBAL_MARKET_MANAGER)
        self.predictor = ContestPredictor(self.history_window_days)
        
        logger.info(f"DataContest初始化完成 - 历史窗口: {self.history_window_days}天, 目标agents: {len(self.target_agents)}个")
    
    async def run_data_contest(self, trigger_time: str, current_factors: List = None) -> List[FactorData]:
        """
        主要接口：运行数据竞争
        
        Args:
            trigger_time: 当前时间
            current_factors: 当天各data agent生成的因子
            
        Returns:
            List[FactorData]: 根据历史表现选择的最优agent组合的当天因子
        """
        logger.info(f"🎯 开始运行DataContest - {trigger_time}")
        
        try:
            current_date = trigger_time.split(' ')[0]
            
            # 步骤1: 加载历史因子
            logger.info("步骤1: 加载历史因子数据")
            agent_factors = self.data_manager.load_historical_factors(current_date)
            
            # 统计信息
            total_factors = sum(len([f for f in factors_list if f is not None]) for factors_list in agent_factors.values())
            total_evaluated = sum(len([f for f in factors_list if f is not None and f.has_contest_data()]) for factors_list in agent_factors.values())
            logger.info(f"加载了 {total_factors} 个历史因子")
            logger.info(f"其中 {total_evaluated} 个已有评估数据，{total_factors - total_evaluated} 个需要评估")
            
            # 步骤2: 评估历史因子（补全缺失的reward）
            await self._evaluate_missing_factors(agent_factors, current_date)
            
            # 步骤3: 预测因子排序
            factor_scores = self._predict_factor_values(current_date, agent_factors)
            
            # 步骤4: 选择优质agent
            selected_agents = self._select_top_agents(factor_scores)
            
            # 步骤5: 从当天因子中筛选出最优agent的因子
            selected_factors = self._get_current_factors_by_agents(current_factors, selected_agents)
            
            # 记录结果
            result = ContestResult(
                selected_factors=selected_factors,
                trigger_time=trigger_time,
                selection_method="simple_topk"
            )
            
            logger.info(f"✅ DataContest完成: {result.get_summary()}")
            return selected_factors
            
        except Exception as e:
            logger.error(f"DataContest运行失败: {e}")
            raise RuntimeError(f"运行失败: {e}")
    
    
    async def _evaluate_missing_factors(self, agent_factors: Dict[str, List[Optional[FactorData]]], current_date: str):
        """评估缺失reward的历史因子"""
        logger.info("步骤2: 评估历史因子")
        
        # 筛选需要评估的因子
        factors_to_evaluate = []
        for agent_name, factors_list in agent_factors.items():
            for factor in factors_list:
                if factor is None:  # 跳过缺位的None
                    continue
                if not factor.has_contest_data():  # 只评估尚未评估的
                    factor_date = factor.trigger_time.split(' ')[0]
                    factors_to_evaluate.append((factor, factor_date))
        
        if not factors_to_evaluate:
            logger.info("所有因子都已有评估数据，跳过评估步骤")
            return
        
        logger.info(f"需要评估 {len(factors_to_evaluate)} 个因子")
        
        # 批量评估
        success_count = 0
        for factor, factor_date in factors_to_evaluate:
            try:
                evaluation_result = await self.evaluator.evaluate_factor(factor, factor_date)
                
                if evaluation_result:
                    # 保存评估结果
                    contest_data = evaluation_result.to_contest_data()
                    if self.data_manager.save_contest_data(factor, contest_data):
                        success_count += 1
                    else:
                        logger.warning(f"保存评估结果失败: {factor.agent_name}")
                else:
                    logger.warning(f"评估失败: {factor.agent_name}")
                    
            except Exception as e:
                logger.error(f"评估异常: {factor.agent_name} - {e}")
        
        logger.info(f"评估完成: {success_count}/{len(factors_to_evaluate)} 个成功")
    
    def _predict_factor_values(self, current_date: str, agent_factors: Dict[str, List[Optional[FactorData]]]) -> dict:
        """预测因子排序"""
        logger.info("步骤3: 预测因子排序")
        
        try:
            factor_scores = self.predictor.predict_factor_values(current_date, agent_factors)
            return factor_scores
        except Exception as e:
            logger.error(f"预测失败: {e}")
            return {}
    
    def _select_top_agents(self, scores: dict) -> List[str]:
        """选择优质agent - 当前使用简单的top-k策略"""
        logger.info("步骤4: 选择优质agent")
        
        if not scores:
            logger.warning("没有预测得分")
            return []
        
        # 按预测得分排序，选择top-k
        sorted_agents = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_k = 3  # 选择前3个agent
        selected_agents = [agent_name for agent_name, score in sorted_agents[:top_k]]
        
        logger.info(f"从 {len(scores)} 个agent中选择了 top-{len(selected_agents)}")
        for i, agent_name in enumerate(selected_agents):
            score = scores[agent_name]
            logger.info(f"  {i+1}. {agent_name} - 得分: {score:.3f}")
        
        return selected_agents
    
    def _get_current_factors_by_agents(self, current_factors: List[FactorData], selected_agents: List[str]) -> List[FactorData]:
        """根据选定的agent，从当天因子中筛选对应的因子"""
        logger.info("步骤5: 筛选当天因子")
        
        if not current_factors:
            logger.warning("没有当天的因子数据")
            return []
            
        if not selected_agents:
            logger.warning("没有选定的agent，返回所有当天因子")
            return current_factors
        
        # 从当天因子中筛选出最优agent的因子
        selected_factors = []
        for factor in current_factors:
            if factor.agent_name in selected_agents:
                selected_factors.append(factor)
        
        logger.info(f"从 {len(current_factors)} 个当天因子中筛选出 {len(selected_factors)} 个优质因子")
        for factor in selected_factors:
            logger.info(f"  - {factor.agent_name}")
        
        return selected_factors
    
