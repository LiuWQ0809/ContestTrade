from loguru import logger
"""
Research权重优化器

基于预测夏普比率进行权重分配
"""

import json
import logging
import numpy as np
from pathlib import Path
from typing import Dict, Optional, List
from research_contest_types import ResearchContestResult

logger = logging.getLogger(__name__)


class ResearchWeightOptimizer:
    """研究权重优化器 - 基于预测夏普比率分配权重"""
    
    def __init__(self, workspace_dir: str):
        self.workspace_dir = Path(workspace_dir)
        self.final_result_dir = self.workspace_dir / "final_result"
        self.final_result_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("ResearchWeightOptimizer初始化完成")
    
    def optimize_weights_by_sharpe(self, predicted_sharpe_ratios: Dict[str, float], trigger_time: str) -> Dict[str, float]:
        """
        基于预测的夏普比率分配权重
        """
        if not predicted_sharpe_ratios:
            raise ValueError("预测夏普比率为空，无法分配权重")
        
        logger.info("基于预测夏普比率分配权重...")
        
        # 只对夏普比率大于0的agent分配权重
        positive_sharpe_agents = {}
        for agent_name, sharpe_ratio in predicted_sharpe_ratios.items():
            if sharpe_ratio > 0:
                positive_sharpe_agents[agent_name] = sharpe_ratio
                logger.debug(f"   {agent_name}: 预测夏普比率={sharpe_ratio:.3f} ✅")
            else:
                logger.debug(f"   {agent_name}: 预测夏普比率={sharpe_ratio:.3f} ❌ (过滤)")
        
        if not positive_sharpe_agents:
            logger.warning("所有agent的预测夏普比率都不大于0，权重都为0")
            # 如果所有夏普比率都不大于0，权重都为0
            n_agents = len(predicted_sharpe_ratios)
            equal_weight = 0 / n_agents
            return {name: equal_weight for name in predicted_sharpe_ratios.keys()}
        
        # 按夏普比率加权分配
        total_sharpe = sum(positive_sharpe_agents.values())
        optimized_weights = {}
        
        for agent_name in predicted_sharpe_ratios.keys():
            if agent_name in positive_sharpe_agents:
                weight = positive_sharpe_agents[agent_name] / total_sharpe
                optimized_weights[agent_name] = weight
                logger.info(f"   {agent_name}: 夏普比率={positive_sharpe_agents[agent_name]:.3f}, 权重={weight:.1%}")
            else:
                optimized_weights[agent_name] = 0.0
                logger.info(f"   {agent_name}: 夏普比率≤0, 权重=0%")
        
        # 验证权重总和
        total_weight = sum(optimized_weights.values())
        logger.info(f"权重分配完成，总权重: {total_weight:.3f}")
        
        return optimized_weights
    
    def save_final_results_by_sharpe(self, trigger_time: str,
                                   optimized_weights: Dict[str, float], 
                                   predicted_sharpe_ratios: Dict[str, float]) -> ResearchContestResult:
        """基于夏普比率保存最终结果"""
        timestamp = trigger_time.replace(' ', '_').replace(':', '')
        
        # 构建最终结果
        result = ResearchContestResult(
            trigger_time=trigger_time,
            optimized_weights=optimized_weights,
            total_signals=len(predicted_sharpe_ratios),
            valid_signals=sum(1 for w in optimized_weights.values() if w > 0),
            selection_method="sharpe_weighted"
        )
        
        # 保存到文件
        result_file = self.final_result_dir / f"final_result_{timestamp}.json"
        
        result_data = {
            'trigger_time': result.trigger_time,
            'optimized_weights': result.optimized_weights,
            'predicted_sharpe_ratios': predicted_sharpe_ratios,
            'summary': {
                'total_signals': result.total_signals,
                'valid_signals': result.valid_signals,
                'selection_method': result.selection_method,
                'avg_predicted_sharpe': np.mean(list(predicted_sharpe_ratios.values())),
                'top_sharpe_agents': sorted(predicted_sharpe_ratios.items(), key=lambda x: x[1], reverse=True)[:3],
                'top_weight_agents': sorted(optimized_weights.items(), key=lambda x: x[1], reverse=True)[:3]
            }
        }
        
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"最终结果已保存到: {result_file}")
        return result
