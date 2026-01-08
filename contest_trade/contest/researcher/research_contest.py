"""
ResearchContest - 统一的研究信号竞争系统

核心功能：
1. Evaluation: 评估历史信号的市场表现，计算reward  
2. Prediction: 基于历史reward预测信号排序
3. Selection: 选择优质信号为投资提供权重分配
"""

import os
import sys
import logging
import asyncio
from pathlib import Path
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.resolve()
sys.path.append(str(PROJECT_ROOT / "contest_trade"))

from models.llm_model import GLOBAL_LLM
from utils.market_manager import GLOBAL_MARKET_MANAGER
from config.config import cfg
from research_contest_types import SignalData, ResearchContestResult
from research_data_manager import ResearchDataManager
from research_predictor import ResearchPredictor
from research_weight_optimizer import ResearchWeightOptimizer
from research_signal_judger import ResearchSignalJudger

logger = logging.getLogger(__name__)


class ResearchContest:
    """研究信号竞争系统主控制器"""
    
    def __init__(self, target_agents: List[str] = None):
        self.history_window_days = 5
        self.target_agents = target_agents or []
        self.data_manager = ResearchDataManager(self.history_window_days, PROJECT_ROOT, target_agents)
        self.data_manager.set_market_manager(GLOBAL_MARKET_MANAGER)
        self.predictor = ResearchPredictor(self.history_window_days)
        self.weight_optimizer = ResearchWeightOptimizer(str(self.data_manager.workspace_dir))
        self.signal_judger = ResearchSignalJudger(str(self.data_manager.workspace_dir), self.history_window_days, self.data_manager)
        
        logger.info(f"ResearchContest初始化完成 - 历史窗口: {self.history_window_days}天, 目标agents: {len(self.target_agents)}个")
    
    async def run_research_contest(self, trigger_time: str, current_signals: Dict[str, SignalData] = None) -> ResearchContestResult:
        logger.info(f"🎯 开始运行ResearchContest - {trigger_time}")
        
        try:
            current_date = trigger_time.split(' ')[0]
            
            # 步骤1: 加载历史信号数据
            logger.info("步骤1: 加载历史信号数据")
            agent_signals = self.data_manager.load_historical_signals(current_date)
            
            # 统计信息
            total_signals = sum(len([s for s in signals_list if s is not None]) for signals_list in agent_signals.values())
            total_evaluated = sum(len([s for s in signals_list if s is not None and s.has_contest_data()]) for signals_list in agent_signals.values())
            logger.info(f"加载了 {total_signals} 个历史信号")
            logger.info(f"其中 {total_evaluated} 个已有评估数据，{total_signals - total_evaluated} 个需要评估")
            
            # 步骤2: 评估历史信号
            await self._evaluate_missing_signals(agent_signals, current_date)
            
            # 步骤3: 获取当天信号的judge评分
            if not current_signals:
                raise ValueError("预测模型需要当天信号的judge评分数据！请提供current_signals参数。")
            
            logger.info("步骤3: 获取当天信号judge评分")
            current_judge_scores = await self._get_current_judge_scores(current_signals, trigger_time)
            
            if not current_judge_scores:
                raise ValueError("无法获取当天信号的judge评分！预测模型需要12个特征，包括7个judge评分特征。")
            
            # 步骤4: 预测未来n天夏普比率
            logger.info("步骤4: 预测未来夏普比率")
            predicted_sharpe_ratios = self._predict_signal_values(current_date, agent_signals, current_judge_scores)
            
            # 步骤5: 基于预测夏普比率分配权重
            logger.info("步骤5: 基于预测夏普比率分配权重")
            optimized_weights = self.weight_optimizer.optimize_weights_by_sharpe(predicted_sharpe_ratios, trigger_time)
            
            # 步骤6: 保存结果
            result = self.weight_optimizer.save_final_results_by_sharpe(trigger_time, optimized_weights, predicted_sharpe_ratios)
            
            logger.info(f"✅ ResearchContest完成: {result.get_summary()}")
            return result
            
        except Exception as e:
            logger.error(f"ResearchContest运行失败: {e}")
            raise RuntimeError(f"运行失败: {e}")

    async def run_research_pipeline(self, trigger_time: str, workspace_dir: str = None) -> Dict[str, Any]:
        print(f"🔬 开始运行Research Contest流程，时间: {trigger_time}")
        
        try:
            result = await self.run_research_contest(trigger_time)
            
            if result:
                print("✅ Research Contest流程完成")
                print(f"   优化权重数量: {len(result.optimized_weights)}")
                print(f"   有效信号数量: {result.valid_signals}")
                
                return {
                    'status': 'success',
                    'trigger_time': trigger_time,
                    'optimized_weights': result.optimized_weights,
                    'predicted_sharpe_ratios': result.predicted_sharpe_ratios,
                    'total_signals': result.total_signals,
                    'valid_signals': result.valid_signals,
                    'selection_method': result.selection_method
                }
            else:
                raise RuntimeError("Research Contest流程失败: 结果为空")
                
        except Exception as e:
            raise RuntimeError(f"Research Contest流程异常: {e}")

    def filter_valid_signals(self, signals_data: Dict[str, SignalData]) -> Dict[str, SignalData]:
        valid_signals = {}
        
        for signal_name, signal_data in signals_data.items():
            has_opportunity = signal_data.has_opportunity
            if has_opportunity.lower() == 'yes':
                valid_signals[signal_name] = signal_data
                print(f"   ✅ 保留有效研究信号: {signal_name} ({signal_data.symbol_name})")
            else:
                print(f"   ❌ 过滤无效研究信号: {signal_name} (has_opportunity={has_opportunity})")
        
        return valid_signals

    def get_signal_details(self, trigger_time: str, signal_names: List[str]) -> Dict[str, Dict]:
        signal_details = {}
        
        current_signals = self.data_manager.load_current_signals(trigger_time)
        
        for signal_name in signal_names:
            if signal_name in current_signals:
                signal = current_signals[signal_name]
                signal_details[signal_name] = {
                    'symbol_name': signal.symbol_name,
                    'action': signal.action,
                    'probability': signal.probability
                }
            else:
                raise ValueError(f"信号 {signal_name} 不存在于当前信号中")
        
        return signal_details

    def format_signal_output(self, optimized_weights: Dict[str, float], 
                           signal_details: Dict[str, Dict]) -> List[str]:
        output_lines = []

        sorted_weights = sorted(optimized_weights.items(), key=lambda x: x[1], reverse=True)
        
        valid_signals_count = 0
        for signal_name, weight in sorted_weights:
            if weight > 0:
                valid_signals_count += 1
                details = signal_details.get(signal_name, {'symbol_name': 'N/A', 'action': 'N/A', 'probability': 'N/A'})
                symbol_name = details['symbol_name']
                action = details['action']
                probability = details.get('probability', 'N/A')
                output_lines.append(f"   {valid_signals_count}. {symbol_name} - {action} - 概率: {probability} - 权重: {weight:.1%}")
        
        if valid_signals_count == 0:
            output_lines.append("   📊 暂无有效研究信号")
        
        return output_lines

    async def train_prediction_model(self) -> bool:
        try:
            model_dir = Path(__file__).parent / "lightgbm_predictor"
            mean_model_path = model_dir / "lgbm_mean_model.joblib"
            std_model_path = model_dir / "lgbm_std_model.joblib"
            
            if mean_model_path.exists() and std_model_path.exists():
                success = self.predictor._load_lightgbm_models()
                if self.predictor.use_lightgbm:
                    print("✅ 成功导入现有的LightGBM模型，跳过训练")
                    return True
                else:
                    print("⚠️ 现有模型加载失败，将重新训练")
            else:
                print("🔍 未发现现有模型文件，需要训练新模型")
                if not mean_model_path.exists():
                    print(f"   缺失文件: {mean_model_path}")
                if not std_model_path.exists():
                    print(f"   缺失文件: {std_model_path}")
            
            print("🤖 开始训练新的Research预测模型...")
            
            training_data = self._collect_historical_training_data()
            
            if not training_data:
                print("❌ 没有可用的训练数据")
                return False

            success = self.predictor.train_lightgbm_model(training_data)
            
            if success:
                print("✅ Research预测模型训练完成")
                print(f"   模型已保存到: {model_dir}")
            else:
                print("❌ Research预测模型训练失败")
            
            return success
            
        except Exception as e:
            print(f"❌ Research预测模型训练/导入异常: {e}")
            return False

    def get_model_status(self) -> Dict[str, Any]:
        model_dir = Path(__file__).parent / "lightgbm_predictor"
        mean_model_path = model_dir / "lgbm_mean_model.joblib"
        std_model_path = model_dir / "lgbm_std_model.joblib"
        
        status = {
            'model_dir': str(model_dir),
            'mean_model_exists': mean_model_path.exists(),
            'std_model_exists': std_model_path.exists(),
            'models_loaded': self.predictor.use_lightgbm,
            'mean_model_path': str(mean_model_path),
            'std_model_path': str(std_model_path)
        }
        
        if mean_model_path.exists():
            status['mean_model_size'] = os.path.getsize(mean_model_path)
            status['mean_model_modified'] = os.path.getmtime(mean_model_path)
                
        if std_model_path.exists():
            status['std_model_size'] = os.path.getsize(std_model_path)
            status['std_model_modified'] = os.path.getmtime(std_model_path)
        
        return status

    def _collect_historical_training_data(self) -> Dict[str, List]:
        """收集历史数据作为训练数据"""

        print("📊 开始收集历史训练数据...")
        
        training_data = {}
        current_date = datetime.now()
        valid_days = 0
        
        for days_back in range(180, 0, -1):
            date = current_date - timedelta(days=days_back)
            date_str = date.strftime("%Y-%m-%d")
            
            try:
                day_signals = self.data_manager.load_historical_signals(date_str)
                
                day_has_data = False
                for agent_name, signals_list in day_signals.items():
                    if agent_name not in training_data:
                        training_data[agent_name] = []
                    
                    for signal in signals_list:
                        if signal is not None:
                            training_data[agent_name].append(signal)
                            day_has_data = True
                
                if day_has_data:
                    valid_days += 1
            
            except Exception as e:
                continue
        
        # 统计训练数据
        total_samples = sum(len(signals) for signals in training_data.values())
        print(f"📈 收集了 {valid_days} 天的历史数据")
        print(f"总计 {total_samples} 个训练样本，覆盖 {len(training_data)} 个agents")
        
        return training_data

    async def _evaluate_missing_signals(self, agent_signals: Dict[str, List[Optional[SignalData]]], current_date: str):
        """评估缺失reward数据的信号（使用数据管理器计算收益率）"""
        logger.info("评估缺失的信号数据")
        
        signals_to_evaluate = []
        for agent_name, signals_list in agent_signals.items():
            for signal in signals_list:
                if signal is None:
                    continue
                if not signal.has_contest_data():
                    signal_date = signal.trigger_time.split(' ')[0]
                    signals_to_evaluate.append((signal, signal_date))
        
        if not signals_to_evaluate:
            logger.info("所有信号都已有评估数据，跳过评估步骤")
            return
        
        logger.info(f"需要评估 {len(signals_to_evaluate)} 个信号")
        
        for signal, signal_date in signals_to_evaluate:
            reward = await self.data_manager.calculate_signal_reward(signal)
            signal.contest_data = {
                'reward': reward,
                'evaluation_date': signal_date,
                'evaluation_method': 'market_return'
            }
        
        logger.info(f"评估完成: {len(signals_to_evaluate)} 个信号全部成功")

    def _predict_signal_values(self, current_date: str, agent_signals: Dict[str, List[Optional[SignalData]]], 
                             current_judge_scores: Dict[str, List[float]]) -> dict:
        """预测信号得分（夏普比率）"""
        logger.info("预测未来夏普比率")
        
        signal_scores = self.predictor.predict_signal_scores(current_date, agent_signals, current_judge_scores)
        return signal_scores
    
    async def _get_current_judge_scores(self, current_signals: Dict[str, SignalData], 
                                      trigger_time: str) -> Dict[str, List[float]]:
        """获取当天信号的judge评分"""
        logger.info(f"获取当天信号judge评分 - {len(current_signals)} 个信号")
        
        llm_config = {
            "api_key": cfg.llm.api_key,
            "api_base": cfg.llm.api_base,
            "model_name": cfg.llm.model_name
        }
        
        judge_scores = await self.signal_judger.judge_signals(
            signals=current_signals,
            trigger_time=trigger_time,
            num_judgers=5,
            llm_config=llm_config
        )
        
        logger.info(f"获得 {len(judge_scores)} 个信号的judge评分")
        return judge_scores


if __name__ == "__main__":
    async def main():
        """测试函数"""
        research_contest = ResearchContest()
        
        test_time = "2025-08-20 09:00:00"
        result = await research_contest.run_research_contest(test_time)
        
        print("\n" + "="*60)
        print("测试结果:")
        print(f"总信号数: {result.total_signals}")
        print(f"有效信号数: {result.valid_signals}")
        print(f"选择方法: {result.selection_method}")
        
        if result.optimized_weights:
            print("\n权重分配 (Top 5):")
            sorted_weights = sorted(result.optimized_weights.items(), key=lambda x: x[1], reverse=True)
            for i, (signal_name, weight) in enumerate(sorted_weights[:5]):
                if weight > 0:
                    print(f"  {i+1}. {signal_name}: {weight:.1%}")
    
    asyncio.run(main())
