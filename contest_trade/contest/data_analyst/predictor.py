from loguru import logger
"""
Contest预测模块

负责：
- 基于历史reward数据进行预测
- 计算因子排序分数
- 支持LightGBM机器学习预测和简单均值预测
"""

import logging
import pandas as pd
import joblib
from pathlib import Path
from typing import List, Dict, Optional, Union

from data_contest_types import FactorData

logger = logging.getLogger(__name__)


class ContestPredictor:
    """因子预测器 - 集成LightGBM预测模型"""
    
    def __init__(self, history_window_days: int = 5):
        self.prediction_window_days = history_window_days
        self.use_lightgbm = False
        
        # 尝试加载LightGBM模型
        self._load_lightgbm_models()
    
    def _load_lightgbm_models(self):
        """加载LightGBM模型"""
        try:
            # 模型文件路径
            model_dir = Path(__file__).parent / "lightgbm_predictor"
            mean_model_path = model_dir / "lgbm_mean_model.joblib"
            std_model_path = model_dir / "lgbm_std_model.joblib"
            
            if mean_model_path.exists() and std_model_path.exists():
                self.model_mean = joblib.load(mean_model_path)
                self.model_std = joblib.load(std_model_path)
                self.use_lightgbm = True
                logger.info("LightGBM模型加载成功，将使用机器学习预测")
            else:
                logger.warning(f"LightGBM模型文件未找到: {model_dir}")
                self.use_lightgbm = False
                
        except Exception as e:
            logger.warning(f"LightGBM模型加载失败，将使用简单均值预测: {e}")
            self.use_lightgbm = False
    
    def _create_features_from_history(self, historical_rewards: List[Union[float, None]]) -> pd.DataFrame:
        """从历史收益中创建模型特征"""
        s = pd.Series(historical_rewards)
        
        # 使用局部中位数填充NaN
        valid_values = s.dropna()
        if not valid_values.empty:
            median_val = valid_values.median()
        else:
            median_val = 0
        s_imputed = s.fillna(median_val)
        
        # 计算特征
        features = {}
        features['reward_mean_1d'] = s_imputed.rolling(window=1).mean().iloc[-1]
        features['reward_mean_3d'] = s_imputed.rolling(window=3).mean().iloc[-1]
        features['reward_std_3d'] = s_imputed.rolling(window=3).std(ddof=0).iloc[-1]
        features['reward_mean_5d'] = s_imputed.rolling(window=5).mean().iloc[-1]
        features['reward_std_5d'] = s_imputed.rolling(window=5).std(ddof=0).iloc[-1]
        
        # 构建DataFrame
        feature_df = pd.DataFrame([features])
        feature_df = feature_df.fillna(0)
        
        feature_cols_order = [
            'reward_mean_1d', 'reward_mean_3d', 'reward_std_3d', 
            'reward_mean_5d', 'reward_std_5d'
        ]
        
        return feature_df[feature_cols_order]
    
    def _predict_single_agent_lightgbm(self, historical_rewards: List[Union[float, None]]) -> float:
        """使用LightGBM预测单个agent的得分"""
        if not self.use_lightgbm or len(historical_rewards) != 5:
            return 0.0
            
        try:
            # 创建特征
            X_pred = self._create_features_from_history(historical_rewards)
            
            # 进行预测
            pred_mean = self.model_mean.predict(X_pred)[0]
            pred_std = self.model_std.predict(X_pred)[0]
            
            # 计算夏普比率作为预测得分
            predicted_sharpe = pred_mean / max(pred_std, 0.01)
            
            return float(predicted_sharpe)
            
        except Exception as e:
            logger.warning(f"LightGBM预测失败，回退到简单均值: {e}")
            # 回退到简单均值预测
            return self._simple_mean_prediction(historical_rewards)
    
    def _simple_mean_prediction(self, historical_rewards: List[Union[float, None]]) -> float:
        """简单均值预测"""
        valid_rewards = [r for r in historical_rewards if r is not None]
        if valid_rewards:
            return sum(valid_rewards) / len(valid_rewards)
        else:
            return 0.0
    
    def predict_factor_values(self, current_date: str, agent_factors: Dict[str, List[Optional[FactorData]]]) -> Dict[str, float]:
        """
        预测因子价值分数
        
        Args:
            current_date: 当前日期 YYYY-MM-DD
            agent_factors: 结构化的历史因子数据 Dict[agent_name, List[Optional[FactorData]]]
            
        Returns:
            Dict[agent_name, predicted_value]: 预测价值分数
        """
        logger.info(f"开始预测因子排序 - 当前日期: {current_date}")
        
        try:
            # 直接从结构化数据计算历史表现
            agent_rewards = self._collect_agent_rewards(agent_factors)
            
            # 计算预测得分
            predicted_scores = self._calculate_predicted_scores(agent_rewards)
            
            logger.info(f"预测完成，{len(predicted_scores)} 个agent有预测得分")
            self._log_prediction_summary(predicted_scores)
            
            return predicted_scores
            
        except Exception as e:
            logger.error(f"预测因子排序失败: {e}")
            raise RuntimeError(f"预测失败: {e}")
    
    def _collect_agent_rewards(self, agent_factors: Dict[str, List[Optional[FactorData]]]) -> Dict[str, List[Optional[float]]]:
        """
        从结构化因子数据中提取reward数据
        
        Args:
            agent_factors: 结构化的历史因子数据 Dict[agent_name, List[Optional[FactorData]]]
            
        Returns:
            Dict[agent_name, List[Optional[reward]]]: 每个agent对应固定5天的reward列表，按时间顺序[最远->最近]，缺位为None
        """
        agent_rewards = {}
        
        for agent_name, factors_list in agent_factors.items():
            rewards_list = []
            for factor in factors_list:
                if factor is None:
                    rewards_list.append(None)  # 缺位对应None
                elif factor.has_contest_data():
                    rewards_list.append(factor.contest_data["reward"])  # 有评估数据
                else:
                    rewards_list.append(None)  # 没有评估数据，暂时为None
                    
            agent_rewards[agent_name] = rewards_list
        
        return agent_rewards
    
    def _calculate_predicted_scores(self, agent_rewards: Dict[str, List[Optional[float]]]) -> Dict[str, float]:
        """计算预测得分 - 根据可用模型选择预测方法"""
        predicted_scores = {}
        
        if self.use_lightgbm:
            logger.info("使用LightGBM模型进行预测")
            for agent_name, rewards in agent_rewards.items():
                # 使用LightGBM预测
                predicted_score = self._predict_single_agent_lightgbm(rewards)
                predicted_scores[agent_name] = predicted_score
        else:
            logger.info("使用简单均值进行预测")
            for agent_name, rewards in agent_rewards.items():
                predicted_scores[agent_name] = self._simple_mean_prediction(rewards)
        
        return predicted_scores
    
    def _log_prediction_summary(self, predicted_scores: Dict[str, float]):
        """打印预测结果摘要"""
        if not predicted_scores:
            return
        
        # 按分数排序
        sorted_scores = sorted(predicted_scores.items(), key=lambda x: x[1], reverse=True)
        
        logger.info("预测结果摘要 (Top 5):")
        for i, (agent_name, score) in enumerate(sorted_scores[:5]):
            logger.info(f"  {i+1}. {agent_name}: {score:.3f}")
    
    
