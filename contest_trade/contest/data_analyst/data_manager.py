from loguru import logger
"""
Contest数据管理模块

负责：
- 加载历史因子数据
- 保存评估结果
- 文件系统操作
"""

import os
import json
import logging
import re
import sys
from pathlib import Path
from typing import List, Optional, Dict
from datetime import datetime, timedelta

# 添加项目根目录到path
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
sys.path.append(str(PROJECT_ROOT))

from utils.market_manager import GLOBAL_MARKET_MANAGER
from data_contest_types import FactorData

logger = logging.getLogger(__name__)


class ContestDataManager:
    """数据管理器"""
    
    def __init__(self, history_window_days: int, project_root: Path, target_agents: List[str] = None):
        self.history_window_days = history_window_days
        self.target_agents = target_agents or []
        self.agents_workspace_path = project_root / "agents_workspace" / "factors"
        
        if not self.agents_workspace_path.exists():
            logger.warning(f"Agents workspace不存在: {self.agents_workspace_path}")
        
        logger.info(f"数据管理器初始化 - 目标agents: {self.target_agents}")
    
    def load_historical_factors(self, current_date: str) -> Dict[str, List[Optional[FactorData]]]:
        """
        加载历史因子数据，保持agent和日期的结构
        
        Args:
            current_date: 当前日期 YYYY-MM-DD
            
        Returns:
            Dict[agent_name, List[Optional[FactorData]]]: 每个agent对应固定长度的因子列表，按时间顺序[最远->最近]，缺位为None
        """
        if not self.agents_workspace_path.exists():
            logger.warning("Agents workspace不存在，返回空字典")
            return {}
        
        # 获取历史交易日列表
        historical_dates = self.get_previous_trading_dates(current_date, self.history_window_days)
        logger.info(f"历史窗口包含 {len(historical_dates)} 个交易日")
        
        agent_factors = {}
        total_loaded = 0
        
        # 对每个目标agent，按日期顺序加载因子
        for agent_name in self.target_agents:
            agent_dir = self.agents_workspace_path / agent_name
            if not agent_dir.exists():
                logger.warning(f"Agent目录不存在: {agent_name}")
                continue
                
            factors_list = []
            for trade_date in historical_dates:
                # 构建文件名：YYYY-MM-DD_09-00-00.json
                filename = f"{trade_date}_09-00-00.json"
                file_path = agent_dir / filename
                
                if file_path.exists():
                    try:
                        factor_data = self._load_factor_from_file(file_path, agent_name)
                        if factor_data:
                            factors_list.append(factor_data)
                            total_loaded += 1
                            logger.debug(f"加载因子: {agent_name} - {trade_date}")
                        else:
                            factors_list.append(None)  # 文件存在但加载失败
                    except Exception as e:
                        logger.error(f"加载因子文件失败: {file_path} - {e}")
                        factors_list.append(None)  # 加载异常
                else:
                    factors_list.append(None)  # 文件不存在，缺位
                    logger.debug(f"因子文件不存在: {agent_name} - {trade_date}")
            
            agent_factors[agent_name] = factors_list
        
        logger.info(f"从 {self.agents_workspace_path} 加载了 {total_loaded} 个历史因子")
        return agent_factors
    
    def _load_factor_from_file(self, json_file: Path, agent_name: str) -> Optional[FactorData]:
        """从JSON文件加载单个因子数据"""
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 验证必要字段
            if 'context_string' not in data or 'trigger_time' not in data:
                logger.warning(f"因子文件缺少必要字段: {json_file}")
                return None
            
            return FactorData(
                agent_name=agent_name,
                trigger_time=data['trigger_time'],
                context_string=data['context_string'],
                file_path=str(json_file),
                contest_data=data.get('contest_data')
            )
            
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"解析JSON文件失败 {json_file}: {e}")
            return None
        except Exception as e:
            logger.error(f"加载因子数据异常 {json_file}: {e}")
            return None
    
    def save_contest_data(self, factor: FactorData, contest_data: dict) -> bool:
        """
        保存评估结果到原JSON文件
        
        Args:
            factor: 因子数据
            contest_data: 评估结果数据
            
        Returns:
            是否保存成功
        """
        if not factor.file_path:
            logger.error(f"无法保存，因子缺少file_path: {factor.agent_name}")
            return False
        
        try:
            # 读取原文件
            with open(factor.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 添加contest_data字段
            data['contest_data'] = contest_data
            
            # 写回文件
            with open(factor.file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            # 更新内存中的数据
            factor.contest_data = contest_data
            
            logger.debug(f"保存contest_data成功: {factor.agent_name}")
            return True
            
        except Exception as e:
            logger.error(f"保存contest_data失败 {factor.file_path}: {e}")
            return False
    
    
    
    def get_previous_trading_dates(self, current_date: str, count: int) -> List[str]:
        """
        获取历史交易日列表
        
        Args:
            current_date: 当前日期 YYYY-MM-DD
            count: 需要的交易日数量
            
        Returns:
            List[str]: 历史交易日列表，格式 YYYY-MM-DD，按时间升序排列
        """
        try:
            # 获取交易日列表
            trade_dates = GLOBAL_MARKET_MANAGER.get_trade_date(market_name="CN-Stock")
            
            # 转换当前日期格式为 YYYYMMDD
            current_date_yyyymmdd = current_date.replace("-", "")
            
            # 找到当前日期之前的所有交易日
            previous_dates = [dt for dt in trade_dates if dt < current_date_yyyymmdd]
            
            # 获取最近的N个交易日
            if len(previous_dates) >= count:
                selected_dates = previous_dates[-count:]
            else:
                logger.warning(f"交易日数据不足，需要{count}个，实际只有{len(previous_dates)}个")
                selected_dates = previous_dates
            
            # 转换格式 YYYYMMDD -> YYYY-MM-DD
            formatted_dates = []
            for date_str in selected_dates:
                formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
                formatted_dates.append(formatted_date)
            
            return formatted_dates
            
        except Exception as e:
            logger.error(f"获取历史交易日失败: {e}")
            # fallback: 生成最近N个工作日
            dates = []
            current = datetime.strptime(current_date, "%Y-%m-%d")
            for i in range(count):
                current -= timedelta(days=1)
                while current.weekday() >= 5:  # 跳过周末
                    current -= timedelta(days=1)
                dates.append(current.strftime("%Y-%m-%d"))
            return dates[::-1]  # 反转，按时间升序