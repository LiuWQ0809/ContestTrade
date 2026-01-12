from loguru import logger
"""
LightGBM模型训练脚本
"""

import sys
import logging
import asyncio
import traceback
from pathlib import Path
from research_contest import ResearchContest

PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.resolve()
sys.path.append(str(PROJECT_ROOT))

# 设置日志
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def async_train_research_models():
    """训练研究信号预测模型"""
    try:
        logger.info("🤖 开始训练LightGBM模型...")
        research_contest = ResearchContest()
        logger.info("📊 开始训练预测模型...")
        success = await research_contest.train_prediction_model()
        
        if success:
            logger.info("✅ 模型训练完成")
            
            logger.info("🔍 验证训练后的模型...")
            if hasattr(research_contest.predictor, 'use_lightgbm') and research_contest.predictor.use_lightgbm:
                logger.info("✅ LightGBM模型验证成功")

                model_dir = Path(__file__).parent
                logger.info(f"💾 模型已保存到: {model_dir}")
                logger.info(f"   - 均值模型: lgbm_mean_model.joblib")
                logger.info(f"   - 标准差模型: lgbm_std_model.joblib")
            else:
                logger.warning("⚠️ 无法验证LightGBM模型状态")
        else:
            logger.error("❌ 模型训练失败")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"❌ 训练过程异常: {e}")
        traceback.print_exc()
        return False

def train_research_models():
    """训练研究信号预测模型"""
    return asyncio.run(async_train_research_models())

if __name__ == "__main__":
    success = train_research_models()
    
    if success:
        print("🎉 训练完成！模型文件已保存在 lightgbm_predictor/ 目录下")
    else:
        print("\n❌ 训练失败，请检查数据和配置")