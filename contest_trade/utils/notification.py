import httpx
from loguru import logger
from config.config import cfg

async def send_pushplus_msg(title, content):
    """发送 PushPlus 微信通知"""
    token = getattr(cfg, "pushplus_token", None)
    if not token:
        logger.warning("未配置 pushplus_token，跳过微信通知")
        return False
    
    url = "http://www.pushplus.plus/send"
    data = {
        "token": token,
        "title": title,
        "content": content,
        "template": "html"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=data, timeout=10)
            if response.status_code == 200:
                logger.info(f"PushPlus 通知发送成功: {title}")
                return True
            else:
                logger.error(f"PushPlus 通知发送失败: {response.status_code}, {response.text}")
                return False
    except Exception as e:
        logger.error(f"PushPlus 通知发送异常: {e}")
        return False
