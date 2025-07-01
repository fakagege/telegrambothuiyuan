import aiohttp
import asyncio
import logging
from typing import Dict, Any
from dotenv import load_dotenv
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 加载环境变量
load_dotenv()

# 公共变量
HASH = os.getenv("ResHash")
COOKIE = os.getenv("ResCookie")
ADMIN_ID = os.getenv("ADMIN_ID")
BOT_TOKEN = os.getenv("BOT_TOKEN")
API_URL = f"https://fragment.com/api?hash={HASH}"

async def send_to_admin(message: str) -> None:
    """向管理员发送消息，临时禁用SSL验证"""
    if not ADMIN_ID or not BOT_TOKEN:
        logger.error("无法发送消息给管理员：未设置 ADMIN_ID 或 BOT_TOKEN")
        return

    telegram_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": ADMIN_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    headers = {"Content-Type": "application/json"}

    logger.warning("为调试禁用SSL验证，请尽快修复证书问题")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                telegram_url,
                json=payload,
                headers=headers,
                ssl=False
            ) as response:
                if response.status != 200:
                    logger.error(f"发送消息给管理员失败，状态码: {response.status}, 响应: {await response.text()}")
                else:
                    logger.info(f"成功发送消息给管理员: {message}")
    except Exception as e:
        logger.error(f"发送消息给管理员失败: {str(e)}")

class PaymentService:
    def __init__(self):
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def send_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """发送请求到Fragment API"""
        data = {}
        for key in ["query", "months", "method"]:
            if key in payload and payload[key] is not None:
                data[key] = str(payload[key]) if isinstance(payload[key], (int, bool)) else payload[key]
        data["method"] = payload["method"]

        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "accept-language": "zh-CN,zh;q=0.9",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Cookie": COOKIE,
            "origin": "https://fragment.com",
            "priority": "u=1, i",
            "referer": "https://fragment.com/premium/gift",
            "sec-ch-ua": '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest"
        }

        try:
            logger.info(f"发送请求: {data['method']}")
            async with self.session.post(API_URL, data=data, headers=headers) as response:
                status = response.status
                text = await response.text()
                logger.info(f"响应状态码: {status}, 原始响应: {text}")
                try:
                    result = await response.json()
                except ValueError:
                    raise Exception(f"响应非JSON格式: {text}")
                if result.get("error"):
                    if result.get("need_verify"):
                        logger.error("需要绑定钱包，请在Fragment网站绑定钱包")
                        raise Exception("请求失败：需要绑定钱包")
                    raise Exception(f"请求失败: {result['error']}")
                return result
        except Exception as e:
            logger.error(f"请求失败: {str(e)}")
            raise

async def check_username_exists(username: str) -> bool:
    """检查Telegram用户名是否存在，使用Fragment API"""
    if not username:
        logger.error("用户名不能为空")
        return False

    # 规范化用户名：移除开头的'@'（如果存在）
    username = username.lstrip('@')

    async with PaymentService() as ps:
        try:
            result = await ps.send_request({
                "query": username,
                "months": 3,  # 检查时的默认时长，不影响验证
                "method": "searchPremiumGiftRecipient"
            })
            logger.info(f"用户名 {username} 检查响应: {result}")
            return bool(result.get("found") and result["found"].get("recipient"))
        except Exception as e:
            if str(e).startswith("请求失败: No Telegram users found"):
                logger.info(f"用户名 {username} 不存在")
                return False
            logger.error(f"检查用户名 {username} 失败: {str(e)}")
            await send_to_admin(f"检查用户名 {username} 失败: {str(e)}")
            return False