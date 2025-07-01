import json
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

# 配置日志
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# 加载环境变量
load_dotenv()

# Telegram Bot 配置
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN 未在环境变量中设置")
    raise ValueError("需要设置 BOT_TOKEN 环境变量")

ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_ID", "").split(",") if id.isdigit()]
if not ADMIN_IDS:
    logger.warning("ADMIN_ID 未设置，管理员列表为空")

CUSTOMER_SUPPORT = os.getenv("CUSTOMER_SUPPORT", "@YourCustomerSupport")

# MySQL 数据库配置
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "bot_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
if not MYSQL_PASSWORD:
    logger.error("MYSQL_PASSWORD 未在环境变量中设置")
    raise ValueError("需要设置 MYSQL_PASSWORD 环境变量")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "bot_database")

# TON 和 Fragment API 配置
HASH = os.getenv("ResHash")
COOKIE = os.getenv("ResCookie")
MNEMONIC = os.getenv("WalletMnemonic")
API_URL = f"https://fragment.com/api?hash={HASH}" if HASH else None
VALID_DURATIONS = {3, 6, 12}

# 从 welcome.json 加载配置
CONFIG_FILE = Path("welcome.json")
try:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
        PRICES = {
            3: float(config["prices"]["3_months"]),
            6: float(config["prices"]["6_months"]),
            12: float(config["prices"]["12_months"]),
        }
        CUSTOMER_SUPPORT = config.get("customer_support", CUSTOMER_SUPPORT)
        PAYMENT_ADDRESS = config.get("payment_address")
        if not PAYMENT_ADDRESS:
            logger.error("welcome.json 中缺少 payment_address 字段")
            raise KeyError("welcome.json 中缺少 payment_address 字段")
    logger.info("成功加载 welcome.json 配置")
except FileNotFoundError:
    logger.error(f"{CONFIG_FILE} 文件未找到")
    raise FileNotFoundError(f"需要 {CONFIG_FILE} 文件")
except json.JSONDecodeError:
    logger.error(f"{CONFIG_FILE} 格式错误")
    raise ValueError(f"{CONFIG_FILE} 格式错误")
except KeyError as e:
    logger.error(f"welcome.json 缺少必要字段: {e}")
    raise KeyError(f"welcome.json 缺少字段: {e}")


def update_config_partial(key_path, value):
    """部分更新 welcome.json 文件"""
    try:
        # 读取现有配置
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        # 根据 key_path 更新指定字段
        if key_path in ["3_months", "6_months", "12_months"]:
            config["prices"][key_path] = float(value)
        elif key_path == "payment_address":
            config["payment_address"] = value
        elif key_path == "customer_support":
            config["customer_support"] = value
        else:
            raise ValueError(f"无效的配置字段: {key_path}")
        
        # 写入更新后的配置
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        logger.info(f"成功更新 welcome.json 的 {key_path} 字段")
        return True, config
    except Exception as e:
        logger.error(f"更新 welcome.json 失败: {str(e)}")
        return False, None