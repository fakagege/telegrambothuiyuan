import json
import time
import base64
import aiohttp
import asyncio
import logging
from typing import Dict, Any, Tuple, Optional
from tonsdk.contract.wallet import Wallets, WalletVersionEnum
from tonsdk.utils import Address, to_nano, bytes_to_b64str
from tonsdk.boc import Cell, begin_cell
from tonsdk.provider import ToncenterClient
from database import is_order_completed, record_completed_order
from config import HASH, COOKIE, MNEMONIC, ADMIN_IDS, BOT_TOKEN, API_URL, VALID_DURATIONS

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 转账锁
transfer_lock = asyncio.Lock()

# API请求限流
api_semaphore = asyncio.Semaphore(5)

async def send_to_admin(message: str, max_retries: int = 3, retry_delay: float = 2.0) -> None:
    """发送消息给所有管理员，临时禁用 SSL 验证"""
    if not ADMIN_IDS or not BOT_TOKEN:
        logger.error("无法发送消息给管理员: ADMIN_IDS 或 BOT_TOKEN 未设置")
        return

    telegram_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    headers = {"Content-Type": "application/json"}

    for admin_id in ADMIN_IDS:
        payload = {
            "chat_id": admin_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        for attempt in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        telegram_url,
                        json=payload,
                        headers=headers,
                        ssl=False
                    ) as response:
                        if response.status != 200:
                            logger.error(f"发送消息给管理员 {admin_id} 失败，状态码: {response.status}, 响应: {await response.text()}")
                        else:
                            logger.info(f"成功发送消息给管理员 {admin_id}: {message}")
                            break
            except Exception as e:
                logger.error(f"发送消息给管理员 {admin_id} 失败（尝试 {attempt + 1}/{max_retries}）: {str(e)}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
            else:
                logger.error(f"发送消息给管理员 {admin_id} 失败，已重试 {max_retries} 次")

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
        async with api_semaphore:
            data = {}
            for key in ["query", "months", "recipient", "id", "show_sender", "mode", "lv", "dh", "transaction", "confirm_method"]:
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

            max_retries = 3
            retry_delay = 2
            for attempt in range(max_retries):
                try:
                    logger.info(f"发送请求: {data['method']} (尝试 {attempt + 1}/{max_retries})")
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
                                logger.error("需要绑定钱包，请在 Fragment 网站手动绑定钱包后重试")
                                raise Exception("请求失败: 需要绑定钱包")
                            raise Exception(f"请求失败: {result['error']}")
                        return result
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"请求异常: {str(e)}")
                        raise
                    logger.warning(f"请求失败，{retry_delay}秒后重试: {str(e)}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2

    async def get_raw_request(self, id: str) -> Tuple[str, str]:
        """获取原始请求数据"""
        try:
            confirm_order_result = await self.send_request({
                "id": id,
                "show_sender": 1,
                "transaction": "1",
                "method": "getGiftPremiumLink",
                "confirm_method": "confirmReq"
            })
            logger.info(f"getGiftPremiumLink 响应: {confirm_order_result}")

            if not confirm_order_result.get("transaction"):
                raise Exception("未获取到交易信息")

            transaction = confirm_order_result["transaction"]
            message = transaction["messages"][0]
            amount = message.get("amount")
            if not amount:
                raise Exception("未获取到交易金额")
            pay_amount = f"{amount/1e9}"

            payload = message.get("payload")
            if not payload:
                raise Exception("未获取到 payload 数据")
            logger.info(f"原始 payload: {payload}")

            def correct_padding(s):
                return s + '=' * ((4 - len(s) % 4) % 4)

            try:
                decoded_bytes = base64.b64decode(correct_padding(payload))
                logger.info(f"解码字节: {decoded_bytes}")
                decoded_str = decoded_bytes.decode('utf-8', errors='ignore')
                logger.info(f"解码字符串: {decoded_str}")
            except Exception as e:
                raise Exception(f"payload 解码失败: {str(e)}")

            ref_index = decoded_str.find('#')
            if ref_index != -1:
                ref = decoded_str[ref_index + 1:].split()[0]
                return ref, pay_amount
            else:
                raise Exception("未找到 Ref# 标识")
        except Exception as e:
            logger.error(f"获取原始请求失败: {str(e)}")
            raise

    @staticmethod
    def extract_ref_from_binary(data: bytes) -> str:
        """从二进制数据中提取引用ID"""
        ref_str = ""
        hash_pos = data.find(b'#')
        if hash_pos != -1:
            hash_pos += 1
            while hash_pos < len(data) and len(ref_str) < 8:
                if chr(data[hash_pos]).isalnum():
                    ref_str += chr(data[hash_pos])
                hash_pos += 1
        return ref_str

async def get_wallet_address(mnemonic: str) -> str:
    """从助记词生成钱包地址"""
    try:
        mnemonics, pub_k, priv_k, wallet = Wallets.from_mnemonics(
            mnemonic.split(),
            WalletVersionEnum.v4r2,
            workchain=0
        )
        return wallet.address.to_string(True, True, False)
    except Exception as e:
        logger.error(f"生成钱包地址失败: {str(e)}")
        raise

async def check_wallet_balance(mnemonic: str) -> float:
    """检查钱包余额"""
    try:
        client = ToncenterClient(
            base_url='https://ton-mainnet.core.chainstack.com',
            api_key='f2a2411bce1e54a2658f2710cd7969c3'
        )
        wallet_address = await get_wallet_address(mnemonic)
        async with aiohttp.ClientSession() as session:
            balance_url = f"{client.base_url}/f2a2411bce1e54a2658f2710cd7969c3/api/v2/getAddressInformation"
            params = {"address": wallet_address}
            headers = {"accept": "application/json"}
            async with session.get(balance_url, params=params, headers=headers) as response:
                balance_response = await response.json()
                logger.info(f"余额响应: {balance_response}")
                if not balance_response.get('ok'):
                    raise Exception("获取余额请求失败")
                if 'result' not in balance_response or 'balance' not in balance_response['result']:
                    raise Exception("响应中没有找到余额信息")
                balance = int(balance_response['result']['balance'])
                balance_ton = balance / 1e9
                logger.info(f"钱包地址: {wallet_address}, 当前余额: {balance_ton} TON")
                return balance_ton
    except Exception as e:
        logger.error(f"检查钱包余额失败: {str(e)}")
        raise

async def transfer_ton(amount: str, payload: str, mnemonic: str, duration: int, telegram_id: int) -> str:
    """执行TON转账，返回交易哈希"""
    async with transfer_lock:
        logger.info("\n=== 开始转账流程 ===")
        logger.info(f"转账金额: {amount} TON")
        logger.info(f"转账数据: {payload}")

        try:
            client = ToncenterClient(
                base_url='https://ton-mainnet.core.chainstack.com',
                api_key='f2a2411bce1e54a2658f2710cd7969c3'
            )

            mnemonics, pub_k, priv_k, wallet = Wallets.from_mnemonics(
                mnemonic.split(),
                WalletVersionEnum.v4r2,
                workchain=0
            )

            wallet_address = wallet.address.to_string(True, True, False)
            logger.info(f"钱包地址: {wallet_address}")

            # 转账前再次检查余额
            balance = await check_wallet_balance(mnemonic)
            if balance < float(amount):
                wallet_address = await get_wallet_address(mnemonic)
                raise Exception(f"余额不足: {balance:.9f} TON < {amount} TON，请向 {wallet_address} 充值")

            seqno_url = f"{client.base_url}/f2a2411bce1e54a2658f2710cd7969c3/api/v2/runGetMethod"
            seqno_data = {
                "address": wallet_address,
                "method": "seqno",
                "stack": []
            }
            headers = {"accept": "application/json", "content-type": "application/json"}
            async with aiohttp.ClientSession() as session:
                async with session.post(seqno_url, json=seqno_data, headers=headers) as response:
                    seqno_response = await response.json()
                    logger.info(f"序列号响应: {seqno_response}")
                    stack = seqno_response['result']['stack']
                    seqno = 0 if not stack else int(stack[0][1].replace('0x', ''), 16)

            duration_text = "1 year" if duration == 12 else f"{duration} months"
            comment = f"Telegram Premium for {duration_text} \n\nRef#{payload}"
            message = begin_cell()\
                .store_uint(0, 32)\
                .store_string(comment)\
                .end_cell()

            to_address = Address("EQBAjaOyi2wGWlk-EDkSabqqnF-MrrwMadnwqrurKpkla9nE")
            logger.info(f"目标地址: {to_address.to_string()}")

            transfer = wallet.create_transfer_message(
                to_addr=to_address,
                amount=to_nano(float(amount), 'ton'),
                payload=message,
                seqno=seqno,
                send_mode=3
            )

            if hasattr(transfer['message'], 'bounce'):
                transfer['message'].bounce = False
            else:
                new_message = begin_cell()\
                    .store_uint(0, 32)\
                    .store_string(comment)\
                    .end_cell()
                new_message.bounce = False
                transfer = wallet.create_transfer_message(
                    to_addr=to_address,
                    amount=to_nano(float(amount), 'ton'),
                    payload=new_message,
                    seqno=seqno,
                    send_mode=3
                )

            boc = transfer['message'].to_boc(False)
            boc_base64 = base64.b64encode(boc).decode('utf-8')

            max_retries = 3
            retry_delay = 2
            send_boc_url = f"https://ton-mainnet.core.chainstack.com/f2a2411bce1e54a2658f2710cd7969c3/api/v2/sendBoc"
            headers = {"accept": "application/json", "content-type": "application/json"}
            payload = {"boc": boc_base64}

            tx_hash = None
            for attempt in range(max_retries):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(send_boc_url, json=payload, headers=headers) as response:
                            send_response = await response.json()
                            logger.info(f"发送交易响应: {send_response}")
                            if not send_response.get('ok'):
                                error_msg = send_response.get('error', '未知错误')
                                if 'rate limit' in str(error_msg).lower():
                                    if attempt < max_retries - 1:
                                        logger.warning(f"遇到速率限制，等待 {retry_delay} 秒后重试...")
                                        await asyncio.sleep(retry_delay)
                                        retry_delay *= 2
                                        continue
                                raise Exception(f"发送交易失败: {error_msg}")
                            
                            tx_hash = send_response.get('result', {}).get('hash', '')
                            if not tx_hash:
                                extra = send_response.get('result', {}).get('@extra', '')
                                if extra:
                                    tx_hash = extra.split(':')[0]
                                else:
                                    tx_hash = 'unknown'
                            logger.info(f"交易发送成功! 交易hash: {tx_hash}")
                            logger.info(f"查看交易: https://tonscan.org/tx/{tx_hash}")
                            break
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise Exception(f"发送交易失败，已重试 {max_retries} 次: {str(e)}")
                    logger.warning(f"发送交易失败，{retry_delay} 秒后重试: {str(e)}")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2

            # 验证转账状态
            await asyncio.sleep(5)
            client = ToncenterClient(
                base_url='https://ton-mainnet.core.chainstack.com',
                api_key='f2a2411bce1e54a2658f2710cd7969c3'
            )
            tx_status = await client.get_transactions(wallet_address, limit=1)
            if not tx_status['transactions'] or tx_status['transactions'][0].get('transaction_id', {}).get('hash') != tx_hash:
                raise Exception("转账状态验证失败，交易未确认")
            logger.info(f"转账状态验证成功，交易hash: {tx_hash}")

            return tx_hash

        except Exception as e:
            logger.error(f"转账失败: {str(e)}")
            raise

async def activate_premium(username: str, duration: int, telegram_id: int) -> Tuple[bool, str, Optional[float]]:
    """为主函数提供给bot调用，激活Premium订阅"""
    if not all([username, duration, HASH, COOKIE, MNEMONIC, ADMIN_IDS, BOT_TOKEN]):
        error_msg = "错误: 缺少必要参数（用户名、持续时间、ResHash、ResCookie、WalletMnemonic、ADMIN_IDS、BOT_TOKEN）"
        logger.error(error_msg)
        await send_to_admin(error_msg)
        return False, "激活失败，请稍后重试或联系客服", None

    if duration not in VALID_DURATIONS:
        error_msg = f"错误: 持续时间必须为3、6或12个月，收到: {duration}"
        logger.error(error_msg)
        await send_to_admin(error_msg)
        return False, "无效的订阅时长，请选择3、6或12个月", None

    async with PaymentService() as ps:
        try:
            # 第一步：获取对方信息
            logger.info("\n=== 第一步：获取对方信息 ===")
            result1 = await ps.send_request({
                "query": username,
                "months": duration,
                "method": "searchPremiumGiftRecipient"
            })
            logger.info(f"searchPremiumGiftRecipient 完整响应: {result1}")
            recipient = result1["found"]["recipient"]
            userName = result1["found"].get("name", "未知")
            logger.info(f"用户昵称: {userName}")
            logger.info(f"唯一标识: {recipient}")
            await asyncio.sleep(1)

            # 第二步：获取预估价格（用于余额检查）
            logger.info("\n=== 第二步：获取预估价格 ===")
            price_check = await ps.send_request({
                "recipient": recipient,
                "months": duration if duration < 12 else "12",
                "method": "initGiftPremiumRequest"
            })
            logger.info(f"price_check 响应: {price_check}")
            estimated_amount = float(price_check["amount"])
            logger.info(f"Telegram Premium {duration if duration < 12 else '1 year'} 价格: {estimated_amount} TON")

            # 第三步：检查钱包余额是否足够
            logger.info("\n=== 检查钱包余额 ===")
            balance = await check_wallet_balance(MNEMONIC)
            duration_text = "1 year" if duration == 12 else f"{duration} months"
            if balance < estimated_amount:
                wallet_address = await get_wallet_address(MNEMONIC)
                admin_message = f"余额不足: 当前余额为 {balance:.9f} TON，所需为 {estimated_amount:.2f} TON\n请向地址 `{wallet_address}` 充值后重试（使用 Tonkeeper、OKX、Binance 等）\n用户: {username}\n时长: {duration_text}"
                await send_to_admin(admin_message)
                logger.warning(f"余额不足，已通知管理员: {admin_message}")
                raise Exception("钱包余额不足")

            # 第四步：正式创建订单（新的 req_id）
            logger.info("\n=== 创建正式订单 ===")
            result2 = await ps.send_request({
                "recipient": recipient,
                "months": duration if duration < 12 else "12",
                "method": "initGiftPremiumRequest"
            })
            logger.info(f"initGiftPremiumRequest 完整响应: {result2}")
            req_id = result2["req_id"]
            amount = float(result2["amount"])
            logger.info(f"订单号: {req_id}")
            logger.info(f"金额(TON): {amount}")

            # 提前检查订单是否已完成
            if await is_order_completed(req_id):
                logger.warning(f"订单 {req_id} 已被处理，跳过重复操作")
                return True, f"用户 {username} 的 {duration_text} Premium 已激活，无需重复操作", amount

            await asyncio.sleep(1)

            # 更新Premium状态
            logger.info("\n=== 更新Premium状态 ===")
            await ps.send_request({
                "mode": "new",
                "lv": "false",
                "dh": "1761547136",
                "method": "updatePremiumState"
            })
            await asyncio.sleep(1)

            # 第五步：确认支付订单
            logger.info("\n=== 确认支付订单 ===")
            confirm_order_result = await ps.send_request({
                "id": req_id,
                "transaction": "1",
                "show_sender": 1,
                "method": "getGiftPremiumLink",
                "confirm_method": "confirmReq"
            })
            logger.info(f"getGiftPremiumLink 完整响应: {confirm_order_result}")

            # 第六步：解码订单数据
            logger.info("\n=== 解码订单数据 ===")
            payload, pay_amount = await ps.get_raw_request(req_id)
            if not payload:
                error_msg = "未获取到有效 payload"
                logger.error(error_msg)
                await send_to_admin(error_msg)
                raise Exception("无法获取订单数据")

            logger.info(f"支付金额: {pay_amount} TON")
            logger.info(f"订单数据: Telegram Premium for {duration_text} \n\nRef#{payload}")

            # 第七步：执行转账
            logger.info("\n=== 执行转账 ===")
            tx_hash = await transfer_ton(pay_amount, payload, MNEMONIC, duration, telegram_id)
            success_msg = f"成功为 {username} 激活 {duration_text} 的 Telegram Premium！金额: {pay_amount} TON，订单号: {req_id}"
            logger.info(success_msg)

            # 记录成功订单
            await record_completed_order(req_id, telegram_id, username, duration, float(pay_amount), tx_hash)

            return True, success_msg, float(pay_amount)

        except Exception as e:
            duration_text = "1 year" if duration == 12 else f"{duration} months"
            if str(e).startswith("请求失败: No Telegram users found"):
                error_msg = f"用户名 {username} 不存在"
                logger.error(error_msg)
                await send_to_admin(f"无法为用户 {username} 激活 Premium：用户名不存在")
                return False, "您输入的用户名不存在，开通会员失败", None
            error_msg = f"激活 Premium 失败: {str(e)}"
            logger.error(error_msg)
            await send_to_admin(f"激活 Premium 失败，用户: {username}, 错误: {str(e)}")
            return False, "激活失败，请稍后重试或联系管理员", None

if __name__ == "__main__":
    import os
    USER_NAME = os.getenv("OpenUserName")
    DURATION = os.getenv("OpenDuration")
    if not USER_NAME or not DURATION:
        logger.error("错误: OpenUserName 和 OpenDuration 必须在环境变量中设置")
        exit(1)
    try:
        DURATION = int(DURATION)
    except ValueError:
        logger.error("错误: OpenDuration 必须为数字")
        exit(1)
    asyncio.run(activate_premium(USER_NAME, DURATION, 0))