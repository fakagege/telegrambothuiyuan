import logging
import random
import requests
import asyncio
import re
from telegram.ext import ContextTypes
from telegram.error import TelegramError
from config import ADMIN_IDS, PAYMENT_ADDRESS
from database import get_db, get_balance, set_balance
from io import BytesIO
from telegram import InlineKeyboardMarkup, InlineKeyboardButton
import qrcode
from PIL import Image
from datetime import datetime, timedelta
import json

# 配置日志
logger = logging.getLogger(__name__)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger.info(f"Imported get_db: {get_db}")

# USDT 相关函数
def validate_trc20_address(address: str) -> bool:
    """验证 TRC20 地址格式（简单正则检查，长度34，T开头）"""
    if not isinstance(address, str):
        return False
    pattern = r'^T[0-9a-zA-Z]{33}$'
    return bool(re.match(pattern, address))

def get_payment_address() -> str:
    """读取 USDT 收款地址从 welcome.json"""
    try:
        with open("welcome.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            payment_address = data.get("payment_address")
            if payment_address and validate_trc20_address(payment_address):
                logger.info(f"成功从 welcome.json 读取支付地址: {payment_address}")
                return payment_address
            else:
                logger.error(f"welcome.json 中的支付地址无效: {payment_address}")
                raise ValueError("无效的支付地址在 welcome.json")
    except FileNotFoundError:
        logger.error("welcome.json 文件未找到")
        raise FileNotFoundError("需要 welcome.json 文件包含有效的 payment_address")
    except json.JSONDecodeError:
        logger.error("welcome.json 格式错误")
        raise ValueError("welcome.json 格式错误")
    except Exception as e:
        logger.error(f"读取 welcome.json 出错: {e}")
        raise RuntimeError(f"无法读取支付地址: {e}")

async def generate_unique_usdt_amount(base_amount: float) -> float:
    """生成唯一的 USDT 订单金额，偏移量在 0.001 到 0.01 之间"""
    while True:
        offset = random.uniform(0.001, 0.01)
        unique_amount = round(base_amount + offset, 4)

        async with get_db() as (conn, cursor):
            await cursor.execute(
                "SELECT 1 FROM usdtpay WHERE amount = %s AND status = 0",
                (unique_amount,)
            )
            if not await cursor.fetchone():
                return unique_amount

async def save_usdt_order_to_db(telegram_id: int, out_trade_no: str, created_at: datetime, expires_at: datetime, amount: float):
    """保存 USDT 订单到数据库"""
    try:
        async with get_db() as (conn, cursor):
            await cursor.execute(
                """
                INSERT INTO usdtpay (telegram_id, out_trade_no, created_at, expires_at, status, amount)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (telegram_id, out_trade_no, created_at, expires_at, 0, amount),
            )
            await conn.commit()
            logger.info(f"成功保存 USDT 订单: {out_trade_no}, 金额: {amount}")
    except Exception as e:
        logger.error(f"保存 USDT 订单到数据库时发生错误: {e}")
        raise

async def cancel_usdt_order(out_trade_no: str) -> bool:
    """取消 USDT 订单"""
    try:
        async with get_db() as (conn, cursor):
            await cursor.execute(
                "UPDATE usdtpay SET status = 2 WHERE out_trade_no = %s AND status = 0", (out_trade_no,)
            )
            success = cursor.rowcount > 0
            await conn.commit()
            logger.info(f"取消订单 {out_trade_no}: {'成功' if success else '失败'}")
            return success
    except Exception as e:
        logger.error(f"取消 USDT 订单时发生错误: {e}")
        return False

async def handle_deposit(update, context, amount_text: str) -> tuple[bool, str]:
    """处理充值请求，生成订单和QR码"""
    user_id = update.effective_user.id
    logger.info(f"get_db in handle_deposit: {get_db}")
    try:
        money = amount_text.strip()
        if not money.replace(".", "", 1).isdigit() or float(money) <= 0:
            return False, "请输入有效的充值金额（正数）。"

        amount = float(money)
        # 检查未处理订单数量
        async with get_db() as (conn, cursor):
            await cursor.execute(
                "SELECT COUNT(*) FROM usdtpay WHERE telegram_id = %s AND status = 0", (user_id,)
            )
            pending_orders_count = (await cursor.fetchone())[0]
            if pending_orders_count >= 3:
                return False, "❗ 你有超过3条未处理的充值订单，请先完成或取消这些订单！"

        # 获取支付地址
        payment_address = get_payment_address()

        # 生成唯一金额和订单
        unique_amount = await generate_unique_usdt_amount(amount)
        out_trade_no = f"usdt_{user_id}_{int(update.message.date.timestamp())}"
        created_at = datetime.now()
        expires_at = created_at + timedelta(minutes=20)

        # 保存订单
        await save_usdt_order_to_db(user_id, out_trade_no, created_at, expires_at, unique_amount)

        # 生成QR码
        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=10,
            border=4,
        )
        qr.add_data(payment_address)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        bio = BytesIO()
        bio.name = "payment_qr.png"
        img.save(bio, "PNG")
        bio.seek(0)

        await update.message.reply_photo(
            photo=bio,
            caption=(
                f"✅ <b>已为你成功创建充值订单</b>\n\n"
                f"❗❗❗ <b>请仔细核对地址，仔细核对需要支付的金额包括小数点</b> ❗❗❗\n\n"
                f"✅ 收款地址：<code>{payment_address}</code>（点击自动复制）\n\n"
                f"💸 订单金额：<code>{unique_amount}</code>（点击复制金额）\n\n"
                f"⏰ 创建时间：{created_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"⏰ 过期时间：{expires_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            ),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("关闭取消订单", callback_data=f"cancel_{out_trade_no}")]]
            ),
        )
        return True, "充值订单已创建，请完成支付！"
    except Exception as e:
        logger.error(f"处理充值时发生错误: {e}")
        return False, f"发生错误: {e}"

async def check_input_usdt(context: ContextTypes.DEFAULT_TYPE):
    """检查 USDT 支付订单状态"""
    logger.info("USDT 订单检查开始")
    base_time = int(datetime.now().timestamp() * 1000) - 700 * 1000

    try:
        payment_address = get_payment_address()
        response = requests.get(
            f"https://apilist.tronscan.org/api/contract/events"
            f"?address={payment_address}"
            f"&start=0&limit=20&start_timestamp={base_time}"
            f"&contract=TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
        )

        if response.status_code == 200 and len(response.json().get('data', [])) > 0:
            tasks = []
            for event in response.json()['data']:
                if event['transferToAddress'] != payment_address:
                    continue
                trc20_balance = float(event['amount']) / 1000000
                tasks.append(handle_usdt_payment(trc20_balance, context))
            await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"检查 USDT 支付订单时发生错误: {e}")

async def handle_usdt_payment(trc20_balance: float, context: ContextTypes.DEFAULT_TYPE):
    """处理每个 USDT 支付的逻辑"""
    try:
        async with get_db() as (conn, cursor):
            await cursor.execute(
                "SELECT telegram_id, out_trade_no FROM usdtpay WHERE amount = %s AND status = 0",
                (trc20_balance,),
            )
            order = await cursor.fetchone()

            if order:
                telegram_id, out_trade_no = order
                await cursor.execute("UPDATE usdtpay SET status = 1 WHERE out_trade_no = %s", (out_trade_no,))
                await cursor.execute(
                    "UPDATE user_balances SET balance = balance + %s WHERE telegram_id = %s",
                    (trc20_balance, telegram_id),
                )
                await cursor.execute(
                    """
                    INSERT INTO okusdt (telegram_id, out_trade_no, created_at, amount)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (telegram_id, out_trade_no, datetime.now(), trc20_balance),
                )
                await conn.commit()

                await context.bot.send_message(
                    chat_id=telegram_id,
                    text=f"✅ 余额充值成功，你的余额已更新！\n\n✅ 已收到你的充值金额：{trc20_balance} USDT",
                )
                logger.info(f"用户 {telegram_id} 余额更新，增加 {trc20_balance} USDT")

                # 通知管理员
                for admin_id in ADMIN_IDS:
                    try:
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=f"用户 {telegram_id} 成功充值 {trc20_balance} USDT。",
                        )
                    except Exception as e:
                        logger.error(f"通知管理员 {admin_id} 失败: {e}")
    except Exception as e:
        logger.error(f"处理 USDT 支付时发生错误: {e}")

async def cleanup_expired_orders(context: ContextTypes.DEFAULT_TYPE):
    """清理过期 USDT 订单"""
    logger.info("=== [开始清理] 清理过期 USDT 订单 ===")
    try:
        async with get_db() as (conn, cursor):
            await cursor.execute("SELECT out_trade_no, telegram_id, expires_at, status FROM usdtpay")
            all_orders = await cursor.fetchall()

            logger.info(f"[读取订单] 共获取订单数：{len(all_orders)}")
            to_delete = []

            for out_trade_no, telegram_id, expires_at, status in all_orders:
                if status != 0:
                    continue

                # 确保 expires_at 是 datetime 对象
                if isinstance(expires_at, str):
                    expires_at = datetime.fromisoformat(expires_at)
                elif not isinstance(expires_at, datetime):
                    logger.error(f"无效的 expires_at 类型: {type(expires_at)} for order {out_trade_no}")
                    continue

                now_local = datetime.now()

                logger.debug(
                    f"[检查订单] {out_trade_no} 用户 {telegram_id} - 过期时间(HKT): {expires_at}, 当前时间(HKT): {now_local}"
                )

                if expires_at < now_local:
                    logger.info(f"[已过期] 订单 {out_trade_no} 应该被清理")
                    notified = False
                    retries = 0
                    while not notified and retries < 3:
                        try:
                            await context.bot.send_message(
                                chat_id=telegram_id,
                                text=f"❗ 你的 USDT 支付订单已过期\n订单号 {out_trade_no}\n已为你清理，可重新创建充值订单。",
                            )
                            notified = True
                            logger.info(f"[通知成功] 用户 {telegram_id}, 订单 {out_trade_no}")
                        except TelegramError as e:
                            err_text = str(e).lower()
                            if "forbidden" in err_text or "bot was blocked" in err_text or getattr(e, "status_code", None) == 403:
                                logger.warning(f"[用户拉黑] 无法通知用户 {telegram_id}，订单 {out_trade_no}，错误：{e}")
                                notified = True
                            else:
                                retries += 1
                                logger.warning(f"[重试通知] 第 {retries} 次 - 用户 {telegram_id}，订单 {out_trade_no}，错误：{e}")
                                if retries < 3:
                                    await asyncio.sleep(5)
                                else:
                                    logger.error(f"[通知失败] 用户 {telegram_id}（订单 {out_trade_no}），已达到最大重试次数 (3)")
                    to_delete.append(out_trade_no)
                else:
                    logger.info(f"[未过期] 订单 {out_trade_no}，expires_at={expires_at}, 当前 HKT={now_local}")

            if to_delete:
                await cursor.executemany("DELETE FROM usdtpay WHERE out_trade_no = %s", [(trade_no,) for trade_no in to_delete])
                await conn.commit()
                logger.info(f"[删除完成] 已删除过期订单：{to_delete}")
            else:
                logger.info("[删除完成] 本次没有需要删除的过期订单。")

        logger.info("✅ 已完成清理过期 USDT 支付订单。")
    except Exception as e:
        logger.error(f"[清理异常] 清理过期 USDT 订单时发生未知错误: {e}")

async def cleanup_old_okusdt_orders(context: ContextTypes.DEFAULT_TYPE):
    """清理超过七天的 okusdt 订单"""
    try:
        async with get_db() as (conn, cursor):
            seven_days_ago = datetime.now() - timedelta(days=7)
            await cursor.execute("DELETE FROM okusdt WHERE created_at < %s", (seven_days_ago,))
            await conn.commit()
            logger.info("已清理超过七天的 okusdt 订单。")
    except Exception as e:
        logger.error(f"清理 okusdt 订单时发生错误: {e}")