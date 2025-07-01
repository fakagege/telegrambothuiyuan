import logging
import re
import uuid
import pytz
import os
import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from telegram import Update, BotCommand, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ConversationHandler,
    filters,
    ContextTypes,
)
from telegram.error import TelegramError
from config import BOT_TOKEN, ADMIN_IDS, PRICES, CUSTOMER_SUPPORT, PAYMENT_ADDRESS, update_config_partial
from database import (
    init_db,
    get_db,
    get_balance,
    set_balance,
    generate_order_id,
    record_purchase_order,
    get_purchase_history,
    is_order_completed,
    record_completed_order,
    close_pool,
    add_to_premium_queue,
    get_next_queue_task,
    update_queue_task_status,
)
from premium_service import activate_premium
from userjiance import check_username_exists
from usdtpay import (
    handle_deposit as usdt_handle_deposit,
    cancel_usdt_order,
    check_input_usdt,
    cleanup_expired_orders,
    cleanup_old_okusdt_orders,
)

# 配置日志
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# 菜单定义
MAIN_KEYBOARD = [["💎购买会员", "📨购买记录"], ["💸余额充值", "👤个人中心"]]
MAIN_MARKUP = ReplyKeyboardMarkup(MAIN_KEYBOARD, resize_keyboard=True, one_time_keyboard=False)

# 用户锁
user_locks = {}

# 状态定义
class States(Enum):
    PURCHASE_ENTER_USERNAME = "purchase_enter_username"
    PURCHASE_SELECT_DURATION = "purchase_select_duration"
    DEPOSIT_AWAITING_AMOUNT = "deposit_awaiting_amount"

# 错误次数限制
MAX_INVALID_ATTEMPTS = 3

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """处理 /start 命令，初始化用户并显示主菜单"""
    user_id = update.effective_user.id
    user = update.effective_user
    username = user.username if user.username else str(user_id)
    async with get_db() as (conn, cursor):
        await cursor.execute(
            """
            INSERT INTO user_balances (telegram_id, username, balance)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE username = %s
            """,
            (user_id, username, Decimal('0.0'), username)
        )
        await conn.commit()
    logger.info(f"用户 {user_id} (@{username}) 启动机器人")
    await update.message.reply_text(
        f"欢迎 @{username} 使用飞机VIP自助开通机器人！\n"
        "💎 官方直充，掉单包补。\n"
        "💎 自助秒开，假一赔十。\n\n"
        "请选择以下选项：",
        reply_markup=MAIN_MARKUP
    )
    context.user_data.clear()  # 清理状态
    return ConversationHandler.END

async def process_purchase(user_id: int, username: str, duration: int, actual_amount: float, conn, cursor) -> tuple[str, Decimal]:
    """原子化处理购买逻辑"""
    try:
        await conn.begin()
        balance = await get_balance(user_id)
        if balance < Decimal(str(actual_amount)):
            raise ValueError(f"余额不足: 当前 {balance:.3f} USDT, 需 {actual_amount:.3f} USDT")
        new_balance = await set_balance(user_id, -actual_amount, conn, cursor)
        order_id = await record_purchase_order(user_id, username, duration, actual_amount, "成功", conn, cursor)
        await conn.commit()
        return order_id, new_balance
    except Exception as e:
        await conn.rollback()
        raise

async def update_admin_config(text: str, update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """处理管理员配置更新和余额调整"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        logger.info(f"用户 {user_id} 尝试管理员操作，但不在 ADMIN_IDS 中")
        return False

    logger.info(f"管理员 {user_id} 输入: {text}")

    # 配置更新模式
    config_patterns = [
        (r"修改3个月价格\s+(\d+\.?\d*)", "3_months", float),
        (r"修改6个月价格\s+(\d+\.?\d*)", "6_months", float),
        (r"修改12个月价格\s+(\d+\.?\d*)", "12_months", float),
        (r"修改USDT地址\s+(\S+)", "payment_address", str),
        (r"修改客服联系方式\s+(\S+)", "customer_support", str),
    ]
    for pattern, key, value_type in config_patterns:
        match = re.match(pattern, text)
        if match:
            try:
                value = value_type(match.group(1))
                success, new_config = update_config_partial(key, value)
                if success:
                    global PRICES, PAYMENT_ADDRESS, CUSTOMER_SUPPORT
                    PRICES = {
                        3: float(new_config["prices"]["3_months"]),
                        6: float(new_config["prices"]["6_months"]),
                        12: float(new_config["prices"]["12_months"]),
                    }
                    PAYMENT_ADDRESS = new_config["payment_address"]
                    CUSTOMER_SUPPORT = new_config["customer_support"]
                    await update.message.reply_text(
                        f"✅ {key} 更新成功！\n"
                        f"当前配置：\n"
                        f"3个月价格: {PRICES[3]:.2f} USDT\n"
                        f"6个月价格: {PRICES[6]:.2f} USDT\n"
                        f"12个月价格: {PRICES[12]:.2f} USDT\n"
                        f"USDT充值地址: {PAYMENT_ADDRESS}\n"
                        f"客服联系方式: {CUSTOMER_SUPPORT}",
                        reply_markup=MAIN_MARKUP
                    )
                else:
                    await update.message.reply_text(
                        f"❌ 更新 {key} 失败，请稍后重试或联系技术支持！",
                        reply_markup=MAIN_MARKUP
                    )
            except Exception as e:
                logger.error(f"更新 {key} 失败: {str(e)}", exc_info=True)
                await update.message.reply_text(
                    f"❌ 输入格式错误或处理失败: {str(e)}",
                    reply_markup=MAIN_MARKUP
                )
            context.user_data.clear()  # 清理状态
            return True

    # 余额调整模式
    match = re.match(r"用户(\d+)\s*([+-])(\d+(\.\d+)?)", text)
    if match:
        target_id = int(match.group(1))
        operation = match.group(2)
        amount = float(match.group(3))
        amount = amount if operation == "+" else -amount
        logger.info(f"管理员 {user_id} 调整用户 {target_id} 余额，金额: {amount}")

        async with get_db() as (conn, cursor):
            await conn.begin()
            try:
                new_balance = await set_balance(target_id, amount, conn, cursor)
                await conn.commit()
                logger.info(f"用户 {target_id} 余额更新成功，金额: {amount}, 新余额: {new_balance}")
            except Exception as e:
                await conn.rollback()
                logger.error(f"余额更新失败，用户 {target_id}: {str(e)}", exc_info=True)
                await update.message.reply_text(
                    f"❌ 余额调整失败: {str(e)}",
                    reply_markup=MAIN_MARKUP
                )
                context.user_data.clear()  # 清理状态
                return True

        # 管理员提示
        admin_response = f"已调整用户 {target_id} 余额 {amount:+.3f} USDT，新余额 {new_balance:.3f} USDT"
        await update.message.reply_text(admin_response, reply_markup=MAIN_MARKUP)

        # 用户端提示
        if amount > 0:
            user_response = f"✅ 管理员给您加 {abs(amount):.3f} USDT 成功！新余额: {new_balance:.3f} USDT"
        else:
            user_response = f"✅ 管理员为您扣除 {abs(amount):.3f} USDT 成功！新余额: {new_balance:.3f} USDT"
        try:
            await context.bot.send_message(
                chat_id=target_id,
                text=user_response,
                reply_markup=MAIN_MARKUP
            )
            logger.info(f"通知用户 {target_id} 成功")
        except TelegramError as e:
            logger.warning(f"无法通知用户 {target_id}: {str(e)}")

        logger.info(f"管理员 {user_id} 调整用户 {target_id} 余额成功，金额: {amount}, 新余额: {new_balance}")
        context.user_data.clear()  # 清理状态
        return True

    logger.info(f"管理员 {user_id} 输入无效格式: {text}")
    await update.message.reply_text(
        f"❌ 无效的管理员命令！请使用正确格式，例如：\n"
        f"用户<telegram_id> +<金额> 或 用户<telegram_id> -<金额>\n"
        f"示例：用户6911326582 +1000 或 用户6911326582 -500.5\n"
        f"查看帮助：/help",
        reply_markup=MAIN_MARKUP
    )
    context.user_data.clear()  # 清理状态
    return True

async def enter_purchase(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """进入购买会员流程，提示输入用户名"""
    user_id = update.effective_user.id
    async with user_locks.get(user_id, asyncio.Lock()):
        context.user_data['state'] = States.PURCHASE_ENTER_USERNAME.value
        context.user_data['invalid_attempts'] = 0  # 初始化错误次数
        keyboard = [[InlineKeyboardButton("返回", callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        await update.message.reply_text(
            "👉 请回复要开通的账号 username：\n注：username 以 @ 开头的名字，回复以下 2 格式都可以：\n@username\nhttps://t.me/username",
            reply_markup=reply_markup,
            disable_web_page_preview=True
        )
    return States.PURCHASE_ENTER_USERNAME.value

async def handle_username(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """处理用户输入的用户名"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    async with user_locks.get(user_id, asyncio.Lock()):
        username_match = re.match(r"^@(\w+)$", text) or re.match(r"^https://t\.me/(\w+)$", text)
        if username_match:
            username = username_match.group(1)
            if await check_username_exists(username):
                context.user_data["username"] = username
                context.user_data['invalid_attempts'] = 0  # 重置错误次数
                context.user_data['state'] = States.PURCHASE_SELECT_DURATION.value
                keyboard = [
                    [
                        InlineKeyboardButton(f"3 个月 ({PRICES[3]:.2f} USDT)", callback_data=f"buy_3_{username}"),
                        InlineKeyboardButton(f"6 个月 ({PRICES[6]:.2f} USDT)", callback_data=f"buy_6_{username}")
                    ],
                    [InlineKeyboardButton(f"1 年 ({PRICES[12]:.2f} USDT)", callback_data=f"buy_12_{username}")],
                    [InlineKeyboardButton("返回", callback_data="back")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await update.message.reply_text(
                    f"开通用户: @{username}\n请选择订阅时长：",
                    reply_markup=reply_markup
                )
                return States.PURCHASE_SELECT_DURATION.value
        # 增加错误次数
        context.user_data['invalid_attempts'] = context.user_data.get('invalid_attempts', 0) + 1
        invalid_count = context.user_data['invalid_attempts']
        context.user_data['state'] = States.PURCHASE_ENTER_USERNAME.value
        keyboard = [[InlineKeyboardButton("返回", callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if invalid_count < MAX_INVALID_ATTEMPTS:
            await update.message.reply_text(
                f"❌无效的用户名格式或用户名不存在，请使用 @username 或 https://t.me/username\n剩余尝试次数: {MAX_INVALID_ATTEMPTS - invalid_count}",
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "输入错误次数已达到上限，自动返回主菜单。",
                reply_markup=MAIN_MARKUP
            )
            context.user_data.clear()  # 清理状态
            return ConversationHandler.END
    return States.PURCHASE_ENTER_USERNAME.value

async def handle_duration_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """处理订阅时长选择"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    data = query.data
    async with user_locks.get(user_id, asyncio.Lock()):
        if data == "back":
            await query.message.edit_text("返回主菜单：", reply_markup=None)
            await query.message.reply_text("请选择以下选项：", reply_markup=MAIN_MARKUP)
            context.user_data.clear()  # 清理状态
            return ConversationHandler.END
        duration_match = re.match(r"^buy_(\d+)_(\w+)$", data)
        if duration_match:
            duration = int(duration_match.group(1))
            username = duration_match.group(2)
            ref_amount = Decimal(str(PRICES[duration]))
            user_balance = await get_balance(user_id)
            if user_balance >= ref_amount:
                try:
                    task_id = await add_to_premium_queue(user_id, username, duration)
                    await query.message.edit_text(
                        f"已将为 @{username} 开通 {duration if duration < 12 else '1 year'} 个月的 Premium 订阅请求加入队列，任务ID: {task_id}\n"
                        "我们将尽快处理，请稍后查看结果。"
                    )
                    await query.message.reply_text("请选择以下选项：", reply_markup=MAIN_MARKUP)
                    context.user_data.clear()  # 清理状态
                    return ConversationHandler.END
                except Exception as e:
                    logger.error(f"添加队列任务失败，用户 {user_id}: {str(e)}", exc_info=True)
                    await query.message.edit_text(
                        f"添加任务失败: {str(e)}\n请稍后重试或联系客服 {CUSTOMER_SUPPORT}",
                        reply_markup=None
                    )
                    await query.message.reply_text("请选择以下选项：", reply_markup=MAIN_MARKUP)
                    context.user_data.clear()  # 清理状态
                    return ConversationHandler.END
            await query.message.edit_text(
                f"❗️你的余额不足 ({user_balance:.2f} USDT ，不足 {ref_amount:.2f} USDT)，请充值后订阅！",
                reply_markup=None
            )
            await query.message.reply_text("请选择以下选项：", reply_markup=MAIN_MARKUP)
            context.user_data.clear()  # 清理状态
            return ConversationHandler.END
        await query.message.edit_text("无效的选择，请重试", reply_markup=None)
        await query.message.reply_text("请选择以下选项：", reply_markup=MAIN_MARKUP)
        context.user_data.clear()  # 清理状态
    return ConversationHandler.END

async def enter_deposit(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """进入余额充值流程，提示输入金额"""
    user_id = update.effective_user.id
    async with user_locks.get(user_id, asyncio.Lock()):
        async with get_db() as (conn, cursor):
            await cursor.execute(
                "SELECT COUNT(*) FROM usdtpay WHERE telegram_id = %s AND status = 0", (user_id,)
            )
            pending_orders_count = (await cursor.fetchone())[0]
            if pending_orders_count >= 3:
                await update.message.reply_text(
                    "❗ 你有超过3条未处理的充值订单，请先完成或取消这些订单！",
                    reply_markup=MAIN_MARKUP
                )
                context.user_data.clear()  # 清理状态
                return ConversationHandler.END
        context.user_data['state'] = States.DEPOSIT_AWAITING_AMOUNT.value
        await update.message.reply_text(
            "请输入充值金额（USDT，例如：10 或 10.5）：",
            reply_markup=MAIN_MARKUP
        )
    return States.DEPOSIT_AWAITING_AMOUNT.value

async def handle_deposit_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """处理用户输入的充值金额"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    logger.info(f"用户 {user_id} 在充值金额输入状态输入: {text}")
    async with user_locks.get(user_id, asyncio.Lock()):
        try:
            # 匹配正数（整数或小数，最多8位小数）
            if re.match(r"^\d+(\.\d{1,8})?$", text):
                success, response = await usdt_handle_deposit(update, context, text)
                logger.info(f"充值处理结果: success={success}, response={response}")
                if success:
                    reply_text = response if response else "充值订单已创建，请完成支付！"
                    await update.message.reply_text(
                        reply_text + "\n请选择以下选项：",
                        reply_markup=MAIN_MARKUP
                    )
                    context.user_data.clear()  # 清理状态
                    return ConversationHandler.END
                else:
                    reply_text = response if response else "充值失败，请重试！"
                    await update.message.reply_text(
                        reply_text + "\n请输入有效的充值金额：",
                        reply_markup=MAIN_MARKUP
                    )
                    context.user_data['state'] = States.DEPOSIT_AWAITING_AMOUNT.value
                    return States.DEPOSIT_AWAITING_AMOUNT.value
            else:
                logger.warning(f"用户 {user_id} 输入无效金额格式: {text}")
                await update.message.reply_text(
                    "请输入有效的数字金额（例如：10 或 10.5）",
                    reply_markup=MAIN_MARKUP
                )
                context.user_data['state'] = States.DEPOSIT_AWAITING_AMOUNT.value
                return States.DEPOSIT_AWAITING_AMOUNT.value
        except Exception as e:
            logger.error(f"处理充值失败，用户 {user_id}: {str(e)}", exc_info=True)
            await update.message.reply_text(
                f"充值失败: {str(e)}\n请稍后重试或联系客服 {CUSTOMER_SUPPORT}",
                reply_markup=MAIN_MARKUP
            )
            context.user_data.clear()  # 清理状态
            return ConversationHandler.END

async def invalid_purchase_username(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """处理购买流程中无效的用户名输入"""
    user_id = update.effective_user.id
    async with user_locks.get(user_id, asyncio.Lock()):
        context.user_data['invalid_attempts'] = context.user_data.get('invalid_attempts', 0) + 1
        invalid_count = context.user_data['invalid_attempts']
        context.user_data['state'] = States.PURCHASE_ENTER_USERNAME.value
        keyboard = [[InlineKeyboardButton("返回", callback_data="back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        if invalid_count < MAX_INVALID_ATTEMPTS:
            await update.message.reply_text(
                "❌无效的用户名格式或用户名不存在，请使用 @username 或 https://t.me/username\n剩余尝试次数: {MAX_INVALID_ATTEMPTS - invalid_count}",
                disable_web_page_preview=True,
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "输入错误次数已达到上限，自动返回主菜单。",
                reply_markup=MAIN_MARKUP
            )
            context.user_data.clear()  # 清理状态
            return ConversationHandler.END
    return States.PURCHASE_ENTER_USERNAME.value

async def invalid_duration_selection(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """处理购买流程中无效的时长选择"""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    async with user_locks.get(user_id, asyncio.Lock()):
        await query.message.edit_text("无效的选择，请重试", reply_markup=None)
        await query.message.reply_text("请选择以下选项：", reply_markup=MAIN_MARKUP)
        context.user_data.clear()  # 清理状态
    return ConversationHandler.END

async def invalid_deposit_amount(update: Update, context: ContextTypes.DEFAULT_TYPE) -> str:
    """处理充值流程中无效的金额输入"""
    user_id = update.effective_user.id
    async with user_locks.get(user_id, asyncio.Lock()):
        context.user_data['state'] = States.DEPOSIT_AWAITING_AMOUNT.value
        await update.message.reply_text(
            "请输入有效的数字金额（例如：10 或 10.5）",
            reply_markup=MAIN_MARKUP
        )
    return States.DEPOSIT_AWAITING_AMOUNT.value

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """取消当前流程"""
    user_id = update.effective_user.id
    async with user_locks.get(user_id, asyncio.Lock()):
        if update.message:
            await update.message.reply_text("已取消，返回主菜单：", reply_markup=MAIN_MARKUP)
        elif update.callback_query:
            await update.callback_query.message.edit_text("已取消，返回主菜单：")
            await update.callback_query.message.reply_text("请选择以下选项：", reply_markup=MAIN_MARKUP)
        context.user_data.clear()  # 清理状态
    return ConversationHandler.END

async def cancel_order(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """处理订单取消"""
    query = update.callback_query
    await query.answer()
    out_trade_no = query.data.replace("cancel_", "")
    try:
        if await cancel_usdt_order(out_trade_no):
            await query.message.edit_caption(caption="订单已成功取消。")
        else:
            await query.message.edit_caption(caption="取消订单失败或订单已处理。")
    except Exception as e:
        logger.error(f"取消订单失败: {str(e)}", exc_info=True)
        await query.message.edit_caption(caption=f"取消订单失败: {str(e)}")

async def process_premium_queue(context: ContextTypes.DEFAULT_TYPE, max_tasks: int = 5):
    """处理 Premium 队列任务"""
    max_retries = 3
    tasks_processed = 0
    while tasks_processed < max_tasks:
        task = await get_next_queue_task()
        if not task:
            logger.info("队列中无待处理任务")
            break
        task_id, telegram_id, username, duration, retry_count = task
        logger.info(f"处理队列任务: 任务ID={task_id}, 用户={telegram_id}, 用户名={username}, 时长={duration}")
        if retry_count >= max_retries:
            await update_queue_task_status(task_id, "failed", "超过最大重试次数")
            await context.bot.send_message(
                chat_id=telegram_id,
                text=f"任务ID: {task_id}\n开通 @{username} 的 {duration if duration < 12 else '1 year'} 个月 Premium 失败: 超过最大重试次数\n"
                     f"请联系客服 {CUSTOMER_SUPPORT}",
                reply_markup=MAIN_MARKUP
            )
            tasks_processed += 1
            continue
        try:
            ref_amount = Decimal(str(PRICES[duration]))
            async with get_db() as (conn, cursor):
                balance = await get_balance(telegram_id)
                if balance < ref_amount:
                    raise ValueError(f"余额不足: 当前 {balance:.3f} USDT, 需 {ref_amount:.3f} USDT")
                success, message, actual_amount = await activate_premium(username, duration, telegram_id)
                logger.info(f"激活结果: success={success}, message={message}, actual_amount={actual_amount}")
                if success:
                    req_id = message.split("订单号: ")[1].split("\n")[0]
                    if await is_order_completed(req_id, conn, cursor):
                        logger.warning(f"订单 {req_id} 已被处理，跳过重复操作")
                        order_id = await record_purchase_order(telegram_id, username, duration, 0.0, "重复", conn, cursor)
                        await update_queue_task_status(task_id, "completed")
                        await context.bot.send_message(
                            chat_id=telegram_id,
                            text=f"任务ID: {task_id}\n用户 @{username} 的 {duration if duration < 12 else '1 year'} 个月 Premium 已开通，无需重复操作\n"
                                 f"订单号: {order_id}",
                            reply_markup=MAIN_MARKUP
                        )
                    else:
                        order_id, new_balance = await process_purchase(telegram_id, username, duration, actual_amount, conn, cursor)
                        await record_completed_order(req_id, telegram_id, username, duration, actual_amount, "N/A", conn, cursor)
                        await update_queue_task_status(task_id, "completed")
                        await context.bot.send_message(
                            chat_id=telegram_id,
                            text=f"任务ID: {task_id}\n{message}\n订单号: {order_id}\n已扣除 {actual_amount:.2f} USDT\n剩余余额: {new_balance:.2f} USDT",
                            reply_markup=MAIN_MARKUP
                        )
                else:
                    order_id = await record_purchase_order(telegram_id, username, duration, 0.0, "失败", conn, cursor)
                    await update_queue_task_status(task_id, "failed", message)
                    await context.bot.send_message(
                        chat_id=telegram_id,
                        text=f"任务ID: {task_id}\n开通失败: {message}\n订单号: {order_id}\n余额未扣除: {balance:.2f} USDT\n"
                             f"请联系客服 {CUSTOMER_SUPPORT}",
                        reply_markup=MAIN_MARKUP
                    )
        except Exception as e:
            logger.error(f"处理队列任务失败，任务ID={task_id}: {str(e)}", exc_info=True)
            await update_queue_task_status(task_id, "failed", str(e))
            await context.bot.send_message(
                chat_id=telegram_id,
                text=f"任务ID: {task_id}\n开通失败: {str(e)}\n请联系客服 {CUSTOMER_SUPPORT}",
                reply_markup=MAIN_MARKUP
            )
        tasks_processed += 1
        await asyncio.sleep(1)

async def purchase_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """显示购买记录（优化分页和排序）"""
    user_id = update.effective_user.id
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    page = max(1, page)
    limit = 5
    offset = (page - 1) * limit
    history = await get_purchase_history(user_id, page=page, limit=limit)
    async with get_db() as (conn, cursor):
        await cursor.execute("SELECT COUNT(*) FROM purchase_orders WHERE telegram_id = %s", (user_id,))
        total_records = (await cursor.fetchone())[0]
    history_text = f"📨购买记录（第 {page} 页，共 {(total_records // limit) + 1} 页）：\n"
    if history:
        for record in history:
            history_text += (
                f"- 订单号: {record['order_id']}\n"
                f"  类型: {record['type']}\n"
                f"  描述: {record['description']}\n"
                f"  金额: {record['amount']}\n"
                f"  状态: {record['status']}\n"
                f"  时间: {record['created_at']}\n\n"
            )
    else:
        history_text += "暂无📨购买记录\n"
    keyboard = []
    if total_records > offset + limit:
        keyboard.append([InlineKeyboardButton(f"下一页 ➡️ (页 {page + 1})", callback_data=f"history_next_{page + 1}")])
    if page > 1:
        keyboard.append([InlineKeyboardButton(f"⬅️ 上一页 (页 {page - 1})", callback_data=f"history_prev_{page - 1}")])
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    await update.message.reply_text(history_text, reply_markup=reply_markup)
    context.user_data.clear()  # 清理状态

async def personal_center(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """显示个人中心信息"""
    user_id = update.effective_user.id
    async with get_db() as (conn, cursor):
        await cursor.execute("SELECT username, balance FROM user_balances WHERE telegram_id = %s", (user_id,))
        result = await cursor.fetchone()
        username = result[0] if result and result[0] else "未设置"
        balance = result[1] if result else Decimal('0.0')
        nickname = update.effective_user.full_name if update.effective_user.full_name else "未设置"
        profile_info = (
            f"昵称：{nickname}\n"
            f"你的ID：<code>{user_id}</code>\n"
            f"用户名：<code>@{username}</code>\n"
            f"余额：<code>{balance:.3f}</code> USDT\n\n"
            f"-----------------------------------\n"
            f"<b>联系客服：</b>{CUSTOMER_SUPPORT}"
        )
        await update.message.reply_text(profile_info, reply_markup=MAIN_MARKUP, parse_mode="HTML")
    context.user_data.clear()  # 清理状态



async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """处理 /help 命令，仅限管理员"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 仅管理员可使用此命令！", reply_markup=MAIN_MARKUP)
        return
    help_content = """
<b>管理员命令用法</b>

<b>配置更新</b>
- <b>修改 3 个月价格</b>:
  <code>修改3个月价格 价格</code>
  示例: <code>修改3个月价格 30.5</code>
- <b>修改 6 个月价格</b>:
  <code>修改6个月价格 价格</code>
  示例: <code>修改6个月价格 55.0</code>
- <b>修改 12 个月价格</b>:
  <code>修改12个月价格 价格</code>
  示例: <code>修改12个月价格 100.0</code>
- <b>修改 USDT 充值地址</b>:
  <code>修改USDT地址 地址</code>
  示例: <code>修改USDT地址 0x1234567890abcdef</code>
- <b>修改客服联系方式</b>:
  <code>修改客服联系方式 联系方式</code>
  示例: <code>修改客服联系方式 @SupportTeam</code>

<b>调整用户余额</b>
- <b>格式</b>:
  <code>用户[telegram_id] +[金额]</code> 或 <code>用户[telegram_id] -[金额]</code>
  示例: <code>用户6911326582 +1000</code>
  示例: <code>用户6911326582 -500.5</code>

<b>列出用户</b>
- <b>命令</b>:
  <code>/listusers [页面]</code>
  示例: <code>/listusers 2</code>

<b>帮助命令</b>
- <b>命令</b>:
  <code>/help</code>
    """
    try:
        await update.message.reply_text(help_content, reply_markup=MAIN_MARKUP, parse_mode="HTML")
        logger.info(f"管理员 {user_id} 访问 /help 命令")
    except Exception as e:
        logger.error(f"处理 /help 命令失败: {str(e)}", exc_info=True)
        await update.message.reply_text(
            f"❌ 处理帮助请求失败: {str(e)}\n请联系技术支持！",
            reply_markup=MAIN_MARKUP
        )
    context.user_data.clear()  # 清理状态




async def list_users(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """列出所有注册用户和有余额的用户，仅限管理员"""
    user_id = update.effective_user.id
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ 仅管理员可使用此命令！", reply_markup=MAIN_MARKUP)
        return
    page = int(context.args[0]) if context.args and context.args[0].isdigit() else 1
    page = max(1, page)
    limit = 10
    offset = (page - 1) * limit
    async with get_db() as (conn, cursor):
        await cursor.execute(
            "SELECT telegram_id, username, balance FROM user_balances ORDER BY telegram_id LIMIT %s OFFSET %s",
            (limit, offset)
        )
        all_users = await cursor.fetchall()
        await cursor.execute("SELECT COUNT(*) FROM user_balances")
        total_users = (await cursor.fetchone())[0]
        await cursor.execute(
            "SELECT telegram_id, username, balance FROM user_balances WHERE balance > 0 ORDER BY balance DESC LIMIT %s OFFSET %s",
            (limit, offset)
        )
        users_with_balance = await cursor.fetchall()
        await cursor.execute("SELECT COUNT(*) FROM user_balances WHERE balance > 0")
        total_balance_users = (await cursor.fetchone())[0]
        all_users_text = f"**🎉 所有注册用户 (第 {page} 页，共 {(total_users // limit) + 1} 页) 🎉**\n"
        for i, (telegram_id, username, balance) in enumerate(all_users, 1):
            username = f"@{username}" if username else f"User{telegram_id}"
            all_users_text += f"✏️ {i + offset}. *ID:* {telegram_id}, *用户名:* {username}, *余额:* {balance:.3f} USDT\n"
        balance_users_text = f"\n**💰 有余额的用户 (第 {page} 页，共 {(total_balance_users // limit) + 1} 页) 💰**\n"
        for i, (telegram_id, username, balance) in enumerate(users_with_balance, 1):
            username = f"@{username}" if username else f"User{telegram_id}"
            balance_users_text += f"❤️ {i + offset}. *ID:* {telegram_id}, *用户名:* {username}, *余额:* {balance:.3f} USDT\n"
        text = all_users_text + balance_users_text + "\n---\n"
        keyboard = []
        if total_users > offset + limit:
            keyboard.append([InlineKeyboardButton(f"下一页 ➡️ (页 {page + 1})", callback_data=f"users_next_{page + 1}")])
        if page > 1:
            keyboard.append([InlineKeyboardButton(f"⬅️ 上一页 (页 {page - 1})", callback_data=f"users_prev_{page - 1}")])
        reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
        await update.message.reply_text(text, reply_markup=reply_markup, parse_mode="Markdown")
    context.user_data.clear()  # 清理状态

async def handle_pagination(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """处理分页按钮点击"""
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    if user_id not in ADMIN_IDS:
        await query.message.edit_text("❌ 仅管理员可使用此功能！")
        return
    match = re.match(r"^(users_next|users_prev|history_next|history_prev)_(\d+)$", data)
    if match:
        prefix, page = match.groups()
        page = int(page)
        context.args = [str(page)]
        if prefix.startswith("users"):
            await list_users(update, context)
        else:
            await purchase_history(update, context)
    context.user_data.clear()  # 清理状态

async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """处理主菜单交互，仅处理非会话中的消息"""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    logger.info(f"用户 {user_id} 输入: {text}, 当前状态: {context.user_data.get('state')}")
    
    # 检查是否在会话状态中
    if context.user_data.get('state') in [state.value for state in States]:
        logger.warning(f"用户 {user_id} 在会话状态中输入 {text}，忽略非菜单处理")
        await update.message.reply_text(
            "请完成当前操作或选择‘返回’取消。",
            reply_markup=MAIN_MARKUP
        )
        return ConversationHandler.END
    
    async with user_locks.get(user_id, asyncio.Lock()):
        # 处理主菜单选项
        if text == "📨购买记录":
            await purchase_history(update, context)
            context.user_data.clear()  # 清理状态
            return ConversationHandler.END
        elif text == "👤个人中心":
            await personal_center(update, context)
            context.user_data.clear()  # 清理状态
            return ConversationHandler.END
        elif text == "💎购买会员":
            return await enter_purchase(update, context)
        elif text == "💸余额充值":
            return await enter_deposit(update, context)
        
        # 仅管理员处理其他文本输入
        if user_id in ADMIN_IDS and await update_admin_config(text, update, context):
            context.user_data.clear()  # 清理状态
            return ConversationHandler.END
        
        # 非菜单选项的友好提示
        logger.info(f"用户 {user_id} 输入无效菜单选项: {text}")
        await update.message.reply_text(
            "请选择主菜单中的选项，或输入有效命令。",
            reply_markup=MAIN_MARKUP
        )
        context.user_data.clear()  # 清理状态
    return ConversationHandler.END

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """处理错误"""
    logger.error(f"更新 {update} 引发错误: {context.error}", exc_info=True)
    if update and update.message:
        await update.message.reply_text(
            f"发生错误，请联系 {CUSTOMER_SUPPORT}",
            reply_markup=MAIN_MARKUP
        )
    context.user_data.clear()  # 清理状态

def main():
    """主函数，运行 bot"""
    if not BOT_TOKEN:
        logger.error("错误: BOT_TOKEN 未设置")
        return
    if not all(k in PRICES for k in [3, 6, 12]):
        logger.error("错误: 会员价格未正确配置")
        return
    utc8_tz = pytz.timezone("Asia/Hong_Kong")
    async def on_startup(app):
        logger.info("正在初始化数据库连接池...")
        await init_db()
        logger.info("数据库连接池初始化完成")
        commands = [BotCommand(command="start", description="启动机器人")]
        await app.bot.set_my_commands(commands)
        logger.info("自定义命令菜单已设置")
    async def on_shutdown(app):
        logger.info("正在关闭数据库连接池...")
        await close_pool()
        logger.info("数据库连接池已关闭")
    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )
    # 统一对话处理器
    conv_handler = ConversationHandler(
        entry_points=[
            MessageHandler(filters.Regex("💎购买会员"), enter_purchase),
            MessageHandler(filters.Regex("💸余额充值"), enter_deposit),
        ],
        states={
            States.PURCHASE_ENTER_USERNAME.value: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_username),
                MessageHandler(filters.ALL & ~filters.COMMAND, invalid_purchase_username),
            ],
            States.PURCHASE_SELECT_DURATION.value: [
                CallbackQueryHandler(handle_duration_selection, pattern="^buy_|^back$"),
                CallbackQueryHandler(invalid_duration_selection),
            ],
            States.DEPOSIT_AWAITING_AMOUNT.value: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_deposit_amount),
                MessageHandler(filters.ALL & ~filters.COMMAND, invalid_deposit_amount),
            ],
        },
        fallbacks=[
            MessageHandler(filters.Regex("返回"), cancel),
            CallbackQueryHandler(cancel, pattern="^back$"),
        ],
        per_message=False,  # 避免状态管理问题
    )
    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu))
    application.add_handler(CallbackQueryHandler(cancel_order, pattern="^cancel_"))
    application.add_handler(CommandHandler("listusers", list_users))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(handle_pagination, pattern="^(users_next|users_prev|history_next|history_prev)_"))
    application.add_error_handler(error_handler)
    job_queue = application.job_queue
    job_queue.scheduler.configure(timezone=utc8_tz)
    job_queue.run_repeating(check_input_usdt, interval=60, first=10)
    job_queue.run_repeating(cleanup_expired_orders, interval=300, first=20)
    job_queue.run_repeating(cleanup_old_okusdt_orders, interval=86400, first=30)
    job_queue.run_repeating(process_premium_queue, interval=5, first=5)
    logger.info("机器人初始化完成，开始运行 Polling")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
