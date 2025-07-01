import aiomysql
import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import uuid
import json
from decimal import Decimal
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

# 配置日志
logger = logging.getLogger(__name__)

# 数据库连接池
_pool = None

async def init_pool():
    """初始化数据库连接池"""
    global _pool
    if _pool is None:
        try:
            _pool = await aiomysql.create_pool(
                host=MYSQL_HOST,
                port=MYSQL_PORT,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                db=MYSQL_DATABASE,
                autocommit=False,
                minsize=2,
                maxsize=20,
                pool_recycle=1800,
                loop=asyncio.get_event_loop(),
                charset="utf8mb4",
                use_unicode=True
            )
            logger.info("MySQL 数据库连接池初始化完成")
        except Exception as e:
            logger.error(f"数据库连接池初始化失败: {str(e)}", exc_info=True)
            raise
    return _pool

async def close_pool():
    """关闭数据库连接池"""
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        logger.info("MySQL 数据库连接池已关闭")

@asynccontextmanager
async def get_db():
    """提供数据库连接和游标，确保正确关闭"""
    pool = await init_pool()
    conn = await pool.acquire()
    cursor = await conn.cursor()
    logger.debug(f"获取连接用于用户操作，连接ID: {id(conn)}")
    try:
        yield conn, cursor
        if conn.autocommit:
            await conn.commit()
    except Exception as e:
        await conn.rollback()
        logger.error(f"数据库操作失败: {str(e)}", exc_info=True)
        raise
    finally:
        await cursor.close()
        pool.release(conn)
        logger.debug(f"释放连接，连接ID: {id(conn)}")

async def init_db():
    """初始化MySQL数据库和表"""
    async with get_db() as (conn, cursor):
        try:
            # 用户余额表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_balances (
                    telegram_id BIGINT PRIMARY KEY,
                    username VARCHAR(255) NOT NULL,
                    balance DECIMAL(10,3) DEFAULT 0.0,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_telegram_id (telegram_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户余额表'
            """)
            # 用户状态表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS user_states (
                    telegram_id BIGINT PRIMARY KEY,
                    state VARCHAR(50),
                    data JSON,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_telegram_id (telegram_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户状态表'
            """)
            # USDT 充值订单表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS usdtpay (
                    out_trade_no VARCHAR(36) PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    expires_at DATETIME,
                    status TINYINT DEFAULT 0,
                    amount DECIMAL(10,4) NOT NULL,
                    INDEX idx_telegram_id (telegram_id),
                    FOREIGN KEY (telegram_id) REFERENCES user_balances(telegram_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='USDT 充值订单表'
            """)
            # 已完成 USDT 订单表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS okusdt (
                    out_trade_no VARCHAR(36) PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    amount DECIMAL(10,3) NOT NULL,
                    INDEX idx_telegram_id (telegram_id),
                    FOREIGN KEY (telegram_id) REFERENCES user_balances(telegram_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='已完成 USDT 订单表'
            """)
            # 购买订单表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS purchase_orders (
                    order_id VARCHAR(36) PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    username VARCHAR(255) NOT NULL,
                    duration TINYINT NOT NULL,
                    amount DECIMAL(10,3) NOT NULL,
                    status ENUM('成功', '失败', '重复') DEFAULT '失败',
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_telegram_id (telegram_id),
                    FOREIGN KEY (telegram_id) REFERENCES user_balances(telegram_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='购买订单表'
            """)
            # 已完成 Premium 订单表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS completed_orders (
                    req_id VARCHAR(36) PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    username VARCHAR(255) NOT NULL,
                    duration TINYINT NOT NULL,
                    amount DECIMAL(10,3) NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    tx_hash VARCHAR(255),
                    INDEX idx_telegram_id (telegram_id),
                    FOREIGN KEY (telegram_id) REFERENCES user_balances(telegram_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='已完成 Premium 订单表'
            """)
            # Premium 任务队列表
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS premium_queue (
                    id BIGINT PRIMARY KEY AUTO_INCREMENT,
                    telegram_id BIGINT NOT NULL,
                    username VARCHAR(255) NOT NULL,
                    duration TINYINT NOT NULL,
                    status ENUM('pending', 'processing', 'completed', 'failed') DEFAULT 'pending',
                    retry_count TINYINT DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    error_message TEXT,
                    INDEX idx_status (status),
                    INDEX idx_telegram_id (telegram_id),
                    FOREIGN KEY (telegram_id) REFERENCES user_balances(telegram_id) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Premium 任务队列表'
            """)
            await conn.commit()
            logger.info("MySQL 数据库初始化完成")
        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}", exc_info=True)
            raise

async def get_balance(telegram_id: int) -> Decimal:
    """获取用户余额"""
    async with get_db() as (conn, cursor):
        await cursor.execute("SELECT balance FROM user_balances WHERE telegram_id = %s", (telegram_id,))
        result = await cursor.fetchone()
        return Decimal(str(result[0])) if result else Decimal('0.0')

async def set_balance(telegram_id: int, amount: float, conn, cursor) -> Decimal:
    """设置用户余额（正数为充值，负数为扣款）"""
    amount = Decimal(str(amount))
    await cursor.execute("SELECT balance FROM user_balances WHERE telegram_id = %s FOR UPDATE", (telegram_id,))
    result = await cursor.fetchone()
    current_balance = Decimal(str(result[0])) if result else Decimal('0.0')
    new_balance = current_balance + amount

    if new_balance < 0:
        raise ValueError(f"余额不足: 当前 {current_balance:.3f}, 尝试扣款 {abs(amount):.3f}")

    await cursor.execute(
        """
        INSERT INTO user_balances (telegram_id, username, balance)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE balance = %s, updated_at = CURRENT_TIMESTAMP
        """,
        (telegram_id, f"User{telegram_id}", new_balance, new_balance)
    )
    logger.info(f"用户 {telegram_id} 余额更新: {current_balance:.3f} -> {new_balance:.3f}")
    return new_balance

async def get_state(telegram_id: int) -> dict:
    """获取用户状态"""
    async with get_db() as (conn, cursor):
        await cursor.execute("SELECT state, data FROM user_states WHERE telegram_id = %s", (telegram_id,))
        result = await cursor.fetchone()
        if result:
            state, data = result
            return {"state": state, "data": json.loads(data) if data else {}}
        return None

async def set_state(telegram_id: int, state: str = None, data: dict = None):
    """设置用户状态"""
    async with get_db() as (conn, cursor):
        if state is None:
            await cursor.execute("DELETE FROM user_states WHERE telegram_id = %s", (telegram_id,))
        else:
            await cursor.execute(
                """
                INSERT INTO user_states (telegram_id, state, data)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE state = %s, data = %s, updated_at = CURRENT_TIMESTAMP
                """,
                (telegram_id, state, json.dumps(data or {}), state, json.dumps(data or {}))
            )
        await conn.commit()
        logger.info(f"设置用户 {telegram_id} 状态: state={state}, data={data}")

def generate_order_id() -> str:
    """生成唯一订单ID"""
    return str(uuid.uuid4())[:8]

async def record_purchase_order(telegram_id: int, username: str, duration: int, amount: float, status: str, conn, cursor) -> str:
    """记录购买订单"""
    order_id = generate_order_id()
    await cursor.execute(
        """
        INSERT INTO purchase_orders (order_id, telegram_id, username, duration, amount, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """,
        (order_id, telegram_id, username, duration, Decimal(str(amount)), status)
    )
    logger.info(f"记录购买订单: {order_id}, 用户: {telegram_id}, 状态: {status}")
    return order_id

async def get_purchase_history(telegram_id: int, page: int = 1, limit: int = 5) -> list:
    """获取购买历史，支持分页"""
    history = []
    async with get_db() as (conn, cursor):
        offset = (page - 1) * limit
        await cursor.execute(
            """
            SELECT order_id, username, duration, amount, status, created_at
            FROM purchase_orders
            WHERE telegram_id = %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """,
            (telegram_id, limit, offset)
        )
        async for record in cursor:
            duration_text = f"{record[2]} 个月" if record[2] < 12 else "1 年"
            description = f"为 @{record[1]} 购买 {duration_text}会员" + (
                "，扣除余额" if record[4] == "成功" else "失败，未扣除余额"
            )
            history.append(
                {
                    "order_id": record[0],
                    "type": "购买",
                    "description": description,
                    "amount": f"-{record[3]:.3f} USDT" if record[4] == "成功" else "0.000 USDT",
                    "status": record[4],
                    "created_at": record[5].strftime("%Y-%m-%d %H:%M:%S")
                }
            )
    return history

async def is_order_completed(req_id: str, conn=None, cursor=None) -> bool:
    """检查订单是否已完成"""
    if conn is None or cursor is None:
        async with get_db() as (conn, cursor):
            await cursor.execute("SELECT req_id FROM completed_orders WHERE req_id = %s", (req_id,))
            return await cursor.fetchone() is not None
    await cursor.execute("SELECT req_id FROM completed_orders WHERE req_id = %s", (req_id,))
    return await cursor.fetchone() is not None

async def record_completed_order(req_id: str, telegram_id: int, username: str, duration: int, amount: float, tx_hash: str, conn, cursor) -> bool:
    """记录已完成订单，确保不重复"""
    await cursor.execute("SELECT req_id FROM completed_orders WHERE req_id = %s FOR UPDATE", (req_id,))
    if await cursor.fetchone():
        logger.warning(f"订单 {req_id} 已存在，跳过重复记录")
        return False
    await cursor.execute(
        """
        INSERT INTO completed_orders (req_id, telegram_id, username, duration, amount, created_at, tx_hash)
        VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
        """,
        (req_id, telegram_id, username, duration, Decimal(str(amount)), tx_hash)
    )
    logger.info(f"记录已完成订单: {req_id}, 用户: {telegram_id}")
    return True

async def add_to_premium_queue(telegram_id: int, username: str, duration: int) -> int:
    """添加任务到 Premium 队列"""
    async with get_db() as (conn, cursor):
        await cursor.execute(
            """
            INSERT INTO premium_queue (telegram_id, username, duration, status)
            VALUES (%s, %s, %s, 'pending')
            """,
            (telegram_id, username, duration)
        )
        task_id = cursor.lastrowid
        await conn.commit()
        logger.info(f"添加任务到队列: 任务ID={task_id}, 用户={telegram_id}, 用户名={username}, 时长={duration}")
        return task_id

async def get_next_queue_task() -> tuple:
    """获取下一个待处理的任务"""
    async with get_db() as (conn, cursor):
        await cursor.execute(
            """
            SELECT id, telegram_id, username, duration, retry_count
            FROM premium_queue
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE
            """
        )
        task = await cursor.fetchone()
        if task:
            task_id, telegram_id, username, duration, retry_count = task
            await cursor.execute(
                """
                UPDATE premium_queue
                SET status = 'processing', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (task_id,)
            )
            await conn.commit()
            return task_id, telegram_id, username, duration, retry_count
        return None

async def update_queue_task_status(task_id: int, status: str, error_message: str = None):
    """更新任务状态"""
    async with get_db() as (conn, cursor):
        if status == 'failed':
            await cursor.execute(
                """
                UPDATE premium_queue
                SET status = %s, error_message = %s, retry_count = retry_count + 1, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (status, error_message, task_id)
            )
        else:
            await cursor.execute(
                """
                UPDATE premium_queue
                SET status = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                (status, task_id)
            )
        await conn.commit()
        logger.info(f"更新任务状态: 任务ID={task_id}, 状态={status}, 错误={error_message}")