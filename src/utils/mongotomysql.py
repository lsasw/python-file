import pymongo
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import logging

# ------------------ 配置区域 ------------------
# MongoDB 配置
MONGO_URL = 'mongodb://d_eagle3_guangzhou_llt_cim:mongo_Dfdb19c@192.168.0.89:27017/d_eagle3_guangzhou_llt_cim'          # MongoDB 地址
MONGO_PORT = 27017                # MongoDB 端口
MONGO_DB = 'd_eagle3_guangzhou_llt_cim'   # MongoDB 数据库名
MONGO_COLLECTION = 'TEMP_DISTTRANSFORMER_LOWVOLT_DETAIL_LIUQI111'  # 集合名

# MySQL 配置
MYSQL_HOST = '192.168.0.89'          # MySQL 地址
MYSQL_PORT = 3306                 # MySQL 端口
MYSQL_DB = 'd_eagle3_hunan_rvm_rvm'   # MySQL 数据库名
MYSQL_USER = 'd_eagle3_hunan_rvm_rvm'      # MySQL 用户名
MYSQL_PASSWORD = 'Dfdb19c'  # MySQL 密码
MYSQL_TABLE = 'TEMP_DISTTRANSFORMER_LOWVOLT_DETAIL_LIUQI222'  # 表名 (与集合名相同)

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
# ----------------------------------------------

def connect_mongo():
    """连接 MongoDB"""
    try:
        client = pymongo.MongoClient(MONGO_URL)
        db = client[MONGO_DB]
        collection = db[MONGO_COLLECTION]
        logger.info("✅ 成功连接到 MongoDB")
        return collection, client
    except Exception as e:
        logger.error(f"❌ 连接 MongoDB 失败: {e}")
        raise

def connect_mysql():
    """连接 MySQL"""
    try:
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            database=MYSQL_DB,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD
        )
        if connection.is_connected():
            logger.info("✅ 成功连接到 MySQL")
            return connection
    except Error as e:
        logger.error(f"❌ 连接 MySQL 失败: {e}")
        raise

def create_mysql_table(cursor):
    """在 MySQL 中创建表"""
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS `{MYSQL_TABLE}` (
        `_id` VARCHAR(24) PRIMARY KEY COMMENT 'MongoDB ObjectId',
        `TRAN_ID` VARCHAR(50),
        `TRAN_NAME` VARCHAR(255),
        `DISTRIBUTION_LINE_ID` BIGINT,
        `DISTRIBUTION_LINE_NAME` VARCHAR(255),
        `DATA_TIME` DATETIME,
        `highVolt` DECIMAL(10,2),
        `lowVolt` DECIMAL(10,2) DEFAULT NULL,
        `gear_type` VARCHAR(100) DEFAULT NULL,
        `gear_status` VARCHAR(100) DEFAULT NULL,
        `INSERT_TIME` DATETIME,
        `DATA_TIME_SIGN` DATE,
        `tran_type` VARCHAR(50),
        INDEX idx_tran_id (`TRAN_ID`),
        INDEX idx_data_time (`DATA_TIME`),
        INDEX idx_insert_time (`INSERT_TIME`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''
    try:
        cursor.execute(create_table_query)
        logger.info(f"✅ 确保 MySQL 表 `{MYSQL_TABLE}` 存在")
    except Error as e:
        logger.error(f"❌ 创建表失败: {e}")
        raise

def mongo_to_mysql_type(value):
    """将 MongoDB 值转换为适合 MySQL 的类型"""
    if isinstance(value, dict):
        # 如果是嵌套文档，可以转为 JSON 字符串，这里示例忽略或转字符串
        return str(value)
    elif isinstance(value, list):
        return str(value)  # 或 json.dumps(value)
    elif isinstance(value, datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    elif value is None:
        return None
    from bson.int64 import Int64
    if isinstance(value, Int64):
        return int(value)
    elif isinstance(value, float):
        return round(value, 6)  # 根据需要调整精度
    else:
        return value

def sync_data():
    """执行同步"""
    mongo_collection = None
    mongo_client = None
    mysql_conn = None
    mysql_cursor = None

    try:
        # 连接数据库
        mongo_collection, mongo_client = connect_mongo()
        mysql_conn = connect_mysql()
        mysql_cursor = mysql_conn.cursor()

        # 创建表
        create_mysql_table(mysql_cursor)

        # 从 MongoDB 读取所有数据
        logger.info("📥 正在从 MongoDB 读取数据...")
        documents = list(mongo_collection.find({}))
        logger.info(f"📊 共读取到 {len(documents)} 条记录")

        if not documents:
            logger.info("📭 MongoDB 中无数据可同步")
            return

        # 构建插入语句
        columns = [
            '_id', 'TRAN_ID', 'TRAN_NAME', 'DISTRIBUTION_LINE_ID',
            'DISTRIBUTION_LINE_NAME', 'DATA_TIME', 'highVolt', 'lowVolt',
            'gear_type', 'gear_status', 'INSERT_TIME', 'DATA_TIME_SIGN', 'tran_type'
        ]
        placeholders = ', '.join(['%s'] * len(columns))
        insert_query = f"INSERT IGNORE  INTO `{MYSQL_TABLE}` ({', '.join(columns)}) VALUES ({placeholders})"

        # 准备数据
        data_to_insert = []
        for doc in documents:
            row = []
            for col in columns:
                value = doc.get(col)
                # 特殊处理 ObjectId
                if col == '_id':
                    value = str(value)  # ObjectId 转字符串
                else:
                    value = mongo_to_mysql_type(value)
                row.append(value)
            data_to_insert.append(tuple(row))

        # 批量插入
        logger.info("📤 正在向 MySQL 插入数据...")
        mysql_cursor.executemany(insert_query, data_to_insert)
        mysql_conn.commit()
        logger.info(f"✅ 成功同步 {mysql_cursor.rowcount} 条记录到 MySQL")

    except Exception as e:
        # 打印错误日志
        logger.error(f"❌ 数据同步失败: {e}")
        if mysql_conn:
            mysql_conn.rollback()
    finally:
        # 关闭连接
        if mysql_cursor:
            mysql_cursor.close()
        if mysql_conn and mysql_conn.is_connected():
            mysql_conn.close()
            logger.info("🔗 已关闭 MySQL 连接")
        if mongo_client:
            mongo_client.close()
            logger.info("🔗 已关闭 MongoDB 连接")
            logger.info("🔗 已关闭 MongoDB 连接")

if __name__ == "__main__":
    logger.info("🔄 开始执行 MongoDB 到 MySQL 数据同步...")
    sync_data()
    logger.info("🎉 数据同步任务结束")