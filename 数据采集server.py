import pymysql
import json
import paho.mqtt.client as mqtt
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import re
import queue
from threading import Thread
from logging.handlers import RotatingFileHandler
from dbutils.pooled_db import PooledDB
from concurrent.futures import ThreadPoolExecutor
import atexit

# =================== 关键参数配置 ===================
# 加载配置文件
try:
    with open("server_config.json", "r") as config_file:
        raw_config = json.load(config_file)
    config = {k: v for k, v in raw_config.items() if not k.startswith("_")}
except (FileNotFoundError, json.JSONDecodeError) as e:
    raise Exception(f"加载配置文件失败: {e}")

# 配置参数示例
MQTT_BROKER = config["MQTT_BROKER"]
MQTT_PORT = config["MQTT_PORT"]
MQTT_USERNAME = config["MQTT_USERNAME"]
MQTT_PASSWORD = config["MQTT_PASSWORD"]
MQTT_TOPIC = config["MQTT_TOPIC"]

SN_RANGE_START = config["SN_RANGE_START"]
SN_RANGE_END = config["SN_RANGE_END"]

MYSQL_CONFIG = config["COLLECT_MYSQL_CONFIG"]
MYSQL_CONFIG["cursorclass"] = pymysql.cursors.DictCursor

# =================== 日志配置 ===================
def configure_logger(log_file_path):
    """配置全局日志"""
    # 清除所有已有的处理器
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # 创建一个按天滚动的日志处理器
    log_handler = TimedRotatingFileHandler(
        log_file_path, when="midnight", interval=1, backupCount=7, encoding="utf-8"
    )
    log_handler.setLevel(logging.INFO)
    log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # 配置全局日志
    logging.basicConfig(level=logging.INFO, handlers=[log_handler])

    return logging.getLogger(__name__)

# 指定日志文件路径
logger = configure_logger("./logs/sensor_collect/sensor_collect_server.log")

# =================== 数据库连接池 ===================
try:
    pool = PooledDB(
        creator=pymysql, maxconnections=10, mincached=2, maxcached=5, blocking=True, **MYSQL_CONFIG
    )
except Exception as e:
    logger.error(f"数据库连接池初始化失败: {e}")
    raise

def get_db_connection():
    try:
        return pool.connection()
    except Exception as e:
        logger.error(f"获取数据库连接失败: {e}")
        raise

# =================== 全局变量 ===================
client = None
message_queue = queue.Queue()
start_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
created_tables = set()
executor = ThreadPoolExecutor(max_workers=10)

# =================== 数据校验与表操作 ===================
def validate_sn(sn):
    """校验 SN 是否在合法范围内"""
    if not (SN_RANGE_START <= sn <= SN_RANGE_END):
        raise ValueError(f"无效的 SN: {sn}，合法范围是 {SN_RANGE_START} 到 {SN_RANGE_END}")

def create_table(sn):
    """确保数据表存在"""
    global created_tables
    validate_sn(sn)
    table_name = f"sensor_data_sn_{sn}"

    if table_name in created_tables:
        return

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                SN INT NOT NULL,
                read_time DATETIME NOT NULL,
                humidity FLOAT NOT NULL,
                temperature FLOAT NOT NULL,
                co2 INT NOT NULL,
                light INT NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        ''')
        conn.commit()
        created_tables.add(table_name)
        logger.info(f"表 {table_name} 已创建或已存在。")
    except pymysql.Error as e:
        logger.error(f"创建表 {table_name} 时发生错误: {e}")
        raise

def insert_data(sn, data):
    """插入数据到数据库"""
    validate_sn(sn)
    table_name = f"sensor_data_sn_{sn}"
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.begin()

        for entry in data:
            if 'read_time' in entry and entry['read_time'] < start_timestamp:
                logger.info(f"数据时间 {entry['read_time']} 小于程序启动时间，跳过插入。")
                continue
            cursor.execute(f'''
                INSERT INTO {table_name} (SN, read_time, humidity, temperature, co2, light)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (entry['SN'], entry['read_time'], entry['humidity'], entry['temperature'], entry['co2'], entry['light']))

        conn.commit()
        logger.info(f"数据成功插入到表 {table_name}")
    except pymysql.Error as e:
        conn.rollback()
        logger.error(f"插入数据到 {table_name} 时发生错误: {e}")
        raise

def normalize_data(entry):
    """规范化数据"""
    entry.setdefault('humidity', 0.0)
    entry.setdefault('temperature', 0.0)
    entry.setdefault('co2', 0)
    entry.setdefault('light', 0)
    entry.setdefault('read_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    entry.setdefault('SN', 0)
    return entry

# =================== MQTT 消息处理 ===================
def process_message(topic, payload):
    try:
        match = re.search(r'SN_(\d+)', topic)
        if match:
            sn = int(match.group(1))
            validate_sn(sn)
            logger.info(f"接收到来自设备 SN_{sn} 的消息")
        else:
            logger.error(f"主题格式不正确，无法提取 SN: {topic}")
            return

        create_table(sn)

        data = json.loads(payload.decode('utf-8'))
        valid_data = []

        for entry in data:
            normalized_entry = normalize_data(entry)

            if not (0 <= normalized_entry['humidity'] <= 100):
                logger.error(f"湿度值不在合理范围内: {normalized_entry['humidity']}")
                continue
            if not (-30 <= normalized_entry['temperature'] <= 60):
                logger.error(f"温度值不在合理范围内: {normalized_entry['temperature']}")
                continue

            valid_data.append(normalized_entry)

        if valid_data:
            insert_data(sn, valid_data)
        else:
            logger.error(f"主题 {topic} 消息没有有效数据，未插入数据库。")
    except ValueError as e:
        logger.error(e)
    except json.JSONDecodeError as e:
        logger.error(f"解析 JSON 数据时发生错误: {e}")
    except Exception as e:
        logger.error(f"处理主题 {topic} 的消息时发生错误: {e}")

def process_queue():
    """处理队列中的消息"""
    while True:
        topic, payload = message_queue.get()
        try:
            executor.submit(process_message, topic, payload)
        finally:
            message_queue.task_done()

Thread(target=process_queue, daemon=True).start()

def on_message(client, userdata, msg):
    if msg.retain:
        logger.info(f"忽略保留消息: {msg.topic}")
        return
    try:
        message_queue.put((msg.topic, msg.payload))
        logger.info(f"消息已放入队列: {msg.topic}")
    except Exception as e:
        logger.error(f"将消息放入队列时发生错误: {e}")

def setup_mqtt_client():
    global client
    client = mqtt.Client()
    client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.reconnect_delay_set(min_delay=1, max_delay=60)
        logger.info(f"成功连接到 MQTT 代理: {MQTT_BROKER}:{MQTT_PORT}")
    except Exception as e:
        logger.error(f"连接到 MQTT 代理时发生错误: {e}")
        raise

    client.subscribe(MQTT_TOPIC)
    logger.info(f"成功订阅 {MQTT_TOPIC} 主题。")
    client.loop_forever()

# =================== 程序启动与关闭 ===================
def shutdown():
    """安全关闭所有连接和释放资源"""
    global client
    try:
        # 关闭 MySQL 连接池
        if pool:
            pool.close()
            logger.info("MySQL 连接池已安全关闭。")
    except Exception as e:
        logger.error(f"关闭 MySQL 连接池时发生错误: {e}")
    
    try:
        # 断开 MQTT 连接
        if client:
            client.disconnect()
            logger.info("MQTT 客户端已安全断开连接。")
    except Exception as e:
        logger.error(f"断开 MQTT 连接时发生错误: {e}")
    
    try:
        # 关闭线程池（如果存在未完成任务，则等待它们完成）
        executor.shutdown(wait=True)
        logger.info("线程池已安全关闭。")
    except Exception as e:
        logger.error(f"关闭线程池时发生错误: {e}")
    
    logger.info("程序已安全退出。")

# 注册安全关闭函数，确保在程序终止时执行
atexit.register(shutdown)


if __name__ == '__main__':
    try:
        logger.info("程序启动中...")
        setup_mqtt_client()  # 启动 MQTT 客户端并订阅主题
    except KeyboardInterrupt:
        logger.info("检测到用户中断 (Ctrl+C)，正在关闭程序...")
        shutdown()
    except Exception as e:
        logger.error(f"程序运行时发生未捕获的错误: {e}")
        shutdown()
    finally:
        logger.info("程序退出完成。")
