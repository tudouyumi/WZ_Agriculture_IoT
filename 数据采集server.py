import pymysql
import json
import paho.mqtt.client as mqtt
import logging
from datetime import datetime
import re
import queue
from threading import Thread

# =================== 关键参数配置 ===================
# MQTT 配置
MQTT_BROKER = ""  # MQTT 代理地址
MQTT_PORT =               # MQTT 代理端口
MQTT_USERNAME = ""  # MQTT 用户名
MQTT_PASSWORD = ""  # MQTT 密码
MQTT_TOPIC = "/sensor_data/#"  # MQTT 订阅主题

# MySQL 配置
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "",
    "password": "",
    "database": "sensor_data",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor  # 返回字典形式结果
}

# 日志配置
LOG_FILE = 'sensor_data.log'     # 日志文件
LOG_ENCODING = 'utf-8'           # 日志文件编码
# ==================================================

# === 日志配置 ===
def configure_logger():
    """配置全局日志"""
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)

logger = configure_logger()

# 全局变量
client = None
message_queue = queue.Queue()
db_connection = None  # 数据库连接

# 程序启动时记录的时间戳
start_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

created_tables = set()  # 缓存已创建的表名

# 连接到 MySQL 数据库
def get_db_connection():
    global db_connection
    if db_connection is None:
        try:
            db_connection = pymysql.connect(**MYSQL_CONFIG)
            logging.info("成功连接到 MySQL 数据库")
        except pymysql.Error as e:
            logging.error(f"MySQL 数据库连接错误: {e}")
            raise
    return db_connection

# 创建表的函数，确保表存在
def create_table(sn):
    global created_tables
    table_name = f"sensor_data_sn_{sn}"  # 动态生成表名
    
    # 如果表已经创建过，则直接返回
    if table_name in created_tables:
        return

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 创建表的 SQL 语句
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,    -- 自增主键
                SN INT NOT NULL,                      -- 设备 SN
                read_time DATETIME NOT NULL,          -- 读数时间                
                humidity FLOAT NOT NULL,              -- 湿度
                temperature FLOAT NOT NULL,           -- 温度
                co2 INT NOT NULL,                     -- CO2 浓度
                light INT NOT NULL                    -- 光照强度
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        ''')
        conn.commit()

        # 将表名加入缓存
        created_tables.add(table_name)
        logging.info(f"表 {table_name} 已创建或已存在，添加到缓存。")
    except pymysql.Error as e:
        logging.error(f"创建表 {table_name} 时发生错误: {e}")
        raise

# 插入数据时加入时间戳比较
def insert_data(sn, data):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 启动事务
        conn.begin()

        for entry in data:
            # 比较数据的时间戳，如果小于程序启动时间，则跳过
            if 'read_time' in entry and entry['read_time'] < start_timestamp:
                logging.info(f"数据时间 {entry['read_time']} 小于程序启动时间，跳过插入。")
                continue  # 跳过插入

            cursor.execute(f'''
                INSERT INTO sensor_data_sn_{sn} (SN, read_time, humidity, temperature, co2, light)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (entry['SN'], entry['read_time'], entry['humidity'], entry['temperature'], entry['co2'], entry['light']))

        # 提交事务
        conn.commit()
        logging.info(f"数据成功插入到表 sensor_data_sn_{sn}")
    except pymysql.Error as e:
        conn.rollback()  # 如果发生错误，回滚事务
        logging.error(f"插入数据到 sensor_data_sn_{sn} 时发生错误: {e}")
        raise

# 规范化数据（检查字段和填充缺失字段）
def normalize_data(entry):
    # 如果缺少某些字段，可以设置默认值
    entry.setdefault('humidity', 0.000)
    entry.setdefault('temperature', 0.000)
    entry.setdefault('co2', 000)
    entry.setdefault('light', 000)
    entry.setdefault('read_time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    entry.setdefault('SN', 000)

    return entry

# 消息处理逻辑
def process_message(topic, payload):
    try:
        match = re.search(r'SN_(\d+)', topic)
        if match:
            sn = int(match.group(1))  # 提取 SN 部分并转换为整数
            logging.info(f"接收到来自设备 SN_{sn} 的消息")
        else:
            logging.error(f"主题格式不正确，无法提取 SN: {topic}")
            return  # 如果主题格式不正确，直接返回

        # 创建表（如果不存在）
        create_table(sn)

        # 解析 JSON 数据
        data = json.loads(payload.decode('utf-8'))

        # 处理每条数据
        valid_data = []
        for entry in data:
            normalized_entry = normalize_data(entry)

            # 数据校验
            if not (0 <= normalized_entry['humidity'] <= 100):
                logging.error(f"湿度值不在合理范围内: {normalized_entry['humidity']}")
                continue
            if not (-40 <= normalized_entry['temperature'] <= 100):
                logging.error(f"温度值不在合理范围内: {normalized_entry['temperature']}")
                continue
            if normalized_entry['read_time'] < start_timestamp:
                logging.info(f"数据时间 {normalized_entry['read_time']} 小于程序启动时间，跳过插入。")
                continue

            valid_data.append(normalized_entry)

        # 插入数据库
        if valid_data:
            insert_data(sn, valid_data)
            logging.info(f"主题 {topic} 消息处理成功，SN {sn}")
        else:
            logging.error(f"主题 {topic} 消息没有有效数据，未插入数据库。")
    except json.JSONDecodeError as e:
        logging.error(f"解析 JSON 数据时发生错误: {e}")
    except Exception as e:
        logging.error(f"处理主题 {topic} 的消息时发生错误: {e}")

# 消费队列中的消息
def process_queue():
    while True:
        topic, payload = message_queue.get()
        try:
            process_message(topic, payload)
        finally:
            message_queue.task_done()

# 启动消息处理线程
Thread(target=process_queue, daemon=True).start()

# 处理 MQTT 消息
def on_message(client, userdata, msg):
    if msg.retain:
        logging.info(f"忽略保留消息: {msg.topic}")
        return
    try:
        message_queue.put((msg.topic, msg.payload))
        logging.info(f"消息已放入队列: {msg.topic}")
    except Exception as e:
        logging.error(f"将消息放入队列时发生错误: {e}")
# 设置 MQTT 客户端
def setup_mqtt_client():
    global client
    client = mqtt.Client()
    client.username_pw_set(username=MQTT_USERNAME, password=MQTT_PASSWORD)
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        logging.info(f"成功连接到 MQTT 代理: {MQTT_BROKER}:{MQTT_PORT}")
    except Exception as e:
        logging.error(f"连接到 MQTT 代理时发生错误: {e}")
        raise

    client.subscribe(MQTT_TOPIC)
    logging.info(f"成功订阅 {MQTT_TOPIC} 主题。")
    client.loop_forever()

def shutdown():
    global db_connection
    if db_connection:
        db_connection.close()
        logging.info("MySQL 数据库连接已关闭。")
    if client is not None:
        client.disconnect()
        logging.info("成功关闭 MQTT 连接。")

if __name__ == '__main__':
    try:
        setup_mqtt_client()
    except Exception as e:
        logging.error(f"设置 MQTT 客户端时发生错误: {e}")
    finally:
        shutdown()
