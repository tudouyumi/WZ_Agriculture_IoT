'''新'''
import os
import time
import pymysql
import logging
from PIL import Image, UnidentifiedImageError
from queue import Queue
from datetime import datetime
from threading import Thread
from inotify.adapters import InotifyTree
from concurrent.futures import ThreadPoolExecutor
import hashlib
from logging.handlers import TimedRotatingFileHandler

# === 配置 ===
with open("server_config.json", "r") as config_file:
    raw_config = json.load(config_file)

# 过滤掉注释键
config = {k: v for k, v in raw_config.items() if not k.startswith("_")}

BASE_DIR = "pictures_data"
COMPRESS_QUALITY = 85


# === 日志配置 ===
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
logger = configure_logger("./logs/data_processing/data_processing_server.log")


DB_CONFIG_SENSOR = {
    "host": "localhost",
    "user": "root",
    "password": "wz",
    "database": "sensor_data",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

DB_CONFIG_PICTURE = {
    "host": "localhost",
    "user": "root",
    "password": "wz",
    "database": "picture_data",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# === 创建目录 ===
os.makedirs(BASE_DIR, exist_ok=True)

# === 数据库连接池 ===
class MySQLConnectionPool:
    def __init__(self, db_config, max_connections=30):
        self._pool = Queue(max_connections)
        for _ in range(max_connections):
            connection = pymysql.connect(**db_config)
            self._pool.put(connection)

    def get_connection(self):
        return self._pool.get()

    def return_connection(self, connection):
        self._pool.put(connection)

    def close_all_connections(self):
        while not self._pool.empty():
            connection = self._pool.get()
            connection.close()

# 初始化连接池
picture_db_pool = MySQLConnectionPool(DB_CONFIG_PICTURE, max_connections=30)
sensor_db_pool = MySQLConnectionPool(DB_CONFIG_SENSOR, max_connections=30)

# 获取数据库连接
def get_connection_from_pool(pool):
    return pool.get_connection()

# === 初始化数据库 ===
def init_picture_db():
    try:
        conn = get_connection_from_pool(picture_db_pool)
        with conn.cursor() as cursor:
            for i in range(1, 11):
                table_name = f"picture_sn_{i}"
                cursor.execute(f'''
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        upload_time DATETIME NOT NULL,
                        original_path VARCHAR(255) NOT NULL,
                        thumbnail_path VARCHAR(255) NOT NULL,
                        device_sn VARCHAR(50) NOT NULL,
                        humidity FLOAT NOT NULL,
                        temperature FLOAT NOT NULL,
                        co2 FLOAT NOT NULL,
                        light FLOAT NOT NULL
                    )
                ''')
        conn.commit()
        picture_db_pool.return_connection(conn)
        logger.info("图片数据库初始化成功，已创建 10 张表。")
    except pymysql.Error as e:
        logger.error(f"图片数据库初始化错误: {e}")

init_picture_db()

# === 从传感器表获取数据 ===
def get_sensor_data(device_sn, upload_time):
    """
    获取指定设备和时间的传感器数据
    """
    try:
        conn = get_connection_from_pool(sensor_db_pool)
        sensor_table = f"sensor_data_sn_{device_sn}"
        with conn.cursor() as cursor:
            cursor.execute(f'''
                SELECT humidity, temperature, co2, light
                FROM {sensor_table}
                WHERE read_time <= %s
                ORDER BY read_time DESC
                LIMIT 1
            ''', (upload_time,))
            result = cursor.fetchone()
        sensor_db_pool.return_connection(conn)
        return result
    except pymysql.Error as e:
        logger.error(f"查询传感器数据时发生错误: {e}")
        return None

# === 使用哈希值代替文件路径，减少内存占用 ===
def get_file_hash(file_path):
    """计算文件路径的哈希值"""
    return hashlib.md5(file_path.encode('utf-8')).hexdigest()

# === 任务队列和去重机制 ===
task_queue = Queue()
processed_files = set()  # 使用哈希值代替文件路径

# === 插入图片记录到数据库 ===
def insert_picture_record(
    table_name, upload_time, original_path, thumbnail_path, device_sn, humidity, temperature, co2, light
):
    """
    插入图片和传感器数据到数据库
    """
    try:
        conn = get_connection_from_pool(picture_db_pool)
        with conn.cursor() as cursor:
            cursor.execute(f'''
                INSERT INTO {table_name} (upload_time, original_path, thumbnail_path, device_sn, humidity, temperature, co2, light)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ''', (upload_time, original_path, thumbnail_path, device_sn, humidity, temperature, co2, light))
        conn.commit()
        picture_db_pool.return_connection(conn)
        logger.info(f"图片记录已插入数据库: {original_path}")
    except pymysql.Error as e:
        logger.error(f"插入图片记录时发生错误: {e}")



# === 处理任务 ===
def process_task(task_queue, processed_files, picture_db_pool):
    while True:
        try:
            original_path, thumbnail_path, _ = task_queue.get()
            file_name = original_path.split('/')[-1]  # 获取文件名，假设路径使用'/'分隔
            
            # 解析设备序列号和时间
            device_sn, file_time_str = file_name.split('_')[:2]  # 从文件名中拆分设备序列号和时间字符串
            file_time_str = file_time_str.split('.')[0]  # 去除文件名后缀部分，获取纯时间字符串
            
            # 设置 upload_time 为文件名中的时间
            upload_time = datetime.strptime(file_time_str, '%Y%m%d%H%M%S').strftime('%Y-%m-%d %H:%M:%S')
            
            logger.info(f"处理任务: 原始图片={original_path}, 设备={device_sn}, 上传时间={upload_time}")

            # 生成缩略图
            with Image.open(original_path) as img:
                img.thumbnail((300, 300))
                img.save(thumbnail_path, quality=COMPRESS_QUALITY)
            logger.info(f"已创建缩略图: {thumbnail_path}")

            # 获取传感器数据
            sensor_data = get_sensor_data(device_sn, upload_time)

            # 如果未找到传感器数据，将值设置为 0
            if not sensor_data:
                logger.warning(f"未找到传感器数据，为图片 {original_path} 填充默认值 0")
                humidity = 0
                temperature = 0
                co2 = 0
                light = 0
            else:
                # 提取传感器数据
                humidity = sensor_data['humidity']
                temperature = sensor_data['temperature']
                co2 = sensor_data['co2']
                light = sensor_data['light']

            # 插入图片记录到数据库
            table_name = f"picture_sn_{device_sn}"
            insert_picture_record(
                table_name, upload_time, original_path, thumbnail_path, device_sn, humidity, temperature, co2, light
            )

        except UnidentifiedImageError:
            logger.warning(f"文件 {original_path} 不是有效的图片。")
        except Exception as e:
            logger.error(f"处理任务时发生错误: {e}")
        finally:
            # 从已处理文件列表中移除
            processed_files.discard(get_file_hash(original_path))
            task_queue.task_done()


# === 使用 inotify 监听目录 ===
def start_inotify():
    try:
        # 监听 BASE_DIR 下的所有目录和子目录
        inotify = InotifyTree(BASE_DIR)
        for event in inotify.event_gen(yield_nones=False):
            (_, event_types, path, filename) = event

            # 跳过目录事件
            if "IN_ISDIR" in event_types:
                continue

            # 只处理文件写入完成的事件
            if "IN_CLOSE_WRITE" in event_types:
                original_path = os.path.join(path, filename)

                # 忽略缩略图目录
                if "thumbnail" in path:
                    continue

                # 检查文件是否是临时文件
                if filename.endswith((".part", ".tmp")):
                    continue

                # 检查文件是否已经处理过
                if get_file_hash(original_path) in processed_files:
                    continue

                # 解析设备编号
                if "SN_" in path:
                    device_sn = path.split("SN_")[1].split("_")[0]
                else:
                    logger.warning(f"无法解析设备编号，跳过文件: {original_path}")
                    continue

                # 创建缩略图目录并加入任务队列
                thumbnail_folder = os.path.join(BASE_DIR, f"SN_{device_sn}_thumbnail")
                os.makedirs(thumbnail_folder, exist_ok=True)
                thumbnail_path = os.path.join(thumbnail_folder, filename)

                logger.info(f"文件已完成传输，加入任务队列: {original_path}")
                processed_files.add(get_file_hash(original_path))  # 使用哈希值标记为已处理
                task_queue.put((original_path, thumbnail_path, device_sn))
            else:
                # 降低非目标事件的日志级别
                logger.debug(f"跳过非文件写入完成的事件: {event_types}")
    except Exception as e:
        logger.error(f"inotify 监听过程中发生错误: {e}")

if __name__ == "__main__":
    try:
        # 清空任务队列和已处理文件集合
        task_queue = Queue()  # 重新初始化任务队列
        processed_files.clear()  # 清空已处理文件集合
        logger.info("任务队列和已处理文件集合已重置。")

        # 启动 inotify 监听线程
        inotify_thread = Thread(target=start_inotify, daemon=True)
        inotify_thread.start()
        logger.info("inotify 监听线程已启动。")

        # 获取CPU核心数
        cpu_count = os.cpu_count()
        # 设置线程数为CPU核心数的3倍
        max_workers = cpu_count * 3
        executor = ThreadPoolExecutor(max_workers=max_workers)

        # 为每个线程池的工作线程提交任务
        for _ in range(max_workers):
            executor.submit(process_task, task_queue, processed_files, picture_db_pool)
        logger.info(f"消费者线程池已启动，最大线程数: {max_workers}")

        # 主程序运行日志
        logger.info("主程序已启动，等待任务...")

        # 主线程保持运行，捕获KeyboardInterrupt进行安全退出
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("收到退出信号，正在安全终止程序...")
        # 关闭线程池
        executor.shutdown(wait=True)
        logger.info("线程池已关闭。")
    except Exception as e:
        logger.error(f"主程序启动时发生错误: {e}")
    finally:
        # 释放数据库连接池资源
        picture_db_pool.close_all_connections()
        sensor_db_pool.close_all_connections()
        logger.info("数据库连接已释放。")
        logger.info("程序已安全退出。")
