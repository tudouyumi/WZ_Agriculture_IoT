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

# === 配置 ===
BASE_DIR = "pictures_data"
COMPRESS_QUALITY = 85
BASE_URL = "http://192.168.0.1:26"

# === 日志配置 ===
def configure_logger():
    """配置全局日志"""
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)

logger = configure_logger()

DB_CONFIG_SENSOR = {
    "host": "localhost",
    "user": "",
    "password": "",
    "database": "",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

DB_CONFIG_PICTURE = {
    "host": "localhost",
    "user": "",
    "password": "",
    "database": "",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# === 创建目录 ===
os.makedirs(BASE_DIR, exist_ok=True)

# === 数据库连接池 ===
class MySQLConnectionPool:
    def __init__(self, db_config, max_connections=10):
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
picture_db_pool = MySQLConnectionPool(DB_CONFIG_PICTURE)
sensor_db_pool = MySQLConnectionPool(DB_CONFIG_SENSOR)

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
                        picture_sensor_id INT NULL
                    )
                ''')
        conn.commit()
        picture_db_pool.return_connection(conn)
        logger.info("图片数据库初始化成功，已创建 10 张表。")
    except pymysql.Error as e:
        logger.error(f"图片数据库初始化错误: {e}")

init_picture_db()

# === 获取最近的传感器数据 ID ===
def get_closest_sensor_id(device_sn, upload_time):
    try:
        conn = get_connection_from_pool(sensor_db_pool)
        sensor_table = f"sensor_data_sn_{device_sn}"
        with conn.cursor() as cursor:
            cursor.execute(f'''
                SELECT id FROM {sensor_table}
                WHERE read_time <= %s
                ORDER BY read_time DESC
                LIMIT 1
            ''', (upload_time,))
            result = cursor.fetchone()
        sensor_db_pool.return_connection(conn)
        return result['id'] if result else None
    except pymysql.Error as e:
        logger.error(f"查询传感器数据时发生错误: {e}")
        return None

# === 任务队列和去重机制 ===
task_queue = Queue()
processed_files = set()  # 用于记录已经处理过的文件路径

# === 文件传输完整性检查 ===
def is_file_complete(file_path, wait_time=2):
    """检查文件是否完成传输（文件大小在指定时间内保持不变）"""
    try:
        initial_size = os.path.getsize(file_path)
        time.sleep(wait_time)  # 等待一段时间
        current_size = os.path.getsize(file_path)
        return initial_size == current_size  # 如果文件大小稳定，则传输完成
    except Exception as e:
        logger.error(f"检查文件传输状态时发生错误: {e}")
        return False

# === 处理任务 ===
def process_task():
    while True:
        try:
            original_path, thumbnail_path, device_sn = task_queue.get()
            logger.info(f"处理任务: 原始图片={original_path}, 设备={device_sn}")

            # 检查文件是否完成传输
            if not is_file_complete(original_path):
                logger.warning(f"文件未完成传输，重新加入队列: {original_path}")
                task_queue.put((original_path, thumbnail_path, device_sn))  # 重新加入队列
                time.sleep(2)  # 延迟后重试
                continue

            # 生成缩略图
            with Image.open(original_path) as img:
                img.thumbnail((100, 100))
                img.save(thumbnail_path, quality=COMPRESS_QUALITY)
            logger.info(f"已创建缩略图: {thumbnail_path}")

            upload_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # 查询最近的传感器数据 ID
            picture_sensor_id = get_closest_sensor_id(device_sn, upload_time)

            # 插入图片记录到数据库
            table_name = f"picture_sn_{device_sn}"
            conn = get_connection_from_pool(picture_db_pool)
            with conn.cursor() as cursor:
                cursor.execute(f'''
                    INSERT INTO {table_name} (upload_time, original_path, thumbnail_path, device_sn, picture_sensor_id)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (upload_time, original_path, thumbnail_path, device_sn, picture_sensor_id))
            conn.commit()
            picture_db_pool.return_connection(conn)
            logger.info(f"图片记录已插入数据库: {original_path}")

        except UnidentifiedImageError:
            logger.warning(f"文件 {original_path} 不是有效的图片。")
        except Exception as e:
            logger.error(f"处理任务时发生错误: {e}")
        finally:
            # 从已处理文件列表中移除
            processed_files.discard(original_path)
            task_queue.task_done()

# === 使用 inotify 监听目录 ===
def start_inotify():
    try:
        # 监听 BASE_DIR 下的所有目录和子目录
        inotify = InotifyTree(BASE_DIR)
        for event in inotify.event_gen(yield_nones=False):
            (_, event_types, path, filename) = event

            # 检查是否是文件写入完成事件
            if "IN_CLOSE_WRITE" in event_types:
                original_path = os.path.join(path, filename)

                # 忽略缩略图目录
                if "thumbnail" in path:
                    logger.info(f"跳过缩略图目录中的文件: {original_path}")
                    continue

                # 检查文件是否是临时文件
                if filename.endswith((".part", ".tmp")):
                    logger.info(f"跳过临时文件: {original_path}")
                    continue

                # 检查文件是否已经处理过
                if original_path in processed_files:
                    logger.info(f"文件已处理过，跳过: {original_path}")
                    continue

                # 检查文件是否完成传输
                if is_file_complete(original_path):
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
                    processed_files.add(original_path)  # 标记为已处理
                    task_queue.put((original_path, thumbnail_path, device_sn))
                else:
                    logger.warning(f"文件未完成传输，跳过: {original_path}")
    except Exception as e:
        logger.error(f"inotify 监听过程中发生错误: {e}")

# === 主入口 ===
if __name__ == "__main__":
    try:
        # 启动 inotify 监听线程
        inotify_thread = Thread(target=start_inotify, daemon=True)
        inotify_thread.start()
        logger.info("inotify 监听线程已启动。")

        # 启动线程池处理任务
        max_workers = 10
        executor = ThreadPoolExecutor(max_workers=max_workers)
        for _ in range(max_workers):
            executor.submit(process_task)
        logger.info(f"消费者线程池已启动，最大线程数: {max_workers}")

        # 防止主线程退出
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("程序已终止。")
    except Exception as e:
        logger.error(f"主程序启动时发生错误: {e}")

