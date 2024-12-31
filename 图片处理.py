import os
import time
import re
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from PIL import Image
import pymysql
import threading
from queue import Queue
import logging
import json
from logging.handlers import TimedRotatingFileHandler

try:
    with open("server_config.json", "r") as config_file:
        raw_config = json.load(config_file)
    config = {k: v for k, v in raw_config.items() if not k.startswith("_")}
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.error(f"加载配置文件失败: {e}")
    raise


# 配置参数
PICTURES_DATA_DIR = config['directories']['pictures_data_dir']
ORIGINAL_PATTERN = re.compile(config['directories']['original_pattern'])
THUMBNAIL_SUFFIX = config['directories']['thumbnail_suffix']
IMAGE_PATTERN = re.compile(config['directories']['image_pattern'])

# 数据库配置
DB_CONFIG = config['database']

# 缩略图配置
THUMBNAIL_SIZE = tuple(config['thumbnail']['size'])
THUMBNAIL_QUALITY = config['thumbnail']['quality']


# 队列和线程配置
TASK_QUEUE = Queue()
# 获取CPU核心数
cpu_count = os.cpu_count()
# 设置线程数为CPU核心数的2倍
NUM_WORKER_THREADS = cpu_count * 2

# 重试配置
RETRY_DELAY = config['retry']['delay_seconds']
MAX_RETRY_ATTEMPTS = config['retry']["max_retry_attempts"]
IMAGE_PATTERN = re.compile(config['directories']['image_pattern'], re.IGNORECASE)  # 支持大小写扩展名

# 配置日志
def configure_logger(log_file_path):
    """配置全局日志"""
    logger = logging.getLogger('data_processing_logger')
    logger.setLevel(logging.INFO)
    
    # 防止重复添加处理器
    if not logger.handlers:
        # 创建一个按天滚动的日志处理器
        log_handler = TimedRotatingFileHandler(
            log_file_path, when="midnight", interval=1, backupCount=7, encoding="utf-8"
        )
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
        # 添加处理器到日志记录器
        logger.addHandler(log_handler)
    
        # 设置不向上传播，防止日志消息被父记录器处理
        logger.propagate = False
    
    return logger

# 指定日志文件路径
logger = configure_logger("./logs/data_processing/data_processing_server.log")

def get_db_connection(db_name):
    return pymysql.connect(
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        host=DB_CONFIG['host'],
        database=db_name,
        charset=DB_CONFIG.get('charset', 'utf8mb4'),
        cursorclass=pymysql.cursors.DictCursor
    )

class ImageHandler(FileSystemEventHandler):
    def __init__(self, original_dir, thumbnail_dir):
        self.original_dir = original_dir
        self.thumbnail_dir = thumbnail_dir

    def on_created(self, event):
        if event.is_directory:
            return
        filepath = event.src_path
        filename = os.path.basename(filepath)
        
        # 过滤临时文件
        if filename.startswith('.~') or filename.startswith('~$'):
            logger.info(f"临时文件 {filename} 被忽略。")
            return

        # 仅处理匹配 image_pattern 的文件
        if not IMAGE_PATTERN.match(filename):
            logger.info(f"文件名 {filename} 不符合图片格式，忽略。")
            return
        
        logger.info(f"检测到新文件: {filename}")

        # 延迟添加任务，确保文件已经传输完成
        def add_task():
            TASK_QUEUE.put((self.original_dir, self.thumbnail_dir, filepath, filename, 0))
            logger.info(f"任务已加入队列: {filename}")

        threading.Timer(2, add_task).start()  # 延迟2秒

    def on_moved(self, event):
        if event.is_directory:
            return
        dest_path = event.dest_path
        filename = os.path.basename(dest_path)

        # 过滤临时文件
        if filename.startswith('.~') or filename.startswith('~$'):
            logger.info(f"临时文件 {filename} 被忽略。")
            return

        # 仅处理匹配 image_pattern 的文件
        if not IMAGE_PATTERN.match(filename):
            logger.info(f"文件名 {filename} 不符合图片格式，忽略。")
            return
        
        # 延迟几秒后将任务加入队列，确保文件系统完成所有写入操作
        def add_task():
            TASK_QUEUE.put((self.original_dir, self.thumbnail_dir, dest_path, filename, 0))
            logger.info(f"remove：任务已加入队列: {filename}")

        threading.Timer(2, add_task).start()  # 延迟2秒

def worker():
    while True:
        try:
            original_dir, thumbnail_dir, filepath, filename, retry_count = TASK_QUEUE.get()
            logger.info(f"开始处理文件: {filename} (重试次数: {retry_count})")

            if is_file_fully_transferred(filepath):
                logger.info(f"文件传输完成: {filename}")
                match = IMAGE_PATTERN.match(filename)
                if not match:
                    logger.error(f"文件名 {filename} 不符合预期格式，跳过。")
                    TASK_QUEUE.task_done()
                    continue

                sn, timestamp_str = match.groups()
                
                try:
                    timestamp = datetime.strptime(timestamp_str, '%Y%m%d%H%M%S')
                except ValueError as ve:
                    logger.error(f"文件名中的时间戳格式错误: {timestamp_str}")
                    TASK_QUEUE.task_done()
                    continue

                # 生成缩略图
                thumbnail_path = generate_thumbnail(filepath, filename, thumbnail_dir)
                if not thumbnail_path:
                    if retry_count < MAX_RETRY_ATTEMPTS:
                        logger.warning(f"缩略图生成失败 for {filename}，将重试。")
                        # 重新加入队列，重试次数加1
                        TASK_QUEUE.put((original_dir, thumbnail_dir, filepath, filename, retry_count + 1))
                        logger.info(f"任务已重新加入队列: {filename} (第 {retry_count + 1} 次重试)")
                    else:
                        logger.error(f"缩略图生成失败，达到最大重试次数: {filename}")
                    TASK_QUEUE.task_done()
                    continue

                # 查询传感器数据
                sensor_data = query_sensor_data(sn, timestamp)
                if not sensor_data:
                    logger.error(f"未找到与 {filename} 时间匹配的传感器数据。")
                    TASK_QUEUE.task_done()
                    continue

                # 插入图片数据到数据库，使用文件名中的时间戳作为 upload_time
                insert_picture_record(sn, filepath, thumbnail_path, sensor_data, timestamp)
            else:
                if retry_count < MAX_RETRY_ATTEMPTS:
                    logger.warning(f"文件 {filename} 传输未完成，将在 {RETRY_DELAY} 秒后重试（第 {retry_count + 1} 次）")
                    # 等待一段时间后重新加入队列，重试次数加1
                    threading.Timer(RETRY_DELAY, lambda: TASK_QUEUE.put((original_dir, thumbnail_dir, filepath, filename, retry_count + 1))).start()
                else:
                    logger.error(f"文件传输未完成，达到最大重试次数: {filename}")
            TASK_QUEUE.task_done()
        except Exception as e:
            logger.exception(f"处理任务时出错: {e}")
            TASK_QUEUE.task_done()

def is_file_fully_transferred(filepath, wait_time=6, retries=3):
    """检测文件是否传输完成。通过尝试获取文件大小是否稳定。"""
    previous_size = -1
    for attempt in range(1, retries + 1):
        try:
            current_size = os.path.getsize(filepath)
            if current_size == previous_size:
                logger.info(f"文件传输完成: {filepath} (大小: {current_size} bytes)")
                return True
            previous_size = current_size
            logger.debug(f"文件传输中: {filepath} (当前大小: {current_size} bytes)")
            time.sleep(wait_time)
        except OSError:
            logger.warning(f"无法访问文件: {filepath} (尝试 {attempt}/{retries})")
            return False
    logger.warning(f"文件传输未完成: {filepath} (最终大小: {current_size} bytes)")
    return False


def generate_thumbnail(original_path, filename, thumbnail_dir):
    try:
        with Image.open(original_path) as img:
            if img.mode not in ('RGB', 'RGBA'):
                img = img.convert('RGB')
            img.thumbnail(THUMBNAIL_SIZE)
            base_name, _ = os.path.splitext(filename)
            thumbnail_filename = f"{base_name}.jpg"  # 统一保存为JPEG
            thumbnail_path = os.path.join(thumbnail_dir, thumbnail_filename)
            img.save(thumbnail_path, "JPEG", quality=THUMBNAIL_QUALITY)
            logger.info(f"缩略图已保存: {thumbnail_path}")
            return thumbnail_path
    except Exception as e:
        logger.exception(f"生成缩略图时出错: {e}")
        return None


def query_sensor_data(sn, image_time):
    try:
        conn = get_db_connection(DB_CONFIG['database_sensor'])
        with conn.cursor() as cursor:
            query = f"""
                SELECT *
                FROM sensor_data_sn_{sn}
                WHERE SN = %s
                ORDER BY ABS(TIMESTAMPDIFF(SECOND, read_time, %s)) ASC
                LIMIT 1
            """
            cursor.execute(query, (sn, image_time))
            result = cursor.fetchone()
        conn.close()
        if result:
            logger.info(f"找到传感器数据: {result}")
        return result
    except Exception as e:
        logger.exception(f"查询传感器数据时出错: {e}")
        return None

def insert_picture_record(sn, original_path, thumbnail_path, sensor_data, upload_time):
    try:
        conn = get_db_connection(DB_CONFIG['database_picture'])
        with conn.cursor() as cursor:
            table_name = f"picture_sn_{sn}"
            # 调整路径格式，确保包含 'pictures_data/' 前缀
            original_rel = os.path.join('pictures_data', os.path.relpath(original_path, PICTURES_DATA_DIR))
            thumbnail_rel = os.path.join('pictures_data', os.path.relpath(thumbnail_path, PICTURES_DATA_DIR))
            device_sn = sn
            humidity = sensor_data['humidity']
            temperature = sensor_data['temperature']
            co2 = sensor_data['co2']
            light = sensor_data['light']

            insert_query = f"""
                INSERT INTO {table_name} 
                (upload_time, original_path, thumbnail_path, device_sn, humidity, temperature, co2, light)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                upload_time,
                original_rel,
                thumbnail_rel,
                device_sn,
                humidity,
                temperature,
                co2,
                light
            ))
        conn.commit()
        conn.close()
        logger.info(f"图片记录已插入到 {table_name} 表中。")
    except Exception as e:
        logger.exception(f"插入图片记录时出错: {e}")

def main():
    # 启动消费者线程
    for i in range(NUM_WORKER_THREADS):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        logger.info(f"启动消费者线程: {i+1}")

    observer = Observer()

    # 遍历 SN_*_original 文件夹
    for sn in range(config['DEVICE_RANGE_START'], config['DEVICE_RANGE_END'] + 1):
        original_dir = os.path.join(PICTURES_DATA_DIR, f"SN_{sn}_original")
        thumbnail_dir = os.path.join(PICTURES_DATA_DIR, f"SN_{sn}_thumbnail")

        # 确保缩略图文件夹存在
        os.makedirs(thumbnail_dir, exist_ok=True)

        handler = ImageHandler(original_dir, thumbnail_dir)
        observer.schedule(handler, path=original_dir, recursive=False)
        logger.info(f"监控目录: {original_dir}")

    observer.start()
    logger.info("开始监控图片传输...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logger.info("停止监控。")
    observer.join()

    # 等待队列中的所有任务完成
    TASK_QUEUE.join()
    logger.info("所有任务已完成。")

if __name__ == "__main__":
    main()
