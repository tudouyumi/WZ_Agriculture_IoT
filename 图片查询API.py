import os
import logging
import pymysql
from flask_cors import CORS
from flask import Flask, request, jsonify, send_from_directory
from queue import Queue
from datetime import datetime
from collections import OrderedDict
from logging.handlers import TimedRotatingFileHandler
import re
try:
    with open("server_config.json", "r") as config_file:
        raw_config = json.load(config_file)
    config = {k: v for k, v in raw_config.items() if not k.startswith("_")}
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.error(f"加载配置文件失败: {e}")
    raise


BASE_DIR = config["BASE_DIR"]
BASE_URL = config["BASE_URL"]
DB_CONFIG_PICTURE = config["DB_CONFIG_PICTURE"]
DEVICE_RANGE = range(config["SN_RANGE_START"], config["SN_RANGE_END"] + 1)

DB_CONFIG_PICTURE["cursorclass"] = pymysql.cursors.DictCursor


# === 日志配置 ===
def configure_logger(log_file_path):
    """配置全局日志，支持路径检测和按天滚动"""
    # 获取日志文件的目录路径
    log_dir = os.path.dirname(log_file_path)
    
    # 如果目录不存在，则创建目录
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
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
logger = configure_logger("./logs/picture_api/picture_api_server.log")

# 测试日志记录
logger.info("日志系统已成功配置。")




# === 初始化 Flask 和 CORS ===
app = Flask(__name__)
CORS(app)
logger.info("已开启跨域支持")

# === 数据库连接池 ===
class MySQLConnectionPool:
    def __init__(self, db_config, max_connections=10):
        self._pool = Queue(max_connections)
        for _ in range(max_connections):
            connection = pymysql.connect(**db_config)
            self._pool.put(connection)

    def get_connection(self):
        try:
            connection = self._pool.get(timeout=5)
            if not connection.open:
                connection = pymysql.connect(**DB_CONFIG_PICTURE)  # 重建连接
            return connection
        except Queue.Empty:
            logger.error("数据库连接池已耗尽")
            raise RuntimeError("数据库连接池已耗尽")

    def return_connection(self, connection):
        if connection and connection.open:
            self._pool.put(connection)

    def close_all_connections(self):
        while not self._pool.empty():
            connection = self._pool.get()
            connection.close()

# 初始化连接池
connection_pool = MySQLConnectionPool(DB_CONFIG_PICTURE)

def get_connection():
    return connection_pool.get_connection()

# === 动态表名获取 ===
def get_table_name(device_sn):
    """根据设备编号动态生成表名"""
    try:
        if int(device_sn) in DEVICE_RANGE:
            return f"picture_sn_{device_sn}"
        else:
            raise ValueError("设备序列号超出范围")
    except ValueError as e:
        logger.error(f"设备编号错误: {e}")
        return None

# === 初始化数据库表 ===
def init_picture_db():
    """初始化图片数据库，创建 10 张表"""
    try:
        connection = pymysql.connect(**DB_CONFIG_PICTURE)
        with connection.cursor() as cursor:
            for i in DEVICE_RANGE:
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
        connection.commit()
        connection.close()
        logger.info("图片数据库初始化成功，已创建 10 张表。")
    except pymysql.Error as e:
        logger.error(f"图片数据库初始化错误: {e}")

init_picture_db()

# === 获取最新图片数据 ===
def get_latest_picture_data(device_sn):
    """获取指定设备的最新图片数据"""
    table_name = get_table_name(device_sn)
    if not table_name:
        return None

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(f'''
                SELECT original_path, thumbnail_path,
                       temperature, humidity, co2, light, upload_time
                FROM {table_name}
                ORDER BY upload_time DESC
                LIMIT 1
            ''')
            result = cursor.fetchone()
        connection_pool.return_connection(conn)

        if result:
            return {
                "original_path": result["original_path"],
                "thumbnail_path": result["thumbnail_path"],
                "temperature": result.get("temperature"),
                "humidity": result.get("humidity"),
                "co2": result.get("co2"),
                "light": result.get("light"),
                "upload_time": result["upload_time"]
            }
        return None
    except pymysql.MySQLError as e:
        logger.error(f"查询设备 {device_sn} 图片数据时发生错误: {e}")
        return None

# === Flask API ===
@app.route('/<path:filename>', methods=['GET'])
def serve_file(filename):
    """提供文件下载服务"""
    directory = os.path.dirname(os.path.abspath(BASE_DIR))
    return send_from_directory(directory, filename)

@app.route('/api/latest_image', methods=['GET'])
def get_latest_image():
    """获取指定设备的最新图片"""
    device_sn = request.args.get('device_sn')
    
    if not re.match(r"^\d+$", str(device_sn)):
        return jsonify({"error": "device_sn 参数是整数"}), 400

    if not device_sn:
        logger.warning("API 请求缺少 device_sn 参数。")
        return jsonify({"error": "device_sn 参数是必须的"}), 400

    latest_picture = get_latest_picture_data(device_sn)
    if latest_picture:
        image_data = OrderedDict([
            ("device_sn", device_sn),
            ("original_url", f"{BASE_URL}/{latest_picture['original_path'].lstrip('./')}"),
            ("thumbnail_url", f"{BASE_URL}/{latest_picture['thumbnail_path'].lstrip('./')}"),
            ("temperature", latest_picture.get("temperature")),
            ("humidity", latest_picture.get("humidity")),
            ("co2", latest_picture.get("co2")),
            ("light", latest_picture.get("light")),
            ("upload_time", latest_picture["upload_time"].strftime('%Y-%m-%d %H:%M:%S'))
        ])
        logger.info(f"API 请求成功: 获取设备 {device_sn} 的最新图片。")
        return jsonify(image_data), 200
    else:
        logger.info(f"未找到设备 {device_sn} 的图片。")
        return jsonify({"error": "未找到对应设备的图片"}), 404

@app.route('/api/all_devices_latest', methods=['GET'])
def get_all_devices_latest():
    """返回所有设备的最新图片数据"""
    devices = list(DEVICE_RANGE)
    all_devices_data = []

    for device_sn in devices:
        latest_picture = get_latest_picture_data(device_sn)
        if latest_picture:
            # 按固定顺序插入字段
            all_devices_data.append(OrderedDict([
                ("device_sn", device_sn),
                ("original_url", f"{BASE_URL}/{latest_picture['original_path'].lstrip('./')}"),
                ("thumbnail_url", f"{BASE_URL}/{latest_picture['thumbnail_path'].lstrip('./')}"),
                ("temperature", latest_picture.get("temperature")),
                ("humidity", latest_picture.get("humidity")),
                ("co2", latest_picture.get("co2")),
                ("light", latest_picture.get("light")),
                ("upload_time", latest_picture["upload_time"].strftime('%Y-%m-%d %H:%M:%S'))
            ]))
        else:
            # 插入空数据时也按相同顺序
            all_devices_data.append(OrderedDict([
                ("device_sn", device_sn),
                ("original_url", None),
                ("thumbnail_url", None),
                ("temperature", None),
                ("humidity", None),
                ("co2", None),
                ("light", None),
                ("upload_time", None)
            ]))

    return jsonify(all_devices_data)


@app.route('/api/images_by_date', methods=['GET'])
def get_images_by_date():
    """获取指定设备在日期时间范围内的图片（不带分页）"""
    device_sn = request.args.get('device_sn')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # 参数校验
    if not device_sn or not start_date or not end_date:
        logger.warning("API 请求缺少必要参数。")
        return jsonify({"error": "device_sn, start_date 和 end_date 参数是必须的"}), 400

    # 校验 device_sn 是否为数字
    if not device_sn.isdigit():
        logger.warning(f"无效的 device_sn: {device_sn}")
        return jsonify({"error": "无效的 device_sn 参数"}), 400

    # 校验日期时间格式
    try:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        if start_date_obj > end_date_obj:
            raise ValueError("起始日期不能晚于结束日期")
    except ValueError as e:
        logger.warning(f"日期格式错误: {e}")
        return jsonify({"error": "日期格式错误，需为 YYYY-MM-DD HH:MM:SS"}), 400

    # 动态获取表名
   
    table_name = get_table_name(device_sn)
    if not table_name:
        logger.warning(f"无效的设备编号: {device_sn}")
        return jsonify({"error": "无效的 device_sn 参数"}), 400
    # 数据查询
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            query = f'''
                SELECT original_path, thumbnail_path, upload_time,
                       humidity, temperature, light, co2
                FROM {table_name}
                WHERE upload_time BETWEEN %s AND %s
                ORDER BY upload_time ASC
            '''
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
        connection_pool.return_connection(conn)

        if results:
            images = [
                OrderedDict([
                    ("device_sn", device_sn),
                    ("original_url", f"{BASE_URL}/{row['original_path'].lstrip('./')}"),
                    ("thumbnail_url", f"{BASE_URL}/{row['thumbnail_path'].lstrip('./')}"),
                    ("temperature", row.get("temperature")),
                    ("humidity", row.get("humidity")),
                    ("co2", row.get("co2")),
                    ("light", row.get("light")),
                    ("upload_time", row['upload_time'].strftime('%Y-%m-%d %H:%M:%S'))
                ])
                for row in results
            ]
            logger.info(f"成功获取设备 {device_sn} 在日期范围 {start_date} - {end_date} 内的图片数据")
            return jsonify(images), 200
        else:
            logger.info(f"未找到设备 {device_sn} 在日期范围 {start_date} - {end_date} 的图片数据")
            return jsonify({"error": "未找到对应条件的图片"}), 404
    except pymysql.MySQLError as e:
        logger.error(f"数据库查询错误: {e}")
        return jsonify({"error": "服务器内部错误"}), 500


# === 主入口 ===
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, threaded=True)
