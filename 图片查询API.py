import os
import logging
import pymysql
from flask_cors import CORS
from flask import Flask, request, jsonify, send_from_directory
from queue import Queue
from datetime import datetime

# === 配置 ===
BASE_DIR = ""
COMPRESS_QUALITY = 85
BASE_URL = "http://192.168.0.1:1111"

DB_CONFIG_PICTURE = {
    "host": "localhost",
    "user": "",
    "password": "",
    "database": "",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# === 日志配置 ===
def configure_logger():
    """配置全局日志"""
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    return logging.getLogger(__name__)

logger = configure_logger()

# === 初始化 Flask 和 CORS ===
app = Flask(__name__)
CORS(app)
logger.info("已开启跨域支持")

# === 手动实现数据库连接池 ===
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
connection_pool = MySQLConnectionPool(DB_CONFIG_PICTURE)

# 获取数据库连接的函数
def get_connection():
    return connection_pool.get_connection()


# === 获取动态表名 ===
def get_table_name(device_sn):
    """根据设备编号动态生成表名"""
    try:
        table_index = int(device_sn)
        if 1 <= table_index <= 10:
            return f"picture_sn_{table_index}"
        else:
            raise ValueError("Device SN 超出范围")
    except ValueError as e:
        logger.error(f"设备编号错误: {e}")
        return None

# === 初始化数据库表 ===
def init_picture_db():
    """初始化图片数据库，创建 10 张表"""
    try:
        conn = get_connection()
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
        connection_pool.return_connection(conn)
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
                SELECT original_path, thumbnail_path, upload_time, picture_sensor_id
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
                "upload_time": result["upload_time"],
                "picture_sensor_id": result["picture_sensor_id"]
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
    if not device_sn:
        logger.warning("API 请求缺少 device_sn 参数。")
        return jsonify({"error": "device_sn 参数是必须的"}), 400

    latest_picture = get_latest_picture_data(device_sn)
    if latest_picture:
        image_data = {
            "original_url": f"{BASE_URL}/{latest_picture['original_path'].lstrip('./')}",
            "thumbnail_url": f"{BASE_URL}/{latest_picture['thumbnail_path'].lstrip('./')}",
            "upload_time": latest_picture["upload_time"].strftime('%Y-%m-%d %H:%M:%S'),
            "picture_sensor_id": latest_picture["picture_sensor_id"]
        }
        logger.info(f"API 请求成功: 获取设备 {device_sn} 的最新图片。")
        return jsonify(image_data), 200
    else:
        logger.info(f"未找到设备 {device_sn} 的图片。")
        return jsonify({"error": "未找到对应设备的图片"}), 404

@app.route('/api/all_devices_latest', methods=['GET'])
def get_all_devices_latest():
    """返回所有设备的最新图片数据"""
    devices = [i for i in range(1, 11)]
    all_devices_data = []

    for device_sn in devices:
        latest_picture = get_latest_picture_data(device_sn)
        if latest_picture:
            all_devices_data.append({
                "device_sn": device_sn,
                "original_url": f"{BASE_URL}/{latest_picture['original_path'].lstrip('./')}",
                "thumbnail_url": f"{BASE_URL}/{latest_picture['thumbnail_path'].lstrip('./')}",
                "upload_time": latest_picture["upload_time"].strftime('%Y-%m-%d %H:%M:%S'),
                "picture_sensor_id": latest_picture["picture_sensor_id"]
            })
        else:
            all_devices_data.append({
                "device_sn": device_sn,
                "original_url": None,
                "thumbnail_url": None,
                "upload_time": None,
                "picture_sensor_id": None
            })

    return jsonify(all_devices_data)

@app.route('/api/images_by_date', methods=['GET'])
def get_images_by_date():
    """获取指定设备在日期范围内的图片"""
    device_sn = request.args.get('device_sn')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    if not device_sn or not start_date or not end_date:
        logger.warning("API 请求缺少必要参数。")
        return jsonify({"error": "device_sn, start_date 和 end_date 参数是必须的"}), 400

    table_name = get_table_name(device_sn)
    if not table_name:
        return jsonify({"error": "无效的 device_sn 参数"}), 400

    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(f'''
                SELECT original_path, thumbnail_path, upload_time, picture_sensor_id
                FROM {table_name}
                WHERE upload_time BETWEEN %s AND %s
                ORDER BY upload_time ASC
            ''', (start_date, end_date))
            results = cursor.fetchall()
        connection_pool.return_connection(conn)

        if results:
            images = [
                {
                    "original_url": f"{BASE_URL}/{row['original_path'].lstrip('./')}",
                    "thumbnail_url": f"{BASE_URL}/{row['thumbnail_path'].lstrip('./')}",
                    "upload_time": row['upload_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    "picture_sensor_id": row['picture_sensor_id']
                }
                for row in results
            ]
            logger.info(f"API 请求成功: device_sn={device_sn}, 日期范围={start_date} 至 {end_date}。")
            return jsonify(images), 200
        else:
            logger.info(f"未找到设备 {device_sn} 在日期范围 {start_date} 至 {end_date} 的图片。")
            return jsonify({"error": "未找到对应条件的图片"}), 404
    except pymysql.Error as e:
        logger.error(f"API 数据库错误: {e}")
        return jsonify({"error": "服务器内部错误"}), 500

# === 主入口 ===
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, threaded=True)
