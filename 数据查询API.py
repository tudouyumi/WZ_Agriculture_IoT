import os
import json
import logging
import pymysql
import csv
import io
from flask_cors import CORS
from flask import Flask, request, jsonify, Response
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
# === 日志配置 ===
# 配置日志
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
logger = configure_logger("./logs/sensor_api/sensor_api_server.log")

app = Flask(__name__)
CORS(app)
logger.info("Flask 应用已启动并启用了 CORS 支持")

try:
    with open("server_config.json", "r") as config_file:
        raw_config = json.load(config_file)
    config = {k: v for k, v in raw_config.items() if not k.startswith("_")}
except (FileNotFoundError, json.JSONDecodeError) as e:
    logging.error(f"加载配置文件失败: {e}")
    raise


DB_CONFIG = config["COLLECT_MYSQL_CONFIG"]
DB_CONFIG["cursorclass"] = pymysql.cursors.DictCursor
DEVICE_RANGE = range(config["SN_RANGE_START"], config["SN_RANGE_END"] + 1)
# 建立 MySQL 数据库连接
def get_db_connection():
    logger.info("建立 MySQL 数据库连接")
    conn = pymysql.connect(**DB_CONFIG)
    return conn

# 将 read_time 格式化为你需要的格式
def format_datetime(row):
    if 'read_time' in row:
        if isinstance(row['read_time'], datetime):
            row['read_time'] = row['read_time'].strftime('%Y-%m-%d %H:%M:%S')
    return row

# 查询数据
@app.route('/query_data', methods=['GET'])
def query_data():
    logger.info("收到 /query_data 请求")
    
    # 获取参数
    device_sn = request.args.get('device_sn')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    # 检查必要参数
    if not device_sn or not start_date or not end_date:
        logger.warning("缺少必要的查询参数")
        return jsonify({"error": "device_sn, start_date, and end_date are required"}), 400

    # 校验日期格式
    try:
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
        if start_date_obj > end_date_obj:
            logger.error("起始日期不能晚于结束日期")
            return jsonify({"error": "start_date cannot be later than end_date"}), 400
    except ValueError:
        logger.error("日期格式错误")
        return jsonify({"error": "Incorrect date format, should be YYYY-MM-DD HH:MM:SS"}), 400

    # 校验 device_sn 合法性
    if not device_sn.isdigit():
        logger.error("设备编号必须为数字")
        return jsonify({"error": "Invalid device_sn, must be a numeric value"}), 400
    
    device_sn_int = int(device_sn)
    
    if device_sn_int not in DEVICE_RANGE:
        logger.error("设备编号超出有效范围: %s", device_sn)
        return jsonify({"error": f"Invalid device_sn, must be between {DEVICE_RANGE.start} and {DEVICE_RANGE.stop - 1}"}), 400

    # 动态 SQL 查询
    query = f'''
        SELECT read_time, temperature, co2, humidity, light
        FROM sensor_data_sn_{device_sn}
        WHERE read_time BETWEEN %s AND %s
        ORDER BY read_time
    '''

    try:
        # 获取数据库连接并执行查询
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)  # 返回字典形式的结果
        logger.info("执行查询: %s", query)
        cursor.execute(query, (start_date, end_date))
        rows = cursor.fetchall()
        conn.close()

        # 格式化查询结果
        formatted_rows = [
            {**row, "read_time": row["read_time"].strftime("%Y-%m-%d %H:%M:%S")}
            for row in rows
        ]

        # 如果查询结果超过 500 条，返回 CSV 文件
        if len(rows) > 500:
            logger.info("查询结果超过 500 条，生成 CSV 文件返回")
            si = io.StringIO()
            csv_writer = csv.DictWriter(si, fieldnames=rows[0].keys())
            csv_writer.writeheader()
            csv_writer.writerows(formatted_rows)

            # 构建文件名
            filename = f"SN_{device_sn}_{start_date.replace(':', '-').replace(' ', '_')}_{end_date.replace(':', '-').replace(' ', '_')}.csv"

            # 返回 CSV 响应
            return Response(
                si.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": f"attachment; filename={filename}"}
            )

        # 返回 JSON 格式数据
        logger.info("返回 JSON 格式数据")
        return jsonify(formatted_rows)

    except pymysql.Error as e:
        logger.error("数据库查询错误: %s", e)
        return jsonify({"error": "Database error occurred, please try again later"}), 500

# 查询最新数据
@app.route('/latest_data', methods=['GET'])
def latest_data():
    logger.info("收到 /latest_data 请求")
    device_sn = request.args.get('device_sn')
    data_id = request.args.get('id')
    amount = request.args.get('amount', default=1)

    if device_sn and (not device_sn.isdigit() or int(device_sn) not in DEVICE_RANGE):
        logger.error("无效的设备编号: %s", device_sn)
        return jsonify({"error": "Invalid device_sn, must be a number between 1 and 10"}), 400

    if not str(amount).isdigit() or int(amount) <= 0:
        logger.error("无效的 amount 参数: %s", amount)
        return jsonify({"error": "Invalid amount, must be a positive integer"}), 400
    amount = int(amount)

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        data = []

        if not device_sn:
            logger.info("查询所有设备的最新数据")
            for sn in DEVICE_RANGE:
                query = f'''
                    SELECT co2, humidity, light, read_time, temperature, id
                    FROM sensor_data_sn_{sn}
                    ORDER BY read_time DESC
                    LIMIT %s
                '''
                cursor.execute(query, (amount,))
                rows = cursor.fetchall()
                for row in rows:
                    row["device_sn"] = sn
                    data.append(format_datetime(row))

        elif device_sn and not data_id:
            logger.info("查询设备 %s 的最新数据", device_sn)
            query = f'''
                SELECT co2, humidity, light, read_time, temperature, id
                FROM sensor_data_sn_{device_sn}
                ORDER BY read_time DESC
                LIMIT %s
            '''
            cursor.execute(query, (amount,))
            rows = cursor.fetchall()
            for row in rows:
                row["device_sn"] = device_sn
                data.append(format_datetime(row))

        elif device_sn and data_id:
            logger.info("查询设备 %s 中 id=%s 的数据", device_sn, data_id)
            query = f'''
                SELECT co2, humidity, light, read_time, temperature, id
                FROM sensor_data_sn_{device_sn}
                WHERE id = %s
            '''
            cursor.execute(query, (data_id,))
            row = cursor.fetchone()
            if row:
                row["device_sn"] = device_sn
                data.append(format_datetime(row))

        conn.close()

        logger.info("返回查询结果")
        response = jsonify(data)
        response.headers['Content-Type'] = 'application/json; charset=utf-8'
        return response

    except pymysql.Error as e:
        logger.error("数据库查询错误: %s", e)
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    logger.info("启动 Flask 服务器")
    app.run(debug=False, threaded=True)
